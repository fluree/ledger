(ns fluree.db.ledger.transact.schema
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log])
  (:import (fluree.db.flake Flake)))

;; functions related to validating and working with schemas inside of transactions

(def ^:const
  type->sid
  {:int     (flake/->sid const/$_tag const/_predicate$type:int)
   :long    (flake/->sid const/$_tag const/_predicate$type:long)
   :bigint  (flake/->sid const/$_tag const/_predicate$type:bigint)
   :float   (flake/->sid const/$_tag const/_predicate$type:float)
   :double  (flake/->sid const/$_tag const/_predicate$type:double)
   :bigdec  (flake/->sid const/$_tag const/_predicate$type:bigdec)
   :instant (flake/->sid const/$_tag const/_predicate$type:instant)
   :string  (flake/->sid const/$_tag const/_predicate$type:string)
   :boolean (flake/->sid const/$_tag const/_predicate$type:boolean)
   :json    (flake/->sid const/$_tag const/_predicate$type:json)
   :geojson (flake/->sid const/$_tag const/_predicate$type:geojson)
   :bytes   (flake/->sid const/$_tag const/_predicate$type:bytes)
   :uuid    (flake/->sid const/$_tag const/_predicate$type:uuid)
   :uri     (flake/->sid const/$_tag const/_predicate$type:uri)
   :ref     (flake/->sid const/$_tag const/_predicate$type:ref)
   :tag     (flake/->sid const/$_tag const/_predicate$type:tag)})


(def ^:const
  valid-type-changes
  "The following types (keys) are allowed to be converted from the
  specified set of types (vals).
  i.e. an existing :int or :instant type can become a :long,
  but nothing can convert into an :int."
  {(:long type->sid)    #{(:int type->sid) (:instant type->sid)}
   (:bigint type->sid)  #{(:int type->sid) (:long type->sid) (:instant type->sid)}
   (:double type->sid)  #{(:float type->sid) (:long type->sid) (:int type->sid)}
   (:float type->sid)   #{(:int type->sid) (:long type->sid)}
   (:bigdec type->sid)  #{(:float type->sid) (:double type->sid) (:int type->sid) (:long type->sid) (:bigint type->sid)}
   (:string type->sid)  #{(:json type->sid) (:geojson type->sid) (:bytes type->sid) (:uuid type->sid) (:uri type->sid)}
   (:instant type->sid) #{(:long type->sid) (:int type->sid)}})


(defn type-sid->name
  "Given a type sid, returns its name as a string."
  [sid]
  (some #(when (= sid (val %)) (util/keyword->str (key %))) type->sid))


(defn check-type-changes
  "Will throw if predicate type mutation is not allowed, else returns 'all-flakes'"
  [pred-flakes new? type-flakes]
  (assert (<= (count type-flakes) 2)
          (str "Somehow there are more than two type flakes for a predicate, provided: " type-flakes))
  (let [old-type          (some #(when (false? (.-op %)) (.-o %)) type-flakes)
        new-type          (some #(when (true? (.-op %)) (.-o %)) type-flakes)
        allowed-old-types (get valid-type-changes new-type (constantly nil))]
    (cond
      ;; new predicate (not a modification), allow
      (and new-type (nil? old-type))
      pred-flakes

      ;; a type retraction + addition, but change is of allowed type
      (and old-type new-type (allowed-old-types old-type))
      pred-flakes

      (and new? (nil? new-type))
      (throw (ex-info (str "A new predicate must have a defined data type.")
                      {:status 400
                       :error  :db/invalid-tx}))

      ;; a type retraction + addition, but change is not allowed or not of allowed type
      (and old-type new-type)
      (throw (ex-info (str "Predicate data type of " (type-sid->name old-type)
                           " cannot be converted from type " (type-sid->name old-type) ".")
                      {:status 400
                       :error  :db/invalid-tx}))

      ;; retraction of type without a new type being specified, not allowed.
      (and old-type (nil? new-type))
      (throw (ex-info (str "An existing predicate type cannot be retracted without specifying a new type.")
                      {:status 400
                       :error  :db/invalid-tx})))))


(defn check-multi-changes
  "multi-cardinality cannot be set to single-cardinality"
  [pred-flakes multi-flakes]
  (assert (<= 1 (count multi-flakes) 2)
          (str "At most there should be a predicate multi retraction and new assertion, provided: " multi-flakes))
  (let [old-multi-val (some #(when (false? (.-op %)) (.-o %)) multi-flakes)
        new-multi-val (some #(when (true? (.-op %)) (.-o %)) multi-flakes)]
    (if (and (true? old-multi-val) (false? new-multi-val))
      (throw (ex-info (str "A multi-cardinality value cannot be set to single-cardinality.")
                      {:status 400
                       :error  :db/invalid-tx}))
      pred-flakes)))


(defn check-component-changes
  "component cannot be set to true for an existing predicate (it can be set to false)"
  [pred-flakes new? component-flakes]
  (assert (<= 1 (count component-flakes) 2)
          (str "At most there should be a predicate component retraction and new assertion, provided: " component-flakes))
  (let [new-component-val (some #(when (true? (.-op %)) (.-o %)) component-flakes)]
    (if (and (not new?)
             (true? new-component-val))
      (throw (ex-info (str "A an existing predicate cannot be set to component: true.")
                      {:status 400
                       :error  :db/invalid-tx}))
      pred-flakes)))

(defn check-unique-changes
  " - unique cannot be set to true for existing predicate if existing values are not unique
   -  unique cannot be set to true if type is boolean"
  [pred-flakes new? pred-sid existing-schema unique-flakes]
  (assert (<= 1 (count unique-flakes) 2)
          (str "At most there should be a predicate unique retraction and new assertion, provided: " unique-flakes))
  (let [new-unique-val (some #(when (true? (.-op %)) (.-o %)) unique-flakes)
        on?            (true? new-unique-val)
        turning-on?    (and on? (not new?))                 ;; unique was false, but now becoming true.
        bool-type?     (or (= :boolean (get-in existing-schema [:pred pred-sid :type]))
                           (some #(and (= (.-p %) const/$_predicate:type)
                                       (= (:boolean type->sid) (.-o %))
                                       (true? (.-op %)))
                                 pred-flakes))]
    (cond
      (true? bool-type?)
      (throw (ex-info (str "A unique predicate cannot be of type boolean.")
                      {:status 400
                       :error  :db/invalid-tx}))
      ;; note: legacy we allowed this but only after validating all existing values were unique
      ;; This check was not thorough enough however, as with time travel there could be historic versions of the data
      ;; where values were not unique.
      ;; While checking values across time could be done, it will require more than just looking at duplicates
      ;; as so long as duplicates never existed at the same moment in time, it could be considered OK.
      ;; For now, this capability is getting turned off.
      (true? turning-on?)
      (throw (ex-info (str "An existing non-unique predicate cannot be set to unique. "
                           "A new predicate could be established, and data can get migrated over, then rename the new "
                           "predicate with the old predicate's name.")
                      {:status 400
                       :error  :db/invalid-tx
                       :flakes pred-flakes}))

      :else pred-flakes)))


(defn- flakes-by-type
  "Groups flakes by predicate types that we care about doing additional
  analysis on"
  [flakes]
  (group-by
    (fn [^Flake flake]
      (let [p (.-p flake)]
        (cond
          (= p const/$_predicate:type) :type
          (= p const/$_predicate:multi) :multi
          (= p const/$_predicate:component) :component
          (= p const/$_predicate:unique) :unique
          (= p const/$_predicate:index) :index
          :else :other)))
    flakes))

(defn predicate-new?
  "Returns true if the predicate is new (doesn't exist in
  the existing / db-before schema."
  [pred-sid existing-schema]
  (not (get-in existing-schema [:pred pred-sid])))


(defn check-remove-from-post
  "Adds any predicate subject flakes that are removing
  an existing index, either via index: true or unique: true to the
  remove-from-post atom in tx-state."
  [flakes index-flakes {:keys [remove-from-post] :as tx-state}]
  (when-let [remove-post-sids (->> index-flakes
                                   (keep #(when (and (true? (.-op %)) (false? (.-o %)))
                                            (.-s %)))
                                   (not-empty))]
    (swap! remove-from-post into remove-post-sids))
  flakes)


(defn validate-schema-predicate
  "To validate a predicate change, need to check the following:
  - types can only be changed from/to certain values
  - unique cannot be set to true for existing predicate if existing values are not unique
  - unique cannot be set to true if type is boolean
  - component cannot be set to true for an existing predicate (it can be set to false)
  - multi-cardinality cannot be set to single-cardinality
  - If new subject, has to specify type. If it has :component true, then :type needs to be ref
  "
  [pred-sid {:keys [validate-fn db-before] :as tx-state}]
  (try
    (let [pred-flakes     (get-in @validate-fn [:c-spec pred-sid])
          existing-schema (:schema db-before)
          new?            (predicate-new? pred-sid existing-schema)
          {:keys [type multi component unique index]} (flakes-by-type pred-flakes)]
      (cond-> pred-flakes
              true (check-type-changes new? type)
              multi (check-multi-changes multi)
              component (check-component-changes new? component)
              unique (check-unique-changes new? pred-sid existing-schema unique)
              (or index unique) (check-remove-from-post (concat unique index) tx-state))
      true)
    ;; any returned exception will halt processing... don't throw here
    (catch Exception e e)))

