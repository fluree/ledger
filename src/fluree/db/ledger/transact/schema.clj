(ns fluree.db.ledger.transact.schema
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.iri :as iri-util]
            [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.query.range :as query-range])
  (:import (fluree.db.flake Flake)))

;; functions related to validating and working with schemas inside of transactions

(def ^:const collection-name-regex #"^[a-zA-Z0-9_][a-zA-Z0-9\.\-_]{0,254}$")
(def ^:const predicate-name-regex #"^[a-zA-Z0-9_][a-zA-Z0-9\.\-_]{0,254}/[a-zA-Z0-9][a-zA-Z0-9\.\-_]{0,254}$")
(def ^:const predicate-contains-__-regex #"^.*__.*$")
(def ^:const predicate-contains-via-regex #"^.*_Via_.*$")


(defn validate-collection-name
  "Ensures any new collection name validates against the collection-name-regex."
  [collection-flakes]
  (when-let [new-name (some->> collection-flakes
                               (some (fn [^Flake schema-flake]
                                       (when (and (= (.-p schema-flake) const/$_collection:name)
                                                  (true? (.-op schema-flake)))
                                         (.-o schema-flake))))
                               (not-empty))]
    (when-not (re-matches collection-name-regex new-name)
      (throw (ex-info (str "Invalid collection name, must start with a-z, A-Z, or 0-9 "
                           "and can also include .-_. Provided: " new-name)
                      {:status 400
                       :error  :db/invalid-collection})))))


(def ^:const
  type->sid
  {:int      (flake/->sid const/$_tag const/_predicate$type:int)
   :long     (flake/->sid const/$_tag const/_predicate$type:long)
   :bigint   (flake/->sid const/$_tag const/_predicate$type:bigint)
   :float    (flake/->sid const/$_tag const/_predicate$type:float)
   :double   (flake/->sid const/$_tag const/_predicate$type:double)
   :bigdec   (flake/->sid const/$_tag const/_predicate$type:bigdec)
   :instant  (flake/->sid const/$_tag const/_predicate$type:instant)
   :date     (flake/->sid const/$_tag const/_predicate$type:date)
   :time     (flake/->sid const/$_tag const/_predicate$type:time)
   :dateTime (flake/->sid const/$_tag const/_predicate$type:dateTime)
   :duration (flake/->sid const/$_tag const/_predicate$type:duration)
   :string   (flake/->sid const/$_tag const/_predicate$type:string)
   :boolean  (flake/->sid const/$_tag const/_predicate$type:boolean)
   :json     (flake/->sid const/$_tag const/_predicate$type:json)
   :geojson  (flake/->sid const/$_tag const/_predicate$type:geojson)
   :bytes    (flake/->sid const/$_tag const/_predicate$type:bytes)
   :uuid     (flake/->sid const/$_tag const/_predicate$type:uuid)
   :uri      (flake/->sid const/$_tag const/_predicate$type:uri)
   :ref      (flake/->sid const/$_tag const/_predicate$type:ref)
   :tag      (flake/->sid const/$_tag const/_predicate$type:tag)})


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
  (when (>= (count type-flakes) 2)
    (throw (ex-info (str "Somehow there are more than two type flakes for a predicate, provided: " type-flakes)
                    {:status 400
                     :error  :db/invalid-tx})))
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
      (throw (ex-info (str "A new predicate must have a defined data type. Invalid Flakes: " (mapv vec pred-flakes))
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
  (when (>= 1 (count multi-flakes) 2)
    (throw (ex-info (str "At most there should be a predicate multi retraction and new assertion, provided: " multi-flakes)
                    {:status 400 :error :db/invalid-tx})))
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
  (when (>= 1 (count component-flakes) 2)
    (throw (ex-info (str "At most there should be a predicate component retraction and new assertion, provided: " component-flakes)
                    {:status 400 :error :db/invalid-tx})))
  (let [new-component-val (some #(when (true? (.-op %)) (.-o %)) component-flakes)]
    (cond
      ;; make sure for any new predicate with :component true, that type is ref.
      (and new? (true? new-component-val))
      (let [type-sid (some (fn [^Flake flake]
                             (when (and (= const/$_predicate:type (.-p flake))
                                        (true? (.-op flake)))
                               (.-o flake)))
                           pred-flakes)]
        (when (not= (:ref type->sid) type-sid)
          (throw (ex-info (str "An predicate with 'component: true' must be of type ref.")
                          {:status 400
                           :error  :db/invalid-predicate}))))


      (and (not new?) (true? new-component-val))
      (throw (ex-info (str "An existing predicate cannot be set to component: true.")
                      {:status 400
                       :error  :db/invalid-predicate}))

      :else ::ok)
    ;; always pass through pred-flakes
    pred-flakes))

(defn check-unique-changes
  " - unique cannot be set to true for existing predicate if existing values are not unique
   -  unique cannot be set to true if type is boolean"
  [pred-flakes new? pred-sid existing-schema unique-flakes]
  (when (>= 1 (count unique-flakes) 2)
    (throw (ex-info (str "At most there should be a predicate unique retraction and new assertion, provided: " unique-flakes)
                    {:status 400 :error :db/invalid-tx})))
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
          (= p const/$rdf:type) :class
          (= p const/$_predicate:type) :type
          (= p const/$_predicate:multi) :multi
          (= p const/$_predicate:component) :component
          (= p const/$_predicate:unique) :unique
          (= p const/$_predicate:index) :index
          (= p const/$_predicate:name) :name
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


(defn- valid-pred-name?
  "Tests and new (:op true) predicate-name-flakes against regex to ensure valid."
  [pred-flakes pred-name-flakes]
  (when (>= 1 (count pred-name-flakes) 2)
    (throw (ex-info (str "At most there should be a predicate name retraction and new assertion, provided: " pred-name-flakes)
                    {:status 400 :error :db/invalid-predicate})))
  (when-let [new-pred-name (some #(when (true? (.-op %)) (.-o %)) pred-name-flakes)]
    (when (or (not (re-matches predicate-name-regex new-pred-name))
              (re-matches predicate-contains-via-regex new-pred-name)
              (re-matches predicate-contains-__-regex new-pred-name))
      (throw (ex-info (str "Invalid predicate name. Must start with a-z, A-Z, or 0-9, and can include .-_. "
                           "Cannot contain '__', '/_', or '_Via_'. Must include a namespace with a single /, "
                           "i.e: 'mynamespace/mypred'. Provided: "
                           new-pred-name)
                      {:status 400 :error :db/invalid-predicate}))))
  pred-flakes)


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
          {:keys [class type multi component unique index name]} (flakes-by-type pred-flakes)]
      (cond-> pred-flakes
              (not class) (check-type-changes new? type)
              multi (check-multi-changes multi)
              component (check-component-changes new? component)
              unique (check-unique-changes new? pred-sid existing-schema unique)
              name (valid-pred-name? name)
              (or index unique) (check-remove-from-post (concat unique index) tx-state))
      true)
    ;; any returned exception will halt processing... don't throw here
    (catch Exception e e)))


(defn remove-from-post-result
  "Executed once db-after is in place. Does a validation of any predicates which were indexed that *may*
  not be indexed any longer. Predicates which had changes to :unique or :index (both trigger indexing) are
  added to the remove-from-post key during processing (done with check-remove-from-post function above).

  During the course of the transaction, we don't have a guarantee of the final state of those predicates
  (i.e. if ':unique true' was set to ':unique false', but separately ':index true' was set, we'd still
  be indexing)... so we validate that indexing for these changes was actually turned off here as a final
  check."
  [{:keys [remove-from-post db-after] :as tx-state}]
  (when-let [removes @remove-from-post]
    ;; need to validate removes are truly index removes in new db (schema might have been changed from transaction)
    (->> removes
         (reduce (fn [acc pred-id]
                   (if (dbproto/-p-prop @db-after :idx? pred-id)
                     acc                                    ;; an :index or :unique were made false, but still indexable
                     (conj acc pred-id)))
                 #{})
         (not-empty))))


(defn generate-schema-flakes
  "Schema predicates/properties from a JSON-LD context are auto-generated when possible.
  This takes the predicate data map for those new schema properties which is stored the
  schema-changes atom and returns the appropriate Flakes to be added to the transaction."
  [{:keys [schema-changes t] :as tx-state}]
  (reduce
    (fn [acc p-data]
      (let [{:keys [id iri type unique multi index upsert component name class subclassOf]} p-data
            flakes (cond-> [(flake/->Flake id const/$iri iri t true nil)]
                           class (conj (flake/->Flake id const/$rdf:type const/$rdfs:Class t true nil))
                           subclassOf (into (map #(flake/->Flake id const/$rdfs:subClassOf % t true nil) subclassOf))
                           type (conj (flake/->Flake id const/$_predicate:type (get type->sid type) t true nil))
                           name (conj (flake/->Flake id const/$_predicate:name name t true nil))
                           unique (conj (flake/->Flake id const/$_predicate:unique true t true nil))
                           multi (conj (flake/->Flake id const/$_predicate:multi true t true nil))
                           index (conj (flake/->Flake id const/$_predicate:index true t true nil))
                           upsert (conj (flake/->Flake id const/$_predicate:upsert true t true nil))
                           component (conj (flake/->Flake id const/$_predicate:component true t true nil)))]
        (into acc flakes)))
    [] (vals @schema-changes)))


(defn register-property
  "Works with generate-property to do an atomic swap of the property and return
  the result map. Handles potential race conditions."
  [p-data {:keys [schema-changes db-before] :as tx-state}]
  (let [iri (:iri p-data)
        res (swap! schema-changes
                   (fn [s-map]
                     (if (get s-map iri)
                       ;; race condition, p-data now exists, don't change atom
                       s-map
                       ;; generate new p-data record
                       (let [next-id (if (empty? s-map)
                                       (inc (get-in db-before [:ecount const/$_predicate]))
                                       (->> (vals s-map)
                                            (map :id)       ;; get :id value from all current s-maps
                                            (apply max)     ;; find current max
                                            inc))
                             p-data* (assoc p-data :id next-id)]
                         (assoc s-map iri p-data*)))))]
    ;; return newly placed p-data, or existing p-data if race condition occurred.
    (get res iri)))


(defn schema-map
  "For schema predicates / properties / classes that don't yet exist, we
  will auto-generate them if enough information in the context is available.
  This represents the complete set of properties we keep in the db's :schema
  and should stay in sync with fluree.db.query.schema/schema-map."
  [{:keys [name id iri class subclassOf type multi unique index upsert component
           noHistory retractDuplicates]
    :or   {index             true
           unique            false
           component         false
           noHistory         false
           retractDuplicates false}}]
  (let [multi?  (if (false? multi)                          ;; we default all refs to true - many specs don't specify @container yet support multi-cardinality
                  false
                  (or multi (= :ref type)))
        ref?    (= :ref type)
        upsert? (and unique (not (false? upsert)))          ;; default to upsert: true if unique and not explicitly set false
        idx?    (boolean (or ref? index unique))]
    {:name               name
     :id                 id                                 ;; may be nil, will add once created
     :iri                iri
     :class              class                              ;; used only for classes
     :subclassOf         subclassOf                         ;; used only for classes
     :equivalentProperty nil
     :type               type
     :ref?               ref?
     :idx?               idx?
     :unique             unique
     :multi              multi?
     :index              index
     :upsert             upsert?
     :component          component
     :noHistory          noHistory
     :restrictCollection nil
     :retractDuplicates  retractDuplicates
     :spec               nil
     :specDoc            nil
     :txSpec             nil
     :txSpecDoc          nil
     :restrictTag        nil
     :fullText           nil}))


(defn generate-property
  "When a property does not exist in the schema, but is sufficiently defined in
  the @context supplied with the transaction, we will auto-generate the property
  and store it in schema-changes in the tx-state to ensure it only gets generated
  once even if used multiple times in the transaction."
  [pred objects idx {:keys [schema-changes] :as tx-state}]
  (let [multi? (sequential? objects)
        obj    (if multi? (first objects) objects)]
    (when-not (:type obj)
      (throw (ex-info (str "Property does not exist in the schema, and the supplied context does not include "
                           "a @type value to create it: " pred " at position: " idx ".")
                      {:status 400 :error :db/invalid-transaction :position idx})))
    (if-let [p-info (get @schema-changes pred)]
      (fn [property] (get p-info property))
      ;; TODO - make :fluree/type consistent
      (let [type    (or (:fluree/type obj)
                        (cond
                          (= "@id" (:type obj)) :ref
                          (string? (:val obj)) :string
                          :else (throw (ex-info (str "Cannot auto-generate: " pred " as its type is not "
                                                     "supported for auto-generation. Type: " (:type obj) ".")
                                                {:status 400 :error :db/invalid-transaction}))))
            ;; TODO - add restrictComponent and others
            p-data  (schema-map {:type  type
                                 :name  pred
                                 :iri   (:iri obj)
                                 :multi (or multi? (= :ref type))})
            p-data* (register-property p-data tx-state)]
        (fn [property] (get p-data* property))))))


(defn resolve-iri
  "Return subject id of resolved IRI, or nil if not resolved."
  [iri {:keys [db-root] :as tx-state}]
  (go-try
    (some-> (<? (query-range/index-range db-root :post = [const/$iri iri]))
            ^Flake first
            (.-s))))


(declare resolve-types)

(defn generate-type
  "Generates an @type/rdf:type (Class)"
  [type-iri context idx {:keys [idents] :as tx-state}]
  (go-try
    (let [ctx-map      (iri-util/item-ctx type-iri context)
          parent-ids   (when-let [parents (:rdfs/subClassOf ctx-map)]
                         ;; parents with an :id can be looked up in the same context, else
                         ;; should have an :iri which will exist outside of context
                         (let [parent-iris (mapv #(or (:id %) (:iri %)) parents)]
                           (<? (resolve-types parent-iris context idx tx-state))))
          expanded-iri (:iri ctx-map)
          p-data       (schema-map {:iri        expanded-iri
                                    :class      true
                                    :subclassOf parent-ids})
          p-data*      (register-property p-data tx-state)
          subject-id   (:id p-data*)]
      (swap! idents assoc expanded-iri subject-id)
      subject-id)))


(defn resolve-cache-iri
  "Attempts to resolve an expanded iri to current database, and if it resolves caches result
   for future lookups within this transaction."
  [expanded-iri {:keys [idents] :as tx-state}]
  (go-try
    (when-let [id (<? (resolve-iri expanded-iri tx-state))]
      (swap! idents assoc expanded-iri id)
      id)))


(defn resolve-type
  "Attempts to resolve @type/rdf:type (Class) and caches result in the tx-state idents atom
  to speed up future lookups. If does not exist, generates types"
  [type-iri context idx {:keys [idents] :as tx-state}]
  (go-try
    (when (nil? type-iri)
      (throw (ex-info (str "JSON-LD @type cannot be nil at position: " idx)
                      {:status 400 :error :db/invalid-transaction})))
    (let [expanded-iri (iri-util/expand type-iri context)]
      (or (get @idents expanded-iri)                        ;; try cache
          (<? (resolve-cache-iri expanded-iri tx-state))    ;; try existing db (and cache if resolves)
          (<? (generate-type type-iri context idx tx-state)))))) ;; generate new and cache


(defn resolve-types
  "Resolves types. If type information is available in the context and resolvable, will
  auto-generate a new type if it does not currently exist."
  [types context idx tx-state]
  (go-try
    (when (not-empty types)
      (let [types* (if (sequential? types) types [types])]
        (loop [[type & r] types*
               i   0
               acc []]
          (if (nil? type)
            acc
            (->> (<? (resolve-type type context (conj idx i) tx-state))
                 (conj acc)
                 (recur r (inc i)))))))))
