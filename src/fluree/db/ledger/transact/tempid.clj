(ns fluree.db.ledger.transact.tempid
  (:refer-clojure :exclude [use set resolve])
  (:require [fluree.db.util.core :as util]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.json :as json]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log]))

;; functions related to generation of tempids

(defrecord TempId [user-string collection key unique?])


(defn TempId?
  [x]
  (instance? TempId x))


(defn register
  "Registers a TempId instance into the tx-state, returns provided TempId unaltered."
  [TempId idx {:keys [tempids] :as tx-state}]
  {:pre [(TempId? TempId)]}
  (swap! tempids update TempId (fnil
                                 #(update % :idx (fn [i] (if (neg-int? (compare i idx)) i idx))) ;; take smallest/first idx
                                 {:idx         idx
                                  :sid         nil
                                  :tempid      TempId
                                  :collection  (:collection TempId)
                                  :user-string (:user-string TempId)}))
  true)


(defn- construct*
  "explicit collection is there for IRIs. The legacy _id format always
  required a collection too be part of the string, where iris will not
  have the collection name embedded and must be inferred"
  [tempid idx explicit-collection]
  (let [[coll id] (if explicit-collection
                    [explicit-collection tempid]
                    (str/split tempid #"[^\._a-zA-Z0-9]" 2))
        key (cond
              explicit-collection (or tempid                ;; possible we have an explicit collection but not IRI
                                      (keyword coll (str (util/random-uuid))))
              id (keyword coll id)
              :else (keyword coll (str (util/random-uuid))))]
    (->TempId tempid coll key (boolean id))))


(defn construct
  "Generates a new tempid record from tempid string. Registers tempid in :tempids atom within
  tx-state to track all tempids in tx, and also their final resolution value.

  defrecord equality will consider tempids with identical values the same, even if constructed separately.
  We therefore construct a tempid regardless if it has already been created, but are careful not to
  update any existing subject id that might have already been mapped to the tempid."
  ([tempid idx tx-state] (construct tempid idx tx-state nil))
  ([tempid idx tx-state collection]
   (let [TempId (construct* tempid idx collection)]
     (register TempId idx tx-state)
     TempId)))


(defn use
  "Returns a tempid that will be used for a Flake object value, but only returns it if
  it already exists. If it does not exist, it means it is a tempid used as a value, but it was never used
  as a subject."
  [tempid idx {:keys [tempids] :as tx-state}]
  (let [TempId (construct* tempid idx nil)]
    (if (contains? @tempids TempId)
      TempId
      (throw (ex-info (str "Tempid " tempid " used as a value, but there is no corresponding subject in the transaction")
                      {:status 400
                       :error  :db/invalid-tx})))))


(defn set
  "Sets a tempid value into the cache. If the tempid was already set by another :upsert
  predicate that matched a different subject, throws an error. Returns set subject-id on success."
  [tempid subject-id {:keys [tempids] :as tx-state}]
  (swap! tempids update tempid
         (fn [{:keys [sid] :as tempid-map}]
           (cond
             (nil? sid) (assoc tempid-map :sid subject-id)  ;; hasn't been set yet, success.
             (= sid subject-id) tempid-map                  ;; resolved, but to the same id - ok
             :else (throw (ex-info (str "Temp-id " (:user-string tempid)
                                        " matches two (or more) subjects based on predicate upserts: "
                                        sid " and " subject-id ".")
                                   {:status 400
                                    :error  :db/invalid-tx})))))
  subject-id)


(defn resolve
  "Returns the subject id of provided tempid, or nil if does not yet exist."
  [tempid {:keys [tempids] :as tx-state}]
  (get-in @tempids [tempid :sid]))


(defn result-map
  "Creates a map of original user tempid strings to the resolved value."
  [{:keys [tempids] :as tx-state}]
  (reduce-kv (fn [acc ^TempId tempid {:keys [sid user-string] :as tempid-map}]
               (if (:unique? tempid)
                 (assoc acc user-string sid)
                 (update acc user-string (fn [[min-sid max-sid]]
                                           [(if min-sid (min min-sid sid) sid)
                                            (if max-sid (max max-sid sid) sid)]))))
             {} @tempids))


(defn tempids-flake
  "Returns flake for tx-meta (transaction sid) that contains a json packaging of the tempids map."
  [tempids-map t]
  (flake/->Flake t const/$_tx:tempids (json/stringify tempids-map) t true nil))

(defn assign-subject-ids
  "Assigns any unresolved tempids with a permanent subject id."
  [{:keys [tempids upserts db-before t] :as tx-state} statements]
  (try
    (let [tempids' @tempids]
      (loop [[{:keys [tempid collection sid] :as tid-map} & r] (sort-by :idx (vals tempids'))
             acc      tempids'
             upserts* #{}
             ;; make sure to set current _tx ecount to 't' value, even if no tempids in transaction
             ecount   (assoc (:ecount db-before) const/$_tx t)]
        (if (nil? tid-map)                                  ;; finished
          (do (reset! tempids acc)
              (when-not (empty? upserts*)
                (reset! upserts upserts*))
              ;; return tx-state, don't need to update ecount in db-after, as dbproto/-with will update it
              tx-state)
          (if sid
            (recur r acc (conj upserts* sid) ecount)
            (let [cid     (dbproto/-c-prop db-before :id collection)
                  next-id (if (= const/$_tx cid)
                            t                               ; _tx collection has special handling as we decrement. Current value held in 't'
                            (if-let [last-sid (get ecount cid)]
                              (inc last-sid)
                              (flake/->sid cid 0)))
                  ecount* (assoc ecount cid next-id)]
              (recur r (assoc-in acc [tempid :sid] next-id) upserts* ecount*)))))
      statements)
    (catch Exception e
      (log/error e (str "Unexpected error assigning permanent id to tempids."
                        "with error: " (.getMessage e))
                 {:ecount      (:ecount db-before)
                  :tempids     @tempids
                  :schema-coll (get-in db-before [:schema :coll])})
      (throw (ex-info (str "Unexpected error assigning permanent id to tempids."
                           "with error: " (.getMessage e))
                      {:status 500 :error :db/unexpected-error}
                      e)))))