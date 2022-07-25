(ns fluree.db.ledger.transact.tempid
  (:refer-clojure :exclude [use set])
  (:require [fluree.db.util.core :as util]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.json :as json]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

;; functions related to generation of tempids

(defrecord TempId [user-string collection key unique?])


(defn TempId?
  [x]
  (instance? TempId x))


(defn register
  "Registers a TempId instance into the tx-state, returns provided TempId unaltered."
  [TempId {:keys [tempids tempids-ordered]}]
  {:pre [(TempId? TempId)]}
  (swap! tempids update TempId identity)                    ;; don't touch any existing value, otherwise nil
  (swap! tempids-ordered conj TempId)                       ;; creation ordered list to be used when assigning subject ids in same order as listed in tx
  true)


(defn new*
  [tempid]
  (let [[collection id] (str/split tempid #"[^\._a-zA-Z0-9]" 2)
        key (if id
              (keyword collection id)
              (keyword collection (str (random-uuid))))]
    (->TempId tempid collection key (boolean id))))


(defn new
  "Generates a new tempid record from tempid string. Registers tempid in :tempids atom within
  tx-state to track all tempids in tx, and also their final resolution value.

  defrecord equality will consider tempids with identical values the same, even if constructed separately.
  We therefore construct a tempid regardless if it has already been created, but are careful not to
  update any existing subject id that might have already been mapped to the tempid."
  [tempid tx-state]
  (let [TempId (new* tempid)]
    (register TempId tx-state)
    TempId))


(defn use
  "Returns a tempid that will be used for a Flake object value, but only returns it if
  it already exists. If it does not exist, it means it is a tempid used as a value, but it was never used
  as a subject."
  [tempid {:keys [tempids]}]
  (let [TempId (new* tempid)]
    (if (contains? @tempids TempId)
      TempId
      (throw (ex-info (str "Tempid " tempid " used as a value, but there is no corresponding subject in the transaction")
                      {:status 400
                       :error  :db/invalid-tx})))))


(defn set
  "Sets a tempid value into the cache. If the tempid was already set by another :upsert
  predicate that matched a different subject, throws an error. Returns set subject-id on success."
  [tempid subject-id {:keys [tempids]}]
  (swap! tempids update tempid
         (fn [existing-sid]
           (cond
             (nil? existing-sid) subject-id                 ;; hasn't been set yet, success.
             (= existing-sid subject-id) subject-id         ;; resolved, but to the same id - ok
             :else (throw (ex-info (str "Temp-id " (:user-string tempid)
                                        " matches two (or more) subjects based on predicate upserts: "
                                        existing-sid " and " subject-id ".")
                                   {:status 400
                                    :error  :db/invalid-tx})))))
  subject-id)


(defn result-map
  "Creates a map of original user tempid strings to the resolved value."
  [{:keys [tempids]}]
  (reduce-kv (fn [acc ^TempId tempid subject-id]
               (if (:unique? tempid)
                 (assoc acc (:user-string tempid) subject-id)
                 (update acc (:user-string tempid) (fn [[min-sid max-sid]]
                                                     [(if min-sid (min min-sid subject-id) subject-id)
                                                      (if max-sid (max max-sid subject-id) subject-id)]))))
             {} @tempids))


(defn flake
  "Returns flake for tx-meta (transaction sid) that contains a json packaging of the tempids map."
  [tempids-map t]
  (flake/->Flake t const/$_tx:tempids (json/stringify tempids-map) t true nil))

(defn assign-subject-ids
  "Assigns any unresolved tempids with a permanent subject id."
  [{:keys [tempids tempids-ordered upserts db-before t] :as tx-state} tx]
  (try
    (let [ecount      (assoc (:ecount db-before) const/$_tx t) ;; make sure to set current _tx ecount to 't' value, even if no tempids in transaction
          tempids-map @tempids]
      (loop [[tempid & r] @tempids-ordered
             tempids-map* tempids-map
             upserts*     #{}
             ecount*      ecount]
        (if (nil? tempid)                                   ;; finished
          (do (reset! tempids tempids-map*)
              (when (seq upserts*)
                (reset! upserts upserts*))
              ;; return tx-state, don't need to update ecount in db-after, as dbproto/-with will update it
              tx-state)
          (if (nil? (get tempids-map tempid))
            (let [cid      (dbproto/-c-prop db-before :id (:collection tempid))
                  next-id  (if (= const/$_tx cid)
                             t                              ; _tx collection has special handling as we decrement. Current value held in 't'
                             (if-let [last-sid (get ecount* cid)]
                               (inc last-sid)
                               (flake/->sid cid 0)))
                  ecount** (assoc ecount* cid next-id)]
              (recur r (assoc tempids-map* tempid next-id) upserts* ecount**))
            (recur r tempids-map* (conj upserts* (get tempids-map tempid)) ecount*))))
      tx)
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
