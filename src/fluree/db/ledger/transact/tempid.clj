(ns fluree.db.ledger.transact.tempid
  (:refer-clojure :exclude [use set])
  (:require [fluree.db.util.core :as util]
            [clojure.string :as str]))

;; functions related to generation of tempids

(defrecord TempId [user-string collection key unique?])


(defn TempId?
  [x]
  (instance? TempId x))


(defn register
  "Registers a TempId instance into the tx-state, returns provided TempId unaltered."
  [TempId {:keys [tempids] :as tx-state}]
  {:pre [(TempId? TempId)]}
  (swap! tempids update TempId identity)                    ;; don't touch any existing value, otherwise nil
  TempId)


(defn new*
  [tempid]
  (let [[collection id] (str/split tempid #"[^\._a-zA-Z0-9]" 2)
        key (if id
              (keyword collection id)
              (keyword collection (str (util/random-uuid))))]
    (->TempId tempid collection key (boolean id))))


(defn new
  "Generates a new tempid record from tempid string. Registers tempid in :tempids atom within
  tx-state to track all tempids in tx, and also their final resolution value.

  defrecord equality will consider tempids with identical values the same, even if constructed separately.
  We therefore construct a tempid regardless if it has already been created, but are careful not to
  update any existing subject id that might have already been mapped to the tempid."
  [tempid tx-state]
  (let [TempId (new* tempid)]
    (register TempId tx-state)))


(defn use
  "Returns a tempid that will be used for a Flake object value, but only returns it if
  it already exists. If it does not exist, it means it is a tempid used as a value, but it was never used
  as a subject."
  [tempid {:keys [tempids] :as tx-state}]
  (let [TempId (new* tempid)]
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
  [{:keys [tempids] :as tx-state}]
  (reduce-kv (fn [acc ^TempId tempid subject-id]
               (if (:unique? tempid)
                 (assoc acc (:user-string tempid) subject-id)
                 (update acc (:user-string tempid) (fn [[min-sid max-sid]]
                                                     [(if min-sid (min min-sid subject-id) subject-id)
                                                      (if max-sid (max max-sid subject-id) subject-id)]))))
             {} @tempids))