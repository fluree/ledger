(ns fluree.db.ledger.transact.identity
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.util.core :as util]))


(defn resolve-ident-strict
  "Resolves ident (from cache if exists). Will throw exception if ident cannot be resolved."
  [ident {:keys [db-root idents] :as tx-state}]
  (go-try
    (if-let [cached (get @idents ident)]
      cached
      (let [resolved (<? (dbproto/-subid db-root ident false))]
        (if (nil? resolved)
          (throw (ex-info (str "Invalid identity, does not exist: " (pr-str ident))
                          {:status 400 :error :db/invalid-tx}))
          (do
            (swap! idents assoc ident resolved)
            resolved))))))

(defn id-type
  "Returns id-type as either:
  - tempid
  - pred-ident (i.e. [_user/username 'janedoe']
  - sid (long integer)

  Throws if none of thse valid types."
  [_id]
  (cond
    (tempid/TempId? _id) :tempid
    (util/pred-ident? _id) :pred-ident
    (int? _id) :sid
    :else (throw (ex-info (str "Invalid _id: " _id)
                          {:status 400 :error :db/invalid-transaction}))))
