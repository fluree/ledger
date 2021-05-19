(ns fluree.db.ledger.transact.identity
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.util.core :as util]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log])
  (:import (fluree.db.flake Flake)))


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


(defn resolve-iri
  [iri idx {:keys [db-root idents] :as tx-state}]
  (go-try
    (if-let [id (contains? @idents iri)]
      id
      (let [resolved (some-> (<? (query-range/index-range db-root :post = [const/$iri iri]))
                             ^Flake first
                             (.-s))
            id       (or resolved (tempid/construct iri idx tx-state true))]
        (swap! idents assoc iri id)
        id))))


(defn id-type
  "Returns id-type as either:
  - :tempid
  - :pred-ident (i.e. [_user/username 'janedoe']
  - :sid (long integer)

  Else throws."
  [_id]
  (cond
    (tempid/TempId? _id) :tempid
    (util/temp-ident? _id) :temp-ident
    (util/pred-ident? _id) :pred-ident
    (int? _id) :sid
    :else (throw (ex-info (str "Invalid _id: " _id)
                          {:status 400 :error :db/invalid-transaction}))))

(defn- temp-flake->flake
  "Transforms a TempId iri flake into a flake."
  [{:keys [tempids t] :as tx-state} [iri tempid]]
  (flake/->Flake (get @tempids tempid) const/$iri iri t true nil))

(defn generate-tempid-flakes
  "Returns a set of flakes for new IRIs"
  [{:keys [idents] :as tx-state}]
  (->> @idents
       (filter #(tempid/TempId? (val %)))
       (map (partial temp-flake->flake tx-state))))
