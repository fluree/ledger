(ns fluree.db.ledger.memorydb
  (:require [clojure.core.async :as async]
            [fluree.db.ledger.bootstrap :as bootstrap]
            [fluree.db.query.schema :as schema]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]))

;; One-off in-memory dbs, eventually move to fluree/db repository so local in-memory dbs can be launched
;; inside application servers, web browsers, ?? - to maintain local state but have all of the other benefits

;; Put here mostly for quickly testing, it will make sense to move most tests to utilize this format.

;; For now, requires bootstrap and transact namespaces, which are only in fluree/ledger

(defn new-db
  "Creates a local, in-memory but bootstrapped db (primarily for testing)."
  [conn ledger]
  (let [pc (async/promise-chan)]
    (async/go
      (let [block-data   (bootstrap/boostrap-memory-db conn ledger nil)
            db-no-schema (:db block-data)
            schema       (<? (schema/schema-map db-no-schema))]
        (async/put! pc (assoc db-no-schema :schema schema))))
    pc))


(defn transact-flakes
  "Transacts a series of preformatted flakes into the in-memory db."
  [db flakes]
  (let [block (inc (:block db))]
    (dbproto/-with (async/<!! db) block flakes)))


(defn transact-tuples
  "Transacts tuples which includes s, p, o and optionally op.
  If op is not explicitly false, it is assumed to be true.

  Does zero validation that tuples are accurate"
  [db tuples]
  (let [t     (dec (:t db))
        flakes (->> tuples
                    (map (fn [[s p o op]]
                           (flake/->Flake s p o t (if (false? op) false true) nil))))]
    (transact-flakes db flakes)))


(defn transact
  "Performs a fully validating transaction to an in-memory db"
  [db transaction]
  ::coming-soon!)

