(ns replay-txns
  (:require [fluree.db.api :as fdb]
            [clojure.tools.logging :as log]
            [fluree.db.storage.core :as storage]
            [fluree.db.session :as session]
            [fluree.db.dbproto :as dbproto]
            [clojure.core.async :as async]))

;; utility to replay transactions


(defn- block-flakes
  "Returns block flakes for specified block"
  [conn network dbid block]
  (-> (storage/block conn network dbid block)
      (async/<!!)
      :flakes))

(defn- nearest-index-point
  "Given a block, finds closest index point prior to or equal to provided block."
  [conn db-ident block]
  (let [db-info       @(fdb/ledger-status conn db-ident)
        index-points  (-> db-info :indexes keys sort reverse)
        closest-point (->> index-points
                           (filter #(<= % block))
                           first)]
    closest-point))


(defn replay-blocks
  "Replay db from an index point through the specified block (inclusive).
  Will only use novelty, so make you have enough memory to hold all new blocks in memory.

  If you don't supply index-point, will use the latest index point available prior to the to-block."
  ([conn db-ident to-block]
   (let [closest-point (nearest-index-point conn db-ident to-block)]
     (log/info "Using Index point: " closest-point)
     (replay-blocks conn db-ident to-block closest-point)))
  ([conn db-ident to-block index-point]
   (let [session (session/session conn db-ident)
         {:keys [network dbid blank-db]} session
         db      (async/<!! (storage/reify-db conn network dbid blank-db index-point))]
     (reduce (fn [db block]
               (let [flakes (block-flakes conn network dbid block)
                     db*    (async/<!! (dbproto/-with db block flakes))]
                 (log/info "Applying block:" block)
                 db*))
             db (range index-point (inc to-block))))))


(defn replay-blocks-and-index
  "Like replay-blocks, but actually writes out a new index *locally*.
  Not for permanent use, as does not notify raft group.

  If used as part of a network db, delete data files before rejoining network."
  ([conn db-ident to-block]
   (let [closest-point (nearest-index-point conn db-ident to-block)]
     (log/info "Using Index point: " closest-point)
     (replay-blocks conn db-ident to-block closest-point)))
  ([conn db-ident to-block index-point]
   (let [session (session/session conn db-ident)
         {:keys [network dbid blank-db]} session
         db      (async/<!! (storage/reify-db conn network dbid blank-db index-point))]
     (reduce (fn [db block]
               (let [flakes (block-flakes conn network dbid block)
                     db*    (async/<!! (dbproto/-with db block flakes))]
                 (log/info "Applying block:" block)
                 db*))
             db (range index-point (inc to-block)))))

  )





(comment

  (def conn (:conn user/system))

  (def testdb (replay-blocks conn "demo/ExpenseDemo" 3451))


  (-> testdb
      :novelty
      :spot)

  (block-flakes conn "demo" "ExpenseDemo" 3451)

  (fdb/query testdb {:select ["*"] :from 387028092980125})

  )