(ns fluree.db.ledger.transact
  (:require [fluree.db.util.log :as log]
            [fluree.db.flake :as flake]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util]
            [fluree.crypto :as crypto]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.session :as session]
            [fluree.db.util.tx :as tx-util]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.ledger.transact.core :as tx-core]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async])
  (:import (fluree.db.flake Flake)))

(set! *warn-on-reflection* true)


(defn valid-authority?
  [db auth authority]
  (go-try
    (if (empty? (<? (dbproto/-search db [auth "_auth/authority" authority])))
      (throw (ex-info (str authority " is not an authority for auth: " auth)
                      {:status 403 :error :db/invalid-auth})) true)))


(defn new-block-map
  "Creates initial block map that ultimately gets returned after block completed."
  ([db]
   (new-block-map db nil))
  ([db prev-hash]
   {:db-orig      db                                         ; original DB before block starts
    :db-before    db                                         ; db before each transaction
    :db-after     db                                         ; db after each transaction
    :cmd-types    #{}
    :block        (inc (:block db))                          ; block number
    :t            (:t db)                                    ; updated with every tx within block
    :before-t     (:t db)                                    ; never updated, t before block
    :hash         nil                                        ; final block hash
    :sigs         nil                                        ; signature(s) of ledgers that seal the block
    :instant      (util/current-time-millis)
    :flakes       (flake/sorted-set-by flake/cmp-flakes-block)
    :block-bytes  0
    :fuel         0
    :txns         {}
    :prev-hash    prev-hash
    :remove-preds #{}}))


(defn merge-tx-into-block
  "Merges transaction results into the block map that ultimately gets returned.

  The block itself also creates a transaction on top of the other transactions, and
  also uses this function to update the db, flakes and bytes. The block tx does not
  have a txid, and intentionally will not exist in the :txns map."
  [block-map tx-result]
  (let [{:keys [db-before db-after bytes fuel flakes remove-preds type t txid hash]} tx-result]
    (-> block-map
        (assoc :db-before db-before
               :db-after db-after
               :t t
               :hash hash)
        (update :flakes into flakes)
        (update :block-bytes + bytes)
        (update :fuel + fuel)
        (update :remove-preds into remove-preds)
        (cond-> type (update :cmd-types conj type)
                txid (assoc-in [:txns txid]
                               (util/without-nils           ;; note the "block" transaction won't have a txid
                                 (-> tx-result
                                     (assoc :id txid)       ;; historically reported out :id and not :txid in tx result map
                                     (select-keys [:id :type :t :status :error :errors
                                                   :tempids :auth :hash :authority
                                                   :bytes :fuel :duration]))))))))


(defn build-block-tx
  "Generates the transaction for the block sealing, and returns the final block-map.
  Note db-before is never updated during transactions, so it is the db before the block
  was started."
  [{:keys [db-after t prev-hash block before-t txns instant] :as block-map} session]
  (go-try
    (let [db-before           db-after
          private-key         (:tx-private-key (:conn session))
          block-t             (dec t)
          prevHash-flake      (flake/->Flake block-t const/$_block:prevHash prev-hash block-t true nil)
          instant-flake       (flake/->Flake block-t const/$_block:instant instant block-t true nil)
          number-flake        (flake/->Flake block-t const/$_block:number block block-t true nil)
          tx-flakes           (mapv #(flake/->Flake block-t const/$_block:transactions % block-t true nil) (range block-t before-t))
          block-flakes        (conj tx-flakes prevHash-flake instant-flake number-flake)
          block-tx-hash       (tx-util/gen-tx-hash block-flakes)
          block-tx-hash-flake (flake/->Flake block-t const/$_tx:hash block-tx-hash block-t true nil)
          ;; We order each txn command according to the t
          txn-hashes          (->> (vals txns)
                                   (sort-by #(* -1 (:t %)))
                                   (map :hash))
          hash                (tx-util/generate-merkle-root (conj txn-hashes block-tx-hash))
          sigs                [(crypto/sign-message hash private-key)]
          hash-flake          (flake/->Flake block-t const/$_block:hash hash block-t true nil)
          sigs-ref-flakes     (loop [[sig & sigs] sigs
                                     acc []]
                                (if-not sig
                                  acc
                                  (let [auth-sid (<? (dbproto/-subid db-before ["_auth/id" (crypto/account-id-from-message hash sig)]))
                                        acc*     (if auth-sid
                                                   (-> acc
                                                       (conj (flake/->Flake block-t const/$_block:ledgers auth-sid block-t true nil))
                                                       (conj (flake/->Flake block-t const/$_block:sigs sig block-t true nil)))
                                                   acc)]
                                    (recur sigs acc*))))
          new-flakes*         (-> (into block-flakes sigs-ref-flakes)
                                  (conj hash-flake)
                                  (conj block-tx-hash-flake))
          db-after*           (<? (dbproto/-with db-before block new-flakes*))]
      {:db-before db-before
       :db-after  db-after*
       :flakes    new-flakes*
       :hash      hash
       :t         block-t
       :fuel      0
       :bytes     (- (get-in db-after* [:stats :size]) (get-in db-before [:stats :size]))})))


(defn retrieve-prev-hash
  "Retrieves the latest block hash."
  [db-current]
  (go-try
    (let [before-t        (:t db-current)
          prev-hash-flake (first (<? (query-range/index-range db-current :spot = [before-t const/$_block:hash])))]
      (when-not prev-hash-flake
        (throw (ex-info (str "Unable to retrieve previous block hash. Unexpected error.")
                        {:status 500
                         :error  :db/unexpected-error})))
      (.-o ^Flake prev-hash-flake))))


(defn propose-block
  "Proposes block to consensus network. Returns core async channel with success or failure."
  [{:keys [group] :as _conn} network ledger-id {:keys [flakes] :as block-map}]
  (let [block-map' (-> block-map
                       (dissoc :fuel :db-orig :db-before :db-after :before-t :prev-hash :remove-preds)
                       (assoc :flakes (into [] flakes)))]
    (log/trace (str "Proposing new block for " network "/" ledger-id ":") block-map')
    (txproto/propose-new-block-async group network ledger-id block-map')))


(defn- error-unexpected-tx
  "Unexpected error processing a transaction error."
  [e {:keys [group] :as conn} network {:keys [id] :as cmd-data}]
  (if-let [message (ex-message e)]
    (log/warn message)
    (log/error e (format "Unexpected transaction error. Removing transaction with id: %s." id)))
  ;; remove transaction in background - possible if no other work tx will get attempted again
  (txproto/remove-command-from-queue group network id)
  true)


(defn- remove-all-txids
  "Under a bad exception case, removes all txids part of the block so they
  don't attempt to get processed again. It does this synchronously and will hold
  the block transactor."
  [conn network block-map]
  (let [txids (keys (:txns block-map))]
    (log/info (str "To prevent additional exceptions, removing all transactions that are part of the block: " txids))
    (doseq [txid txids]
      (let [res (async/<!! (txproto/remove-command-from-queue (:group conn) network txid))]
        (when (util/exception? res)
          (log/error res (str "Fatal error, after an error processing a block "
                              "an unexpected error happened trying to remove the involved "
                              "transactions from raft state: " txids))
          (System/exit 1))))))


(defn- catch-build-block-exception
  "Unexpected error while building a new block around transaction(s)."
  [conn network block-map block-result]
  (if-not (util/exception? block-result)
    block-result
    (let [ex-msg (ex-message block-result)]
      (if ex-msg
        (log/warn ex-msg)
        (log/error block-result
                   (str "Unexpected error while building a new block around transactions: "
                        (keys (:txns block-map)) ".")))
      (remove-all-txids conn network block-map)
      ;; explicitly returning nil will halt farther processing on block
      nil)))


(defn- error-propose-new-block
  "Error handler for unexpected exception when proposing a new block."
  [conn network block-result new-block-resp-ex]
  (log/error new-block-resp-ex (str "Unexpected consensus error proposing new block"))
  (remove-all-txids conn network block-result))


(defn build-block
  "Builds a new block with supplied transaction(s)."
  [{:keys [conn network ledger-id] :as session} db-before transactions]
  (go-try
    (when (nil? db-before)
      ;; TODO - think about this error, if it is possible, and what to do with any pending transactions
      (log/warn "Unable to find a current db. Db transaction processor closing for db: %s/%s." network ledger-id)
      (session/close session)
      (throw (ex-info (format "Unable to find a current db for: %s/%s." network ledger-id)
                      {:status 400 :error :db/invalid-transaction})))
    (let [prev-hash (<? (retrieve-prev-hash db-before))]
      ;; perform each transaction in order
      (loop [[cmd-data & r] transactions
             block-map      (new-block-map db-before prev-hash)]
        (let [block-map* (try
                           (->> cmd-data
                                tx-util/validate-command
                                (tx-core/transact block-map)
                                <?
                                (merge-tx-into-block block-map))
                           (catch Throwable e
                             (error-unexpected-tx e conn network cmd-data)
                             ;; return unalterted block-map
                             block-map))]
          (if r
            (recur r block-map*)
            (if (:db-after block-map*)
              (let [block-result    (some->> (async/<! (build-block-tx block-map* session))
                                             (catch-build-block-exception conn network block-map*) ;; will return nil if exception
                                             (merge-tx-into-block block-map*))
                    reindexed-db    (indexing/reindexed-db session)
                    block-result*   (when block-result
                                      (cond
                                        reindexed-db
                                        (<? (indexing/merge-new-index session block-result reindexed-db))

                                        ;; at novelty-max - may or may not be a reindex in process. Need to wait.
                                        (indexing/novelty-max? session (:db-after block-result))
                                        (<? (indexing/novelty-max-block session block-result))

                                        :else
                                        block-result))

                    block-approved? (when block-result*
                                      (async/<! (propose-block conn network ledger-id block-result*)))]
                (cond
                  (true? block-approved?)
                  (do
                    (when (indexing/novelty-min? session (:db-after block-result*))
                      (indexing/ensure-indexing session block-result*))
                    block-result*)

                  (util/exception? block-approved?)
                  (error-propose-new-block conn network block-result block-approved?)

                  :else
                  (do
                    (log/warn "Proposed block was not accepted by the network because: "
                              block-approved?
                              "Proposed block: "
                              (dissoc block-result* :db-orig :db-before :db-after))
                    false)))
              (do ; all transactions errored out
                (log/info "Block processing resulted in no new blocks, all transaction attempts had issues.")
                false))))))))
