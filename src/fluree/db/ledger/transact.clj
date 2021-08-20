(ns fluree.db.ledger.transact
  (:require [clojure.tools.logging :as log]
            [fluree.db.flake :as flake]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util]
            [fluree.crypto :as crypto]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.session :as session]
            [fluree.db.util.tx :as tx-util]
            [fluree.db.query.fql :as fql]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.ledger.transact.core :as tx-core]
            [fluree.db.query.range :as query-range]))


(defn valid-authority?
  [db auth authority]
  (go-try
    (if (empty? (<? (dbproto/-search db [auth "_auth/authority" authority])))
      (throw (ex-info (str authority " is not an authority for auth: " auth)
                      {:status 403 :error :db/invalid-auth})) true)))


(defn build-block
  "Builds a new block with supplied transaction(s)."
  [session transactions]
  (go-try
    (let [private-key   (:tx-private-key (:conn session))
          db-current    (<? (session/current-db session))
          _             (when (nil? db-current)
                          ;; TODO - think about this error, if it is possible, and what to do with any pending transactions
                          (log/warn "Unable to find a current db. Db transaction processor closing for db: %s/%s." (:network session) (:dbid session))
                          (session/close session)
                          (throw (ex-info (format "Unable to find a current db for: %s/%s." (:network session) (:dbid session))
                                          {:status 400 :error :db/invalid-transaction})))
          block         (inc (:block db-current))
          block-instant (util/current-time-millis)
          before-t      (:t db-current)
          prev-hash     (some-> (query-range/index-range db-current :spot = [before-t const/$_block:hash])
                                <?
                                first
                                .-o) ; get hash (object) from flake
          _             (when-not prev-hash
                          (throw (ex-info (str "Unable to retrieve previous block hash. Unexpected error.")
                                          {:status 500
                                           :error  :db/unexpected-error})))]
      ;; perform each transaction in order
      (loop [[cmd-data & r] transactions
             next-t           (dec before-t)
             db-root          db-current
             block-bytes      0
             block-fuel       0
             block-flakes     (flake/sorted-set-by flake/cmp-flakes-block)
             cmd-types        #{}
             txns             {}
             remove-preds-acc #{}]
        (let [start-time    (util/current-time-millis)
              tx-result     (<? (tx-core/transact db-root cmd-data next-t block-instant))
              {:keys [db-after bytes fuel flakes tempids auth authority status error errors
                      hash remove-preds]} tx-result
              block-bytes*  (+ block-bytes bytes)
              block-fuel*   (+ block-fuel fuel)
              block-flakes* (into block-flakes flakes)
              cmd-type      (:type tx-result)
              cmd-types*    (conj cmd-types cmd-type)
              txns*         (assoc txns (:id cmd-data) (util/without-nils
                                                         {:t         next-t ;; subject id
                                                          :status    status
                                                          :error     error
                                                          :errors    errors
                                                          :tempids   tempids
                                                          :bytes     bytes
                                                          :id        (:id cmd-data)
                                                          :fuel      fuel
                                                          :duration  (str (- (util/current-time-millis) start-time) "ms")
                                                          :auth      auth
                                                          :hash      hash
                                                          :authority authority
                                                          :type      cmd-type}))
              remove-preds* (into remove-preds-acc remove-preds)]
          (if r
            (recur r (dec next-t) db-after block-bytes* block-fuel* block-flakes* cmd-types* txns*
                   remove-preds*)
            (let [block-t             (dec next-t)
                  prevHash-flake      (flake/->Flake block-t const/$_block:prevHash prev-hash block-t true nil)
                  instant-flake       (flake/->Flake block-t const/$_block:instant block-instant block-t true nil)
                  number-flake        (flake/->Flake block-t const/$_block:number block block-t true nil)
                  tx-flakes           (mapv #(flake/->Flake block-t const/$_block:transactions % block-t true nil) (range block-t before-t))
                  block-flakes        (conj tx-flakes prevHash-flake instant-flake number-flake)
                  block-tx-hash       (tx-util/gen-tx-hash block-flakes)
                  block-tx-hash-flake (flake/->Flake block-t const/$_tx:hash block-tx-hash block-t true nil)
                  ;; We order each txn command according to the t
                  txn-hashes          (->> (vals txns*)
                                           (sort-by #(* -1 (:t %)))
                                           (map :hash))
                  hash                (tx-util/generate-merkle-root (conj txn-hashes block-tx-hash))
                  sigs                [(crypto/sign-message hash private-key)]
                  hash-flake          (flake/->Flake block-t const/$_block:hash hash block-t true nil)
                  sigs-ref-flakes     (loop [[sig & sigs] sigs
                                             acc []]
                                        (if-not sig
                                          acc
                                          (let [auth-sid (<? (dbproto/-subid db-current ["_auth/id" (crypto/account-id-from-message hash sig)]))
                                                acc*     (if auth-sid
                                                           (-> acc
                                                               (conj (flake/->Flake block-t const/$_block:ledgers auth-sid block-t true nil))
                                                               (conj (flake/->Flake block-t const/$_block:sigs sig block-t true nil)))
                                                           acc)]
                                            (recur sigs acc*))))
                  new-flakes*         (-> (into block-flakes sigs-ref-flakes)
                                          (conj hash-flake)
                                          (conj block-tx-hash-flake))
                  all-flakes          (into block-flakes* new-flakes*)
                  latest-db           (<? (session/current-db session))
                  ;; if db was indexing and is now complete, add all flakes to newly indexed db... else just add new block flakes to latest db.
                  db-after*           (cond
                                        (not= (:block latest-db) (:block db-current))
                                        (throw (ex-info "While performing transactions, latest db became newer. Cancelling."
                                                        {:status 500 :error :db/unexpected-error}))

                                        ;; nothing has changed, just add block flakes to latest db
                                        (= (get-in db-current [:stats :indexed]) (get-in latest-db [:stats :indexed]))
                                        (<? (dbproto/-with db-after block (sort flake/cmp-flakes-spot-novelty new-flakes*)))

                                        ;; database has been re-indexed while we were transacting. Use latest indexed
                                        ;; version and reapply all flakes from this block
                                        :else
                                        (do
                                          (log/info "---> While transacting, database has been reindexed. Reapplying all block flakes to latest."
                                                    {:original-index (get-in db-current [:stats :indexed]) :latest-index (get-in latest-db [:stats :indexed])})
                                          (<? (dbproto/-with latest-db block all-flakes))))
                  block-result        {:db-before   db-current
                                       :db-after    db-after*
                                       :cmd-types   cmd-types*
                                       :block       block
                                       :t           block-t
                                       :hash        hash
                                       :sigs        sigs
                                       :instant     block-instant
                                       :flakes      (into [] all-flakes)
                                       :block-bytes (- (get-in db-after* [:stats :size]) (get-in db-current [:stats :size]))
                                       :txns        txns*}
                  ;; update db status for tx group
                  new-block-resp      (<? (txproto/propose-new-block-async
                                            (-> session :conn :group) (:network session)
                                            (:dbid session) (dissoc block-result :db-before :db-after)))]
              (if (true? new-block-resp)
                (do
                  (<? (indexing/index* session {:remove-preds remove-preds*}))
                  block-result)
                (do
                  (log/warn "Proposed block was not accepted by the network because: "
                            (pr-str new-block-resp)
                            "Proposed block: "
                            (dissoc block-result :db-before :db-after))
                  false)))))))))
