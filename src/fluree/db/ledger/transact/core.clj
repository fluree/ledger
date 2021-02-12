(ns fluree.db.ledger.transact.core
  (:require [fluree.db.util.async :refer [<? go-try merge-into? channel?]]
            [fluree.db.session :as session]
            [fluree.db.util.log :as log]
            [fluree.db.query.fql :as fql]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.tx :as tx-util]
            [fluree.crypto :as crypto]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.constants :as const])
  (:import (fluree.db.flake Flake)
           (java.time Instant)
           (java.util UUID)
           (java.net URI)))


(defprotocol ITxSubject
  (-iri [this] "Returns the IRI of the subject")
  (-iri-full [this] "Expands IRI's prefix if prefix defined")
  (-id [this] "Returns the internal Fluree integer ID of the subject")
  (-children [this] "Returns any embedded children subjects as new Subjects")
  (-flakes [this] "Returns list of flakes")
  (-retractions [this] "Returns retraction flakes")
  (-flatten [this] "Finds any nested transactions and returns new Subjects"))

(defrecord SubjectJSONLD [json-ld tx-cache schema]
  ITxSubject
  (-flakes [_])
  (-iri [_] (get json-ld "@id"))

  )

(defn flatten-subject
  )

(defrecord SubjectJSON [json tx-cache schema]
  ITxSubject
  (-flakes [_])
  (-iri [_] :none)
  (-flatten [_] )


  )

(defn to-subjects
  [tx]
  (map #(map->SubjectJSON {:json %}) tx)
  )


(comment


  (def sub (map->Subject {:json-ld {"@id"         "ex:person#Brian"
                                    "rdf:type"    "Student"
                                    "person:name" "Brian"}}))

  (-iri sub)


  (def sample-tx [{:_id "_collection"
                   :name "_collection/test"}
                  {:_id 12345
                   :sub [{:_id "mysub1"
                          :name "blah"}
                         {:_id "mysub2"
                          :name "blah2"}
                         {:_id "mysub3"
                          :name "blah3"
                          :nested {:_id "subsub"
                                   :name "blah4"}}]}
                  :_id ["test/me" 123]
                  :asub {:_id "hi"
                         :name "hithere"}])
  (-> (to-subjects sample-tx)
      (-flatten))

  )


(defn build-transaction
  [session db cmd-data next-t block-instant]
  (go-try
    (let [tx-map          (tx-util/validate-command (:command cmd-data))
          deps            (-> tx-map :cmd json/parse :deps)
          _               (when-not (<? (deps-succeeded? db deps))
                            (throw (ex-info (str "One or more of the dependencies for this transaction failed: " deps)
                                            {:status 403 :error :db/invalid-auth})))
          ; kick off resolving auth id and authority in parallel
          {:keys [auth authority]} tx-map
          auth_id-ch      (dbproto/-subid db ["_auth/id" auth] true)
          authority_id-ch (when authority
                            (let [authority-id (if (string? authority) ["_auth/id" authority] authority)]
                              (dbproto/-subid db authority-id true)))
          cmd-type        (keyword (:type tx-map))
          tx**            (case cmd-type
                            :tx (->> (:tx tx-map) (flatten-transaction db (atom {})) <? pre-validate-transaction)
                            :new-db (create-new-db-tx tx-map))
          tx-map*         (-> (assoc tx-map :tx tx**)
                              (add-tx-meta next-t))
          auth_id         (async/<! auth_id-ch)
          _               (when (util/exception? auth_id)
                            (throw (ex-info (str "Auth id for transaction does not exist in the database: " auth)
                                            {:status 403 :error :db/invalid-auth})))
          ;; validate authority is valid or throw
          _               (when authority
                            (let [authority_id (async/<! authority_id-ch)
                                  _            (when (util/exception? authority_id)
                                                 (throw (ex-info (str "Authority " authority " does not exist.")
                                                                 {:status 403 :error :db/invalid-auth})))]
                              (<? (valid-authority? db auth_id authority_id))))
          roles           (<? (auth/roles db auth_id))
          tx-permissions  (-> (<? (permissions/permission-map db roles :transact))
                              (assoc :auth auth_id))]
      (<? (do-transact! session db tx-map* tx-permissions auth_id next-t block-instant)))
    (catch Exception e
      (let [error            (ex-data e)
            fast-forward-db? (:tt-id db)
            status           (or (:status error) 500)
            error-str        (str status " "
                                  (if (:error error)
                                    (str (util/keyword->str (:error error)))
                                    "db/unexpected-error")
                                  " "
                                  (.getMessage e))
            tx-map           (try (tx-util/validate-command (:command cmd-data)) (catch Exception e nil))
            {:keys [auth authority]} tx-map
            resp-map         (<? (generate-tx-error-flakes db next-t tx-map (:command cmd-data) error-str))
            db-after         (if fast-forward-db?
                               (<? (dbproto/-forward-time-travel db (:flakes resp-map)))
                               (<? (dbproto/-with-t db (:flakes resp-map))))
            bytes            (- (get-in db-after [:stats :size]) (get-in db [:stats :size]))]
        (when (= 500 status)
          (log/error e "Unexpected error processing transaction. Please report issues to Fluree."))
        (assoc resp-map
          :status status
          :error error-str
          :bytes bytes
          :db-after db-after
          ;; TODO - report out fuel in error within ex-info for use here
          :fuel (or (:fuel (ex-data e)) 0)
          :auth auth
          :authority authority
          :type (keyword (:type tx-map)))))))


(defn build-block
  "Builds a new block with supplied transaction(s)."
  [session transactions]
  (go-try
    (let [private-key   (:tx-private-key (:conn session))
          db-before-ch  (session/current-db session)
          db-before     (<? db-before-ch)
          _             (when (nil? db-before)
                          ;; TODO - think about this error, if it is possible, and what to do with any pending transactions
                          (log/warn "Unable to find a current db. Db transaction processor closing for db: %s/%s." (:network session) (:dbid session))
                          (session/close session)
                          (throw (ex-info (format "Unable to find a current db for: %s/%s." (:network session) (:dbid session))
                                          {:status 400 :error :db/invalid-transaction})))
          block         (inc (:block db-before))
          block-instant (Instant/now)
          prev-hash     (<? (fql/query db-before {:selectOne "?hash"
                                                  :where     [["?t" "_block/number" (:block db-before)]
                                                              ["?t" "_block/hash" "?hash"]]}))
          _             (when-not prev-hash
                          (throw (ex-info (str "Unable to retrieve previous block hash. Unexpected error.")
                                          {:status 500
                                           :error  :db/unexpected-error})))
          before-t      (:t db-before)]
      ;; perform each transaction in order
      (loop [[cmd-data & r] transactions
             next-t           (dec before-t)
             db               db-before
             block-bytes      0
             block-fuel       0
             block-flakes     (flake/sorted-set-by flake/cmp-flakes-block)
             cmd-types        #{}
             txns             {}
             remove-preds-acc #{}]
        (let [tx-result     (<? (build-transaction session db cmd-data next-t block-instant))
              {:keys [db-after bytes fuel flakes tempids auth authority status error hash
                      remove-preds]} tx-result
              block-bytes*  (+ block-bytes bytes)
              block-fuel*   (+ block-fuel fuel)
              block-flakes* (into block-flakes flakes)
              cmd-type      (:type tx-result)
              cmd-types*    (conj cmd-types cmd-type)
              txns*         (assoc txns (:id cmd-data) (util/without-nils
                                                         {:t         next-t ;; subject id
                                                          :status    status
                                                          :error     error
                                                          :tempids   tempids
                                                          :bytes     bytes
                                                          :id        (:id cmd-data)
                                                          :fuel      fuel
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
                  instant-flake       (flake/->Flake block-t const/$_block:instant (.toEpochMilli ^Instant block-instant) block-t true nil)
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
                  all-flakes          (into block-flakes* new-flakes*)
                  latest-db           (<? (session/current-db session))
                  ;; if db was indexing and is now complete, add all flakes to newly indexed db... else just add new block flakes to latest db.
                  db-after*           (cond
                                        (not= (:block latest-db) (:block db-before))
                                        (throw (ex-info "While performing transactions, latest db became newer. Cancelling."
                                                        {:status 500 :error :db/unexpected-error}))

                                        ;; nothing has changed, just add block flakes to latest db
                                        (= (get-in db-before [:stats :indexed]) (get-in latest-db [:stats :indexed]))
                                        (<? (dbproto/-with db-after block (sort flake/cmp-flakes-spot-novelty new-flakes*)))

                                        ;; database has been re-indexed while we were transacting. Use latest indexed
                                        ;; version and reapply all flakes from this block
                                        :else
                                        (do
                                          (log/info "---> While transacting, database has been reindexed. Reapplying all block flakes to latest."
                                                    {:original-index (get-in db-before [:stats :indexed]) :latest-index (get-in latest-db [:stats :indexed])})
                                          (<? (dbproto/-with latest-db block all-flakes))))
                  block-result        {:db-before   db-before
                                       :db-after    db-after*
                                       :cmd-types   cmd-types*
                                       :block       block
                                       :t           block-t
                                       :hash        hash
                                       :sigs        sigs
                                       :instant     (.toEpochMilli ^Instant block-instant)
                                       :flakes      (into [] all-flakes)
                                       :block-bytes (- (get-in db-after* [:stats :size]) (get-in db-before [:stats :size]))
                                       :txns        txns*}]

              ;; update db status for tx group
              (let [new-block-resp (<? (txproto/propose-new-block-async
                                         (-> session :conn :group) (:network session)
                                         (:dbid session) (dissoc block-result :db-before :db-after)))]
                (if (true? new-block-resp)
                  (do
                    ;; update cached db
                    ;(let [new-db-ch (async/promise-chan)]
                    ;  (async/put! new-db-ch (:db-after block-result))
                    ;  (session/cas-db! session db-before-ch new-db-ch))
                    ;; reindex if needed
                    ;; to do -add opts
                    (<? (indexing/index* session {:remove-preds remove-preds*}))
                    block-result)
                  (do
                    (log/warn "Proposed block was not accepted by the network because: "
                              (pr-str new-block-resp)
                              "Proposed block: "
                              (dissoc block-result :db-before :db-after))
                    false))))))))))


(comment

  (-> (URI. "http://清华大学.cn/")
      (.toString))


  (URI.
    "http://example.com:5050/&#x0410;&#x043B;&#x0435;
  &#x043A;&#x0441;&#x0430;&#x043D;&#x0434;&#x0440;&#x0421;&#x043E;&#x043B;&#x0436;
  &#x0435;&#x043D;&#x0438;&#x0446;&#x044B;&#x043D;/000100")

  )