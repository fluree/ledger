(ns fluree.db.ledger.api.closed-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.api.auth :as fdb-auth]
            [fluree.db.query.http-signatures :as http-sig]
            [fluree.db.util.json :as json]
            [org.httpkit.client :as http]
            [fluree.db.api :as fdb])
  (:import (java.util UUID)))

(use-fixtures :once (partial test/test-system
                             {:fdb-api-open false}))


(def endpoint (str "http://localhost:" @test/port "/fdb/"))

(defn transact-and-query-user?
  "Returns a vector of [success? txn-response query-response]. success? will be
  true iff the given private-key can transact and query back out a _user in the
  given ledger. For verifying permissions / ownership."
  [ledger private-key]
  (let [base-req       {:headers {"content-type" "application/json"}}
        txn-endpoint   (str endpoint ledger "/transact")
        username       (str "tester-" (UUID/randomUUID))
        txn-req        (assoc base-req
                         :body (json/stringify
                                 [{:_id "_user", :username username}]))
        signed-txn-req (http-sig/sign-request :post txn-endpoint txn-req
                                              private-key)

        {txn-status :status :as txn-response}
        @(http/post txn-endpoint signed-txn-req)

        query-endpoint (str endpoint ledger "/query")
        users-query    (-> {:select ["*"], :from "_user"}
                           json/stringify
                           (as-> $q
                                 (assoc base-req :body $q)
                                 (http-sig/sign-request :post
                                                        query-endpoint $q
                                                        private-key)))

        {query-status :status, query-body :body :as query-response}
        @(http/post query-endpoint users-query)]
    [(and (= 200 txn-status query-status)
          (= username (-> query-body json/parse first :_user/username)))
     txn-response query-response]))


(deftest new-db-test
  (testing "new db (ledger actually) is owned by request signer"
    (let [{:keys [private id]} (fdb-auth/new-private-key)
          new-db-endpoint   (str endpoint "new-db")
          base-req          {:headers {"content-type" "application/json"}}
          ledger            (str "test/ledger-" (UUID/randomUUID))
          create-req        (assoc base-req
                              :body (json/stringify {:db/id ledger}))
          signed-create-req (http-sig/sign-request :post new-db-endpoint
                                                   create-req private)
          {create-status :status} @(http/post new-db-endpoint
                                              signed-create-req)
          _                 (fdb/wait-for-ledger-ready
                              (:conn test/system) ledger)
          query-endpoint    (str endpoint ledger "/query")
          owners-query      (-> {:select ["*"], :from "_auth"}
                                json/stringify
                                (as-> $q
                                      (assoc base-req :body $q)
                                      (http-sig/sign-request :post
                                                             query-endpoint $q
                                                             private)))

          {owners-query-status :status, owners-query-body :body}
          @(http/post query-endpoint owners-query)]
      (is (every? #(= 200 %) [create-status owners-query-status]))
      (is (= id (-> owners-query-body json/parse first :_auth/id)))))

  (testing "request signer can transact against new db (actually ledger) they create"
    (let [{:keys [private]} (fdb-auth/new-private-key)
          new-db-endpoint   (str endpoint "new-db")
          base-req          {:headers {"content-type" "application/json"}}
          ledger            (str "test/ledger-" (UUID/randomUUID))
          create-req        (assoc base-req
                              :body (json/stringify {:db/id ledger}))
          signed-create-req (http-sig/sign-request :post new-db-endpoint
                                                   create-req private)
          {create-status :status} @(http/post new-db-endpoint
                                              signed-create-req)
          _                 (fdb/wait-for-ledger-ready
                              (:conn test/system) ledger)]
      (is (= 200 create-status))
      (let [[success? txn-response query-response]
            (transact-and-query-user? ledger private)]
        (is success? (str "Transaction response: " (pr-str txn-response)
                          "Query response: " (pr-str query-response))))))

  (testing "new db (ledger actually) is owned by owners specified in request"
    (let [{private-1 :private, auth-id-1 :id} (fdb-auth/new-private-key)
          {private-2 :private, auth-id-2 :id} (fdb-auth/new-private-key)
          {private-3 :private, auth-id-3 :id} (fdb-auth/new-private-key)
          new-db-endpoint   (str endpoint "new-db")
          base-req          {:headers {"content-type" "application/json"}}
          ledger            (str "test/ledger-" (UUID/randomUUID))
          create-req        (assoc base-req
                              :body (json/stringify
                                      {:db/id  ledger
                                       :owners [auth-id-2 auth-id-3]}))
          signed-create-req (http-sig/sign-request :post new-db-endpoint
                                                   create-req private-1)
          {create-status :status} @(http/post new-db-endpoint
                                              signed-create-req)
          _                 (fdb/wait-for-ledger-ready
                              (:conn test/system) ledger)
          query-endpoint    (str endpoint ledger "/query")
          owners-query      (-> {:select ["*"], :from "_auth"}
                                json/stringify
                                (as-> $q
                                      (assoc base-req :body $q)
                                      (http-sig/sign-request :post
                                                             query-endpoint $q
                                                             private-2)))

          {owners-query-status :status, owners-query-body :body}
          @(http/post query-endpoint owners-query)]
      (is (every? #(= 200 %) [create-status owners-query-status]))
      (is (= #{auth-id-1 auth-id-2 auth-id-3}
             (->> owners-query-body
                  json/parse
                  (map :_auth/id)
                  set)))
      (is (every? #(transact-and-query-user? ledger %)
                  [private-1 private-2 private-3])))))

(deftest history-query-collection-name-test
  (testing "Query history of flakes with _collection/name predicate"
    (let [{:keys [private id]} (fdb-auth/new-private-key)
          ledger         (test/rand-ledger test/ledger-endpoints
                                           {:owners [id]})
          _              (test/transact-schema ledger "chat-alt.edn"
                                               :clj)
          query          {:history [nil 40]}
          base-req       {:headers {"content-type" "application/json"}
                          :body    (json/stringify query)}
          query-endpoint (str endpoint ledger "/history")
          signed-req     (http-sig/sign-request :post query-endpoint base-req
                                                private)
          {:keys [status body] :as resp} @(http/post query-endpoint signed-req)
          result         (json/parse body)]
      (is (= 200 status)
          (str "Response was: " (pr-str resp)))
      (is (every? (fn [flakes]
                    (every? #(= 40 (second %)) flakes))
                  (map :flakes result))))))

(deftest query-block-two-test
  (let [{:keys [private id]} (fdb-auth/new-private-key)
        ledger         (test/rand-ledger test/ledger-endpoints {:owners [id]})
        _              (test/transact-schema ledger "chat-alt.edn" :clj)
        query          {:block 2}
        base-req       {:headers {"content-type" "application/json"}
                        :body    (json/stringify query)}
        query-endpoint (str endpoint ledger "/block")
        signed-req     (http-sig/sign-request :post query-endpoint base-req
                                              private)
        {:keys [status body] :as resp} @(http/post query-endpoint signed-req)
        result         (json/parse body)]

    (is (= 200 status)
        (str "Response was: " (pr-str resp)))

    (is (= 2 (-> result first :block)))

    (is (every? #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs
                  :flakes}
                (-> result first keys)))))