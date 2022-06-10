(ns fluree.db.peer.clojure-client-tests
  (:require  [clojure.test :as t :refer [deftest testing is]]
             [fluree.db.api :as fdb]
             [fluree.db.test-helpers :as test-helpers]
             [clojure.core.async :as async]))

(deftest clj-api-test
  (let [ledger-port (test-helpers/get-free-port)
        ledger-server (test-helpers/start-server {:fdb-api-port ledger-port
                                                  :fdb-mode "ledger"
                                                  :fdb-group-servers "ledger-server@localhost:11001"
                                                  :fdb-group-this-server "ledger-server"
                                                  :fdb-storage-type "memory"
                                                  :fdb-consensus-type "in-memory"})
        conn (fdb/connect (str "http://localhost:" ledger-port))
        ledger "test/test1"]
    (testing "no ledgers"
      (is (= []
             @(fdb/ledger-list conn))))
    (testing "successful ledger creation"
      (is (some? @(fdb/new-ledger conn "test/test1")))
      (is (= [["test" "test1"]]
             @(fdb/ledger-list conn)))
      (is (= true
             (fdb/wait-for-ledger-ready conn ledger))))
    (testing "successful transaction"
      (let [tx-report @(fdb/transact conn ledger [{:_id "_user" :_user/username "dan"}])]

        (is (= 200
               (:status tx-report)))
        #_(is (= {}
                 (async/<!! (fdb/monitor-tx-async conn ledger (:id tx-report) (* 30 1000)))))))
    ;; TODO: we should query {:select ["*"] :from "_user"} => [{:_id 87960930223081, "_user/username" "dan"}]
    (testing "successful query"
      (is (= [{:_id 50,
               "_predicate/name" "_user/username",
               "_predicate/doc"
               "Unique account ID (string). Emails are nice for business apps.",
               "_predicate/type" "string",
               "_predicate/unique" true}]
             @(fdb/query (fdb/db conn ledger)
                         {:select {"?p" ["*"]}
                          :where [["?p" "_predicate/name" "_user/username"]]}))))


    (test-helpers/stop-server ledger-server)
    (fdb/close conn)))
