(ns fluree.db.peer.server-health-tests
  (:require [clojure.test :refer :all]
            [byte-streams :as bs]
            [fluree.db.peer.server-health :as srv-health]
            [fluree.db.test-helpers :as test]
            [fluree.db.util.json :as json]
            [clojure.string :as str]))

(def ^:const instant-now (System/currentTimeMillis))
(def ^:const instant-oldest-txn (- instant-now 5000))
(def ^:const state-leases {:servers {:server-id {:id :DEF :expire (+ instant-now 300000)}}})
(def ^:const state-cmd-queue {"test"   {"txid" {:data    {:cmd "" :sig ""}
                                                :size    400
                                                :txid    "txid"
                                                :network "network"
                                                :dbid    "dbid"
                                                :instant instant-oldest-txn}}
                              "fluree" {"txid" {:data    {:cmd "" :sig ""}
                                                :size    2400
                                                :txid    "txid"
                                                :network "network"
                                                :dbid    "dbid"
                                                :instant instant-now}}})
(def ^:const state-new-db-queue {"test" {"id" {:network "test"
                                               :dbid    "new-ledger"
                                               :command {:cmd "command-json" :sig "sig"}}}})

(deftest server-health-tests
  (testing "remove-deep"
    (let [original {:level-1     {:level-2     {:private-key "abcd" :other "1234"}
                                  :private-key "4567"}
                    :private-key "7890"}
          expected {:level-1 {:level-2 {:other "1234"}}}]
      (is (= expected (srv-health/remove-deep [:private-key] original)))))
  (testing "extract-state-from-leases"
    (let [res (srv-health/extract-state-from-leases state-leases instant-now)]
      (is (vector? res))
      (is (-> res first (test/contains-every? :id :active?)))))
  (testing "parse-command-queue"
    (let [res (srv-health/parse-command-queue state-cmd-queue)]
      (is (vector? res))
      (is (-> res
              first
              (test/contains-every? :txn-count :txn-oldest-instant)))))
  (testing "parse-new-db-queue"
    (let [res (srv-health/parse-new-db-queue state-new-db-queue)]
      (is (vector? res))
      (is (= 1 (count res)))))
  (testing "get-consensus-state"
    (let [state   (-> test/system
                      :group
                      :state-atom
                      deref
                      (assoc :leases state-leases)
                      (assoc :cmd-queue state-cmd-queue)
                      (assoc :new-db-queue state-new-db-queue)
                      atom)
          system* (assoc-in test/system [:group :state-atom] state)
          res     (srv-health/get-consensus-state system*)]
      (is (map? res))
      (is (test/contains-every? res :open-api :raft :svr-state :oldest-pending-txn-instant))
      (is (= instant-oldest-txn (:oldest-pending-txn-instant res)))))
  (testing "get-request-timeout"
    (testing "integer"
      (let [res (srv-health/get-request-timeout {:headers {:request-timeout 5}} nil)]
        (is (= 5 res))))
    (testing "default value"
      (let [res (srv-health/get-request-timeout {:headers {}} 25)]
        (is (= 25 res))))
    (testing "string"
      (let [res (srv-health/get-request-timeout {:headers {:request-timeout "5"}} nil)]
        (is (= 5 res)))))
  (testing "health handler"
    (testing "standard"
      (let [res  (srv-health/health-handler test/system nil)
            body (-> res :body bs/to-string json/parse)]
        (is (= srv-health/http-ok (:status res)))
        (is (map? body))
        (is (test/contains-every? body :ready :status :utilization))))
    (testing "missing group; is transactor?"
      (let [res  (srv-health/health-handler {:config {:transactor? true}} nil)
            body (-> res :body bs/to-string json/parse)]
        (is (= srv-health/http-ok (:status res)))
        (is (= "ledger" (:status body)))))
    (testing "query peer"
      (let [res  (srv-health/health-handler {:config {:transactor? false}} nil)
            body (-> res :body bs/to-string json/parse)]
        (is (= srv-health/http-ok (:status res)))
        (is (= "query" (:status body))))))
  (testing "network state handler"
    (testing "consensus - no queue"
      (let [res  (srv-health/nw-state-handler test/system nil)
            body (-> res :body bs/to-string json/parse)]
        (is (= srv-health/http-ok (:status res)))
        (is (map? body))
        (is (test/contains-every? body :open-api :raft :svr-state :oldest-pending-txn-instant))))
    (testing "consensus - with queue"
      (let [state   (-> test/system
                        :group
                        :state-atom
                        deref
                        (assoc :leases state-leases)
                        (assoc :cmd-queue state-cmd-queue)
                        (assoc :new-db-queue state-new-db-queue)
                        atom)
            system* (assoc-in test/system [:group :state-atom] state)
            res     (srv-health/nw-state-handler system* nil)
            body    (-> res :body bs/to-string json/parse)]
        (is (= srv-health/http-ok (:status res)))
        (is (map? body))
        (is (test/contains-every? body :open-api :raft :svr-state :oldest-pending-txn-instant))
        (is (= instant-oldest-txn (:oldest-pending-txn-instant body)))))
    (testing "timeout"
      (let [group-state (get-in test/system [:group :state-atom])
            slow-state  (promise)
            slow-system (assoc-in test/system [:group :state-atom] slow-state)
            res         (srv-health/nw-state-handler slow-system {:headers {:request-timeout 1}})
            _           (future
                          (Thread/sleep 1000)
                          (deliver slow-state @group-state))
            body        (-> res :body bs/to-string json/parse)]
        (is (= srv-health/http-timeout (:status res)))
        (is (string? body))
        (is (str/starts-with? body "Client Timeout."))))))
