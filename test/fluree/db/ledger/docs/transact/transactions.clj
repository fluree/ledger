(ns fluree.db.ledger.docs.transact.transactions
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.ledger.main-test :as test]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]
            [fluree.db.util.log :as log]))

(use-fixtures :once test/test-system)

(defn get-db
  ([ledger-name]
   (get-db ledger-name {}))
  ([ledger-name opts]
   (fdb/db (:conn test/system) ledger-name opts)))

(defn get-conn
  []
  (:conn test/system))

(defn issue-consecutive-transactions
  ([ledger txns]
   (issue-consecutive-transactions ledger txns {}))
  ([ledger txns opts]
   (let [conn (get-conn)]
     (async/go-loop [results []
                     [txn & r] txns]
       (let [resp    (async/<!! (fdb/transact-async conn ledger txn opts))
             results (conj results resp)]
         (if r
           (recur results r)
           results))))))

;; Add collections

(deftest add-collections*
  (testing "Add the person collection")
  (let [collection-txn  [{:_id "_collection"
                          :name "person"}]
        collection-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact collection-txn))]

    (is (= 200 (:status collection-resp)))
    (is (= 2 (:block collection-resp)))
    (is (= 1 (count (:tempids collection-resp))))))

;; Add predicates

(deftest add-predicates
  (testing "Add predicates for the chat app")
  (let [predicate-txn   [{:_id "_predicate"
                          :name "person/handle"
                          :type "string"
                          :unique true}
                         {:_id "_predicate"
                          :name "person/fullName"
                          :type "string"}
                         {:_id "_predicate"
                          :name "person/user"
                          :type "ref"
                          :restrictCollection "_user"}
                         {:_id "_predicate"
                          :name "person/age"
                          :type "int"}]
        collection-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact predicate-txn))]

    (is (= 200 (:status collection-resp)))
    (is (= 3 (:block collection-resp)))
    (is (= 4 (count (:tempids collection-resp))))))

(deftest transact-with-temp-ids
  (testing "Issue basic transactions with temporary ids")
  (let [predicate-txn   [{:_id "_user$lEliasz", :username "lEliasz", :auth ["_auth$temp", "_auth$other"]}
                         {:_id "person",
                          :handle "lEliasz",
                          :fullName "Louis Eliasz",
                          :user "_user$lEliasz"}
                         {:_id "_auth$temp", :id "tempAuthRecord"}
                         {:_id "_auth$other", :id "otherAuthRecord"}]
        collection-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact predicate-txn))]

    (is (= 200 (:status collection-resp)))
    (is (-> collection-resp :tempids (get "_user$lEliasz")))))

(deftest nested-transaction
  (testing "Issue a transaction with nested data rather than tempID reference")
  (let [predicate-txn [{:_id "person",
                        :handle "ajohnson",
                        :fullName "Andrew Johnson"
                        :user
                        {:_id "_user", :username "ajohnson"}}]


        collection-resp (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact predicate-txn))]

   (is (= 200 (:status collection-resp)))
   (is (and (-> collection-resp :tempids (get "person$1")) (-> collection-resp :tempids (get "_user$1"))))))


(deftest upserting-data
  (testing "Issue a transaction to upsert data instead of creating a new entity")
  (let [failure-txn   [{:_id "person", :handle "ajohnson", :age 26}]
        failure-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact failure-txn))
        _a            (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact [{:_id ["_predicate/name" "person/handle"], :upsert true}]))
        _             (Thread/sleep 2000)
        success-txn   [{:_id "person", :handle "ajohnson", :age 26}]
        success-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact success-txn))]

    (is (and (= 200 (:status success-resp)) (not (= 200 (:status failure-resp)))))))

(deftest deleting-data
  (testing "Issue a transaction to delete parts of data and to delete an entire subject")
  (let [delete-txn        [{:_id ["person/handle" "ajohnson"] :_action "delete"}
                           {:_id ["person/handle" "lEliasz"] :fullName nil}
                           {:_id ["_user/username" "lEliasz"] :auth [["_auth/id" "tempAuthRecord"]] :_action "delete"}]
        delete-resp       (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact delete-txn))
        _                 (Thread/sleep 2000)
        test-query        {:select ["*", {"person/user" ["*", {"_user/auth" ["*"]}]}] :from "person"}
        test-resp         (async/<!! (fdb/query-async (basic/get-db test/ledger-query+transact) test-query))
        remaining-person  (some #(when (get % "person/handle") %) test-resp)]

    (is (= 200 (:status delete-resp)))
    (is (not (some #(= "ajohnson" (get % "person/handle")) test-resp)))
    (is (some #(= "lEliasz" (get % "person/handle")) test-resp))
    (is (not (get remaining-person "person/fullName")))
    (is (= 1 (-> remaining-person (get "person/user") (get "_user/auth") count)))))


(deftest transaction-basics
  (testing "Testing suite for the Transaction section of the docs")
  (add-collections*)
  (add-predicates)
  (transact-with-temp-ids)
  (nested-transaction)
  (upserting-data)
  (deleting-data))


