(ns fluree.db.ledger.docs.getting-started.basic-schema
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]))

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
  (testing "Add the person, chat, comment, artist, and movie collections"
    (let [collection-txn  [{:_id "_collection"
                            :name "person"}
                           {:_id "_collection"
                            :name "chat"}
                           {:_id "_collection"
                            :name "comment"}
                           {:_id "_collection"
                            :name "artist"}
                           {:_id "_collection"
                            :name "movie"}]
          collection-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-chat collection-txn))]
      (println collection-resp)
      ;; status should be 200
      (is (= 200 (:status collection-resp)))

      ;; block should be 2
      (is (= 2 (:block collection-resp)))

      ;; there should be 5 tempids
      (is (= 5 (count (:tempids collection-resp)))))))

;; Add predicates

(deftest add-predicates
  (testing "Add predicates for the chat app")
  (let [filename  "../test/fluree/db/ledger/Resources/ChatApp/chatPreds.edn"
        predicate-txn   (edn/read-string (slurp (io/resource filename)))
        collection-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-chat predicate-txn))]

    ;; status should be 200
    (is (= 200 (:status collection-resp)))

    ;; block should be 3
    (is (= 3 (:block collection-resp)))

    ;; there should be 16 tempids
    (is (= 17 (count (:tempids collection-resp))))))

;; Add sample data

(deftest add-sample-data
  (testing "Add sample data for the chat app")
  (let [filename  "../test/fluree/db/ledger/Resources/ChatApp/chatAppData.edn"
        predicate-txn   (edn/read-string (slurp (io/resource filename)))
        collection-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-chat predicate-txn))]

    ;; status should be 200
    (is (= 200 (:status collection-resp)))

    ;; block should be 4
    (is (= 4 (:block collection-resp)))

    ;; there should be 17 tempids
    (is (= 17 (count (:tempids collection-resp))))))


(deftest graphql-txn
  (testing "Add to Person collection by way of GraphQL mutation syntax")
  (let [graphql-txn       {:query "mutation addPeople ($myPeopleTx: JSON) { transact(tx: $myPeopleTx)}"
                           :variables {:myPeopleTx "[{ \"_id\": \"person\", \"handle\": \"aSmith\", \"fullName\": \"Alice Smith\" }, { \"_id\": \"person\", \"handle\": \"aVargas\", \"fullName\": \"Alex Vargas\" }]"}}
        collection-resp   (async/<!! (fdb/graphql-async (get-conn) test/ledger-chat graphql-txn))]

    (is (= 200 (:status collection-resp)))
    (is (= 5 (:block collection-resp)))
    (is (= 2 (count (:tempids collection-resp))))))


(deftest basic-schema-test
  (testing "Setting up the schema and sample data in the Getting Started section")
  (add-collections*)
  (add-predicates)
  (add-sample-data)
  (graphql-txn))
