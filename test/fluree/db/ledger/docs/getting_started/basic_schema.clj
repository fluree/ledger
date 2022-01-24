(ns fluree.db.ledger.docs.getting-started.basic-schema
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async :refer [<!!]]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]))

(use-fixtures :once test/test-system-deprecated)

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
    (let [collection-txn  [{:_id  "_collection"
                            :name "person"}
                           {:_id  "_collection"
                            :name "chat"}
                           {:_id  "_collection"
                            :name "comment"}
                           {:_id  "_collection"
                            :name "artist"}
                           {:_id  "_collection"
                            :name "movie"}]
          collection-resp (async/<!! (fdb/transact-async (get-conn) test/ledger-chat collection-txn))]

      ;; status should be 200
      (is (= 200 (:status collection-resp)))

      ;; block should be 2
      (is (= 2 (:block collection-resp)))

      ;; tempids should only have _collection key with two-tuple integer range
      (is (= #{"_collection"} (-> collection-resp :tempids keys set)))
      (is (= 2 (-> collection-resp :tempids (get "_collection") count)))
      (is (->> collection-resp :tempids (#(get % "_collection")) (every? int?))))))

;; Add predicates

(deftest add-predicates
  (testing "Add predicates for the chat app"
    (let [filename      "schemas/chat-preds.edn"
          predicate-txn (-> filename io/resource slurp edn/read-string)
          resp          (->> predicate-txn
                             (fdb/transact-async (get-conn) test/ledger-chat)
                             <!!)]

      ;; status should be 200
      (is (= 200 (:status resp)))

      ;; block should be 3
      (is (= 3 (:block resp)))

      ;; there should be 16 _predicate tempids
      (is (= 17 (-> resp :tempids (test/get-tempid-count "_predicate")))))))

;; Add sample data

(deftest add-sample-data
  (testing "Add sample data for the chat app"
    (let [filename "data/chat.edn"
          data-txn (-> filename io/resource slurp edn/read-string)
          resp     (->> data-txn
                        (fdb/transact-async (get-conn) test/ledger-chat)
                        <!!)]

      ;; status should be 200
      (is (= 200 (:status resp)))

      ;; block should be 4
      (is (= 4 (:block resp)))

      ;; there should be 17 tempids
      (is (= 15 (count (:tempids resp))))

      ;; there should be 3 chat tempids that were not unique
      (is (= 3 (-> resp :tempids (test/get-tempid-count "chat")))))))


(deftest graphql-txn
  (testing "Add to Person collection by way of GraphQL mutation syntax"
    (let [graphql-txn     {:query     "mutation addPeople ($myPeopleTx: JSON) { transact(tx: $myPeopleTx)}"
                           :variables {:myPeopleTx "[{ \"_id\": \"person\", \"handle\": \"aSmith\", \"fullName\": \"Alice Smith\" }, { \"_id\": \"person\", \"handle\": \"aVargas\", \"fullName\": \"Alex Vargas\" }]"}}
          collection-resp (async/<!! (fdb/graphql-async (get-conn) test/ledger-chat graphql-txn))]

      (is (= 200 (:status collection-resp)))
      (is (= 5 (:block collection-resp)))
      ;; tempids should only have person key
      (is (= #{"person"} (-> collection-resp :tempids keys set)))
      ;; should be two persons added
      (is (= 2 (-> collection-resp :tempids (test/get-tempid-count "person")))))))


(deftest basic-schema-test
  (testing "Setting up the schema and sample data in the Getting Started section"
    (add-collections*)
    (add-predicates)
    (add-sample-data)
    (graphql-txn)))
