(ns fluree.db.ledger.docs.schema.collections
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.string :as str]))

(use-fixtures :once test/test-system)

(deftest add-collection-long-desc
  (testing "Add long description to collections.")
  (let [long-desc-txn [{:_id "_predicate", :name "_collection/longDescription", :type "string"}]
        res  (async/<!! (fdb/transact-async (basic/get-conn)
                                            test/ledger-chat
                                            long-desc-txn
                                            {:timeout 120000}))

        add-long-desc-txn [{:_id ["_collection/name" "person"],
                            :longDescription "I have a lot to say about this collection, so this is a longer description about the person collection"}
                           {:_id "_collection",
                            :name "animal",
                            :longDescription "I have a lot to say about this collection, so this is a longer description about the animal collection"}]

        add-long-desc-res (async/<!! (fdb/transact-async (basic/get-conn)
                                                         test/ledger-chat
                                                         add-long-desc-txn
                                                         {:timeout 120000}))]

    (is (= 200 (:status res)))
    (is (= 200 (:status add-long-desc-res)))

    (is (= 1 (-> res :tempids count)))
    (is (= 1 (-> add-long-desc-res :tempids count)))

    (is (= 9 (-> res :flakes count)))
    (is (= 10 (-> add-long-desc-res :flakes count)))))


(deftest query-collection-name-predicate
  (testing "Query the _predicate/name predicate")
  (let [query-collection-name {:select ["*"]
                               :from  ["_predicate/name" "_collection/name"]}
        db  (basic/get-db test/ledger-chat)
        res (-> (async/<!! (fdb/query-async db query-collection-name))
                first)]

    (is (= "_collection/name" (get res "_predicate/name")))))


(deftest collection-upsert
  (testing "Attempt to upsert _collection/name, then set upsert")
  (let [txn [{:_id "_collection", :name "_user", :doc "The user's collection"}]
        res (-> (async/<!! (fdb/transact-async (basic/get-conn)
                                               test/ledger-chat
                                               txn
                                               {:timeout 120000}))
                test/safe-Throwable->map
                :cause)
        set-upsert [{:_id ["_predicate/name" "_collection/name"], :upsert true}]
        upsertRes  (async/<!! (fdb/transact-async (basic/get-conn)
                                                  test/ledger-chat
                                                  set-upsert
                                                  {:timeout 120000}))
        attemptToUpsertRes  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat txn))]

    (is (str/includes? res "Predicate _collection/name does not allow upsert"))
    (is (str/includes? res "duplicates an existing _collection/name (_user)"))

    (is (= 200 (:status upsertRes)))

    (is (= 200 (:status attemptToUpsertRes)))

    (is (= 9 (-> attemptToUpsertRes :flakes count)))))


(deftest collections-test
  (add-collection-long-desc)
  (query-collection-name-predicate)
  (collection-upsert))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (collections-test))
