(ns fluree.db.ledger.docs.query.graphql
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]))

(use-fixtures :once test/test-system-deprecated)

(deftest basic-graphql
  (testing "Basic GraphQL query of chat collection")
  (let [graphql-query      {:query "{ graph {\n  chat {\n    _id\n    message\n  }\n}\n}"}
        res (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query))]
    (is (coll? (:chat res)))
    (is (every? #(:_id %) (:chat res)))))

(deftest graphql-crawl-query
  (testing "GraphQL query that crawls from chat to referenced person")
  (let [graphql-query      {:query "{ graph {\n  chat {\n    _id\n    message\n    person {\n        _id\n        handle\n    }\n  }\n}\n}"}
        res (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query))]
    (is (-> res :chat first (get-in ["person" "handle"]) string?))
    (is (-> res :chat first (get-in ["person" :_id]) number?))))

(deftest graphql-wildcard-query
  (testing "GraphQL query that implements a non-GraphQL-supported wildcard operator")
  (let [graphql-query      {:query "{ graph {\n  person {\n    *\n  }\n}\n}"}
        res (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query))]

    (is (every? #(:_id %) (:person res)))))

(deftest graphql-reverse-crawl-query
  (testing "GraphQL query that reverse-crawls from person back to chat")
  (let [graphql-query      {:query "{ graph {\n  person {\n    chat_Via_person {\n      _id\n      instant\n      message\n    }\n  }\n}\n}"}
        res (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query))]

    (is (-> res :person first (get "chat/_person") vector?))))

(deftest graphql-query-single-block
  (testing "GraphQL query that specifies single block")
  (let [graphql-query      {:query "query  {\n  block(from: 3, to: 3)\n}"}
        res (first (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query)))]

    (is (= (:block res) 3))
    (is (every? #(= (type %) fluree.db.flake.Flake) (-> res :flakes)))))

(deftest graphql-query-range-blocks
  (testing "GraphQL query that specifies a range of blocks")
  (let [graphql-query      {:query "query  {\n  block(from: 3, to: 5)\n}"}
        res (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query))
        blocks (filter #(not (nil? %)) res)]

    (is (every? #(get % :block) blocks))))

(deftest graphql-query-blocks-lower-limit
  (testing "GraphQL query that specifies a range of blocks from lower limit")
  (let [graphql-query      {:query "query  {\n  block(from: 3)\n}"}
        res (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query))]

    (is (every? #(get % :block) res))))

(deftest graphql-test
  (basic-graphql)
  (graphql-crawl-query)
  (graphql-wildcard-query)
  (graphql-reverse-crawl-query)
  (graphql-query-single-block)
  (graphql-query-range-blocks)
  (graphql-query-blocks-lower-limit))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (graphql-test))

