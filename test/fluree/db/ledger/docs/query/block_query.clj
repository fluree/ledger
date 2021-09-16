(ns fluree.db.ledger.docs.query.block-query
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.string :as str]))

(use-fixtures :once test/test-system)

(deftest query-single-block
  (testing "Select single block by block number")
  (let [query {:block 3}
        res   (-> (async/<!! (fdb/block-query-async (basic/get-conn) test/ledger-chat query)) first)]

    (is (= (-> res keys set) #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs :flakes}))

    (is (= 84 (count (:flakes res))))))

(deftest query-single-block-with-ISO-string
  (testing "Select single block with ISO-8601 wall clock time")
  (let [query {:block "2017-11-14T20:59:36.097Z"}
        res   (try (async/<!! (fdb/block-query-async (basic/get-conn) test/ledger-chat query))
                   (catch Exception e e))]
    (is (= "There is no data as of 1510693176097" (ex-message res)))))



(deftest query-single-block-with-duration
  (testing "Select single block with duration string")
  (let [query {:block "PT1H"}
        res   (try (async/<!! (fdb/block-query-async (basic/get-conn) test/ledger-chat query))
                   (catch Exception e e))]
    (is (str/includes? (ex-message res) "There is no data as of "))))


;; TODO - looks like block range now inclusive? Fix.
(deftest query-block-range
  (testing "Select ranges of blocks")
  (let [query {:block [3 5]}
        res    (async/<!! (fdb/block-query-async (basic/get-conn) test/ledger-chat query))]
    (is (= 3 (count res)))

    (is (= (-> res first keys set) #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs :flakes}))

    (is (= (-> res second keys set) #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs :flakes}))

    (is (nil? (nth res 2)))))


(deftest query-block-range-lower-limit
  (testing "Select ranges of blocks")
  (let [query {:block [3]}
        res   (async/<!! (fdb/block-query-async (basic/get-conn) test/ledger-chat query))]

    (is (= 2 (count res)))

    (is (= (-> res first keys set) #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs :flakes}))

    (is (= (-> res second keys set) #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs :flakes}))))


(deftest query-block-range-pretty-print
  (testing "Pretty prints a block range")
  (let [query {:block [3]
               :prettyPrint true}
        res    (async/<!! (fdb/block-query-async (basic/get-conn) test/ledger-chat query))
        flakeKeys3 (-> res second :flakes keys set)]

    (is (= 2 (count res)))

    (is (= (-> res first keys set) #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs :flakes}))

    (is (= (-> res second keys set) #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs :flakes}))

    (is (= flakeKeys3 #{:asserted :retracted}))))

(deftest block-query-test
  (query-single-block)
  (query-single-block-with-ISO-string)
  (query-single-block-with-duration))
  ;(query-block-range)
  ;(query-block-range-lower-limit)
  ;(query-block-range-pretty-print)


(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (block-query-test))
