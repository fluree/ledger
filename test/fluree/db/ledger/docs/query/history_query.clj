(ns fluree.db.ledger.docs.query.history-query
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log]
            [fluree.db.util.core :as util]))

(use-fixtures :once test/test-system)

(deftest history-of-subject
  (testing "History of Subject up to block 4")
  (let [history-query {:history 369435906932737
                       :block   4}
        db            (basic/get-db test/ledger-chat)
        res           @(fdb/history-query db history-query)
        _             (when (util/exception? res) (throw res))
        flakes        (-> res first :flakes)]
    (is (= 5 (count flakes)))

    (is (= (-> (map first flakes) set) #{369435906932737}))))

(deftest history-of-two-tuple
  (testing "History of Subject, using two-tuple, up to block 4")
  (let [history-query {:history ["person/handle" "zsmith"]
                       :block   4}
        db            (basic/get-db test/ledger-chat)
        res           @(fdb/history-query db history-query)
        _             (when (util/exception? res) (throw res))
        flakes        (-> res first :flakes)]
    (is (= 12 (count flakes)))

    (is (= (-> (map first flakes) set) #{351843720888321}))))

(deftest history-with-flake-format
  (testing "History Query With Flake Format")
  (let [history-query {:history [["person/handle" "zsmith"] "person/follows"]}
        db            (basic/get-db test/ledger-chat)
        res           @(fdb/history-query db history-query)
        _             (when (util/exception? res) (throw res))
        flakes        (-> res first :flakes)]

    (is (= 1 (count flakes)))

    (is (= #{:block :t :flakes} (-> res first keys set)))

    (is (= (-> flakes first (flake/Flake->parts)) [351843720888321 1003 351843720888320 -7 true nil]))))


(deftest history-with-flake-format-pretty-print
  (testing "History Query With Flake Format")
  (let [history-query {:history      [nil "person/handle" "jdoe"]
                       :prettyPrint true}
        db            (basic/get-db test/ledger-chat)
        res           @(fdb/history-query db history-query)]
    (when (util/exception? res) (throw res))
    (is (= 1 (count res)))

    (is (= #{:block :retracted :asserted :t} (-> res first keys set)))))

(deftest history-query-test
  (history-of-subject)
  (history-of-two-tuple)
  (history-with-flake-format)
  (history-with-flake-format-pretty-print))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (history-query-test))