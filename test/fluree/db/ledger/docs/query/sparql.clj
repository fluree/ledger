(ns fluree.db.ledger.docs.query.sparql
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]))

(use-fixtures :once test/test-system)

(deftest basic-sparql
  (testing "SPARQL query with two-triple WHERE clause")
  (let [sparql-query      "SELECT ?person ?fullName \nWHERE {\n  ?person fd:person/handle \"jdoe\".\n  ?person fd:person/fullName ?fullName.\n}"
        db  (basic/get-db test/ledger-chat)
        res  (first (async/<!! (fdb/sparql-async db sparql-query)))]

    (is (= (first res) 351843720888321))
    (is (= (last res) "Jane Doe"))))

(deftest sparql-max-function-in-select
  (testing "SPARQL query MAX function as selector")
  (let [sparql-query      "SELECT ?fullName (MAX(?favNums) AS ?max)\nWHERE {\n  ?person fd:person/favNums ?favNums.\n  ?person fd:person/fullName ?fullName\n}\n"
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/sparql-async db sparql-query))]

    (is (every? #(and (string? (first %)) (number? (last %))) res))))


(deftest sparql-multi-clause-with-semicolon
  (testing "SPARQL query with where clauses separated by semicolon")
  (let [sparql-query      "SELECT ?person ?fullName ?favNums\nWHERE {\n  ?person fd:person/handle \"jdoe\";\n          fd:person/fullName ?fullName;\n          fd:person/favNums  ?favNums.\n}"
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/sparql-async db sparql-query))]

    (is (every? #(= (count %) 3) res))
    (is (every? #(and (= 351843720888321 (first %)) (= "Jane Doe" (get % 1)) (number? (last %))) res))))


(deftest sparql-clause-with-comma
  (testing "SPARQL query with same subject/predicate, using commas to separate different objects")
  (let [sparql-query      "SELECT ?person\nWHERE {\n  ?person fd:person/handle \"jdoe\", \"zsmith\".\n}\n"
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/sparql-async db sparql-query))]

    ;This test is very stupid because we don't have a great example for a ?person with two same-subject-predicate objects
    (is (empty? res))))


(deftest sparql-test
  (basic-sparql)
  (sparql-max-function-in-select)
  ;(sparql-multi-clause-with-semicolon)
  (sparql-clause-with-comma))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (sparql-test))
