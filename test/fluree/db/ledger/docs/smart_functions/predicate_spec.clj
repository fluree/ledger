(ns fluree.db.ledger.docs.smart-functions.predicate-spec
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [clojure.core.async :as async]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]))

(use-fixtures :once test/test-system)

(deftest non-negative
  (let [non-neg-spec  [{:_id "_fn$nonNeg",
                        :name "nonNegative?",
                        :doc "Checks that a value is non-negative",
                        :code "(<= 0 (?o))"}
                       {:_id ["_predicate/name" "person/favNums"],
                        :spec ["_fn$nonNeg"]}]
        add-spec-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat non-neg-spec))
        test-spec     [{:_id "person", :handle "aJay", :favNums [12 -4 57]}]
        test-resp     (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat test-spec))
                          test/safe-Throwable->map :cause)]

   (is (= "Predicate spec failed for predicate: person/favNums." test-resp))))

(deftest predicate-spec-test
  (non-negative))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (predicate-spec-test))







