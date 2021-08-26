(ns fluree.db.ledger.docs.smart-functions.collection-spec
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [clojure.core.async :as async]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]))

(use-fixtures :once test/test-system)

(deftest full-name-req-test
  (let [non-neg-spec  [{:_id ["_collection/name" "person"], :spec ["_fn$fullNameReq"], :specDoc "A person is required to have a fullName."}
                       {:_id "_fn$fullNameReq",
                        :name "fullNameReq",
                        :code "(boolean (get (query (str \"{\\\"select\\\": [\\\"*\\\"], \\\"from\\\": \" (?sid) \"}\")) \"person/fullName\"))"}]
        add-spec-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat non-neg-spec))
        test-spec     [{:_id "person", :handle "noFullName"}]
        test-resp     (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat test-spec))
                          test/safe-Throwable->map :cause)]

   (is (= "Collection spec failed for: person. A person is required to have a fullName."
          test-resp))))

(deftest full-name-req-test-retractions
  (let [add-person  [{:_id "person", :handle "deleteMe", :fullName "To Be Deleted"}]
        _ (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat add-person))
        test-spec-1 [{:_id ["person/handle" "deleteMe" ] :fullName nil}]
        test-resp-1 (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat test-spec-1))
                        test/safe-Throwable->map :cause)
        test-spec-2 [{:_id ["person/handle" "deleteMe" ] :_action "delete"}]
        test-resp-2 (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat test-spec-2))]

    (is (= "Collection spec failed for: person. A person is required to have a fullName."
           test-resp-1))
    (is (map? test-resp-2))
    (is (= 200 (:status test-resp-2)))
    (is (= 8 (-> test-resp-2 :flakes count)))))

(deftest collection-spec-test
  (full-name-req-test)
  (full-name-req-test-retractions))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (full-name-req-test)
  (full-name-req-test-retractions))


