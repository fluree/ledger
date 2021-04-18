(ns fluree.db.ledger.ledger-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [fluree.db.ledger.test-helpers :as test]

            [fluree.db.ledger.api.downloaded :as api]

            [fluree.db.ledger.docs.getting-started.basic-schema :as basic-schema]

            [fluree.db.ledger.docs.query.basic-query :as basic-query]
            [fluree.db.ledger.docs.query.block-query :as block-query]
            [fluree.db.ledger.docs.query.history-query :as history-query]
            [fluree.db.ledger.docs.query.advanced-query :as advanced-query]
            [fluree.db.ledger.docs.query.analytical-query :as analytical-query]
            [fluree.db.ledger.docs.query.sparql :as sparql]
            [fluree.db.ledger.docs.query.graphql :as graphql]

            [fluree.db.ledger.docs.smart-functions.intro :as intro]
            [fluree.db.ledger.docs.smart-functions.predicate-spec :as predicate-spec]
            [fluree.db.ledger.docs.smart-functions.collection-spec :as collection-spec]
            [fluree.db.ledger.docs.smart-functions.rule-example :as rule-example]
            [fluree.db.ledger.docs.smart-functions.in-transactions :as in-transactions]

            [fluree.db.ledger.docs.identity.auth :as auth]
            [fluree.db.ledger.docs.identity.signatures :as signatures]

            [fluree.db.ledger.docs.schema.collections :as collections]
            [fluree.db.ledger.docs.schema.predicates :as predicates]

            [fluree.db.ledger.docs.examples.cryptocurrency :as cryptocurrency]
            [fluree.db.ledger.docs.examples.supply-chain :as supply-chain]
            [fluree.db.ledger.docs.examples.voting :as voting]
            [fluree.db.ledger.general.todo-permissions :as todo-perm]))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (log/error ex "Uncaught exception on" (.getName thread)))))

;; TODO - tests fail - commented out for convenience:
;; API - (test-gen-flakes-query-transact-with)
        ; look at query-with, test-transact-with
;; BLOCK - range should be exclusive to be compatible
        ;(query-block-range)
        ;(query-block-range-lower-limit)
        ;(query-block-range-pretty-print)
;; Advanced Query - multi-query should not throw error, check on recur
        ;(crawl-graph-two-with-recur)
        ;(multi-query-with-error)
;; Analytical Queries
        ; analytical-select-one-with-aggregate-sum - don't know why failing
        ; (analytical-select-one-with-aggregate-sample) - sample 10 is returning 9, why?
        ; (analytical-across-sources-wikidata) - not failing, but test not done
;; SPARQL
        ; sparql-multi-clause-with-semicolon - don't know why failing. Look into

;; AUTH
        ; Have a not that create-authority sometimes fails. Run many times to check if it was fixed.

(deftest all-open-tests
  (is (= :success
         (test/test-system
           (fn []
             ;; 1- API
             (test/print-banner "API Tests")
             (api/api-test)

             ;; 2- Docs
             (test/print-banner "Docs Tests")
             (basic-schema/basic-schema-test)

             ;; 3- Query
             (test/print-banner "Query Tests")
             (basic-query/basic-query-test)
             (block-query/block-query-test)
             (history-query/history-query-test)
             (advanced-query/advanced-query-test)
             (analytical-query/analytical-query-test)
             (sparql/sparql-test)
             (graphql/graphql-test)

             ;; 4- Transact

             ;; 5- Identity
             (test/print-banner "Identity Tests")
             (auth/auth-test)

             ;; 6- Smart Functions
             (test/print-banner "Smart Function Tests")
             (intro/intro-test)
             (predicate-spec/predicate-spec-test)
             (collection-spec/collection-spec-test)
             (rule-example/rule-example-test)
             (in-transactions/in-transactions-test)

             ;; 7- Schema
             (test/print-banner "Schema Tests")
             (collections/collections-test)
             (predicates/predicates-test)

             ;; 8- Examples
             (test/print-banner "Example Tests")
             (cryptocurrency/cryptocurrency-test)
             (supply-chain/supply-chain-test)
             (voting/voting-test)

             ;; 9- General
             (test/print-banner "General Tests")
             (todo-perm/todo-auth-tests))))))

(comment

  ;; Can call the below tests like function - each needs to be run separately

  ;; (run-tests) runs all tests in this namespace - has the best logging,
  ;; returns a summary and logs out "FAIL in ...." for each failure.
  (run-tests)

  ;; this needs to be run separately- closed api test harness. Best to follow
  ;; this to the page, and run the test, otherwise the results are not visible
  ;; for some reason?
  (signatures/Signatures-export)

  ;; This one can
  (test/test-system (fn [] (api/standalone-test-gen-flakes-query-transact-with))))
