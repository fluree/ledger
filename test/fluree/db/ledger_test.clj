(ns fluree.db.ledger-test
  "The tests run by this namespace should be converted to idiomatic Clojure
  tests and removed from `all-open-tests` below. Once they are all converted,
  this file should be deleted."
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]

            [fluree.db.ledger.api.open-test :as api]

            [fluree.db.ledger.docs.getting-started.basic-schema :as basic-schema]

            [fluree.db.ledger.docs.query.block-query :as block-query]
            [fluree.db.ledger.docs.query.history-query :as history-query]
            [fluree.db.ledger.docs.query.advanced-query :as advanced-query]
            [fluree.db.ledger.docs.query.sparql :as sparql]
            [fluree.db.ledger.docs.query.graphql :as graphql]
            [fluree.db.ledger.docs.query.sql-query :as sql]

            [fluree.db.ledger.docs.transact.transactions :as transactions]

            [fluree.db.ledger.docs.identity.auth :as auth]

            [fluree.db.ledger.docs.schema.collections :as collections]
            [fluree.db.ledger.docs.schema.predicates :as predicates]

            [fluree.db.ledger.docs.examples.cryptocurrency :as cryptocurrency]
            [fluree.db.ledger.docs.examples.supply-chain :as supply-chain]
            [fluree.db.ledger.docs.examples.voting :as voting]
            [fluree.db.ledger.general.todo-permissions :as todo-perm]
            [fluree.db.ledger.general.invoice-tests :as invoice]
            [fluree.db.peer.server-health-tests :as sh-test]
            [fluree.db.peer.http-api-tests :as http-api-test]))

;; NOTE: The tests run by this namespace should be converted to idiomatic
;; Clojure tests and removed from `all-open-tests` below. Once they are all
;; converted, this file should be deleted.

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
         (test/test-system-deprecated
           (fn []
             ;; 1- Docs
             (test/print-banner "Docs Tests")
             (basic-schema/basic-schema-test)

             ;; 2- Query
             (test/print-banner "Query Tests")
             (block-query/block-query-test)
             (history-query/history-query-test)
             (advanced-query/advanced-query-test)
             (sparql/sparql-test)
             (graphql/graphql-test)
             (sql/query-tests)

             ;; 3- Transactions
             (test/print-banner "Transaction Tests")
             (transactions/transaction-basics)

             ;; 4- Identity
             (test/print-banner "Identity Tests")
             (auth/auth-test)

             ;; 5- Schema
             (test/print-banner "Schema Tests")
             (collections/collections-test)
             (predicates/predicates-test)

             ;; 6- Examples
             (test/print-banner "Example Tests")
             (cryptocurrency/cryptocurrency-test)
             (supply-chain/supply-chain-test)
             (voting/voting-test)

             ;; 7- General
             (test/print-banner "General Tests")
             (todo-perm/todo-auth-tests)
             (invoice/invoice-tests)


             ;; 8-Password Auth
             (test/print-banner "Password Authentication Tests")
             (http-api-test/http-api-tests)


             ;; 9- Server health
             (test/print-banner "Server Health")
             (sh-test/server-health-tests))))))

(comment

  ;; Can call the below tests like function - each needs to be run separately

  ;; (run-tests) runs all tests in this namespace - has the best logging,
  ;; returns a summary and logs out "FAIL in ...." for each failure.
  (run-tests)

  (test/test-system-deprecated (fn [] (api/standalone-test-gen-flakes-query-transact-with))))
