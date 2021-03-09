(ns fluree.db.ledger.transact.core-test
  (:require [clojure.test :refer :all]))

;; test these things:
;; - if unique predicate resolves to a different existing subject, allow transaction to happen only if a different part of tx retracts existing flake
;; - tempids - add correctly, multiple identical new create just one flake
;; - incoming transaction cannot have a negative value? - if we allow, need to validate for prior tx, and doesn't overwrite hash, etc.
;; dependent transactions look like it may not work correct - revisit
;; check if a new tag is used and we (a) properly auto-generate tag if needed and (b) don't auto-generate it if the transaction has the new tag as a transaction item
;; check transaction expirations work
;; check duplicate transactions (txid) won't work
;; test two txi for same subject with single-cardinality value doesn't get added twice (either resolved via unique :true, or just using same pred-ident for _id)
;; test error and execution stops when fuel runs out

(deftest sql-query-parser-test
  (testing "IRI support"
    )
  (testing "Transaction children properly extracted"
    (let [nested-tx [{:_id "_collection"
                      :name "_collection/test"}
                     {:_id 12345
                      :sub [{:_id "mysub1"
                             :name "blah"}
                            {:_id "mysub2"
                             :name "blah2"}
                            {:_id "mysub3"
                             :name "blah3"
                             :nested {:_id "subsub"
                                      :name "blah4"}}]}
                     :_id ["test/me" 123]
                     :asub {:_id "hi"
                            :name "hithere"}]]
      )
    ))