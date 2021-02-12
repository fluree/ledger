(ns fluree.db.ledger.transact.core-test
  (:require [clojure.test :refer :all]))


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