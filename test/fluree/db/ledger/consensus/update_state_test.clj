(ns fluree.db.ledger.consensus.update-state-test
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.consensus.update-state :as update-state]))

(deftest pool-test
  (testing "Capped state pools"
    (let [state     {}
          pool-path [:foo :bar]]
      (testing "when empty"
        (let [k :baz
              v :bip]
          (is (not (update-state/in-pool? state pool-path k v))
              "does not contain any data")))
      (testing "after adding data"
        (let [k         :baz
              v         :bip
              new-state (update-state/put-pool state 10 pool-path k v)]
          (is (update-state/in-pool? new-state pool-path k v)
              "contains that data")
          (testing "when at capacity"
            (let [limit 1
                  k'          :bop
                  v'          :bap
                  newer-state (update-state/put-pool new-state limit pool-path k' v')]
              (is (not (update-state/in-pool? newer-state pool-path k' v'))
                  "does not add more data"))))))))
