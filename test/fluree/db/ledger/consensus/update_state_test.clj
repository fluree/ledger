(ns fluree.db.ledger.consensus.update-state-test
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.consensus.update-state :as update-state]))

(deftest pool-test
  (testing "Capped state pools"
    (let [state     {}
          pool-path [:foo :bar]]
      (testing "with added data"
        (let [k         :baz
              k'        :nope
              v         :bip
              new-state (update-state/put-pool state 10 pool-path k v)]
          (is (update-state/in-pool? new-state pool-path k v)
              "contains the added data")
          (is (not (update-state/in-pool? new-state pool-path k' v))
                  "does not contain additional data"))))))
