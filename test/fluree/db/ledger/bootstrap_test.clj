(ns fluree.db.ledger.bootstrap-test
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.bootstrap :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.flake :as flake]))

(use-fixtures :once test/test-system)

(deftest bootstrap-memory-db-test
  (testing "all block 1 metadata flakes are present"
    (let [conn           (:conn test/system)
          ledger         (test/rand-ledger "test/bootstrap")
          pred->id       (predicate->id-map bootstrap-txn)
          ;; expected-preds is a set of tuples of predicate-id and the number
          ;; of flakes w/ that pred that should appear
          expected-preds (->> #{["_tx/id" 1] ["_tx/nonce" 1] ["_block/number" 1]
                                ["_block/instant" 1] ["_block/transactions" 2]}
                              (reduce (fn [acc [pred card]]
                                        (assoc acc (pred->id pred) card))
                                      {})
                              set)
          actual-preds   (->> (bootstrap-memory-db conn ledger)
                              :flakes
                              (reduce (fn [acc flake]
                                        (update acc (flake/p flake)
                                                (fnil inc 0)))
                                      {})
                              set)]
      (is (every? actual-preds expected-preds)))))
