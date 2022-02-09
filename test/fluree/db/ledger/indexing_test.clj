(ns fluree.db.ledger.indexing-test
  (:require [clojure.test :refer :all]
            [fluree.db.flake :as flake]
            [fluree.db.index :as index]
            [fluree.db.storage.core :as storage]
            [fluree.db.ledger.indexing :as indexing]))

(defn inc-tx
  [tx]
  (dec tx))

(defn tx-range
  "Special range function for transaction values because transaction values are
  negative so new transactions are smaller than older ones"
  [oldest newest]
  (reverse (range newest oldest)))

(deftest integrate-novelty-test
  (testing "integrate-novelty with existing index tree"
    (let [network       "network"
          lgr-id        "ledger"

          ;; The specific index and comparator shouldn't matter. We just have to
          ;; be consistent with the choice.
          idx           :spot
          cmp           (get index/default-comparators idx)
          first-tx      -5
          last-tx       -10
          old-txns      (tx-range first-tx last-tx)

          first-subj    10
          last-subj     15
          subjs         (range first-subj last-subj)

          first-pred    100
          last-pred     105
          old-preds     (range first-pred last-pred)

          first-obj     1000
          last-obj      1005
          old-objs      (range first-obj last-obj)

          old-flakes    (map flake/->Flake
                             subjs old-preds old-objs
                             old-txns (repeat true) (repeat {}))

          old-leaf-id   (storage/random-leaf-id network lgr-id idx)
          old-leaf      (-> (index/new-leaf network lgr-id cmp old-flakes)
                            (assoc :id old-leaf-id))

          old-branch-id (storage/random-branch-id network lgr-id idx)
          old-branch    (-> (index/new-branch network lgr-id cmp [old-leaf])
                            (assoc :id old-branch-id))

          old-index     [old-leaf old-branch]]

      (testing "with empty novelty"
        (let [new-tx             (inc-tx last-tx)
              novelty            (flake/sorted-set-by cmp)
              index-xf           (indexing/integrate-novelty idx new-tx novelty #{})
              subject-under-test (into [] index-xf old-index)]
          (is (= subject-under-test old-index)
              "doesn't change the index")))

      (testing "adding lower sorted flakes"
        (let [new-tx             (inc-tx last-tx)
              lower-subj         (inc last-subj)

              novelty-flakes     (map flake/->Flake
                                      (repeat lower-subj) old-preds old-objs
                                      (repeat new-tx) (repeat true) (repeat {}))
              novelty            (apply flake/sorted-set-by cmp novelty-flakes)
              lowest-novelty     (first novelty)

              index-xf           (indexing/integrate-novelty idx new-tx novelty #{})
              subject-under-test (into [] index-xf old-index)]
          (is (->> subject-under-test
                   (filter :leftmost?)
                   (map :first)
                   (every? #{lowest-novelty}))
              "sets the first novelty item as first flake for every leftmost node"))))))
