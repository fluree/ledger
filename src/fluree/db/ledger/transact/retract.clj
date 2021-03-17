(ns fluree.db.ledger.transact.retract
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.flake :as flake]
            [clojure.core.async :as async]))

;;; functions to retract existing flakes from the ledger

(defn subject
  "Returns retraction flakes for an entire subject. Also returns retraction
  flakes for any refs to that subject."
  [subject-id {:keys [db-root t] :as tx-state}]
  (go-try
    (let [flakes (query-range/index-range db-root :spot = [subject-id])
          refs   (query-range/index-range db-root :opst = [subject-id])]
      (->> (<? flakes)
           (concat (<? refs))
           (map #(flake/flip-flake % t))
           (into [])))))


(defn flake
  "Retracts one or more flakes given a subject, predicate, and optionally an object value."
  [subject-id predicate-id object {:keys [db-root t] :as tx-state}]
  (go-try
    (let [flakes (if (= ::delete object)
                   (query-range/index-range db-root :spot = [subject-id predicate-id])
                   (query-range/index-range db-root :spot = [subject-id predicate-id object]))]
      (->> (<? flakes)
           (map #(flake/flip-flake % t))))))


;; TODO - below, instead of async/into,could use a transducer to return a single clean channel that concats, and not need to use go-try here
(defn multi
  "Like retract flake, but takes a list of objects that must be retracted"
  [subject-id predicate-id objects tx-state]
  (go-try
    (->> objects
         (map #(flake subject-id predicate-id % tx-state))
         async/merge
         (async/into [])
         <?
         (apply concat))))
