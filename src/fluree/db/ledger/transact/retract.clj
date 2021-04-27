(ns fluree.db.ledger.transact.retract
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.flake :as flake]
            [clojure.core.async :as async]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log])
  (:import (fluree.db.flake Flake)))

;;; functions to retract existing flakes from the ledger

(declare subject)

(defn- component-flake?
  "Returns true if the predicate in the flake is defined as
  :component true, meaning its value points to subject that
  directly a 'component' of this subject and would need to be
  deleted if this flake."
  [db flake]
  (true? (dbproto/-p-prop db :component (.-p flake))))


(defn retract-components
  "Checks flakes to see if any are a component, and if so, finds additional retractions and returns."
  [flakes {:keys [db-root] :as tx-state}]
  (go-try
    (loop [[^Flake flake & r] flakes
           components #{}]
      (if (nil? flake)
        components
        (if (component-flake? db-root flake)
          ;; If component, calls itself again (via 'subject' fn) to continue to recur components until there are none
          (let [c-flakes (<? (subject (.-o flake) tx-state))]
            (recur r (into components c-flakes)))
          (recur r components))))))


(defn subject
  "Returns retraction flakes for an entire subject. Also returns retraction
  flakes for any refs to that subject."
  [subject-id {:keys [db-root t] :as tx-state}]
  (go-try
    (let [flakes     (<? (query-range/index-range db-root :spot = [subject-id]))
          refs       (<? (query-range/index-range db-root :opst = [subject-id]))
          components (<? (retract-components flakes tx-state))]
      (->> flakes
           (concat refs)
           (map #(flake/flip-flake % t))
           (concat components)
           (into [])))))


(defn flake
  "Retracts one or more flakes given a subject, predicate, and optionally an object value."
  [subject-id predicate-id object {:keys [db-root t] :as tx-state}]
  (go-try
    (let [flakes     (<? (query-range/index-range db-root :spot = [subject-id predicate-id object]))
          components (when (dbproto/-p-prop db-root :component predicate-id)
                       (<? (retract-components flakes tx-state)))]
      (->> flakes
           (map #(flake/flip-flake % t))
           (into components)))))

