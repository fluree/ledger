(ns fluree.db.ledger.cleanup
  (:require [clojure.tools.logging :as log]
            [fluree.db.ledger.snapshot :as snapshot]
            [fluree.db.storage.core :as storage]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.session :as session]
            [fluree.db.operations :as ops]
            [clojure.core.async :as async]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto])
  (:import (java.time Instant)))

;; cleanup old index segments, delete DBs, etc.


(defn garbage-tuples
  "Returns a list of two-tuples of [block timestamp] for each previously
  written db that still exists."
  [conn network ledger-id]
  (let [ledger-status (txproto/ledger-status (:group conn) network ledger-id)
        latest-index  (:index ledger-status)]
    (->> (:indexes ledger-status)
         ;; exclude current index
         (filter #(not= latest-index (first %)))
         ;; sort oldest blocks to newest
         (sort-by first)
         (into []))))


(defn remove-garbage
  [conn network ledger-id block]
  (let [garbage     (-> (storage/read-garbage conn network ledger-id block)
                        :garbage)

        garbage-key (storage/ledger-garbage-key network ledger-id block)]
    (doseq [key garbage]
      (storage/storage-write conn key nil))
    (storage/storage-write conn garbage-key nil)
    true))


(defn garbage-collect
  "Runs garbage collection process for all garbage, or optionally up to specified time or block.

  to-time should be epoch milliseconds
  to-block should be the block number

  Either, neither or both can be included.
  Will remove garbage up to, but not including that point in time."
  [conn network ledger-id & {:keys [to-time to-block]}]
  (let [tuples       (garbage-tuples conn network ledger-id) ;; tuples in ascending order based on block/time
        current-root (-> tuples last first)
        filter-fn    (cond
                       (and to-time to-block) #(and (<= (second %) to-time) (<= (first %) to-block))
                       to-time #(<= (second %) to-time)
                       to-block #(<= (first %) to-block)
                       :else (constantly true))
        filtered     (take-while filter-fn tuples)]
    (log/info "Garbage collection start: " ledger-id)
    (doseq [[block _] filtered]
      (remove-garbage conn network ledger-id block)
      ;; remove db-root key, but not the current root key
      (when (not= block current-root)
        (let [ledger-root-key (storage/ledger-root-key network ledger-id block)]
          (log/info "Removing ledger root: " ledger-root-key)
          (storage/storage-write conn ledger-root-key nil))))
    true))


;; database deletion

(defn delete-all-index-children
  "From any branch index, delete all children.
   If children are branches, recursively deletes them."
  [conn idx-branch]
  (go-try
    (let [idx      (<? (dbproto/-resolve idx-branch))
          children (vals (:children idx))
          leaf?    (:leaf (first children))]
      (doseq [child children]
        (if leaf?
          (do
            ;; delete history
            (storage/storage-write conn (str (:id child) "_his") nil)
            ;; delelete leaf
            (storage/storage-write conn (:id child) nil))
          (<? (delete-all-index-children conn child))))
      ;; now delete the main branch called once children are all gone
      (storage/storage-write conn (:id idx-branch) nil))))


(defn delete-ledger-indexes
  "Deletes all keys for all four indexes for a ledger."
  [conn network ledger-id]
  (go-try
    (let [ledger      (str network "/" ledger-id)
          session     (session/session conn ledger)
          blank-db    (:blank-db session)
          ledger-info (<? (ops/ledger-info-async conn [network ledger-id]))
          db          (<? (storage/reify-db conn network ledger-id blank-db (:index ledger-info)))
          idxs        [:spot :psot :post :opst]]
      (doseq [idx idxs]
        (<? (delete-all-index-children conn (get db idx)))))))

(defn delete-ledger-blocks
  "Deletes all keys for all four indexes for a db.
  Returns the block ID of the last deleted block."
  [conn network ledger-id]
  (loop [block 1]
    (let [block-key (storage/ledger-block-key network ledger-id block)
          res       (storage/storage-write conn block-key nil)]
      (if (true? res)
        (recur (inc block))
        (dec block)))))


;; TODO - need to figure out a way to detect, and finish this process if it is interrupted
(defn delete-ledger-async
  "Deletes a DB. Optionally ensures an snapshot is first completed if snapshot? is truthy."
  ([conn network ledger-id] (delete-ledger-async conn network ledger-id true))
  ([conn network ledger-id snapshot?]
   (future
     (let [dbstatus       (txproto/ledger-status (:group conn) network ledger-id)
           last-idx-block (:index dbstatus)
           ledger-ident   (str network "/" ledger-id)
           message        (str "Ledger deletion begun for: " ledger-ident " at: " (Instant/now) ".")]
       (log/info message)
       (txproto/update-ledger-status (:group conn) network ledger-id "deleting")
       ;; message out to all peers new status for this ledger
       (when snapshot?
         (snapshot/create-snapshot conn network ledger-id))
       ;; first garbage collect everything
       (garbage-collect conn network ledger-id)
       ;; then delete all indexes
       (async/<!! (delete-ledger-indexes conn network ledger-id))
       ;; then delete all blocks
       (delete-ledger-blocks conn network ledger-id)
       ;; then delete last ledger-root
       (let [ledger-root-key (storage/ledger-root-key network ledger-id last-idx-block)]
         (storage/storage-write conn ledger-root-key nil))
       (log/info (str "Ledger deletion ended for: " ledger-ident " at: " (Instant/now) ".")) true))))

