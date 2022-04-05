(ns fluree.db.ledger.garbage-collect
  (:require [clojure.core.async :refer [go]]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.log :as log]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]))

(set! *warn-on-reflection* true)

;; takes care of garbage collection for a db.

(defn delete-file-raft
  "Deletes a file from the RAFT network based on the file key."
  [conn key]
  (go-try
    (let [group (:group conn)]
      (<? (txproto/storage-write-async group key nil)))))


(defn process-index
  "Garbage collections a specific index point."
  [conn network dbid idx-point]
  (go-try
    (let [group        (:group conn)
          garbage-keys (:garbage (<? (storage/read-garbage conn network dbid idx-point)))]
      (log/info "Garbage collecting index point " idx-point " for ledger " network "/" dbid ".")
      ;; delete index point first so it won't show up in dbinfo
      (txproto/remove-index-point group network dbid idx-point)
      ;; remove db-root
      (<? (delete-file-raft conn (storage/ledger-root-key network dbid idx-point)))
      ;; remove all index segments that were garbage collected
      (doseq [k garbage-keys]
        (<? (delete-file-raft conn k)))
      ;; remove garbage file
      (<? (delete-file-raft conn (storage/ledger-garbage-key network dbid idx-point)))
      (log/info "Finished garbage collecting index point " idx-point " for ledger " network "/" dbid "."))))


(defn process
  "Collects garbage (deletes indexes) for any index point(s) between from-block and to-block.
  If from and to blocks are not specified, collects all index points except for current one."
  ([conn network ledger-id]
   (let [ledger-info (txproto/ledger-info (:group conn) network ledger-id)
         idx-points  (-> (into #{} (keys (:indexes ledger-info)))
                         ;; remove current index point
                         (disj (:index ledger-info)))]
     (if (empty? idx-points)
       (go false) ;; nothing to garbage collect
       (process conn network ledger-id (apply min idx-points) (apply max idx-points)))))
  ([conn network ledger-id from-block to-block]
   (go-try
     (let [[from-block to-block] (if (> from-block to-block) ;; make sure from-block is smallest number
                                   [to-block from-block]
                                   [from-block to-block])
           ledger-info           (txproto/ledger-info (:group conn) network ledger-id)
           index-points          (-> (into #{} (keys (:indexes ledger-info)))
                                     (disj (:index ledger-info))) ;; remove current index point
           filtered-index-points (->> index-points
                                      (filter #(<= from-block % to-block))
                                      ;; do smallest indexes first
                                      (sort))]
       (log/info "Garbage collecting ledger " network "/" ledger-id " for index points: " filtered-index-points)
       (doseq [idx-point filtered-index-points]
         (<? (process-index conn network ledger-id idx-point)))
       (log/info "Done garbage collecting ledger " network "/" ledger-id " for index points: " filtered-index-points)
       true))))
