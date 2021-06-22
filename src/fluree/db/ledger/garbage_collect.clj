(ns fluree.db.ledger.garbage-collect
  (:require [fluree.db.storage.core :as storage]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]))

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
          dbinfo       (txproto/ledger-info group network dbid)
          idx-time     (get-in dbinfo [:indexes idx-point])
          ;_ (when-not idx-time
          ;    (throw (ex-info (str "Index point " idx-point " for db " network "/" dbid " does not exist!")
          ;                    {:status 400 :error :db/no-index})))
          garbage-keys (:garbage (<? (storage/read-garbage conn network dbid idx-point)))]
      (log/info "Garbage collecting index point " idx-point " for ledger " network "/" dbid ".")
      ;; delete index point first so it won't show up in dbinfo
      (txproto/remove-index-point group network dbid idx-point)
      ;; remove db-root
      (<? (delete-file-raft conn (storage/ledger-root-key network dbid idx-point)))
      ;; remove all index segments that were garbage collected
      (doseq [k garbage-keys]
        (let [k (str/replace k #"_his$" "-his")]            ;; TODO - temporary fix for add-garbage in indexing.clj added wrong post-fix for history files, remove in future versions!!
          (<? (delete-file-raft conn k))))
      ;; remove garbage file
      (<? (delete-file-raft conn (storage/ledger-garbage-key network dbid idx-point)))
      (log/info "Finished garbage collecting index point " idx-point " for ledger " network "/" dbid "."))))


(defn process
  "Collects garbage (deletes indexes) for any index point(s) between from-block and to-block.
  If from and to blocks are not specified, collects all index points except for current one."
  ([conn network dbid]
   (let [dbinfo     (txproto/ledger-info (:group conn) network dbid)
         idx-points (-> (into #{} (keys (:indexes dbinfo)))
                        ;; remove current index point
                        (disj (:index dbinfo)))]
     (if (empty? idx-points)
       false                                                ;; nothing to garbage collect
       (process conn network dbid (apply min idx-points) (apply max idx-points)))))
  ([conn network dbid from-block to-block]
   (go-try
     (let [[from-block to-block] (if (> from-block to-block) ;; make sure from-block is smallest number
                                   [to-block from-block]
                                   [from-block to-block])
           dbinfo                (txproto/ledger-info (:group conn) network dbid)
           index-points          (-> (into #{} (keys (:indexes dbinfo)))
                                     (disj (:index dbinfo))) ;; remove current index point
           filtered-index-points (->> index-points
                                      (filter #(<= from-block % to-block))
                                      ;; do smallest indexes first
                                      (sort))]
       (log/info "Garbage collecting ledger " network "/" dbid " for index points: " filtered-index-points)
       (doseq [idx-point filtered-index-points]
         (<? (process-index conn network dbid idx-point)))
       (log/info "Done garbage collecting ledger " network "/" dbid " for index points: " filtered-index-points)
       true))))
