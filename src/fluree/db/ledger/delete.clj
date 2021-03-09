(ns fluree.db.ledger.delete
  (:require [fluree.db.ledger.garbage-collect :as gc]
            [fluree.db.storage.core :as storage]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.index :as index]
            [fluree.db.session :as session]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]))

;; for deleting a current db

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
            (<? (storage/storage-write conn (str (:id child) "-his") nil))
            ;; delete leaf
            (<? (gc/delete-file-raft conn (:id child))))
          (<? (delete-all-index-children conn child))))
      ;; now delete the main branch called once children are all gone
      (<? (gc/delete-file-raft conn (:id idx-branch))))))


(defn delete-db-indexes
  "Deletes all keys for all four indexes for a db."
  [conn network dbid idx-point]
  (go-try
    (let [session  (session/session conn (str network "/" dbid))
          blank-db (:blank-db session)
          db       (<? (storage/reify-db conn network dbid blank-db idx-point))]
      (doseq [idx index/types]
        (<? (delete-all-index-children conn (get db idx)))))))

(defn all-versions
  [conn storage-block-key]
  (go-try (loop [n        1
                 versions []]
            (let [version-key (str storage-block-key "--v" n)]
              (if (<? (storage/storage-exists? conn version-key))
                (recur (inc n) (conj versions version-key))
                versions)))))

(defn delete-all-blocks
  "Deletes blocks and versions of blocks."
  [conn network dbid block]
  (go-try
    (doseq [block (range 1 (inc block))]
      (let [block-key (storage/ledger-block-file-path network dbid block)
            versions  (<? (all-versions conn block-key))
            to-delete (conj versions block-key)]
        (doseq [file to-delete]
          (<? (gc/delete-file-raft conn file)))))))

(defn delete-lucene-indexes
  "Deletes the full-text (lucene) indexes for a ledger."
  [conn network dbid]
  (go-try
    (let [indexer       (-> conn :full-text/indexer :process)
          db            (<? (session/db conn (str network "/" dbid) nil))]
      (<? (indexer {:action :forget, :db db})))))

(defn process
  "Deletes a current DB, deletes block files."
  [conn network dbid]
  (go-try
    ;; mark status as deleting, so nothing new will get a handle on this db
    (txproto/update-ledger-status (:group conn) network dbid "deleting")
    (let [group     (:group conn)
          dbinfo    (txproto/ledger-info group network dbid)
          block     (:block dbinfo)
          idx-point (:index dbinfo)]

      ;; do a full garbage collection first. If nothing exists to gc, will throw
      (gc/process conn network dbid)

      ;; delete full-text indexes
      (<? (delete-lucene-indexes conn network dbid))

      ;; need to delete all index segments for the current index.
      (<? (delete-db-indexes conn network dbid idx-point))

      ;; need to explicitly do a garbage collection of the 'current' node
      (<? (gc/process-index conn network dbid idx-point))

      ;; delete all blocks
      (<? (delete-all-blocks conn network dbid block))

      ;;; remove current index from raft db status
      (txproto/remove-current-index group network dbid)

      ;; mark status as archived
      (txproto/remove-ledger group network dbid)

      ;; all done!
      true)))
