(ns fluree.db.ledger.upgrade.raft-db-ledger
  (:require [fluree.db.util.log :as log]
            [fluree.raft.log :as raft-log]
            [clojure.java.io :as io])
  (:import java.io.File
           (java.nio.file CopyOption Files Path StandardCopyOption)))

(defn raft?
  [settings]
  (-> settings :fdb-consensus-type (= "raft"N)))

(defn raft-log-directory
  [settings]
  (:fdb-group-log-directory settings))

(defn db->ledger
  [l]
  (if (= :append-entry (get l 2))
    (update-in l [3 :entry 0] (fn [entry-type]
                                (case entry-type
                                  :new-db         :new-ledger
                                  :initialized-db :initialized-ledger
                                  entry-type)))
    l))

(defn write-entry
  [f l]
  (let [idx        (first l)
        entry-type (get raft-log/entry-types' idx)]
    (case entry-type
      :current-term (let [term (get l 1)]
                      (raft-log/write-current-term f term))
      :voted-for    (let [[_ term _ voted-for] l]
                      (raft-log/write-voted-for f term voted-for))
      :snapshot     (let [[_ snap-term _ snap-idx] l]
                      (raft-log/write-snapshot f snap-idx snap-term))
      (let [[idx _ _ entry] l]
        (raft-log/write-new-command f idx entry)))))

(defn move-file
  [^File source ^File target]
  (let [source-path (.toPath source)
        target-path (.toPath target)
        opt-replace (into-array CopyOption [(StandardCopyOption/REPLACE_EXISTING)])]
    (Files/move source-path target-path opt-replace)))

(defn rewrite-logs
  [settings]
  (when (raft? settings)
    (let [log-dir   (raft-log-directory settings)
          log-files (->> log-dir
                         file-seq
                         (filter (fn [^File f]
                                   (.isFile f))))]
      (doseq [^File f log-files]
        (let [tmp-file (File/createTempFile (.getName f) ".tmp")]
          (->> f
               raft-log/read-log-file
               (map db->ledger)
               (map (partial write-entry tmp-file))
               dorun)
          (move-file tmp-file f))))))
