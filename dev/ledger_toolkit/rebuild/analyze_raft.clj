(ns rebuild.analyze-raft
  "Analyze RAFT snapshot and log files to understand their structure"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :as pp]
            [clojure.edn :as edn]
            [taoensso.nippy :as nippy])
  (:import [java.io DataInputStream BufferedInputStream FileInputStream]
           [java.util.zip InflaterInputStream]))

(defn log [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

(defn read-raft-snapshot
  "Read and analyze a RAFT snapshot file"
  [snapshot-file]
  (log "Reading snapshot: %s" snapshot-file)
  (try
    ;; Fluree uses Nippy for snapshot serialization
    (let [snapshot-bytes (with-open [in (io/input-stream snapshot-file)
                                     out (java.io.ByteArrayOutputStream.)]
                           (io/copy in out)
                           (.toByteArray out))
          data (nippy/thaw snapshot-bytes)]

      (log "Snapshot data structure:")
      (pp/pprint data)

      ;; Analyze the structure
      (log "\n=== Snapshot Analysis ===")
      (log "Top-level keys: %s" (pr-str (keys data)))

      (when (:networks data)
        (log "\nNetworks found: %s" (pr-str (keys (:networks data))))
        (doseq [[network network-data] (:networks data)]
          (log "  Network '%s' structure:" network)
          (log "    DBs: %s" (pr-str (keys (:dbs network-data))))
          (doseq [[db db-data] (:dbs network-data)]
            (log "      DB '%s': %s" db (pr-str db-data)))))

      (when (:_work data)
        (log "\n_work structure: %s" (pr-str (:_work data))))

      (when (:_worker data)
        (log "\n_worker structure: %s" (pr-str (:_worker data))))

      (when (:leases data)
        (log "\nLeases structure: %s" (pr-str (:leases data))))

      data)
    (catch Exception e
      (log "ERROR reading snapshot: %s" (.getMessage e))
      (.printStackTrace e))))

(defn read-raft-log-entry
  "Read a single RAFT log entry"
  [dis]
  (try
    (let [term (.readLong dis)
          entry-type (.readInt dis)
          data-length (.readInt dis)
          data-bytes (byte-array data-length)]
      (.readFully dis data-bytes)
      ;; Try to deserialize as Nippy first, fall back to EDN
      (let [data (try
                   (nippy/thaw data-bytes)
                   (catch Exception _
                     ;; Fall back to EDN/string parsing
                     (try
                       (let [data-str (String. data-bytes "UTF-8")]
                         (edn/read-string data-str))
                       (catch Exception _
                         ;; Just return as string if can't parse
                         (String. data-bytes "UTF-8")))))]
        {:term term
         :entry-type entry-type
         :data data}))
    (catch java.io.EOFException _
      nil)))

(defn read-raft-log
  "Read and analyze a RAFT log file"
  [log-file]
  (log "Reading RAFT log: %s" log-file)
  (try
    (with-open [fis (FileInputStream. log-file)
                bis (BufferedInputStream. fis)
                dis (DataInputStream. bis)]
      ;; Read version
      (let [version (.readInt dis)
            _ (log "Log version: %d" version)
            entries (loop [entries []
                           entry-num 1]
                      (if-let [entry (read-raft-log-entry dis)]
                        (do
                          (when (<= entry-num 10) ;; Show first 10 entries
                            (log "Entry %d: term=%d, type=%d"
                                 entry-num (:term entry) (:entry-type entry))
                            (when (<= entry-num 3) ;; Show full data for first 3
                              (log "  Data: %s" (pr-str (:data entry)))))
                          (recur (conj entries entry) (inc entry-num)))
                        entries))]

        (log "\n=== Log Analysis ===")
        (log "Total entries: %d" (count entries))

        ;; Analyze entry types
        (let [by-type (group-by :entry-type entries)]
          (log "Entry types distribution:")
          (doseq [[entry-type entries-of-type] by-type]
            (log "  Type %d: %d entries" entry-type (count entries-of-type))))

        ;; Analyze terms
        (let [terms (distinct (map :term entries))]
          (log "Terms found: %s" (pr-str (sort terms))))

        ;; Look for specific command patterns
        (log "\nSample of commands (first 20 non-nil):")
        (doseq [entry (take 20 (filter #(some? (:data %)) entries))]
          (let [data (:data entry)]
            (cond
              (map? data)
              (do
                (log "  Command keys: %s" (pr-str (keys data)))
                (when (:networks data)
                  (log "    Networks affected: %s" (pr-str (keys (:networks data)))))
                (when (:_work data)
                  (log "    _work content: %s" (pr-str (:_work data)))))
              
              (string? data)
              (log "  String data (first 200 chars): %s" 
                   (pr-str (subs data 0 (min 200 (count data)))))
              
              :else
              (log "  Data type: %s, value: %s" 
                   (type data) 
                   (pr-str (take 100 (str data)))))))

        entries))
    (catch Exception e
      (log "ERROR reading log: %s" (.getMessage e))
      (.printStackTrace e))))

(defn analyze-group-directory
  "Analyze all RAFT files in a group directory"
  [group-dir]
  (let [dir (io/file group-dir)]
    (if (.exists dir)
      (do
        (log "=== Analyzing RAFT Group Directory: %s ===" (.getPath dir))

        ;; Find snapshot files
        (let [snapshots (->> (.listFiles dir)
                             (filter #(str/ends-with? (.getName %) ".snapshot"))
                             (sort-by #(.getName %)))]
          (log "\nFound %d snapshot files:" (count snapshots))
          (doseq [snapshot snapshots]
            (log "  - %s (%.2f KB)"
                 (.getName snapshot)
                 (/ (.length snapshot) 1024.0)))

          ;; Analyze latest snapshot
          (when-let [latest (last snapshots)]
            (log "\nAnalyzing latest snapshot: %s" (.getName latest))
            (read-raft-snapshot latest)))

        ;; Find log files
        (let [logs (->> (.listFiles dir)
                        (filter #(str/ends-with? (.getName %) ".raft"))
                        (sort-by #(.getName %)))]
          (log "\nFound %d RAFT log files:" (count logs))
          (doseq [log-file logs]
            (log "  - %s (%.2f KB)"
                 (.getName log-file)
                 (/ (.length log-file) 1024.0)))

          ;; Analyze latest log
          (when-let [latest (last logs)]
            (log "\nAnalyzing latest log: %s" (.getName latest))
            (read-raft-log latest))))
      (log "Directory does not exist: %s" group-dir))))

(defn compare-raft-states
  "Compare RAFT state from two different sources"
  [original-group-dir rebuilt-group-dir]
  (log "=== Comparing RAFT States ===")

  (log "\n--- ORIGINAL (Production) ---")
  (analyze-group-directory original-group-dir)

  (log "\n--- REBUILT ---")
  (analyze-group-directory rebuilt-group-dir)

  (log "\n=== Key Differences to Check ===")
  (log "1. Snapshot version numbers")
  (log "2. Network and DB names")
  (log "3. Server assignments in _work")
  (log "4. Block and index numbers")
  (log "5. Entry types in logs"))

;; Example usage
(comment
  ;; Analyze RAFT files
  (analyze-group-directory "/path/to/data/group")

  ;; Read a specific snapshot
  (read-raft-snapshot "/path/to/data/group/12345.snapshot")

  ;; Read a specific log
  (read-raft-log "/path/to/data/group/0.raft")

  ;; Compare original vs rebuilt
  (compare-raft-states
   "/path/to/original/group"
   "/path/to/rebuilt/group"))