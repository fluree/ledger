(ns rebuild.write-raft-log-entries
  "Write RAFT log entries to update ledger state with new indexes"
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io DataOutputStream BufferedOutputStream FileOutputStream]
           [java.util UUID]))

(defn log [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

(defn find-latest-raft-log
  "Find the latest RAFT log file number"
  [log-dir]
  (->> (io/file log-dir)
       (.listFiles)
       (filter #(.endsWith (.getName %) ".raft"))
       (map #(.getName %))
       (keep (fn [name]
              (when-let [m (re-matches #"(\d+)\.raft" name)]
                (Long/parseLong (second m)))))
       sort
       last))

(defn find-index-files
  "Find all index root files in the rebuild"
  [base-path network dbid]
  (let [root-dir (io/file base-path "ledger" network dbid "root")]
    (when (.exists root-dir)
      (->> (.listFiles root-dir)
           (map #(.getName %))
           (keep (fn [name]
                  (when-let [m (re-matches #"(\d+)\.fdbd" name)]
                    (Long/parseLong (second m)))))
           sort))))

(defn find-latest-block
  "Find the latest block number"
  [base-path network dbid]
  (let [block-dir (io/file base-path "ledger" network dbid "block")]
    (when (.exists block-dir)
      (->> (.listFiles block-dir)
           (map #(.getName %))
           (keep (fn [name]
                  (when-let [m (re-matches #"(\d+)\.fdbd" name)]
                    (Long/parseLong (second m)))))
           sort
           last))))

(defn write-raft-log-entry!
  "Write a single RAFT log entry in the proper format"
  [dos entry-data term]
  ;; Write term (long)
  (.writeLong dos term)
  ;; Write entry type (int) - 3 for append-entry
  (.writeInt dos 3)
  ;; Serialize the entry data
  (let [entry-str (pr-str {:entry entry-data
                           :id (str (UUID/randomUUID))})
        entry-bytes (.getBytes entry-str "UTF-8")]
    ;; Write data length (int)
    (.writeInt dos (count entry-bytes))
    ;; Write data
    (.write dos entry-bytes)))

(defn create-new-raft-log!
  "Create a new RAFT log file with entries to update ledger state"
  [log-dir log-number term entries]
  (let [log-file (io/file log-dir (str log-number ".raft"))]
    (with-open [fos (FileOutputStream. log-file)
                bos (BufferedOutputStream. fos)
                dos (DataOutputStream. bos)]
      ;; Write version number
      (.writeInt dos 9)
      
      ;; Write each entry
      (doseq [entry entries]
        (write-raft-log-entry! dos entry term)))
    
    (log "Created new RAFT log: %s with %d entries" (.getName log-file) (count entries))))

(defn build-ledger-update-entries
  "Build RAFT log entries to update ledger state"
  [network dbid latest-block index-points server-name]
  (let [timestamp (System/currentTimeMillis)
        latest-index (when (seq index-points) (last index-points))
        ;; Build indexes map with timestamps
        indexes-map (into {} 
                          (map (fn [idx] [idx timestamp])
                               index-points))
        ;; Ledger state to write
        ledger-state (cond-> {:block latest-block}
                       latest-index (assoc :status :ready
                                          :index latest-index
                                          :indexes indexes-map)
                       (not latest-index) (assoc :status :reindex))]
    
    ;; Return entries to write
    [;; Ensure network exists
     [:assoc-in [:networks network :dbs] {dbid {}}]
     [:assoc-in [:new-db-queue network] {}]
     [:assoc-in [:cmd-queue network] {}]
     
     ;; Assign work to this server
     [:assoc-in [:_work :networks network] server-name]
     [:assoc-in [:_worker server-name :networks network] (+ timestamp 5000)]
     
     ;; Update ledger state with new block and indexes
     [:assoc-in [:networks network :dbs dbid] ledger-state]
     
     ;; Mark network as initialized
     [:assoc-in [:networks network :initialized?] true]]))

(defn write-raft-update!
  "Write RAFT log entries to update ledger with new indexes
   Options:
   - :base-path - Root directory with rebuilt db
   - :network - Network name  
   - :dbid - Database ID
   - :server-name - Server name (default: myserver)
   - :term - RAFT term to use (optional, will use 1 if not provided)"
  [{:keys [base-path network dbid server-name term]
    :or {server-name "myserver"
         term 1}}]
  
  (log "=== Writing RAFT Log Entries for Index Update ===")
  (log "Base path: %s" base-path)
  (log "Network: %s, DB: %s" network dbid)
  (log "Server: %s" server-name)
  
  (let [log-dir (io/file base-path "group")
        ;; Find the latest log number
        latest-log-num (find-latest-raft-log log-dir)
        new-log-num (inc (or latest-log-num 0))
        
        ;; Find indexes and blocks from rebuilt database
        index-points (find-index-files base-path network dbid)
        latest-block (find-latest-block base-path network dbid)]
    
    (log "Found %d index points: %s" (count index-points) (pr-str index-points))
    (log "Latest block: %d" latest-block)
    (log "Will create new log file: %d.raft" new-log-num)
    
    (if (and latest-block (seq index-points))
      (let [entries (build-ledger-update-entries network dbid latest-block index-points server-name)]
        (log "Creating %d RAFT log entries" (count entries))
        
        ;; Write the new log file
        (create-new-raft-log! log-dir new-log-num term entries)
        
        (log "\n✓ RAFT log entries written successfully")
        (log "The ledger state will be updated when RAFT loads")
        (log "New state will have:")
        (log "  - Block: %d" latest-block)
        (log "  - Latest index: %d" (last index-points))
        (log "  - Total indexes: %d" (count index-points))
        
        {:log-file (str new-log-num ".raft")
         :entries-count (count entries)
         :latest-block latest-block
         :latest-index (last index-points)
         :index-count (count index-points)})
      
      (throw (ex-info "No blocks or indexes found in rebuilt database"
                      {:base-path base-path
                       :network network
                       :dbid dbid
                       :blocks-found latest-block
                       :indexes-found (count index-points)})))))

;; Example usage
(comment
  ;; Write RAFT log entries to update ledger state
  (write-raft-update! {:base-path "/path/to/data"
                       :network "mynetwork"
                       :dbid "mydb"
                       :server-name "myserver"
                       :term 1}))