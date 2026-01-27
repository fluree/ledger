(ns rebuild.append-raft-entries
  "Append RAFT log entries using Fluree's RAFT log API"
  (:require [clojure.java.io :as io]
            [fluree.raft.log :as raft-log])
  (:import [java.util UUID]))

(defn log [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

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

(defn build-ledger-state
  "Build the ledger state map"
  [latest-block index-points]
  (let [timestamp (System/currentTimeMillis)
        latest-index (when (seq index-points) (last index-points))
        ;; Build indexes map with timestamps
        indexes-map (into {} 
                          (map (fn [idx] [idx timestamp])
                               index-points))]
    (cond-> {:block latest-block}
      latest-index (assoc :status :ready
                         :index latest-index
                         :indexes indexes-map)
      (not latest-index) (assoc :status :reindex))))

(defn append-raft-entries!
  "Append RAFT entries to update ledger state
   Options:
   - :base-path - Root directory with rebuilt db
   - :network - Network name  
   - :dbid - Database ID
   - :server-name - Server name (default: myserver)"
  [{:keys [base-path network dbid server-name]
    :or {server-name "myserver"}}]
  
  (log "=== Appending RAFT Log Entries ===")
  (log "Base path: %s" base-path)
  (log "Network: %s, DB: %s" network dbid)
  (log "Server: %s" server-name)
  
  (let [log-dir (str base-path "/group/")
        ;; Find the latest log file
        latest-log-index (raft-log/latest-log-index log-dir "raft")
        latest-log-file (io/file log-dir (str latest-log-index ".raft"))
        
        ;; Read the log to get the latest index and term
        log-entries (raft-log/read-log-file latest-log-file)
        ;; Get the last entry's index and term
        last-entry (last log-entries)
        latest-term (or (:term last-entry) 1)
        latest-index (or (:index last-entry) 0)
        
        ;; Find indexes and blocks from rebuilt database
        index-points (find-index-files base-path network dbid)
        latest-block (find-latest-block base-path network dbid)
        
        _ (log "Found %d index points: %s" (count index-points) (pr-str index-points))
        _ (log "Latest block: %d" latest-block)
        _ (log "Appending to log file: %s (term=%d, index=%d)" 
               (.getName latest-log-file) latest-term latest-index)
        
        ;; Build the ledger state
        ledger-state (build-ledger-state latest-block index-points)
        timestamp (System/currentTimeMillis)
        
        ;; Build entries to append
        entries [{:term latest-term
                 :entry [:assoc-in [:networks network :dbs] {dbid {}}]
                 :id (str (UUID/randomUUID))}
                 
                {:term latest-term
                 :entry [:assoc-in [:new-db-queue network] {}]
                 :id (str (UUID/randomUUID))}
                 
                {:term latest-term
                 :entry [:assoc-in [:cmd-queue network] {}]
                 :id (str (UUID/randomUUID))}
                 
                {:term latest-term
                 :entry [:assoc-in [:_work :networks network] server-name]
                 :id (str (UUID/randomUUID))}
                 
                {:term latest-term
                 :entry [:assoc-in [:_worker server-name :networks network] (+ timestamp 5000)]
                 :id (str (UUID/randomUUID))}
                 
                {:term latest-term
                 :entry [:assoc-in [:networks network :dbs dbid] ledger-state]
                 :id (str (UUID/randomUUID))}
                 
                {:term latest-term
                 :entry [:assoc-in [:networks network :initialized?] true]
                 :id (str (UUID/randomUUID))}]]
    
    ;; Append the entries using the RAFT log API
    (raft-log/append latest-log-file entries latest-index latest-index)
    
    (log "\n✓ Appended %d RAFT log entries" (count entries))
    (log "Ledger state updated with:")
    (log "  - Block: %d" latest-block)
    (log "  - Latest index: %d" (or (last index-points) 0))
    (log "  - Total indexes: %d" (count index-points))
    
    {:entries-count (count entries)
     :latest-block latest-block
     :latest-index (last index-points)
     :index-count (count index-points)}))

;; Example usage
(comment
  ;; Append RAFT entries to update ledger state
  (append-raft-entries! {:base-path "/path/to/data"
                         :network "mynetwork"
                         :dbid "mydb"
                         :server-name "myserver"}))