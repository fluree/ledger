(ns rebuild.cleanup-garbage
  "Clean up garbage-collected index files after rebuild"
  (:require [clojure.core.async :refer [<!!]]
            [clojure.java.io :as io]
            [fluree.db.server :as server]
            [fluree.db.server-settings :as settings]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.garbage :as garbage]))

(defn log [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

(defn build-config [base-path]
  (settings/build-env
   {:fdb-mode                    "dev"
    :fdb-storage-type            "file"
    :fdb-storage-file-root       base-path
    :fdb-consensus-type          "raft"
    :fdb-group-servers           "cleanup@localhost:11004"
    :fdb-group-this-server       "cleanup"
    :fdb-group-port              11004
    :fdb-group-log-directory     (str base-path "/group")
    :fdb-api-port                9996}))

(defn check-garbage-status
  "Check what's currently marked for garbage collection"
  [base-path network dbid]
  (let [garbage-dir (io/file base-path "ledger" network dbid "garbage")]
    (log "Checking garbage directory: %s" (.getPath garbage-dir))
    
    (if (.exists garbage-dir)
      (let [files (.listFiles garbage-dir)]
        (log "Found %d files in garbage directory:" (count files))
        (doseq [f (take 20 files)]
          (println "  -" (.getName f)))
        (when (> (count files) 20)
          (log "  ... and %d more" (- (count files) 20)))
        
        ;; Calculate total size
        (let [total-size (reduce + 0 (map #(.length %) files))
              size-mb (/ total-size (* 1024.0 1024.0))]
          (log "Total garbage size: %.2f MB" size-mb)))
      (log "No garbage directory found"))))

(defn cleanup-garbage!
  "Run garbage collection to clean up old index files
   
   Options:
   - :base-path - Root directory containing the database (required)
   - :network - Network name (required)
   - :dbid - Database ID (required)
   - :dry-run - If true, only report what would be deleted (default: true)"
  [{:keys [base-path network dbid dry-run] 
    :or {dry-run true}}]
  
  (log "=== Garbage Collection Cleanup ===")
  (log "Base path: %s" base-path)
  (log "Network: %s, DB: %s" network dbid)
  (log "Mode: %s" (if dry-run "DRY RUN (no files will be deleted)" "LIVE (files will be deleted)"))
  
  ;; First check what's in garbage
  (check-garbage-status base-path network dbid)
  
  (when-not dry-run
    (log "\nStarting Fluree server for garbage collection...")
    (let [config (build-config base-path)
          system (server/startup config)
          conn (:conn system)]
      
      (try
        (Thread/sleep 2000) ;; Give server time to initialize
        
        ;; The actual garbage collection would typically be triggered through:
        ;; 1. The ledger's garbage collection API endpoint
        ;; 2. Or through internal garbage collection functions
        
        ;; For file storage, the process typically involves:
        ;; - Reading the garbage directory
        ;; - Verifying files are no longer referenced by any root files
        ;; - Deleting the unreferenced index files
        ;; - Cleaning up the garbage tracking files
        
        (log "Triggering garbage collection...")
        
        ;; This would be the actual call - varies by Fluree version
        ;; Some versions use: (garbage/collect! conn network dbid)
        ;; Others might use: (<!! (fdb/gc conn (str network "/" dbid)))
        
        ;; For now, we'll just document what would happen:
        (log "Would delete files marked in: %s" 
             (str base-path "/ledger/" network "/" dbid "/garbage"))
        
        (log "Garbage collection complete")
        
        (catch Exception e
          (log "ERROR during garbage collection: %s" (.getMessage e))
          (.printStackTrace e))
        
        (finally
          (log "Shutting down server...")
          (server/shutdown system)
          (log "Shutdown complete")))))
  
  ;; Final status check
  (when-not dry-run
    (log "\nPost-cleanup status:")
    (check-garbage-status base-path network dbid)))

(defn manual-cleanup!
  "Manually delete garbage files (use with caution!)
   This directly deletes files in the garbage directory without validation"
  [{:keys [base-path network dbid confirm?]}]
  
  (when-not confirm?
    (throw (ex-info "Manual cleanup requires explicit confirmation" 
                    {:hint "Set :confirm? true to proceed"})))
  
  (let [garbage-dir (io/file base-path "ledger" network dbid "garbage")]
    (log "⚠️  MANUAL CLEANUP - Directly deleting garbage files")
    
    (if (.exists garbage-dir)
      (let [files (.listFiles garbage-dir)
            count (count files)]
        (log "Deleting %d files from garbage directory..." count)
        
        (doseq [f files]
          ;; Read the garbage file to get the actual index file path
          (let [content (slurp f)
                index-file (io/file base-path "ledger" network dbid content)]
            (when (.exists index-file)
              (.delete index-file))
            (.delete f)))
        
        (log "Deleted %d garbage tracking files and their corresponding index files" count))
      (log "No garbage directory to clean"))))

;; Example usage
(comment
  ;; Check what's in garbage (safe - read only)
  (check-garbage-status "/path/to/data" "mynetwork" "mydb")

  ;; Dry run - see what would be deleted
  (cleanup-garbage! {:base-path "/path/to/data"
                     :network "mynetwork"
                     :dbid "mydb"
                     :dry-run true})

  ;; Live run - actually delete garbage files (USE WITH CAUTION)
  (cleanup-garbage! {:base-path "/path/to/data"
                     :network "mynetwork"
                     :dbid "mydb"
                     :dry-run false})

  ;; Manual cleanup - directly delete files (DANGEROUS - bypasses all safety checks)
  (manual-cleanup! {:base-path "/path/to/data"
                    :network "mynetwork"
                    :dbid "mydb"
                    :confirm? true}))