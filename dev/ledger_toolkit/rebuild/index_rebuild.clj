(ns rebuild.index-rebuild
  "Incremental index rebuild from block files with configurable thresholds"
  (:require [clojure.core.async :as async :refer [<!!]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.stacktrace :as stacktrace]
            [fluree.db.server :as server]
            [fluree.db.server-settings :as settings]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.reindex :as reindex]
            [fluree.db.storage.core :as storage]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]))

(defn log-progress
  "Log progress message with timestamp"
  [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

(defn get-block-files
  "Get all block files in directory, sorted by block number"
  [block-dir]
  (let [dir (io/file block-dir)]
    (if (.exists dir)
      (->> (.listFiles dir)
           (filter #(str/ends-with? (.getName %) ".fdbd"))
           (map (fn [f]
                  {:file f
                   :name (.getName f)
                   :size (.length f)
                   :block-num (Long/parseLong (subs (.getName f) 0 15))}))
           (sort-by :block-num))
      [])))

(defn validate-blocks
  "Validate block files have no gaps"
  [block-files]
  (let [block-nums (map :block-num block-files)]
    (when (seq block-nums)
      (let [expected (range (first block-nums) (inc (last block-nums)))]
        (when-not (= block-nums expected)
          (let [missing (remove (set block-nums) expected)]
            (throw (ex-info "Missing block files detected"
                            {:missing-blocks missing
                             :found-blocks block-nums}))))))))

(defn calculate-cumulative-size
  "Calculate cumulative size in MB for a sequence of block files"
  [block-files]
  (double (/ (reduce + 0 (map :size block-files)) (* 1024 1024))))

(defn build-config
  "Build server configuration for rebuild"
  [{:keys [base-path max-mb parallelism group-servers group-port api-port]
    :or {max-mb 200
         parallelism 4
         group-servers "myserver@localhost:11002"
         group-port 11002
         api-port 9998}}]
  (let [canonical-path (settings/canonicalize-path base-path)
        ;; Use myserver to match production
        server-name "myserver"
        servers (str server-name "@localhost:" group-port)]
    (settings/build-env
     {:fdb-mode                    "dev"
      :fdb-storage-type            "file"
      :fdb-storage-file-root       base-path
      :fdb-consensus-type          "raft"
      :fdb-group-servers           servers
      :fdb-group-this-server       server-name
      :fdb-group-port              group-port
      :fdb-group-log-directory     (str canonical-path "/group")  ;; Keep RAFT logs with dataset
      :fdb-group-snapshot-path     "group/snapshots"              ;; Relative to storage root
      :fdb-api-port                api-port
      :fdb-parallelism             parallelism
      :fdb-memory-reindex          (str (max 1 (int (* 0.8 max-mb))) "mb")
      :fdb-memory-reindex-max      (str max-mb "mb")})))

(defn initialize-raft-state!
  "Initialize RAFT state to match production structure"
  [conn network dbid]
  (let [group       (:group conn)
        this-server (:this-server group)
        timestamp   (System/currentTimeMillis)]
    (log-progress "Initializing RAFT state for server %s, network %s, db %s" this-server network dbid)
    
    ;; 1. Assign network to server (creates _work entry)
    (log-progress "  Setting _work assignment...")
    (txproto/kv-assoc-in group [:_work :networks network] this-server)
    
    ;; 2. Register worker (creates _worker entry)
    (log-progress "  Registering worker...")
    (txproto/kv-assoc-in group [:_worker this-server :networks network] timestamp)
    
    ;; 3. Initialize network structure if needed
    (when-not (txproto/kv-get-in group [:networks network])
      (log-progress "  Initializing network structure...")
      (txproto/kv-assoc-in group [:networks network :dbs] {})
      (txproto/kv-assoc-in group [:networks network :initialized?] true))
    
    ;; 4. Initialize queues if needed
    (when-not (txproto/kv-get-in group [:cmd-queue network])
      (log-progress "  Initializing command queue...")
      (txproto/kv-assoc-in group [:cmd-queue network] {}))
    
    (when-not (txproto/kv-get-in group [:new-db-queue network])
      (log-progress "  Initializing new-db queue...")
      (txproto/kv-assoc-in group [:new-db-queue network] {}))
    
    (log-progress "RAFT state initialization complete")))

(defn assign-network!
  "Assign this server to the network so index points can be written"
  [conn network]
  (let [group       (:group conn)
        this-server (:this-server group)]
    (log-progress "Assigning server %s to network %s for index point writes..." this-server network)
    (txproto/kv-assoc-in group [:_work :networks network] this-server)
    (log-progress "Network assignment complete")))

(defn verify-block-readable
  "Verify we can read block 1 as a sanity check"
  [conn network dbid]
  (log-progress "Verifying block 1 is readable...")
  (let [block-1 (storage/read-block conn network dbid 1)]
    (if block-1
      (log-progress "✓ Block 1 readable, size: %d bytes" (count (pr-str block-1)))
      (throw (ex-info "Cannot read block 1"
                      {:network network :dbid dbid})))))

(defn rebuild-with-builtin
  "Use built-in reindex with novelty thresholds"
  [conn network dbid {:keys [max-mb] :or {max-mb 200}}]
  (log-progress "Starting built-in reindex with max novelty %d MB" max-mb)
  (log-progress "Memory settings - min: %s, max: %s"
                (-> conn :meta :novelty-min)
                (-> conn :meta :novelty-max))

  (let [result (<!! (reindex/reindex conn network dbid
                                     {:status      "ready"
                                      :message     (str "Reindex with max ~" max-mb "MB")
                                      :novelty-max (* max-mb 1024 1024)}))]
    (log-progress "Built-in reindex completed")
    result))

(defn sync-full-text
  "Sync full-text index after rebuild"
  [conn network dbid]
  (log-progress "Syncing full-text index...")
  (try
    (let [db      (<!! (fdb/db conn (str network "/" dbid)))
          indexer (-> conn :full-text/indexer :process)]
      (when indexer
        (<!! (indexer {:action :sync :db db}))
        (log-progress "Full-text index synced")))
    (catch Exception e
      (log-progress "Warning: Full-text sync failed: %s" (.getMessage e)))))

(defn verify-rebuild
  "Verify the rebuild succeeded"
  [conn network dbid]
  (log-progress "Verifying rebuild results...")

  ;; Check RAFT state
  (let [group (:group conn)
        raft-state @(:state group)
        ledger-state (get-in raft-state [:networks network :dbs dbid])]
    (log-progress "RAFT ledger state: %s" (pr-str ledger-state))

    (when-not (:index ledger-state)
      (log-progress "Warning: No index recorded in RAFT state"))

    ;; Check we can query (retry DB load briefly, log class/ex-data on error)
    (try
      (let [ledger (str network "/" dbid)
            db     (loop [i 0]
                     (let [res (<!! (fdb/db conn ledger))]
                       (cond
                         (some? res) res
                         (>= i 40)  res
                         :else (do (Thread/sleep 250) (recur (inc i))))))]
        (when (nil? db)
          (throw (ex-info "DB not loaded for verification" {:ledger ledger})))
        (let [res (<!! (fdb/query-async db {:select ["?c"]
                                            :where  [["?c" "_collection/name" "?name"]]}))]
          (when (instance? Throwable res)
            (throw res))
          (log-progress "✓ Ledger queryable, found %d collections" (count res))))
      (catch Exception e
        (log-progress "Warning: Cannot query ledger: %s class=%s data=%s"
                      (or (.getMessage e) "") (str (class e)) (pr-str (ex-data e)))))))

(defn rebuild!
  "Main entry point for index rebuild
   Options:
   - :base-path - Root directory (parent of ledger/) containing block files (required)
   - :network - Network name (required)
   - :dbid - Database ID (required)
   - :max-mb - Maximum MB before triggering index (default 200)
   - :parallelism - Processing parallelism (default 4)
   - :validate - Validate blocks before processing (default true)"
  [{:keys [base-path network dbid max-mb validate]
    :or {max-mb 200
         validate true}
    :as opts}]

  (when-not (and base-path network dbid)
    (throw (ex-info "Missing required options"
                    {:required [:base-path :network :dbid]
                     :provided (keys opts)})))

  (log-progress "=== Starting Index Rebuild ===")
  (log-progress "Base path: %s (should be parent of ledger/)" base-path)
  (log-progress "Network: %s, DB: %s" network dbid)
  (log-progress "Max MB per index: %d" max-mb)

  (let [block-dir (io/file base-path "ledger" network dbid "block")
        _ (log-progress "Looking for blocks in: %s" (.getPath block-dir))
        _ (when-not (.exists block-dir)
            (throw (ex-info "Block directory does not exist. Ensure base-path is the parent of ledger/"
                            {:path (.getPath block-dir)
                             :hint "If blocks are at /tmp/foo/ledger/network/db/block/, use --base-path /tmp/foo"})))

        block-files (get-block-files block-dir)
        _ (log-progress "Found %d block files" (count block-files))

        _ (when (empty? block-files)
            (throw (ex-info "No block files found" {:block-dir (.getPath block-dir)})))

        _ (when validate
            (log-progress "Validating block sequence...")
            (validate-blocks block-files)
            (log-progress "✓ Block sequence valid"))

        total-size-mb (calculate-cumulative-size block-files)
        _ (log-progress "Total size: %.2f MB" total-size-mb)
        _ (log-progress "Will trigger index every ~%d MB" max-mb)

        config (build-config opts)
        _ (log-progress "Starting Fluree server...")
        system (server/startup config)
        conn (:conn system)]

    (try
      ;; Initialize full RAFT state to match production structure
      (initialize-raft-state! conn network dbid)

      ;; Verify we can read blocks
      (verify-block-readable conn network dbid)

      ;; Run the reindex - this will create the ledger entry in RAFT when it writes index points
      (log-progress "Beginning reindex process...")
      (let [result (rebuild-with-builtin conn network dbid opts)]

        (log-progress "Reindex completed: %s" (pr-str result))

        ;; NOW sync full-text index after index roots exist
        (sync-full-text conn network dbid)

        ;; Verify results
        (verify-rebuild conn network dbid)

        (log-progress "=== Rebuild Complete ===")
        result)

      (catch Exception e
        (log-progress "ERROR: %s" (.getMessage e))
        (log-progress "Stack trace: %s" (with-out-str (stacktrace/print-stack-trace e)))
        (throw e))

      (finally
        (log-progress "Shutting down server...")
        (server/shutdown system)
        (log-progress "Shutdown complete")))))

(comment
  ;; Example usage:
  ;; base-path should be the parent directory of "ledger/"
  ;; e.g., if blocks are at /path/to/data/ledger/mynetwork/mydb/block/
  ;;       use base-path "/path/to/data"
  (rebuild! {:base-path "/path/to/data"
             :network "mynetwork"
             :dbid "mydb"
             :max-mb 200}))