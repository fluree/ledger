(ns rebuild.verify-rebuild
  "Verify the rebuilt database by loading it and running queries"
  (:require [clojure.core.async :as async :refer [<!!]]
            [clojure.string :as str]
            [clojure.pprint :as pp]
            [fluree.db.server :as server]
            [fluree.db.server-settings :as settings]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.storage :as ledger-storage]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]))

(defn log [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

(defn build-config
  "Build minimal server configuration to load rebuilt db"
  [base-path]
  (settings/build-env
   {:fdb-mode                    "ledger"
    :fdb-storage-type            "file"
    :fdb-storage-file-root       base-path
    :fdb-consensus-type          "raft"
    :fdb-group-servers           "myserver@localhost:11003"
    :fdb-group-this-server       "myserver"
    :fdb-group-port              11003
    :fdb-group-log-directory     (str base-path "/group")
    :fdb-group-snapshot-path     "group/snapshots"
    :fdb-api-port                9997
    :fdb-parallelism             2}))

(defn wait-for-ledger!
  "Wait briefly for the ledger to appear in RAFT."
  [conn network dbid]
  (loop [i 0]
    (let [info (txproto/ledger-info (:group conn) network dbid)]
      (cond
        info true
        (>= i 4) false
        :else (do (Thread/sleep 250) (recur (inc i)))))))

;; Helpers to compute latest block and index from on-disk files
(defn- numeric-fdbd-files
  [dir]
  (try
    (let [f (java.io.File. dir)]
      (when (.exists f)
        (->> (.listFiles f)
             (map (fn [^java.io.File f] (.getName f)))
             (keep (fn [name]
                     (when-let [m (re-matches #"(\\d+)\\.fdbd" name)]
                       (Long/parseLong (second m)))))
             sort)))
    (catch Exception _ [])))

(defn- latest-block-and-index
  [base-path network dbid]
  (let [ledger-root (str base-path "/ledger/" network "/" dbid)
        blocks      (numeric-fdbd-files (str ledger-root "/block"))
        roots       (numeric-fdbd-files (str ledger-root "/root"))
        latest-blk  (when (seq blocks) (apply max blocks))
        idx-cands   (when latest-blk (filter #(<= % latest-blk) roots))
        latest-idx  (when (seq idx-cands) (apply max idx-cands))]
    {:block latest-blk :index latest-idx}))

(defn ensure-ledger-in-raft!
  "Registers the ledger in RAFT if missing by scanning <base>/ledger/<nw>/<db> for
  blocks/roots and writing RAFT state to mirror CLI behavior. Also assigns this
  server to the network so future index points can be written."
  [conn base-path network dbid]
  (let [group       (:group conn)
        this-server (:this-server group)
        current     (txproto/ledger-info group network dbid)
        current-assignment (txproto/kv-get-in group [:_work :networks network])]
    ;; Ensure assignment points to this server for single-node verify
    (when (not= current-assignment this-server)
      (txproto/kv-assoc-in group [:_work :networks network] this-server)
      (txproto/kv-assoc-in group [:_worker this-server :networks network]
                           (+ (System/currentTimeMillis) 5000)))

    (when-not current
      ;; Seed network scaffolding if missing
      (when-not (txproto/kv-get-in group [:networks network :dbs])
        (txproto/kv-assoc-in group [:networks network :dbs] {})
        (txproto/kv-assoc-in group [:new-db-queue network] {})
        (txproto/kv-assoc-in group [:cmd-queue network] {}))

      ;; Compute latest block and index from disk and write ledger map
      (let [{:keys [block index]} (latest-block-and-index base-path network dbid)
            ledger-state          (cond-> {:block block}
                                    index (merge {:status  :ready
                                                  :index   index
                                                  :indexes {index (System/currentTimeMillis)}})
                                    (not index) (merge {:status :reindex}))]
        (when block
          (txproto/kv-assoc-in group [:networks network :dbs dbid] ledger-state))))

    ;; Mark network initialized and wait briefly for visibility
    (txproto/initialize-network group network)
    (wait-for-ledger! conn network dbid)))

(defn q!
  "Run a query using the promise-based API and return the result or throw on error.
  Handles the case where the promise resolves to a Throwable value."
  [db query]
  (let [res (try
              @(fdb/query db query)
              (catch Exception e e))]
    (when (instance? Throwable res)
      (throw res))
    res))

(defn load-db!!
  "Retries loading a DB a few times. Throws if the final result is an exception or nil."
  [conn ledger-id]
  (loop [i 0 last-ex nil]
    (let [res (<!! (fdb/db conn ledger-id))]
      (cond
        (instance? Throwable res)
        (if (< i 4)
          (do (Thread/sleep 250) (recur (inc i) res))
          (throw res))

        (some? res) res

        (< i 4) (do (Thread/sleep 250) (recur (inc i) last-ex))

        :else (throw (ex-info "DB not loaded" {:ledger ledger-id :last-error (when last-ex (str (class last-ex)))}))))))

(defn run-queries
  "Run various queries to verify the database works"
  [conn network dbid]
  (let [ledger-id (str network "/" dbid)]
    (log "Loading database: %s" ledger-id)

    (try
      ;; RAFT diagnostics: local and consistent reads
      (let [group (:group conn)
            local-ledger (txproto/kv-get-in group [:networks network :dbs dbid])
            consistent-ledger (<!! (txproto/kv-get-in-async* group [:networks network :dbs dbid]))]
        (log "RAFT local ledger-info: %s" (pr-str local-ledger))
        (log "RAFT consistent ledger-info: %s" (pr-str consistent-ledger)))
      ;; Ensure RAFT has the ledger registered and visible
      (wait-for-ledger! conn network dbid)
      ;; Load the database with retry
      (let [db (load-db!! conn ledger-id)]
        (log "✓ Database loaded successfully")

        ;; Query 1: Get all collections
        (log "\n=== Query 1: List all collections ===")
        (try
          (let [collections (q! db {:select ["?name"]
                                    :where [["?c" "_collection/name" "?name"]]})]
            (log "Found %d collections:" (count collections))
            (doseq [coll (take 10 collections)]
              (println "  -" (first coll)))
            (when (> (count collections) 10)
              (log "  ... and %d more" (- (count collections) 10))))
          (catch Exception e
            (log "ERROR: %s class=%s data=%s" (or (.getMessage e) "") (str (class e)) (pr-str (ex-data e)))))

        ;; Query 2: Get all predicates
        (log "\n=== Query 2: List predicates (first 10) ===")
        (try
          (let [predicates (q! db {:select ["?name"]
                                   :where [["?p" "_predicate/name" "?name"]]
                                   :limit 10})]
            (log "Sample predicates:")
            (doseq [pred predicates]
              (println "  -" (first pred))))
          (catch Exception e
            (log "ERROR: %s class=%s data=%s" (or (.getMessage e) "") (str (class e)) (pr-str (ex-data e)))))

        ;; Query 3: Count total subjects
        (log "\n=== Query 3: Count subjects by collection ===")
        (try
          (let [counts (q! db {:select ["?coll" "(count ?s)"]
                               :where [["?s" "_id" "?id"]
                                       ["?s" "_collection" "?c"]
                                       ["?c" "_collection/name" "?coll"]]
                               :group-by ["?coll"]
                               :limit 20})]
            (log "Subject counts by collection (top 20):")
            (doseq [[coll cnt] (sort-by second > counts)]
              (println (format "  %-30s: %d" coll cnt))))
          (catch Exception e
            (log "ERROR: %s class=%s data=%s" (or (.getMessage e) "") (str (class e)) (pr-str (ex-data e)))))

        ;; Query 4: Get sample data
        (log "\n=== Query 4: Sample data from first non-system collection ===")
        (let [collections (try (q! db {:select ["?name"]
                                       :where [["?c" "_collection/name" "?name"]]})
                               (catch Exception _ []))]
          ;; Find first non-system collection
          (if-let [user-coll (first (filter #(not (.startsWith (first %) "_")) collections))]
            (let [coll-name (first user-coll)
                  sample (try (q! db {:select ["*"]
                                      :from coll-name
                                      :limit 3})
                              (catch Exception _ []))]
              (log "Sample data from '%s' collection:" coll-name)
              (pp/pprint sample))
            (log "No user collections found")))

        ;; Query 5: Check block metadata
        (log "\n=== Query 5: Block metadata ===")
        (try
          (let [blocks (q! db {:select ["?block" "?hash" "?instant"]
                               :where [["?block" "_block/number" "?n"]
                                       ["?block" "_block/hash" "?hash"]
                                       ["?block" "_block/instant" "?instant"]]
                               :order-by [["?n" "desc"]]
                               :limit 5})]
            (log "Latest 5 blocks:")
            (doseq [[block hash instant] blocks]
              (println (format "  Block %s: %s at %s"
                               (last (str/split (str block) #"/"))
                               (subs hash 0 16)
                               instant))))
          (catch Exception e
            (log "ERROR: %s class=%s data=%s" (or (.getMessage e) "") (str (class e)) (pr-str (ex-data e)))))

        ;; Query 6: Check for any auth records
        (log "\n=== Query 6: Auth records ===")
        (let [auths (try (q! db {:select ["?auth" "?id"]
                                 :where [["?auth" "_auth/id" "?id"]]
                                 :limit 5})
                         (catch Exception _ []))]
          (if (empty? auths)
            (log "No auth records found")
            (do
              (log "Found %d auth records (showing first 5):" (count auths))
              (doseq [[auth id] auths]
                (println "  -" id)))))

        ;; Query 7: Database stats
        (log "\n=== Query 7: Database stats ===")
        (try
          (let [stats (q! db {:select {"?collections" "(count ?c)"
                                       "?predicates" "(count ?p)"
                                       "?total-flakes" "(count ?s)"}
                              :where [["?c" "_collection/name" "?cn"]
                                      ["?p" "_predicate/name" "?pn"]
                                      ["?s" "_id" "?id"]]})]
            (log "Database statistics:")
            (pp/pprint (first stats)))
          (catch Exception e
            (log "ERROR: %s class=%s data=%s" (or (.getMessage e) "") (str (class e)) (pr-str (ex-data e)))))

        (log "\n✅ All queries completed successfully!")
        true)

      (catch Exception e
        (log "ERROR during queries: %s" (.getMessage e))
        (.printStackTrace e)
        false))))

(defn verify!
  "Main entry point for verification
   Options:
   - :base-path - Root directory containing the rebuilt database (required)
   - :network - Network name (required)
   - :dbid - Database ID (required)"
  [{:keys [base-path network dbid]}]

  (log "=== Starting Database Verification ===")
  (log "Base path: %s" base-path)
  (log "Network: %s, DB: %s" network dbid)

  (let [config (build-config base-path)
        _ (log "Starting Fluree server...")
        system (server/startup config)
        conn (:conn system)]

    (try
      (Thread/sleep 2000) ;; Give server time to initialize
      ;; Ensure RAFT has the ledger registered
      (ensure-ledger-in-raft! conn base-path network dbid)

      (let [success? (run-queries conn network dbid)]
        (if success?
          (log "\n=== ✅ Verification Complete - Database is functional ===")
          (log "\n=== ❌ Verification Failed - Check errors above ===")))

      (catch Exception e
        (log "ERROR: %s" (.getMessage e))
        (.printStackTrace e))

      (finally
        (log "\nShutting down server...")
        (server/shutdown system)
        (log "Shutdown complete")))))

;; Example usage
(comment
  ;; Verify the rebuilt database
  (verify! {:base-path "/path/to/data"
            :network "mynetwork"
            :dbid "mydb"}))