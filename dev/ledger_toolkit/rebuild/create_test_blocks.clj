(ns rebuild.create-test-blocks
  "Create a simple test ledger with many small blocks for testing consolidation"
  (:require [clojure.core.async :as async :refer [<!!]]
            [fluree.db.api :as fdb]
            [fluree.db.server :as server]
            [fluree.db.server-settings :as settings]
            [clojure.java.io :as io]))

(defn log [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

(defn build-config [base-path]
  (settings/build-env
   {:fdb-mode                    "dev"
    :fdb-storage-type            "file"
    :fdb-storage-file-root       base-path
    :fdb-consensus-type          "raft"
    :fdb-group-servers           "testserver@localhost:11010"
    :fdb-group-this-server       "testserver"
    :fdb-group-port              11010
    :fdb-group-log-directory     (str base-path "/group")
    :fdb-group-snapshot-path     "group/snapshots"
    :fdb-api-port                9990
    :fdb-parallelism             2
    :fdb-memory-reindex          "5mb"
    :fdb-memory-reindex-max      "10mb"}))

(defn clean-directory! [base-path]
  (let [base-dir (io/file base-path)]
    (when (.exists base-dir)
      (log "Cleaning existing directory...")
      (doseq [f (reverse (file-seq base-dir))]
        (when (.isFile f) (.delete f)))
      (doseq [f (reverse (file-seq base-dir))]
        (when (and (.isDirectory f) (not= f base-dir))
          (.delete f))))))

(defn create-test-ledger!
  [{:keys [base-path network dbid num-blocks]
    :or {base-path "/tmp/fluree-test-blocks"
         network "test"
         dbid "blocks"
         num-blocks 100}}]

  (log "=== Creating Test Dataset with %d blocks ===" num-blocks)
  (log "Base path: %s" base-path)
  (log "Network: %s, DB: %s" network dbid)

  (clean-directory! base-path)

  (let [config (build-config base-path)
        system (server/startup config)
        conn   (:conn system)]
    (try
      (log "Creating ledger...")
      @(fdb/new-ledger conn (str network "/" dbid))
      (log "✓ Ledger created")

      (let [ledger-id (str network "/" dbid)]
        ;; Create schema
        (log "Creating schema...")
        @(fdb/transact conn ledger-id
                       [{:_id "_collection" :name "person"}
                        {:_id "_predicate" :name "person/name" :type "string"}
                        {:_id "_predicate" :name "person/age" :type "int"}])
        (log "✓ Schema created")

        ;; Create many small blocks - just add/update/delete people
        (log "Creating %d blocks with small transactions..." (- num-blocks 2))
        (doseq [i (range 1 (- num-blocks 1))]
          (cond
            ;; Every 10th block: add a new person
            (zero? (mod i 10))
            (do
              @(fdb/transact conn ledger-id
                             [{:_id "person"
                               :person/name (str "Person" i)
                               :person/age (+ 20 (mod i 50))}])
              (when (zero? (mod i 20))
                (log "  Block %d: Added person" (+ i 2))))

            ;; Every 5th block: update an existing person
            (zero? (mod i 5))
            (let [target-person (max 10 (- i 10))]
              @(fdb/transact conn ledger-id
                             [{:_id ["person/name" (str "Person" target-person)]
                               :person/age (+ 25 (mod i 40))}])
              (when (zero? (mod i 20))
                (log "  Block %d: Updated person" (+ i 2))))

            ;; Other blocks: small updates
            :else
            (when (> i 20)
              (let [target (- i (mod i 10))]
                @(fdb/transact conn ledger-id
                               [{:_id ["person/name" (str "Person" target)]
                                 :person/age (+ 30 (mod i 30))}])))))

        (Thread/sleep 1000) ;; Wait for final index

        ;; Query final state
        (log "\n=== Final State ===")
        (let [db @(fdb/db conn ledger-id)
              people @(fdb/query db {:select ["*"] :from "person"})]
          (log "Block: %d" (:block db))
          (log "People count: %d" (count people))
          (log "Sample person: %s" (pr-str (first people)))))

      (log "\n=== Test Data Created Successfully ===")
      (log "Location: %s" base-path)
      (log "Run rebuild with:")
      (log "  clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj --base-path %s --network %s --dbid %s"
           base-path network dbid)

      {:success true :base-path base-path :network network :dbid dbid}

      (catch Exception e
        (log "ERROR: %s" (.getMessage e))
        (.printStackTrace e)
        {:success false :error (.getMessage e)})

      (finally
        (log "\nShutting down...")
        (server/shutdown system)
        (log "Done")))))

;; Run immediately
(create-test-ledger! {:base-path "/tmp/fluree-test-blocks"
                      :network "test"
                      :dbid "blocks"
                      :num-blocks 100})
