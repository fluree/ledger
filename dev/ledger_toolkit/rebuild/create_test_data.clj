(ns rebuild.create-test-data
  "Create a small test ledger with known data for testing block consolidation"
  (:require [clojure.core.async :as async :refer [<!!]]
            [fluree.db.api :as fdb]
            [fluree.db.server :as server]
            [fluree.db.server-settings :as settings]
            [clojure.java.io :as io]))

(defn log [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

(defn build-config
  "Build test server configuration"
  [base-path]
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
    :fdb-parallelism             2}))

(defn create-test-ledger!
  "Create a test ledger with predictable data for testing consolidation"
  [{:keys [base-path network dbid]
    :or {base-path "/tmp/fluree-test-rebuild"
         network "test"
         dbid "consolidation"}}]

  (log "=== Creating Test Data for Block Consolidation ===")
  (log "Base path: %s" base-path)
  (log "Network: %s, DB: %s" network dbid)

  ;; Clean up existing data
  (let [base-dir (io/file base-path)]
    (when (.exists base-dir)
      (log "Cleaning existing test directory...")
      (doseq [f (file-seq base-dir)
              :when (.isFile f)]
        (.delete f))
      (doseq [f (reverse (file-seq base-dir))
              :when (.isDirectory f)]
        (.delete f))))

  (let [config (build-config base-path)
        _      (log "Starting test Fluree server...")
        system (server/startup config)
        conn   (:conn system)]

    (try
      (log "Creating ledger: %s/%s" network dbid)
      (<!! (fdb/new-ledger conn (str network "/" dbid)))
      (log "✓ Ledger created")

      ;; Block 2: Create schema and initial users
      (log "\nBlock 2: Creating schema and initial users...")
      (<!! (fdb/transact-async conn (str network "/" dbid)
                               [{:_id "_collection" :name "person"}
                                {:_id "_predicate" :name "person/name" :type "string"}
                                {:_id "_predicate" :name "person/age" :type "int"}
                                {:_id "_predicate" :name "person/email" :type "string" :unique true}]))
      (log "✓ Schema created")

      (Thread/sleep 100)

      ;; Block 3: Add users Alice, Bob, Charlie
      (log "\nBlock 3: Adding initial users...")
      (<!! (fdb/transact-async conn (str network "/" dbid)
                               [{:_id "person" :person/name "Alice" :person/age 30 :person/email "alice@test.com"}
                                {:_id "person" :person/name "Bob" :person/age 25 :person/email "bob@test.com"}
                                {:_id "person" :person/name "Charlie" :person/age 35 :person/email "charlie@test.com"}]))
      (log "✓ Added 3 users")

      (Thread/sleep 100)

      ;; Block 4: Update Alice's age
      (log "\nBlock 4: Updating Alice...")
      (<!! (fdb/transact-async conn (str network "/" dbid)
                               [{:_id ["person/email" "alice@test.com"] :person/age 31}]))
      (log "✓ Alice updated")

      (Thread/sleep 100)

      ;; Block 5: Delete Bob
      (log "\nBlock 5: Deleting Bob...")
      (<!! (fdb/transact-async conn (str network "/" dbid)
                               [{:_id ["person/email" "bob@test.com"] :_action "delete"}]))
      (log "✓ Bob deleted")

      (Thread/sleep 100)

      ;; Block 6: Update Charlie and add Dave
      (log "\nBlock 6: Updating Charlie and adding Dave...")
      (<!! (fdb/transact-async conn (str network "/" dbid)
                               [{:_id ["person/email" "charlie@test.com"] :person/age 36}
                                {:_id "person" :person/name "Dave" :person/age 28 :person/email "dave@test.com"}]))
      (log "✓ Charlie updated, Dave added")

      (Thread/sleep 100)

      ;; Block 7: Update Alice again
      (log "\nBlock 7: Updating Alice again...")
      (<!! (fdb/transact-async conn (str network "/" dbid)
                               [{:_id ["person/email" "alice@test.com"] :person/age 32}]))
      (log "✓ Alice updated again")

      (Thread/sleep 500) ;; Wait for indexing to complete

      ;; Query final state
      (log "\n=== Final State (Before Consolidation) ===")
      (let [db (<!! (fdb/db conn (str network "/" dbid)))
            persons (<!! (fdb/query-async db {:select ["*"] :from "person" :orderBy ["person/name"]}))]
        (log "Block: %d" (:block db))
        (log "Persons in database:")
        (doseq [person persons]
          (log "  - %s (age %d, email %s)"
               (:person/name person)
               (:person/age person)
               (:person/email person))))

      (log "\n=== Expected Final State After Consolidation ===")
      (log "Persons:")
      (log "  - Alice (age 32, email alice@test.com)")
      (log "  - Charlie (age 36, email charlie@test.com)")
      (log "  - Dave (age 28, email dave@test.com)")
      (log "\nBob should be GONE (deleted in block 5)")

      (log "\n=== Test Data Created Successfully ===")
      (log "Location: %s" base-path)
      (log "Network: %s" network)
      (log "DB ID: %s" dbid)
      (log "\nNext steps:")
      (log "1. Run rebuild script on this data")
      (log "2. Verify final state matches expected")
      (log "3. Confirm Bob is absent")

      {:base-path base-path
       :network network
       :dbid dbid
       :success true}

      (catch Exception e
        (log "ERROR: %s" (.getMessage e))
        (.printStackTrace e)
        {:success false :error (.getMessage e)})

      (finally
        (log "\nShutting down test server...")
        (server/shutdown system)
        (log "Shutdown complete")))))

(defn -main [& args]
  (let [opts (loop [remaining args
                     result {}]
               (if (empty? remaining)
                 result
                 (let [[arg val & rest] remaining]
                   (case arg
                     "--base-path" (recur rest (assoc result :base-path val))
                     "--network" (recur rest (assoc result :network val))
                     "--dbid" (recur rest (assoc result :dbid val))
                     "--help" (do
                               (println "Usage: clojure -M dev/ledger_toolkit/rebuild/create_test_data.clj [options]

Options:
  --base-path PATH   Test data directory (default: /tmp/fluree-test-rebuild)
  --network NAME     Network name (default: test)
  --dbid ID         Database ID (default: consolidation)
  --help            Show this help message

Example:
  clojure -M dev/ledger_toolkit/rebuild/create_test_data.clj
  clojure -M dev/ledger_toolkit/rebuild/create_test_data.clj --base-path /tmp/my-test")
                               (System/exit 0))
                     (do
                       (println "Unknown option:" arg)
                       (System/exit 1))))))]
    (let [result (create-test-ledger! opts)]
      (if (:success result)
        (System/exit 0)
        (System/exit 1)))))

;; For REPL usage
(comment
  (create-test-ledger! {:base-path "/tmp/fluree-test-rebuild"
                        :network "test"
                        :dbid "consolidation"})
  )

;; Run if invoked directly
(when (= *file* (first *command-line-args*))
  (apply -main (rest *command-line-args*)))
