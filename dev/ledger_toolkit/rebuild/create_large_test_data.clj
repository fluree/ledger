(ns rebuild.create-large-test-data
  "Create a substantial test ledger with 100+ blocks, multiple retracts/adds to test consolidation"
  (:require [clojure.core.async :as async :refer [<!!]]
            [fluree.db.api :as fdb]
            [fluree.db.server :as server]
            [fluree.db.server-settings :as settings]
            [clojure.java.io :as io]))

(defn log [msg & args]
  (println (str "[" (java.time.LocalDateTime/now) "] " (apply format msg args))))

(defn build-config
  "Build test server configuration with low reindex thresholds to force multiple index points"
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
    :fdb-parallelism             4
    ;; Set low thresholds to force multiple index points during data creation
    :fdb-memory-reindex          "5mb"    ;; Trigger reindex at 5MB
    :fdb-memory-reindex-max      "10mb"})) ;; Force reindex at 10MB

(defn clean-directory!
  "Recursively delete directory contents"
  [base-path]
  (let [base-dir (io/file base-path)]
    (when (.exists base-dir)
      (log "Cleaning existing directory: %s" base-path)
      (doseq [f (reverse (file-seq base-dir))]
        (when (.isFile f)
          (.delete f)))
      (doseq [f (reverse (file-seq base-dir))]
        (when (and (.isDirectory f) (not= f base-dir))
          (.delete f))))))

(defn tx-with-delay!
  "Execute transaction with optional delay and error handling"
  [conn ledger-id tx delay-ms]
  (try
    (let [result (<!! (fdb/transact-async conn ledger-id tx))]
      (when (instance? Throwable result)
        (throw result))
      (when (pos? delay-ms)
        (Thread/sleep delay-ms))
      result)
    (catch Exception e
      (log "Transaction failed: %s" (.getMessage e))
      (throw e))))

(defn create-schema!
  "Create schema for test data"
  [conn ledger-id]
  (log "Creating schema...")
  (tx-with-delay! conn ledger-id
                  [{:_id "_collection" :name "person"}
                   {:_id "_predicate" :name "person/name" :type "string"}
                   {:_id "_predicate" :name "person/age" :type "int"}
                   {:_id "_predicate" :name "person/email" :type "string" :unique true}
                   {:_id "_predicate" :name "person/handle" :type "string" :unique true}
                   {:_id "_predicate" :name "person/status" :type "string"}
                   {:_id "_predicate" :name "person/score" :type "int"}

                   {:_id "_collection" :name "company"}
                   {:_id "_predicate" :name "company/name" :type "string" :unique true}
                   {:_id "_predicate" :name "company/employees" :type "int"}
                   {:_id "_predicate" :name "company/revenue" :type "bigint"}

                   {:_id "_collection" :name "product"}
                   {:_id "_predicate" :name "product/name" :type "string"}
                   {:_id "_predicate" :name "product/price" :type "float"}
                   {:_id "_predicate" :name "product/sku" :type "string" :unique true}
                   {:_id "_predicate" :name "product/inStock" :type "boolean"}]
                  100)
  (log "✓ Schema created"))

(defn create-initial-data!
  "Create initial batch of data (blocks 3-12)"
  [conn ledger-id]
  (log "\nCreating initial data (10 blocks)...")

  ;; Block 3-4: Create 50 people
  (doseq [batch (partition-all 25 (range 1 51))]
    (tx-with-delay! conn ledger-id
                    (mapv (fn [i]
                            {:_id "person"
                             :person/name (str "Person" i)
                             :person/email (str "person" i "@test.com")
                             :person/handle (str "user" i)
                             :person/age (+ 20 (mod i 40))
                             :person/status "active"
                             :person/score (* i 10)})
                          batch)
                    50))
  (log "✓ Created 50 people (2 blocks)")

  ;; Block 5-6: Create 30 companies
  (doseq [batch (partition-all 15 (range 1 31))]
    (tx-with-delay! conn ledger-id
                    (mapv (fn [i]
                            {:_id "company"
                             :company/name (str "Company" i)
                             :company/employees (+ 10 (* i 5))
                             :company/revenue (* i 1000000)})
                          batch)
                    50))
  (log "✓ Created 30 companies (2 blocks)")

  ;; Block 7-12: Create 100 products
  (doseq [batch (partition-all 20 (range 1 101))]
    (tx-with-delay! conn ledger-id
                    (mapv (fn [i]
                            {:_id "product"
                             :product/name (str "Product" i)
                             :product/sku (str "SKU" i)
                             :product/price (+ 9.99 (* i 0.5))
                             :product/inStock (even? i)})
                          batch)
                    50))
  (log "✓ Created 100 products (6 blocks)")
  (log "Initial data complete: 12 blocks total"))

(defn update-data-phase!
  "Update existing data (blocks 13-52, ~40 blocks)"
  [conn ledger-id]
  (log "\nUpdate phase (40 blocks)...")

  ;; Blocks 13-27: Update people's ages and scores (15 blocks)
  (doseq [i (range 1 51)]
    (when (zero? (mod i 4)) ; Update every 4th person per block
      (tx-with-delay! conn ledger-id
                      [{:_id ["person/email" (str "person" i "@test.com")]
                        :person/age (+ 25 (mod i 35))
                        :person/score (+ (* i 10) (rand-int 100))}]
                      20)))
  (log "✓ Updated people data (15 blocks)")

  ;; Blocks 28-42: Update companies (15 blocks)
  (doseq [i (range 1 31)]
    (when (zero? (mod i 2)) ; Update every other company
      (tx-with-delay! conn ledger-id
                      [{:_id ["company/name" (str "Company" i)]
                        :company/employees (+ 20 (* i 7))
                        :company/revenue (* i 1500000)}]
                      20)))
  (log "✓ Updated company data (15 blocks)")

  ;; Blocks 43-52: Update product prices (10 blocks)
  (doseq [batch (partition-all 10 (range 1 101))]
    (tx-with-delay! conn ledger-id
                    (mapv (fn [i]
                            {:_id ["product/sku" (str "SKU" i)]
                             :product/price (+ 19.99 (* i 0.75))})
                          batch)
                    50))
  (log "✓ Updated product prices (10 blocks)")
  (log "Update phase complete: 52 blocks total"))

(defn deletion-phase!
  "Delete and re-add data (blocks 53-82, ~30 blocks)"
  [conn ledger-id]
  (log "\nDeletion phase (30 blocks)...")

  ;; Blocks 53-62: Delete every 5th person (10 blocks)
  (doseq [i (range 5 51 5)]
    (tx-with-delay! conn ledger-id
                    [{:_id ["person/email" (str "person" i "@test.com")]
                      :_action "delete"}]
                    30))
  (log "✓ Deleted 10 people (10 blocks)")

  ;; Blocks 63-72: Re-add some people with different data (10 blocks)
  (doseq [i (range 5 46 5)]
    (tx-with-delay! conn ledger-id
                    [{:_id "person"
                      :person/name (str "Person" i "-v2")
                      :person/email (str "person" i "@test.com")
                      :person/handle (str "user" i "-new")
                      :person/age (+ 30 (mod i 30))
                      :person/status "reactivated"
                      :person/score (* i 20)}]
                    30))
  (log "✓ Re-added 9 people (10 blocks)")

  ;; Blocks 73-82: Delete 10 companies (10 blocks)
  (doseq [i (range 21 31)]
    (tx-with-delay! conn ledger-id
                    [{:_id ["company/name" (str "Company" i)]
                      :_action "delete"}]
                    30))
  (log "✓ Deleted 10 companies (10 blocks)")
  (log "Deletion phase complete: 82 blocks total"))

(defn final-modifications!
  "Final round of modifications (blocks 83-112, ~30 blocks)"
  [conn ledger-id]
  (log "\nFinal modifications (30 blocks)...")

  ;; Blocks 83-97: Toggle product stock status (15 blocks)
  (doseq [batch (partition-all 7 (range 1 101))]
    (tx-with-delay! conn ledger-id
                    (mapv (fn [i]
                            {:_id ["product/sku" (str "SKU" i)]
                             :product/inStock (odd? i)})
                          batch)
                    40))
  (log "✓ Updated product stock (15 blocks)")

  ;; Blocks 98-107: Update person status (10 blocks)
  (doseq [i (range 1 51)]
    (when (zero? (mod i 5))
      (tx-with-delay! conn ledger-id
                      [{:_id ["person/handle" (if (#{5 10 15 20 25 30 35 40 45} i)
                                                (str "user" i "-new")
                                                (str "user" i))]
                        :person/status (if (even? i) "inactive" "premium")}]
                      30)))
  (log "✓ Updated person status (10 blocks)")

  ;; Blocks 108-112: Final company updates (5 blocks)
  (doseq [i (range 1 21 4)]
    (tx-with-delay! conn ledger-id
                    [{:_id ["company/name" (str "Company" i)]
                      :company/employees (+ 50 (* i 10))
                      :company/revenue (* i 2000000)}]
                    50))
  (log "✓ Final company updates (5 blocks)")
  (log "Final modifications complete: 112 blocks total"))

(defn verify-final-state
  "Query and display final state"
  [conn ledger-id]
  (log "\n=== Verifying Final State ===")
  (let [db (<!! (fdb/db conn ledger-id))]
    (log "Final block number: %d" (:block db))
    (log "Final t: %d" (:t db))

    (let [person-count (<!! (fdb/query-async db {:selectOne "?count"
                                                  :where [["?p" "person/name" "?name"]]
                                                  :opts {:meta true}}))
          company-count (<!! (fdb/query-async db {:selectOne "?count"
                                                   :where [["?c" "company/name" "?name"]]
                                                   :opts {:meta true}}))
          product-count (<!! (fdb/query-async db {:selectOne "?count"
                                                   :where [["?pr" "product/sku" "?sku"]]
                                                   :opts {:meta true}}))]
      (log "\nFinal counts:")
      (log "  People: %d (expected: ~49-50, some deleted/re-added)"
           (or (get-in person-count [:meta :count]) 0))
      (log "  Companies: %d (expected: 20, 10 were deleted)"
           (or (get-in company-count [:meta :count]) 0))
      (log "  Products: %d (expected: 100)"
           (or (get-in product-count [:meta :count]) 0)))

    ;; Sample some data
    (let [sample-people (<!! (fdb/query-async db {:select ["*"]
                                                   :from "person"
                                                   :limit 3
                                                   :orderBy ["person/handle"]}))
          sample-company (<!! (fdb/query-async db {:select ["*"]
                                                    :from "company"
                                                    :limit 2}))]
      (log "\nSample people:")
      (doseq [p sample-people]
        (log "  - %s (age: %d, status: %s, score: %d)"
             (:person/name p)
             (:person/age p)
             (:person/status p)
             (:person/score p)))

      (log "\nSample companies:")
      (doseq [c sample-company]
        (log "  - %s (employees: %d, revenue: %d)"
             (:company/name c)
             (:company/employees c)
             (:company/revenue c))))))

(defn create-large-test-ledger!
  "Main function to create large test dataset"
  [{:keys [base-path network dbid]
    :or {base-path "/tmp/fluree-large-test-rebuild"
         network "test"
         dbid "large"}}]

  (log "=== Creating Large Test Dataset (100+ blocks) ===")
  (log "Base path: %s" base-path)
  (log "Network: %s, DB: %s" network dbid)
  (log "Configuration: Low reindex thresholds to force multiple index points\n")

  ;; Clean up existing data
  (clean-directory! base-path)

  (let [config (build-config base-path)
        _      (log "Starting Fluree server...")
        system (server/startup config)
        conn   (:conn system)]

    (try
      (log "Creating ledger: %s/%s" network dbid)
      @(fdb/new-ledger conn (str network "/" dbid))
      (log "✓ Ledger created (Block 1)")

      (let [ledger-id (str network "/" dbid)]
        ;; Block 2: Schema
        (create-schema! conn ledger-id)

        ;; Blocks 3-12: Initial data
        (create-initial-data! conn ledger-id)

        ;; Blocks 13-52: Updates
        (update-data-phase! conn ledger-id)

        ;; Blocks 53-82: Deletions and re-adds
        (deletion-phase! conn ledger-id)

        ;; Blocks 83-112: Final modifications
        (final-modifications! conn ledger-id)

        ;; Verify final state
        (Thread/sleep 2000) ;; Wait for any pending indexes
        (verify-final-state conn ledger-id))

      (log "\n=== Test Data Creation Complete ===")
      (log "Location: %s" base-path)
      (log "Expected blocks: ~112")
      (log "Expected index points: Multiple (due to low reindex thresholds)")
      (log "\nNext steps:")
      (log "1. Run: clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj --base-path %s --network %s --dbid %s"
           base-path network dbid)
      (log "2. Verify the rebuild creates consolidated indexes")
      (log "3. Check that queries return correct final state")
      (log "4. Confirm deleted entities (Person45, Companies 21-30) are absent")

      {:base-path base-path
       :network network
       :dbid dbid
       :success true}

      (catch Exception e
        (log "ERROR: %s" (.getMessage e))
        (.printStackTrace e)
        {:success false :error (.getMessage e)})

      (finally
        (log "\nShutting down server...")
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
                               (println "Usage: clojure -M dev/ledger_toolkit/rebuild/create_large_test_data.clj [options]

Options:
  --base-path PATH   Test data directory (default: /tmp/fluree-large-test-rebuild)
  --network NAME     Network name (default: test)
  --dbid ID         Database ID (default: large)
  --help            Show this help message

Example:
  clojure -M dev/ledger_toolkit/rebuild/create_large_test_data.clj
  clojure -M dev/ledger_toolkit/rebuild/create_large_test_data.clj --base-path /tmp/my-large-test")
                               (System/exit 0))
                     (do
                       (println "Unknown option:" arg)
                       (System/exit 1))))))]
    (let [result (create-large-test-ledger! opts)]
      (if (:success result)
        (System/exit 0)
        (System/exit 1)))))

;; For REPL usage
(comment
  (create-large-test-ledger! {:base-path "/tmp/fluree-large-test-rebuild"
                              :network "test"
                              :dbid "large"})
  )

;; Run immediately when loaded with clojure -M
(create-large-test-ledger! {:base-path "/tmp/fluree-large-test-rebuild"
                            :network "test"
                            :dbid "large"})
