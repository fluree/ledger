(ns rebuild.test-rebuild
  "Test and validation utilities for index rebuild"
  (:require [rebuild.index-rebuild :as rebuild]
            [clojure.java.io :as io]
            [fluree.db.api :as fdb]
            [clojure.core.async :refer [<!!]]))

(defn validate-block-files
  "Validate block files exist and are readable"
  [base-path network dbid]
  (let [block-dir (io/file base-path "ledger" network dbid "block")]
    (println "\n=== Validating Block Files ===")
    (println "Block directory:" (.getPath block-dir))
    
    (if-not (.exists block-dir)
      (do
        (println "❌ Block directory does not exist!")
        false)
      
      (let [block-files (rebuild/get-block-files block-dir)]
        (println "Found" (count block-files) "block files")
        
        (if (empty? block-files)
          (do
            (println "❌ No block files found!")
            false)
          
          (do
            (println "\nFirst 5 blocks:")
            (doseq [bf (take 5 block-files)]
              (println (format "  Block %015d: %.2f MB"
                               (:block-num bf)
                               (/ (:size bf) (* 1024.0 1024.0)))))
            
            (println "\nLast 5 blocks:")
            (doseq [bf (take-last 5 block-files)]
              (println (format "  Block %015d: %.2f MB"
                               (:block-num bf)
                               (/ (:size bf) (* 1024.0 1024.0)))))
            
            (println (format "\nTotal size: %.2f MB"
                             (rebuild/calculate-cumulative-size block-files)))
            
            ;; Check for gaps
            (try
              (rebuild/validate-blocks block-files)
              (println "✅ No gaps in block sequence")
              true
              (catch Exception e
                (println "❌ Block validation failed:" (.getMessage e))
                (when-let [data (ex-data e)]
                  (println "Missing blocks:" (:missing-blocks data)))
                false))))))))

(defn test-rebuild
  "Test rebuild with specified blocks"
  [base-path network dbid]
  (println "\n=== Testing Rebuild ===")
  (println "WARNING: Block files may contain nil values that could cause issues")
  (println "If errors occur, wait for regenerated blocks without nil values\n")

  ;; First validate
  (if-not (validate-block-files base-path network dbid)
    (println "\n⚠️ Validation failed. Skipping rebuild.")

    ;; Proceed with rebuild
    (do
      (println "\nStarting rebuild with 200MB threshold...")
      (println "This may take a while depending on the size of the data...\n")

      (try
        (let [result (rebuild/rebuild! {:base-path base-path
                                        :network network
                                        :dbid dbid
                                        :max-mb 200
                                        :validate false ;; Already validated
                                        :parallelism 4})]
          (println "\n✅ Test completed successfully!")
          (println "Result:" (pr-str result)))

        (catch Exception e
          (println "\n❌ Rebuild failed:")
          (println (.getMessage e))

          ;; Check if it's a nil value issue
          (when (or (re-find #"nil" (.getMessage e))
                    (re-find #"NullPointer" (.getMessage e)))
            (println "\n⚠️ This appears to be a nil value issue.")
            (println "Please regenerate blocks without nil values and proper sort order."))

          (when-let [data (ex-data e)]
            (println "\nError details:" (pr-str data))))))))

(defn dry-run
  "Perform a dry run - just validate without rebuilding"
  [base-path network dbid]
  (println "\n=== Dry Run - Validation Only ===")
  (validate-block-files base-path network dbid))

(comment
  ;; Test rebuild
  (test-rebuild "/path/to/data" "mynetwork" "mydb")

  ;; Just validate without rebuilding
  (dry-run "/path/to/data" "mynetwork" "mydb")

  ;; Test with different settings
  (rebuild/rebuild! {:base-path "/path/to/data"
                     :network "mynetwork"
                     :dbid "mydb"
                     :max-mb 100  ;; Smaller chunks
                     :parallelism 2}))