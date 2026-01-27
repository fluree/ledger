(ns rebuild.full-rebuild
  "Complete rebuild workflow: clean RAFT state, rebuild indexes, verify"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn run-step!
  "Execute a step in the rebuild process"
  [step-name script-path & args]
  (println (str "\n" (apply str (repeat 60 "=")) "\n"
                "STEP: " step-name "\n"
                (apply str (repeat 60 "="))))
  
  (let [cmd (vec (concat ["clojure" "-M" script-path] args))
        _ (println "Running:" (str/join " " cmd))
        process (ProcessBuilder. cmd)
        _ (.redirectErrorStream process true)
        proc (.start process)
        reader (io/reader (.getInputStream proc))
        exit-code (atom nil)]
    
    ;; Read output in real-time
    (doseq [line (line-seq reader)]
      (println line))
    
    ;; Wait for completion
    (reset! exit-code (.waitFor proc))
    
    (if (zero? @exit-code)
      (println (str "\n✓ " step-name " completed successfully"))
      (throw (ex-info (str "Step failed: " step-name) 
                      {:step step-name :exit-code @exit-code})))))

(defn full-rebuild!
  "Execute the complete rebuild workflow"
  [{:keys [base-path network dbid server-name blocks-source-path]
    :or {server-name "myserver"}}]

  (when-not (and base-path network dbid blocks-source-path)
    (throw (ex-info "Missing required parameters: base-path, network, dbid, and blocks-source-path are all required"
                    {:base-path base-path :network network :dbid dbid :blocks-source-path blocks-source-path})))
  
  (println "\n==================================================")
  (println "        FLUREE LEDGER INDEX REBUILD")
  (println "==================================================")
  (println "Configuration:")
  (println "  Base path:" base-path)
  (println "  Network:" network)
  (println "  Database:" dbid)
  (println "  Server:" server-name)
  (println "  Blocks source:" blocks-source-path)
  (println "==================================================\n")
  
  (try
    ;; Step 1: Clean workspace
    (println "\n=== Step 1: Preparing workspace ===")
    (let [workspace (io/file base-path)]
      (when (.exists workspace)
        (println "Cleaning existing workspace...")
        ;; Clean RAFT files
        (let [group-dir (io/file workspace "group")]
          (when (.exists group-dir)
            (doseq [f (.listFiles group-dir)
                    :when (and (.isFile f)
                              (or (.endsWith (.getName f) ".raft")
                                  (.endsWith (.getName f) ".snapshot")))]
              (.delete f)
              (println "  Deleted:" (.getName f)))
            
            (let [snapshots-dir (io/file group-dir "snapshots")]
              (when (.exists snapshots-dir)
                (doseq [f (.listFiles snapshots-dir)
                        :when (.isFile f)]
                  (.delete f)
                  (println "  Deleted:" (.getName f)))))))
        
        ;; Clean index/root directories if they exist
        (let [ledger-dir (io/file workspace "ledger" network dbid)]
          (when (.exists ledger-dir)
            (doseq [subdir ["index" "root"]
                    :let [dir (io/file ledger-dir subdir)]]
              (when (.exists dir)
                (doseq [f (.listFiles dir)
                        :when (.isFile f)]
                  (.delete f))
                (println (str "  Cleaned " subdir " directory")))))))
      
      (.mkdirs workspace)
      (println "✓ Workspace prepared"))
    
    ;; Step 2: Initialize clean RAFT state
    (println "\n=== Step 2: Initializing clean RAFT state ===")
    (load-file "dev/ledger_toolkit/rebuild/init_raft_for_rebuild.clj")
    (rebuild.init-raft-for-rebuild/init-raft!
     {:base-path base-path
      :network network
      :dbid dbid
      :server-name server-name
      :start-index 1})
    (println "✓ RAFT state initialized")
    
    ;; Step 3: Run the rebuild
    (run-step! "Index Rebuild" 
               "dev/ledger_toolkit/rebuild/run_rebuild.clj"
               "--base-path" base-path
               "--network" network
               "--dbid" dbid
               "--blocks-path" blocks-source-path)
    
    ;; Step 4: Verify the rebuilt database
    (run-step! "Database Verification"
               "dev/ledger_toolkit/rebuild/run_verify.clj"
               "--base-path" base-path
               "--network" network
               "--dbid" dbid)
    
    (println "\n==================================================")
    (println "        ✅ REBUILD COMPLETED SUCCESSFULLY")
    (println "==================================================")
    (println "\nThe rebuilt database is ready at:" base-path)
    (println "\nYou can now:")
    (println "  1. Start a Fluree server pointing to this directory")
    (println "  2. Query the database using network:" network "db:" dbid)
    true
    
    (catch Exception e
      (println "\n==================================================")
      (println "        ❌ REBUILD FAILED")
      (println "==================================================")
      (println "Error:" (.getMessage e))
      (when-let [data (ex-data e)]
        (println "Details:" (pr-str data)))
      (.printStackTrace e)
      false)))

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
                     "--server-name" (recur rest (assoc result :server-name val))
                     "--blocks-source-path" (recur rest (assoc result :blocks-source-path val))
                     "--help" (do 
                               (println "Usage: clojure -M dev/ledger_toolkit/rebuild/full_rebuild.clj [options]

Options:
  --base-path PATH           Root directory for rebuild (required)
  --network NETWORK          Network name (required)
  --dbid DBID               Database ID (required)
  --blocks-source-path PATH  Path to source block files (required)
  --server-name NAME         Server name (default: myserver)
  --help                    Show this help message

Example:
  clojure -M dev/ledger_toolkit/rebuild/full_rebuild.clj \\
    --base-path /path/to/output \\
    --network mynetwork \\
    --dbid mydb \\
    --blocks-source-path /path/to/source/ledger/mynetwork/mydb/block")
                               (System/exit 0))
                     (do
                       (println "Unknown option:" arg)
                       (System/exit 1))))))]
    (if (full-rebuild! opts)
      (System/exit 0)
      (System/exit 1))))

;; Run if invoked directly
(when (= *file* (first *command-line-args*))
  (apply -main (rest *command-line-args*)))