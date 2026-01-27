;; Run directly by loading files instead of using require
(load-file "dev/ledger_toolkit/rebuild/index_rebuild.clj")
(require '[clojure.string :as str])

(defn parse-args
  "Parse command line arguments into options map"
  [args]
  (let [defaults {:max-mb 200
                  :use-file-threshold false
                  :validate true
                  :parallelism 4}]
    (loop [remaining args
           opts defaults]
      (if (empty? remaining)
        opts
        (let [[arg val & rest] remaining]
          (case arg
            "--base-path" (recur rest (assoc opts :base-path val))
            "--network" (recur rest (assoc opts :network val))
            "--dbid" (recur rest (assoc opts :dbid val))
            "--max-mb" (recur rest (assoc opts :max-mb (Integer/parseInt val)))
            "--parallelism" (recur rest (assoc opts :parallelism (Integer/parseInt val)))
            "--use-file-threshold" (recur rest (assoc opts :use-file-threshold true))
            "--no-validate" (recur rest (assoc opts :validate false))
            "--help" (do (println "Usage: clojure -M -m clojure.main dev/ledger_toolkit/rebuild/run_rebuild.clj -- [options]

Options:
  --base-path PATH         Root directory (parent of ledger/) containing blocks (required)
                          Example: If blocks are at /tmp/foo/ledger/network/db/block/
                                  Use: --base-path /tmp/foo
  --network NETWORK       Network name (required)
  --dbid DBID            Database ID (required)
  --max-mb MB            Maximum MB before indexing (default: 200)
  --parallelism N        Processing parallelism (default: 4)
  --no-validate          Skip block validation
  --help                 Show this help message

Examples:
  clojure -M -m clojure.main dev/ledger_toolkit/rebuild/run_rebuild.clj -- \\
    --base-path /path/to/data \\
    --network mynetwork \\
    --dbid mydb \\
    --max-mb 200")
                         (System/exit 0))
            (do
              (println "Unknown option:" arg)
              (System/exit 1))))))))

(defn -main
  [& args]
  (let [opts (parse-args args)]
    (when-not (and (:base-path opts) (:network opts) (:dbid opts))
      (println "Error: Missing required arguments")
      (println "Run with --help for usage information")
      (System/exit 1))

    (try
      (println "\n=== Index Rebuild Runner ===")
      (println "Configuration:" (pr-str opts))
      (println)

      (let [result (rebuild.index-rebuild/rebuild! opts)]
        (println "\n✅ Rebuild completed successfully!")
        (System/exit 0))

      (catch Exception e
        (println "\n❌ Rebuild failed:")
        (println (.getMessage e))
        (when-let [data (ex-data e)]
          (println "Error data:" (pr-str data)))
        (System/exit 1)))))

;; Handle different invocation methods
(cond
  ;; When invoked via: clojure -M -m clojure.main script.clj -- args
  ;; *command-line-args* will be: ("script.clj" "--" "arg1" "arg2" ...)
  (and (seq *command-line-args*)
       (str/ends-with? (first *command-line-args*) ".clj")
       (= "--" (second *command-line-args*)))
  (apply -main (drop 2 *command-line-args*))
  
  ;; When invoked via: clojure -M script.clj args  
  ;; *command-line-args* will be: ("arg1" "arg2" ...)
  (and (seq *command-line-args*)
       (not (str/ends-with? (str (first *command-line-args*)) ".clj")))
  (apply -main *command-line-args*)
  
  ;; Default: skip first arg if it's the script name
  (and (seq *command-line-args*)
       (str/ends-with? (str (first *command-line-args*)) ".clj"))
  (apply -main (rest *command-line-args*))
  
  ;; No args
  :else
  (-main))