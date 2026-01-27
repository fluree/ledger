;; Simple runner for database verification
(load-file "dev/ledger_toolkit/rebuild/verify_rebuild.clj")

(defn parse-args [args]
  (let [defaults {}]
    (loop [remaining args
           opts defaults]
      (if (empty? remaining)
        opts
        (let [[arg val & rest] remaining]
          (case arg
            "--base-path" (recur rest (assoc opts :base-path val))
            "--network" (recur rest (assoc opts :network val))
            "--dbid" (recur rest (assoc opts :dbid val))
            "--help" (do (println "Usage: clojure -M dev/ledger_toolkit/rebuild/run_verify.clj [options]

Options:
  --base-path PATH    Root directory with rebuilt db (required)
  --network NETWORK   Network name (required)
  --dbid DBID        Database ID (required)
  --help             Show this help message

Example:
  clojure -M dev/ledger_toolkit/rebuild/run_verify.clj --base-path /path/to/data --network mynetwork --dbid mydb")
                         (System/exit 0))
            (do
              (println "Unknown option:" arg)
              (System/exit 1))))))))

(let [opts (parse-args *command-line-args*)]
  (when-not (and (:base-path opts) (:network opts) (:dbid opts))
    (println "Error: Missing required arguments (--base-path, --network, --dbid)")
    (println "Run with --help for usage information")
    (System/exit 1))
  (println "\n=== Running Database Verification ===")
  (println "Configuration:" (pr-str opts))
  (rebuild.verify-rebuild/verify! opts))