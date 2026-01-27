;; Wrapper to run the optimized incremental compressor (local copy)

(ns ledger-toolkit.compress-blocks
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [ledger-toolkit.block-processor.optimized-incremental :as processor]))

(defn parse-args [args]
  (let [defaults {:flakes-per-block 50000}
        {:keys [opts]} (loop [r args, acc defaults]
                         (if (empty? r)
                           {:opts acc}
                           (let [[k v & more] r]
                             (case k
                               "--source-base-path" (recur more (assoc acc :source-base-path v))
                               "--output-base-path" (recur more (assoc acc :output-base-path v))
                               "--network" (recur more (assoc acc :network v))
                               "--dbid" (recur more (assoc acc :dbid v))
                               "--flakes-per-block" (recur more (assoc acc :flakes-per-block (Integer/parseInt v)))
                               (do (println "Unknown option:" k) (System/exit 1))))))]
    (doseq [k [:source-base-path :output-base-path :network :dbid]]
      (when-not (get opts k)
        (println "Missing required option:" (name k))
        (System/exit 1)))
    opts))

(defn -main [& args]
  (let [{:keys [source-base-path output-base-path network dbid flakes-per-block]} (parse-args args)
        ;; Pass ledger roots to the processor; it will append /<network>/<dbid>/{block,root}
        source-ledger-root (str (io/file source-base-path "ledger"))
        output-ledger-root (str (io/file output-base-path "ledger"))]
    ;; Ensure network/db directories exist in the output root (generator will also mkdirs as needed)
    (.mkdirs (io/file output-ledger-root network dbid "block"))
    (println "=== Compressing Blocks ===")
    (println "Source ledger root:" source-ledger-root)
    (println "Output ledger root:" output-ledger-root)
    (println (str "Reading blocks from: " (io/file source-ledger-root network dbid "block")))
    (println (str "Writing blocks to:   " (io/file output-ledger-root network dbid "block")))
    (processor/process-blocks-optimized-incremental
      {:source-path source-ledger-root
       :output-path output-ledger-root
       :network network
       :dbid dbid
       :flakes-per-block flakes-per-block})
    (println "✓ Compression complete")))

(when (= *file* (first *command-line-args*))
  (apply -main (rest *command-line-args*)))

