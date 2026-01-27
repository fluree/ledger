(ns ledger-toolkit.block-processor.stats
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]))

(defn create-block-stats [block-num user-flakes metadata-flakes retractions assertions source-blocks source-flakes-total source-metadata-total dropped-flakes processing-time-ms]
  {:block-number block-num
   :timestamp (System/currentTimeMillis)
   :iso-timestamp (str (java.time.Instant/now))
   :flakes {:total (+ user-flakes metadata-flakes)
            :user user-flakes
            :metadata metadata-flakes
            :assertions assertions
            :retractions retractions
            :dropped dropped-flakes}
   :source {:blocks {:start (first source-blocks)
                     :end (last source-blocks)
                     :count (count source-blocks)}
            :flakes {:total source-flakes-total
                     :metadata source-metadata-total
                     :user (- source-flakes-total source-metadata-total)}}
   :processing {:time-ms processing-time-ms
                :throughput-flakes-per-sec (if (> processing-time-ms 0)
                                             (long (/ (* source-flakes-total 1000) processing-time-ms))
                                             0)}})

(defn create-session-stats [start-time total-source-blocks total-source-flakes total-output-blocks total-output-flakes total-retractions-eliminated skipped-blocks end-time]
  (let [duration-ms (- end-time start-time)
        duration-sec (/ duration-ms 1000.0)]
    {:session {:start-timestamp start-time :end-timestamp end-time :duration-ms duration-ms :duration-human (format "%.2f seconds" duration-sec)}
     :source {:total-blocks total-source-blocks :total-flakes total-source-flakes}
     :output {:total-blocks total-output-blocks :total-flakes total-output-flakes :compression-ratio (if (> total-source-flakes 0) (format "%.2f%%" (* 100.0 (- 1.0 (/ (double total-output-flakes) (double total-source-flakes))))) "0%")}
     :optimization {:retractions-eliminated total-retractions-eliminated :average-flakes-per-block (if (> total-output-blocks 0) (long (/ total-output-flakes total-output-blocks)) 0)}
     :performance {:throughput-blocks-per-sec (if (> duration-sec 0) (format "%.2f" (double (/ total-source-blocks duration-sec))) "0") :throughput-flakes-per-sec (if (> duration-sec 0) (format "%.0f" (double (/ total-source-flakes duration-sec))) "0")}
     :skipped-blocks {:count (count skipped-blocks) :blocks (take 100 skipped-blocks)}}))

(defn write-block-stats [stats-dir block-stats]
  (let [file-path (str stats-dir "/block-" (format "%015d" (:block-number block-stats)) "-stats.json")]
    (.mkdirs (io/file stats-dir))
    (spit file-path (json/generate-string block-stats {:pretty true}))
    (println (format "Stats written to %s" file-path))
    block-stats))

(defn write-session-stats [stats-dir session-stats]
  (let [file-path (str stats-dir "/session-stats.json")]
    (spit file-path (json/generate-string session-stats {:pretty true}))
    (println (format "Session stats written to %s" file-path))
    session-stats))

(defn print-detailed-block-stats [block-stats running-totals]
  (let [source-flakes (get-in block-stats [:source :flakes :total])
        output-flakes (get-in block-stats [:flakes :total])
        dropped-flakes (get-in block-stats [:flakes :dropped])
        compression-pct (if (> source-flakes 0) (format "%.1f%%" (* 100.0 (- 1.0 (/ (double output-flakes) (double source-flakes))))) "N/A")
        throughput (get-in block-stats [:processing :throughput-flakes-per-sec])]
    (println "================================================================================")
    (println (format "BLOCK %d GENERATED" (:block-number block-stats)))
    (println "--------------------------------------------------------------------------------")
    (println (format "  Output:  %d flakes (%d user, %d metadata)" output-flakes (get-in block-stats [:flakes :user]) (get-in block-stats [:flakes :metadata])))
    (println (format "           %d assertions, %d retractions, %d dropped" (get-in block-stats [:flakes :assertions]) (get-in block-stats [:flakes :retractions]) dropped-flakes))
    (println (format "  Source:  Blocks %d-%d (%d blocks)" (get-in block-stats [:source :blocks :start]) (get-in block-stats [:source :blocks :end]) (get-in block-stats [:source :blocks :count])))
    (println (format "           %d flakes (%d user, %d metadata)" source-flakes (get-in block-stats [:source :flakes :user]) (get-in block-stats [:source :flakes :metadata])))
    (println (format "  Metrics: Compression: %s | Processing: %dms | Throughput: %d flakes/sec" compression-pct (get-in block-stats [:processing :time-ms]) throughput))
    (when running-totals
      (println "--------------------------------------------------------------------------------")
      (println (format "  Running Totals: %d blocks processed | %d output blocks | %d flakes dropped" (:source-blocks running-totals) (:output-blocks running-totals) (:dropped-flakes running-totals))))
    (println "================================================================================")))

(defn print-session-summary [session-stats]
  (let [source-blocks (get-in session-stats [:source :total-blocks])
        source-flakes (get-in session-stats [:source :total-flakes])
        output-blocks (get-in session-stats [:output :total-blocks])
        output-flakes (get-in session-stats [:output :total-flakes])
        dropped-flakes (get-in session-stats [:optimization :retractions-eliminated])
        compression-ratio (get-in session-stats [:output :compression-ratio])
        duration (get-in session-stats [:session :duration-human])
        skipped-count (get-in session-stats [:skipped-blocks :count] 0)]
    (println "")
    (println "================================================================================")
    (println "PROCESSING COMPLETE")
    (println "================================================================================")
    (println (format "  Total Source: %d blocks, %d flakes" source-blocks source-flakes))
    (println (format "  Total Output: %d blocks, %d flakes" output-blocks output-flakes))
    (println (format "  Compression:  %s" compression-ratio))
    (println (format "  Optimization: %d flakes dropped (assertion+retraction pairs)" dropped-flakes))
    (when (> skipped-count 0)
      (println (format "  Gaps found:   %d missing blocks in sequence" skipped-count)))
    (println (format "  Duration:     %s" duration))
    (println "================================================================================")))

