(ns ledger-toolkit.block-processor.optimized-incremental
  (:require [ledger-toolkit.block-processor.incremental-reader :as inc-reader]
            [ledger-toolkit.block-processor.optimized-state :as opt-state]
            [ledger-toolkit.block-processor.generator :as generator]
            [ledger-toolkit.block-processor.metadata :as metadata]
            [ledger-toolkit.block-processor.stats :as stats]
            [fluree.db.flake :as flake]
            [clojure.java.io :as io]))

(defn write-output-block-immediately
  [batch-info output-path network dbid output-block-num stats-dir running-totals last-block-hash]
  (let [start-time (System/currentTimeMillis)
        batch-facts (:batch-facts batch-info)
        source-metadata (:source-metadata batch-info)
        ;; Use mapping where block-t is the subject used by the transactor for previous hash lookups:
        ;; block-t = -(2B+1); tx-t = block-t + 1 = -(2B)
        block-t (- (+ (* output-block-num 2) 1))
        tx-t (inc block-t)
        assertion-flakes (map (fn [[s p o]] (flake/->Flake s p o tx-t true nil)) (:assertion-facts batch-facts))
        retraction-flakes (map (fn [[s p o]] (flake/->Flake s p o tx-t false nil)) (:retraction-facts batch-facts))
        user-flakes (concat assertion-flakes retraction-flakes)
        metadata-flakes (metadata/generate-block-metadata output-block-num (assoc source-metadata :block-t block-t :tx-t tx-t))
        all-flakes (concat user-flakes metadata-flakes)
        sorted-flakes (sort flake/cmp-flakes-block all-flakes)
        block-data {:block output-block-num :t block-t :flakes sorted-flakes}
        _ (generator/write-block-file output-path network dbid output-block-num block-data (count user-flakes) (count metadata-flakes))
        ;; Determine the hash we used for this block and store it for chaining in the next block
        used-block-hash (or (:block-hash source-metadata) (str "consolidated-block-" output-block-num))
        _ (reset! last-block-hash used-block-hash)
        processing-time (- (System/currentTimeMillis) start-time)
        block-stats (stats/create-block-stats output-block-num (count user-flakes) (count metadata-flakes) (count retraction-flakes) (count assertion-flakes) (:source-blocks batch-info) (:source-flakes-total batch-info) (:source-metadata-total batch-info) (:dropped-flakes batch-info) processing-time)]
    (swap! running-totals update :source-blocks + (count (:source-blocks batch-info)))
    (swap! running-totals update :output-blocks inc)
    (swap! running-totals update :dropped-flakes + (:dropped-flakes batch-info))
    (swap! running-totals update :source-flakes + (:source-flakes-total batch-info))
    (swap! running-totals update :output-flakes + (count user-flakes))
    (stats/write-block-stats stats-dir block-stats)
    (stats/print-detailed-block-stats block-stats @running-totals)))

(defn count-metadata-flakes [flakes]
  (count (filter metadata/is-metadata-flake? flakes)))

(defn initialize-processing-context [output-path stats-dir]
  (let [stats-dir-path (or stats-dir (str output-path "/stats"))]
    (.mkdirs (io/file output-path))
    (.mkdirs (io/file stats-dir-path))
    {:stats-dir-path stats-dir-path
     :initial-state (opt-state/initial-optimized-state)
     :skipped-blocks (atom [])
     :running-totals (atom {:source-blocks 0 :output-blocks 0 :dropped-flakes 0 :source-flakes 0 :output-flakes 0})
     :last-block-hash (atom nil)}))

(defn process-final-batch [{:keys [current-state current-source-blocks current-source-flakes current-source-metadata output-block-num source-metadata last-block-hash]} {:keys [output-path network dbid stats-dir-path running-totals]}]
  (let [final-facts (opt-state/get-optimized-facts current-state)
        dropped-flakes (:dropped-flakes-count current-state)
        metadata-with-chain (assoc source-metadata :prev-hash @last-block-hash)]
    (when (> (:total-facts final-facts) 0)
      (println (format "Final batch: %d facts (%d assertions, %d retractions, %d dropped)" (:total-facts final-facts) (count (:assertion-facts final-facts)) (count (:retraction-facts final-facts)) dropped-flakes))
      (write-output-block-immediately {:batch-facts final-facts :source-blocks current-source-blocks :source-flakes-total current-source-flakes :source-metadata-total current-source-metadata :dropped-flakes dropped-flakes :source-metadata metadata-with-chain} output-path network dbid output-block-num stats-dir-path running-totals last-block-hash))))

(defn write-final-stats [{:keys [session-start-time output-path network dbid stats-dir-path running-totals skipped-blocks]}]
  (generator/generate-db-root output-path network dbid (:output-blocks @running-totals))
  (let [session-end-time (System/currentTimeMillis)
        session-stats (stats/create-session-stats session-start-time (:source-blocks @running-totals) (:source-flakes @running-totals) (:output-blocks @running-totals) (:output-flakes @running-totals) (:dropped-flakes @running-totals) @skipped-blocks session-end-time)]
    (stats/write-session-stats stats-dir-path session-stats)
    (stats/print-session-summary session-stats)))

(defn process-block [block-data block-num current-state current-source-blocks current-source-flakes current-source-metadata output-block-num {:keys [output-path network dbid stats-dir-path running-totals flakes-per-block last-block-hash]}]
  (let [block-flakes (:flakes block-data)
        new-source-metadata (metadata/extract-block-metadata block-flakes block-num)
        metadata-count (count-metadata-flakes block-flakes)
        updated-state (opt-state/process-block-flakes-optimized current-state block-flakes)
        batch-stats (opt-state/get-batch-stats updated-state)
        new-source-blocks (conj current-source-blocks block-num)
        new-source-flakes (+ current-source-flakes (count block-flakes))
        new-source-metadata-count (+ current-source-metadata metadata-count)]
    (when (zero? (mod block-num 1000))
      (println (format "Processed block %d. Current batch: %d assertions, %d retractions, %d total" block-num (:assertions batch-stats) (:retractions batch-stats) (:total-facts batch-stats))))
    (if (>= (:total-facts batch-stats) flakes-per-block)
      (let [batch-facts (opt-state/get-optimized-facts updated-state)
            dropped-flakes (:dropped-flakes-count updated-state)
            committed-state (opt-state/commit-batch-to-state updated-state)
            metadata-with-chain (assoc new-source-metadata :prev-hash @last-block-hash)]
        (println (format "Outputting batch after block %d => Writing output block %d: %d facts (%d assertions, %d retractions, %d dropped)" block-num output-block-num (:total-facts batch-facts) (count (:assertion-facts batch-facts)) (count (:retraction-facts batch-facts)) dropped-flakes))
        (write-output-block-immediately {:batch-facts batch-facts :source-blocks new-source-blocks :source-flakes-total new-source-flakes :source-metadata-total new-source-metadata-count :dropped-flakes dropped-flakes :source-metadata metadata-with-chain} output-path network dbid output-block-num stats-dir-path running-totals last-block-hash)
        {:current-state committed-state :output-block-num (inc output-block-num) :current-source-blocks [] :current-source-flakes 0 :current-source-metadata 0 :last-source-metadata new-source-metadata :consecutive-missing 0})
      {:current-state updated-state
       :output-block-num output-block-num
       :current-source-blocks new-source-blocks
       :current-source-flakes new-source-flakes
       :current-source-metadata new-source-metadata-count
       :last-source-metadata new-source-metadata
       :consecutive-missing 0})))

(defn process-blocks-loop [{:keys [source-path output-path network dbid flakes-per-block]} context]
  (let [{:keys [stats-dir-path initial-state skipped-blocks running-totals last-block-hash]} context]
    (loop [current-state initial-state block-num 1 output-block-num 1 current-source-blocks [] current-source-flakes 0 current-source-metadata 0 last-source-metadata {} consecutive-missing 0 pending-gaps []]
      (if (>= consecutive-missing 10)
        {:current-state current-state :current-source-blocks current-source-blocks :current-source-flakes current-source-flakes :current-source-metadata current-source-metadata :output-block-num output-block-num :source-metadata last-source-metadata :last-block-hash last-block-hash}
        (if-let [block-data (inc-reader/read-block-file source-path network dbid block-num)]
          (let [_ (when (seq pending-gaps) (swap! skipped-blocks into pending-gaps))
                result (process-block block-data block-num current-state current-source-blocks current-source-flakes current-source-metadata output-block-num {:output-path output-path :network network :dbid dbid :stats-dir-path stats-dir-path :running-totals running-totals :flakes-per-block flakes-per-block :last-block-hash last-block-hash})]
            (recur (:current-state result) (inc block-num) (:output-block-num result) (:current-source-blocks result) (:current-source-flakes result) (:current-source-metadata result) (:last-source-metadata result) (:consecutive-missing result) []))
          (do
            (when (zero? consecutive-missing) (println (format "Block %d not found, checking ahead for more blocks..." block-num)))
            (recur current-state (inc block-num) output-block-num current-source-blocks current-source-flakes current-source-metadata last-source-metadata (inc consecutive-missing) (if (< consecutive-missing 10) (conj pending-gaps block-num) pending-gaps))))))))

(defn process-blocks-optimized-incremental [{:keys [source-path output-path network dbid flakes-per-block stats-dir] :or {flakes-per-block 50000}}]
  (let [session-start-time (System/currentTimeMillis)]
    (println "Starting optimized incremental block processing with statistics...")
    (println (format "Target: %d flakes per output block" flakes-per-block))
    (let [context (initialize-processing-context output-path stats-dir)]
      (println "Processing blocks incrementally with optimized retraction handling...")
      (let [final-state (process-blocks-loop {:source-path source-path :output-path output-path :network network :dbid dbid :flakes-per-block flakes-per-block} context)]
        (process-final-batch final-state {:output-path output-path :network network :dbid dbid :stats-dir-path (:stats-dir-path context) :running-totals (:running-totals context)})
        (write-final-stats {:session-start-time session-start-time
                            :output-path output-path
                            :network network
                            :dbid dbid
                            :stats-dir-path (:stats-dir-path context)
                            :running-totals (:running-totals context)
                            :skipped-blocks (:skipped-blocks context)})))))

