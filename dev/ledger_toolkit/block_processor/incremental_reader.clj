(ns ledger-toolkit.block-processor.incremental-reader
  (:require [clojure.java.io :as io]
            [fluree.db.serde.avro :as avro]
            [fluree.db.util.core :as util]
            [fluree.db.serde.protocol :as serde-proto]
            [fluree.db.util.log :as log]))

(defn block-file-exists?
  [base-path network dbid block-num]
  (let [file-path (str base-path "/" network "/" dbid "/block/"
                       (util/zero-pad block-num 15) ".fdbd")
        file (io/file file-path)]
    (.exists file)))

(defn find-max-block-incremental
  [base-path network dbid]
  (let [missing-tolerance 10]
    (loop [block-num 1
           consecutive-missing 0
           max-found 0
           actual-gaps []]
      (cond
        (>= consecutive-missing missing-tolerance)
        (let [real-gaps (filter #(< % max-found) actual-gaps)]
          {:max-block max-found :skipped-blocks real-gaps :lookahead-checked consecutive-missing})
        :else
        (if (block-file-exists? base-path network dbid block-num)
          (do
            (when (zero? (mod block-num 100000))
              (println (format "Scanning blocks... currently at block %d" block-num)))
            (recur (inc block-num) 0 block-num actual-gaps))
          (if (> max-found 0)
            (do
              (when (zero? consecutive-missing)
                (println (format "Block %d not found, checking ahead..." block-num)))
              (recur (inc block-num) (inc consecutive-missing) max-found (conj actual-gaps block-num)))
            (recur (inc block-num) consecutive-missing max-found actual-gaps)))))))

(defn read-block-file
  [base-path network dbid block-num]
  (let [file-path (str base-path "/" network "/" dbid "/block/"
                       (util/zero-pad block-num 15) ".fdbd")
        file (io/file file-path)]
    (when (.exists file)
      (try
        (let [data (with-open [is (io/input-stream file)]
                     (let [bytes (byte-array (.length file))]
                       (.read is bytes)
                       bytes))
              serde (avro/avro-serde)]
          (serde-proto/-deserialize-block serde data))
        (catch Exception e
          (log/error e (str "Error reading block file: " file-path))
          nil)))))

(defn process-blocks-incrementally
  [processor-fn base-path network dbid & {:keys [chunk-size] :or {chunk-size 100000}}]
  (let [{:keys [max-block skipped-blocks]} (find-max-block-incremental base-path network dbid)]
    (println (format "Found blocks up to %d (with %d gaps)" max-block (count skipped-blocks)))
    (when (seq skipped-blocks)
      (println (format "Warning: Found gaps in block sequence: %s" (take 10 skipped-blocks))))
    (loop [start-block 1]
      (when (<= start-block max-block)
        (let [end-block (min (+ start-block chunk-size -1) max-block)]
          (println (format "Processing blocks %d to %d..." start-block end-block))
          (doseq [block-num (range start-block (inc end-block))]
            (when-not (contains? (set skipped-blocks) block-num)
              (when-let [block-data (read-block-file base-path network dbid block-num)]
                (processor-fn block-num block-data))))
          (recur (+ start-block chunk-size)))))
    {:total-blocks max-block :skipped-blocks skipped-blocks}))
