(ns ledger-toolkit.block-processor.generator
  (:require [clojure.java.io :as io]
            [fluree.db.serde.avro :as avro]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.serde.protocol :as serde-proto]))

;; Subject ID formulas used by the transactor for block B:
;; block-t = -(2*B+1)
;; tx-t    = block-t + 1 = -(2*B)

(defn block->block-t
  "Calculate the block t-value (subject) for a given block number"
  [block-num]
  (- (+ (* 2 block-num) 1)))  ; -(2*B+1)

(defn block->tx-t
  "Calculate the transaction t-value for a given block number"
  [block-num]
  (inc (block->block-t block-num)))  ; block-t + 1 => -(2*B)

;; Predicate constants (from user feedback)
(def block-hash-pred 1)       ; _block/hash
(def block-prevhash-pred 2)   ; _block/prevHash
(def block-txns-pred 3)       ; _block/transactions
(def block-ledgers-pred 4)    ; _block/ledgers
(def block-instant-pred 5)    ; _block/instant
(def block-number-pred 6)     ; _block/number
(def tx-hash-pred 99)         ; _tx/hash
(def tx-id-pred 100)          ; _tx/id
(def tx-auth-pred 101)        ; _tx/auth
(def tx-authority-pred 102)   ; _tx/authority
(def tx-nonce-pred 103)       ; _tx/nonce

(defn generate-block-metadata
  "Generate the required metadata flakes for a block"
  [block-num {:keys [hash prev-hash instant tx-hash tx-id]}]
  (let [block-subject (block->block-t block-num)
        tx-t (block->tx-t block-num)]
    (cond-> []
      ;; Block metadata - all use block-subject as both subject and t
      true (conj [block-subject block-hash-pred hash block-subject true])
      true (conj [block-subject block-number-pred block-num block-subject true])
      true (conj [block-subject block-instant-pred instant block-subject true])
      true (conj [block-subject block-txns-pred tx-t block-subject true])
      (and prev-hash (> block-num 1)) (conj [block-subject block-prevhash-pred prev-hash block-subject true])

      ;; Transaction metadata - use tx-t as both subject and t
      tx-hash (conj [tx-t tx-hash-pred tx-hash tx-t true])
      tx-id (conj [tx-t tx-id-pred tx-id tx-t true]))))

(defn write-block-file
  [output-path network dbid block-num block-data user-count metadata-count]
  (let [;; Use correct subject formulas
        correct-block-t (block->block-t block-num)
        correct-tx-t (block->tx-t block-num)

        ;; Update block-data with correct t value (block-t)
        corrected-block-data (assoc block-data
                                    :t correct-block-t
                                    :block block-num)

        file-path (str output-path "/" network "/" dbid "/block/"
                       (util/zero-pad block-num 15) ".fdbd")
        file-dir (io/file (.getParent (io/file file-path)))
        _ (.mkdirs file-dir)
        serde (avro/avro-serde)
        serialized-data (serde-proto/-serialize-block serde corrected-block-data)]
    (with-open [os (io/output-stream file-path)]
      (.write os serialized-data))
    (println (format "Wrote block %d with %d flakes (%d user + %d metadata) to %s"
                     block-num (count (:flakes corrected-block-data)) user-count metadata-count file-path))
    (println (format "  Using block-t: %d, tx-t: %d" correct-block-t correct-tx-t))))

(defn generate-db-root
  [output-path network dbid last-block]
  (let [root-data {:ledger-id (str network "/" dbid)
                   :block last-block
                   :status "ready"
                   :message nil
                   :fork nil
                   :forkBlock nil}
        ;; Write to directory 'root' with numbered file, to match reindexer expectations
        root-dir (io/file output-path network dbid "root")
        _ (.mkdirs root-dir)
        root-file (io/file root-dir (str (util/zero-pad last-block 15) ".fdbd"))
        serde (avro/avro-serde)
        serialized-data (serde-proto/-serialize-db-pointer serde root-data)]
    (with-open [os (io/output-stream root-file)]
      (.write os serialized-data))
    (println (format "Generated database root pointer file %s pointing to block %d"
                     (.getPath root-file) last-block))))

