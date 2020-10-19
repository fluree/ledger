(ns fluree.db.ledger.snapshot
  (:require [clojure.java.io :as io]
            [abracad.avro :as avro]
            [fluree.db.serde.avro :as avro-serde]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [clojure.core.async :as async]
            [fluree.db.session :as session]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.ledger.storage.filestore :as filestore]
            [fluree.db.ledger.consensus.dbsync2 :as dbsync2]
            [fluree.db.ledger.reindex :as reindex]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fdb]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.ledger.bootstrap :as bootstrap]
            [fluree.db.constants :as const]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.util.schema :as schema]
            [fluree.db.ledger.merkle :as merkle]
            [clojure.string :as str]))

(defn snapshot-file-dir
  [conn network dbid]
  (str (-> conn :meta :storage-directory) network "/" dbid "/snapshot/"))

(defn only-files
  "Filter a sequence of files/directories by the .isFile property of
  java.io.File"
  [network dbid file-s]
  (reduce (fn [acc file]
            (if (and (.isFile file) (str/includes? (.getName file) ".avro"))
              (conj acc (str network "/" dbid "/snapshot/" (.getName file)))
              acc)) [] file-s))

(defn list-snapshots
  [conn network dbid]
  (let [dir      (snapshot-file-dir conn network dbid)
        children (file-seq (io/file dir))]
    (only-files network dbid children)))

(defn snapshot-file-path
  [network dbid]
  (str network "/" dbid "/snapshot/" (System/currentTimeMillis) ".avro"))

(defn check-get-snapshot-file-path
  [conn network dbid file-key]
  (go-try (let [base-path (or (-> conn :meta :storage-directory) "data")
                file-path (str base-path file-key)]

            ;; ensure path to file exists
            (do (io/make-parents (io/file file-path))
                ;; ensure there is block data for the DB, else we'd possibly overwrite an existing snapshot with null data
                (when-not (<? (storage/read-block conn network dbid 1))
                  (throw (ex-info (str "Snapshot called for db: " network "/" dbid ", however there is no block data for it.")
                                  {:status 400
                                   :error  :db/invalid-snapshot})))
                file-path))))

(defn create-snapshot
  "Puts all blocks into a single log, AVRO encoded, and zipped.

  Stores on the local file system."
  ([conn network dbid] (create-snapshot conn network dbid (snapshot-file-path network dbid)))
  ([conn network dbid file-key]
   (go-try
     (let [file-path (<? (check-get-snapshot-file-path conn network dbid file-key))]
       (with-open [adf (avro/data-file-writer "snappy" avro-serde/FdbBlock-schema file-path)]
         (loop [block 1]
           (let [block-data (<? (storage/read-block conn network dbid block))]
             (when-not (nil? block-data)
               (.append adf block-data)
               (recur (inc block))))))
       file-key))))

;; TODO - allow snapshots to be located elsewhere, i.e. S3 buckets.
(defn get-snapshot-remote
  [conn storage-dir file-path]
  (if (filestore/exists? (str storage-dir file-path))
    (go-try true)
    (let [remote-copy-fn (dbsync2/remote-copy-fn* conn (-> conn :group :raft :other-servers) 3000)
          res-chan       (async/chan)]
      (remote-copy-fn file-path res-chan))))

(defn read-snapshot
  "Reads a sequence of blocks from an existing snapshot file."
  [file-path]
  (with-open [adf (avro/data-file-reader file-path)]
    (doall (seq adf))))


(defn add-meta-flakes
  [flakes txid timestamp block data-t block-t auth-id prevHash]
  (let [
        ;; Tx flakes first
        sorted              (-> (flake/sorted-set-by flake/cmp-flakes-block flakes) first)
        encoded-flakes      (->> sorted
                                 (mapv #(vector (.-s %) (.-p %) (.-o %) (.-t %) (.-op %) (.-m %)))
                                 (json/stringify))
        tx-hash             (crypto/sha3-256 encoded-flakes)
        tx-hash-flake       (flake/->Flake block-t const/$_tx:hash tx-hash block-t true nil)
        tx-flakes           (conj sorted tx-hash-flake)

        ;; Then block flakes
        prevHash-flake      (when prevHash
                              (flake/new-flake block-t const/$_block:prevHash prevHash block-t true))
        instant-flake       (flake/->Flake block-t const/$_block:instant timestamp block-t true nil)
        number-flake        (flake/->Flake block-t const/$_block:number block block-t true nil)
        tx-flakes'          (mapv #(flake/->Flake block-t const/$_block:transactions % block-t true nil)
                                  (range block-t data-t))
        block-flakes        (remove nil? (conj tx-flakes' prevHash-flake instant-flake number-flake))
        encoded-flakes      (->> block-flakes
                                 (mapv #(vector (.-s %) (.-p %) (.-o %) (.-t %) (.-op %) (.-m %)))
                                 (json/stringify))
        metadata-hash       (crypto/sha3-256 encoded-flakes)
        hash                (merkle/generate-merkle-root [tx-hash metadata-hash])
        hash-flake          (flake/->Flake block-t const/$_block:hash hash block-t true nil)
        metadata-hash-flake (flake/->Flake block-t const/$_tx:hash metadata-hash block-t true nil)
        all-flakes          (-> (into tx-flakes block-flakes)
                                (conj hash-flake)
                                (conj metadata-hash-flake))]
    {:block  block
     :t      block-t
     :flakes all-flakes
     :hash   hash}))

(defn generate-block
  [db flakes block data-t block-t auth-id prevHash]
  (go-try (let [timestamp (System/currentTimeMillis)
                cmd       (json/stringify {:db     (str (:network db) "/" (:dbid db)) :type "tx"
                                           :expire (+ timestamp 10000) :nonce timestamp})
                txid      (crypto/sha3-256 cmd)]
            (add-meta-flakes flakes txid timestamp block data-t block-t auth-id prevHash))))

(defn separate-flakes
  [flakes]
  (loop [[flake & r] flakes
         genesis-flakes []
         schema-flakes  []
         data-flakes    []
         last-flakes    []]
    (cond (and flake (> 0 (.-s flake)))
          (recur r genesis-flakes schema-flakes data-flakes last-flakes)

          (and flake (schema/is-genesis-flake? flake))
          (recur r (conj genesis-flakes (flake/change-t flake -1)) schema-flakes data-flakes last-flakes)

          (and flake (schema/is-schema-flake? flake)
               (not (#{const/$_predicate:spec const/$_predicate:txSpec const/$_collection:spec} (.-p flake))))
          (recur r genesis-flakes (conj schema-flakes (flake/change-t flake -3)) data-flakes last-flakes)

          ;; TODO - we apply these last, but smart function could still have issues if we have to split them across
          ;; multiple blocks, I suspect this to be a very rare occurrence
          (and flake (schema/is-schema-flake? flake)
               (#{const/$_predicate:spec const/$_predicate:txSpec const/$_collection:spec} (.-p flake)))
          (recur r genesis-flakes schema-flakes data-flakes (conj last-flakes (flake/change-t flake -5)))

          flake
          (recur r genesis-flakes schema-flakes (conj data-flakes (flake/change-t flake -5)) last-flakes)

          :else
          [genesis-flakes schema-flakes (concat data-flakes last-flakes)])))

;; TODO - what's the best way to do this??
(defn get-all-active-flakes
  [flakes]
  (loop [[flake & r] flakes
         acc []]
    (cond (not flake)
          acc

          (false? (.-op flake))
          (let [rest-flakes (filter #(not= (take 3 flake) (take 3 %)) r)
                acc         (filter #(not= (take 3 flake) (take 3 %)) acc)]
            (recur rest-flakes acc))

          :else
          (recur r (conj acc flake)))))

(defn get-auth-subid-strict
  [db auth]
  (go-try
    (or (<? (fdb/query-async (go-try db) {:selectOne "?id" :where [["?id", "_auth/id", auth]]}))
        (throw (ex-info (str "The auth id corresponding to the provided private key does not exist in the ledger. Provided: " auth) {:status 400 :error :db/invalid-auth})))))

(def max-block-size 100000)

(defn create-snapshot-no-history
  "Puts all blocks into a single log, AVRO encoded, and zipped.

  Stores on the local file system."
  ([conn network dbid]
   (let [private-key (-> conn :tx-private-key)]
     (create-snapshot-no-history conn network dbid private-key)))
  ([conn network dbid private-key]
   (create-snapshot-no-history conn network dbid private-key (snapshot-file-path network dbid)))
  ([conn network dbid private-key file-key]
   (go-try
     (let [file-path (<? (check-get-snapshot-file-path conn network dbid file-key))]
       (with-open [adf (avro/data-file-writer "snappy" avro-serde/FdbBlock-schema file-path)]
         (let [db            (<? (fdb/db conn (str network "/" dbid)))
               spot-nodes    (-> db :spot dbproto/-resolve <? :children vals)
               spot-flakes   (loop [[node & r] spot-nodes
                                    flakes []]
                               (if-not node
                                 flakes
                                 (recur r (concat flakes (-> (<? (dbproto/-resolve node)) :flakes)))))
               spot-novelty  (-> db :novelty :spot)
               spot-flakes*  (into spot-flakes spot-novelty)
               active-flakes (get-all-active-flakes spot-flakes*)
               [genesis-flakes schema-flakes data-flakes] (separate-flakes active-flakes)
               auth          (crypto/account-id-from-private private-key)
               auth-subid    (<? (get-auth-subid-strict db auth))

               ;; Write genesis block
               block-data-1  (<? (generate-block db genesis-flakes 1 -1 -2 auth-subid nil))


               gen-hash      (:hash block-data-1)
               _             (.append adf (dissoc block-data-1 :hash))
               ;; TODO - will potentially have to split schema flakes
               ;; Write schema block
               block-data-2  (<? (generate-block db schema-flakes 2 -3 -4 auth-subid gen-hash))
               schema-hash   (:hash block-data-2)
               _             (.append adf (dissoc block-data-2 :hash))
               split?        (> (flake/size-bytes data-flakes) max-block-size)]
           (if-not split?
             ;; If not split, just write the entire non-gen flakes at once
             ;; No need to change ts
             (let [block-data-3 (<? (generate-block db data-flakes 3 -5 -6 auth-subid schema-hash))]
               (.append adf (dissoc block-data-3 :hash)))

             ;; Else split the non-gen-flakes into > 1mb chunks
             ;; 1mb, so we don't have to worry about overestimating
             (loop [[flake & r] data-flakes
                    block-size    0
                    current-block []
                    block         3
                    data-t        -5
                    block-t       -6
                    prevHash      schema-hash]
               (if (not flake)
                 ;; There are no more flakes, write current block and exit loop
                 (let [block-data (<? (generate-block db current-block block data-t block-t auth-subid prevHash))]
                   (.append adf (dissoc block-data :hash)))
                 (let [size-flake    (flake/size-flake flake)
                       total-size*   (+ block-size size-flake)
                       current-block (conj current-block (flake/change-t flake data-t))]
                   (if (> total-size* max-block-size)
                     (let [block-data
                           (<? (generate-block db current-block block data-t block-t auth-subid prevHash))]
                       (do (.append adf (dissoc block-data :hash))
                           (recur r 0 [] (inc block) (dec data-t) (- data-t 2) (:hash block-data))))
                     (recur r total-size* current-block block data-t block-t prevHash)))))))) file-key))))


(defn create-db-from-snapshot
  [conn network dbid snapshot max-block command]
  (go-try
    (let [storage-dir   (-> conn :meta :storage-directory)
          _             (<?? (get-snapshot-remote conn storage-dir snapshot))
          max-block     (if (int? max-block) max-block Integer/MAX_VALUE)
          db-blocks     (read-snapshot (str storage-dir snapshot))
          highest-block (apply max (map :block db-blocks))
          to-block      (min highest-block max-block)]
      (doseq [block db-blocks]
        (if (<= (:block block) max-block)
          (<? (storage/write-block conn network dbid block))))

      (<? (txproto/register-genesis-block-async (:group conn) network dbid to-block))
      (let [indexed-db (<?? (reindex/reindex conn network dbid {:status "ready"}))
            txid       (crypto/sha3-256 (:cmd command))
            group      (-> indexed-db :conn :group)
            block      (:block indexed-db)
            _          (<?? (txproto/initialized-ledger-async group txid network dbid block nil (get-in indexed-db [:stats :indexed])))
            sess       (session/session conn (str network "/" dbid))
            _          (session/close sess)]
        indexed-db))))



