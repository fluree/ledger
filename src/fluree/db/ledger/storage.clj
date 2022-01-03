(ns fluree.db.ledger.storage
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [fluree.db.ledger.util :as util :refer [go-try <?]]
            [fluree.db.util.log :as log]
            [fluree.db.storage.core :as storage])
  (:import (java.io File)))

(set! *warn-on-reflection* true)

(defn key->unix-path
  "Given an optional base-path and our key, returns the storage path as a
  UNIX-style `/`-separated path.

  TODO: We need to get rid of the encoding form on the key... for sure!"
  ([key] (key->unix-path nil key))
  ([base-path key]
   (let [key       (if (or (str/ends-with? key ".avro")
                           (str/ends-with? key ".snapshot"))
                     key
                     (str key ".fdbd"))
         split-key (str/split key #"_")
         file      (apply io/file base-path split-key)]
     (.toString ^File file))))


(defn block-storage-path
  "For a ledger server, will return the relative storage path it is using for blocks for a given ledger."
  [network dbid]
  (io/file network dbid "block"))


(defn block-file?
  "Given a file map as output by :storage-list function on the connection,
  returns true if it matches the block file format.

  This is used for directory listings that presume all the files should be block files,
  but possible end-user put a different file in there manually."
  [file-map]
  (-> file-map
      :name
      (re-matches #".+/[0-9]+\.fdbd$")))


(defn block-files
  "Returns async chan containing single list of block files on disk for a given ledger"
  [{:keys [storage-list] :as conn} network dbid]
  (go-try
    (->> (block-storage-path network dbid)
         storage-list
         <?
         (filter block-file?))))


(defn- block-file->block-long
  "Takes a block file in format as returned by :storage-list function on the connection
  and returns a long integer block if the file size is > 0 and it matches the block file
  format."
  [block-file-map]
  (when (> (:size block-file-map) 0)
    (some->> (:name block-file-map)
             (re-find #"\D?([0-9]+)\.fdbd$")
             ^String second
             Long/parseLong)))

(defn blocks
  "Returns a list of blocks on disk as long integers whose file sizes are > 0
  and match the prescribed block file path format."
  [{:keys [storage-list] :as conn} network dbid]
  (go-try
    (->> (block-storage-path network dbid)
         storage-list
         <?
         (keep block-file->block-long)
         sort)))


(defn block-exists?
  "Returns core async channel with true if block for given ledger exists on disk."
  [{:keys [storage-exists] :as conn} network dbid block]
  (-> (storage/ledger-block-key network dbid block)
      storage-exists))

(defn index-root-exists?
  "Returns core async channel with true if index root exist for given ledger on disk."
  [{:keys [storage-exists] :as conn} network dbid index-point]
  (-> (storage/ledger-root-key network dbid index-point)
      storage-exists))


(defn- ledger-file-data
  "Returns map of data for ledger if it matches."
  [file]
  (let [path (str (:url file))]
    (when (> (:size file) 0)
      (if-let [[_ network ledger block] (re-find #"([^/]+)/([^/]+)/block/(\d+)\.fdbd" path)]
        {:network network
         :ledger  ledger
         :block   (Long/parseLong block)}
        (if-let [[_ network ledger index] (re-find #"([^/]+)/([^/]+)/root/(\d+)\.fdbd" path)]
          {:network network
           :ledger  ledger
           :index   (Long/parseLong index)}
          nil)))))


(defn- missing-sequence
  "Checks a number sequence to ensure it is contiguous, returns first missing sequence
  as a two-tuple of, e.g. [5 7] indicating 6 is missing."
  [num-seq]
  (let [sorted (sort num-seq)]
    (loop [[next & r] (rest sorted)
           prev (first sorted)]
      (when next
        (if (not= (inc prev) next)
          [prev next]
          (recur r next))))))


(defn ledgers
  "Returns a core async channel with a map whose keys are [network ledger]
  and values are a map of {:block x :index y :indexes [1 42 ...]} to represent the largest block
  and index point on disk along with all index points."
  [{:keys [storage-list] :as conn}]
  (go-try
    (let [all-files  (<? (storage-list nil))
          ;; creates map of {[network ledger] {:block (1 2 8 5 2 ...) :index (1 42 6)}} that needs to find contiguous blocks/indexes still
          ledger-map (reduce
                       (fn [acc file]
                         (if-let [{:keys [network ledger block index]} (ledger-file-data file)]
                           (update acc [network ledger]
                                   (fn [state]
                                     (if block
                                       (assoc state :block (conj (:block state) block))
                                       (assoc state :index (conj (:index state) index)))))
                           acc))
                       {} all-files)]
      (reduce-kv
        (fn [acc ledger {:keys [block index]}]
          (when-let [[block-gap-prev block-gap-next] (missing-sequence block)]
            (throw (ex-info (str "Ledger has a gap in blocks. Either find missing blocks, or delete blocks to recover. "
                                 "Ledger: " (first ledger) "/" (second ledger) ". Blocks missing between: " block-gap-prev
                                 " and " block-gap-next ".")
                            {:status 500 :error :db/invalid-file})))
          (let [max-block (apply max block)
                max-index (apply max index)]
            (when (> max-index max-block)
              (throw (ex-info (str "Ledger index point is beyond the farthest block, if blocks cannot be recovered, "
                                   "remove index point, and possibly redindex ledger. "
                                   "Ledger: " (first ledger) "/" (second ledger) ". Index point: " max-index
                                   " last block: " max-block ".")
                              {:status 500 :error :db/invalid-file})))
            (assoc acc ledger {:block   max-block
                               :index   max-index
                               :indexes (into [] (sort index))})))
        {}
        ledger-map))))
