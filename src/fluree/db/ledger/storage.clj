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
         (keep block-file->block-long))))


(defn block-exists?
  "Returns core async channel with true if block for given ledger exists on disk."
  [{:keys [storage-exists] :as conn} network dbid block]
  (-> (storage/ledger-block-key network dbid block)
      storage-exists))
