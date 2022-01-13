(ns fluree.db.ledger.storage.filestore
  (:require [clojure.java.io :as io]
            [fluree.db.util.log :as log]
            [clojure.core.async :as async]
            [fluree.db.ledger.storage :refer [key->unix-path]]
            [fluree.db.ledger.storage.crypto :as crypto])
  (:import (java.io ByteArrayOutputStream FileNotFoundException File)))

(set! *warn-on-reflection* true)

(defn write-file
  [^bytes val path]
  (try
    (with-open [out (io/output-stream (io/file path))]
      (.write out val))
    (catch FileNotFoundException _
      (try
        (io/make-parents (io/file path))
        (with-open [out (io/output-stream (io/file path))]
          (.write out val))
        (catch Exception e
          (log/error (str "Unable to create storage directory: " path
                          " with error: " (.getMessage e) ". Permission issue?"))
          (log/error (str "Fatal Error, shutting down!"))
          (System/exit 1))))
    (catch Exception e (throw e))))


(defn read-file
  "Returns nil if file does not exist."
  [path]
  (try
    (with-open [xin  (io/input-stream path)
                xout (ByteArrayOutputStream.)]
      (io/copy xin xout)
      (.toByteArray xout))
    (catch FileNotFoundException _
      nil)
    (catch Exception e (throw e))))


(defn exists?
  [path]
  (.exists (io/file path)))


(defn safe-delete
  [path]
  (if (exists? path)
    (try
      (clojure.java.io/delete-file path)
      true
      (catch Exception e (str "exception: " (.getMessage e))))
    false))


(defn storage-read
  [base-path key]
  (log/trace "Storage read: " {:base-path base-path :key key})
  (->> key
       (key->unix-path base-path)
       read-file))


(defn storage-read-async
  [base-path key]
  (async/thread
    (storage-read base-path key)))


(defn storage-exists?
  [base-path key]
  (->> key
       (key->unix-path base-path)
       exists?))

(defn storage-exists?-async
  [base-path key]
  (async/thread
    (storage-exists? base-path key)))


(defn connection-storage-read
  "Default function for connection storage."
  ([base-path] (connection-storage-read base-path nil))
  ([base-path encryption-key]
   (if encryption-key
     (fn [key]
       (async/thread (let [data (storage-read base-path key)]
                      (when data (crypto/decrypt-bytes data encryption-key)))))
     (fn [key]
       (async/thread (storage-read base-path key))))))


(defn connection-storage-exists?
  "Default function for connection storage."
  [base-path]
  (fn [key]
    (storage-exists?-async base-path key)))


(defn storage-write
  [base-path key data]
  (log/trace "Storage write: " {:base-path base-path :key key :data data})
  (if (nil? data)
    (->> key
         (key->unix-path base-path)
         (safe-delete))
    (->> key
         (key->unix-path base-path)
         (write-file data))))


(defn storage-write-async
  [base-path key data]
  (async/thread
    (storage-write base-path key data)))


(defn connection-storage-write
  "Default function for connection storage writing."
  ([base-path] (connection-storage-write base-path nil))
  ([base-path encryption-key]
   (if encryption-key
     (fn [key data]
       (let [enc-data (crypto/encrypt-bytes data encryption-key)]
         (storage-write-async base-path key enc-data)))
     (fn [key data]
       (storage-write-async base-path key data)))))


(defn storage-rename
  [base-path old-key new-key]
  (.renameTo
    (io/file (key->unix-path base-path old-key))
    (io/file (key->unix-path base-path new-key))))


(defn storage-rename-async
  [base-path old-key new-key]
  (async/thread
    (storage-rename base-path old-key new-key)))


(defn connection-storage-rename
  [base-path]
  (fn [old-key new-key]
    (storage-rename-async base-path old-key new-key)))


(defn storage-delete
  [base-path key]
  (let [path (key->unix-path base-path key)]
    (safe-delete path)))


(defn storage-delete-async
  [base-path key]
  (async/thread
    (storage-delete base-path key)))


(defn connection-storage-delete
  [base-path]
  (fn [key]
    (storage-delete-async base-path key)))


(defn storage-list
  "Returns a sequence of data maps with keys `#{:name :size :url}` representing
  the files at `base-path/path`."
  [base-path path]
  (let [full-path (str base-path path)]
    (log/debug "storage-list full-path:" full-path)
    (when (exists? full-path)
      (let [files (-> full-path io/file file-seq)]
        (map (fn [^File f] {:name (.getName f), :url (.toURI f), :size (.length f)})
             files)))))


(defn storage-list-async
  [base-path path]
  (async/thread
    (storage-list base-path path)))


(defn connection-storage-list
  [base-path]
  (fn [path]
    (storage-list-async base-path path)))


(comment

  (def enc-key (byte-array (fluree.crypto.util/hash-string-key "testbp" 32)))
  (def test-ba (byte-array (fluree.crypto.util/hash-string-key "hi" 32)))

  enc-key
  test-ba

  (count (encrypt-bytes test-ba enc-key))

  (vec (encrypt-bytes test-ba enc-key))

  (= (vec test-ba)
     (-> test-ba
         (crypto/encrypt-bytes enc-key)
         (crypto/decrypt-bytes enc-key)
         (vec))))
