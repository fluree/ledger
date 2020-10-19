(ns fluree.db.ledger.storage.filestore
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [fluree.db.util.async :as f-async]
            [clojure.core.async :as async]
            [fluree.crypto :as crypto])
  (:import (java.io ByteArrayOutputStream)
           (java.security SecureRandom)
           (java.nio ByteBuffer)))


(defn random-bytes
  "Generate a random byte array of provided size"
  [size]
  (let [seed (byte-array size)]
    (.nextBytes (SecureRandom.) seed)
    seed))


(defn encrypt-bytes
  [ba enc-key]
  (let [iv       (random-bytes 16)
        enc      (crypto/aes-encrypt ba iv enc-key :none)
        bb       (ByteBuffer/allocate (+ (count enc) 18))
        f-format (byte-array [2 1])]
    (doto bb
      (.put f-format)
      (.put iv)
      (.put enc)
      (.rewind))
    (.array bb)))


(defn decrypt-bytes
  [ba enc-key]
  (let [bb             (ByteBuffer/wrap ba)
        encrypted-flag (.get bb)
        encrypted?     (= 2 encrypted-flag)                 ;; first byte is encrypted? (2) or not (1)
        _              (when-not encrypted?
                         (throw (ex-info (str "Attempting to read encrypted file, but not flagged as encrypted.")
                                         {:status 500
                                          :error  :db/storage-error})))
        format         (.get bb)                            ;; second byte is file format (only 1 for now - only one format)
        iv             (byte-array 16)                      ;; initialization vector
        _              (.get bb iv)
        enc            (byte-array (.remaining bb))
        _              (.get bb enc)
        data           (try
                         (crypto/aes-decrypt enc iv enc-key :none :bytes)
                         (catch javax.crypto.BadPaddingException e
                           ;; incorrect decryption key used
                           (log/error (str "Files cannot be properly decoded. Have you changed the "
                                           ":fdb-encryption-secret config setting? Fatal error, exiting."))
                           (System/exit 1)))]
    data))


(defn write-file
  [val path]
  (try
    (with-open [out (io/output-stream (io/file path))]
      (.write out val))
    (catch java.io.FileNotFoundException e
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
    (catch java.io.FileNotFoundException e
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


(defn get-file-path
  "Given a base path and our key, creates a file path.

  We need to get rid of the encoding form on the key... for sure!"
  [base-path key]
  (let [key       (if (str/includes? key ".avro")
                    key (str key ".fdbd"))
        split-key (-> key
                      (str/split #"_"))]
    (apply io/file base-path split-key)))


(defn storage-read
  [base-path key]
  (log/trace "Storage read: " {:base-path base-path :key key})
  (->> (get-file-path base-path key)
       (read-file)))


(defn storage-read-async
  [base-path key]
  (async/thread
    (->> (get-file-path base-path key)
         (read-file))))


(defn storage-exists?
  [base-path key]
  (->> (get-file-path base-path key)
       (exists?)))

(defn storage-exists?-async
  [base-path key]
  (async/thread
    (->> (get-file-path base-path key)
         (exists?))))


(defn connection-storage-read
  "Default function for connection storage."
  ([base-path] (connection-storage-read base-path nil))
  ([base-path encryption-key]
   (if encryption-key
     (fn [key]
       (async/thread (let [data (storage-read base-path key)]
                       (when data (decrypt-bytes data encryption-key)))))
     (fn [key]
       (async/thread (storage-read base-path key))))))


(defn connection-storage-exists
  "Default function for connection storage."
  [base-path]
  (fn [key]
    (storage-exists?-async base-path key)))


(defn storage-write
  [base-path key data]
  (log/trace "Storage write: " {:base-path base-path :key key :data data})
  (if (nil? data)
    (->> (get-file-path base-path key)
         (safe-delete))
    (->> (get-file-path base-path key)
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
       (let [enc-data (encrypt-bytes data encryption-key)]
         (storage-write-async base-path key enc-data)))
     (fn [key data]
       (storage-write-async base-path key data)))))

(defn storage-rename
  [base-path old-key new-key]
  (.renameTo
    (get-file-path base-path old-key)
    (get-file-path base-path new-key)))

(defn storage-rename-async
  [base-path old-key new-key]
  (async/thread
    (storage-rename base-path old-key new-key)))

(defn connection-storage-rename
  [base-path]
  (fn [old-key new-key]
    (storage-rename-async base-path old-key new-key)))

(comment

  (def enc-key (byte-array (fluree.crypto.util/hash-string-key "testbp" 32)))
  (def test-ba (byte-array (fluree.crypto.util/hash-string-key "hi" 32)))

  enc-key
  test-ba

  (count (encrypt-bytes test-ba enc-key))

  (vec (encrypt-bytes test-ba enc-key))

  (= (vec test-ba)
     (-> test-ba
         (encrypt-bytes enc-key)
         (decrypt-bytes enc-key)
         (vec)))

  )