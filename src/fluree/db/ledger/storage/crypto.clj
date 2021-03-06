(ns fluree.db.ledger.storage.crypto
  (:require [clojure.tools.logging :as log]
            [fluree.crypto :as crypto])
  (:import (java.nio ByteBuffer)
           (java.security SecureRandom)
           (javax.crypto BadPaddingException)))

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
                         (catch BadPaddingException e
                           ;; incorrect decryption key used
                           (log/error (str "Files cannot be properly decoded. Have you changed the "
                                           ":fdb-encryption-secret config setting? Fatal error, exiting."))
                           (System/exit 1)))]
    data))

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
