(ns fluree.db.ledger.storage.s3store
  (:refer-clojure :exclude [read list])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [cognitect.aws.client.api :as aws])
  (:import (java.io ByteArrayOutputStream)))

(defn read
  "Returns a byte array of the data under key `k` of this store's S3 bucket."
  [{:keys [client bucket]} k]
  (with-open [out (ByteArrayOutputStream.)]
    (let [{in :Body} (aws/invoke client {:op      :GetObject
                                         :request {:Bucket bucket, :Key k}})]
      (when in
        (io/copy in out)
        (.close in)
        (.toByteArray out)))))

(defn write
  "Writes the byte array `v` under the key `k` in this store's S3 bucket"
  [{:keys [client bucket]} k v]
  (let [body (bytes v)]
    (aws/invoke client {:op      :PutObject
                        :request {:Bucket bucket, :Key k, :Body body}})))

(defn list
  "Returns a sequence of data maps with keys
  `#{:Key, :LastModified, :ETag, :Size, :StorageTag}` representing the files in
  this store's S3 bucket."
  [{:keys [client bucket]}]
  (:Contents (aws/invoke client {:op      :ListObjectsV2
                                 :request {:Bucket bucket}})))

(defn exists?
  "Returns `true` if there is an object under key `k` of this store's S3 bucket."
  [store k]
  (->> store
       list
       (map :Key)
       (some #{k})
       boolean))

(defn delete
  "Removes the object under key `k` of this store's S3 bucket."
  [{:keys [client bucket]} k]
  (aws/invoke client {:op      :DeleteObject
                      :request {:Bucket bucket, :Key k}}))

(defn rename
  "Moves the object under key `k` to key `new-k` of this store's S3 bucket."
  [{:keys [client bucket] :as store} k new-k]
  (let [src (str/join "/" [bucket k])]
    (aws/invoke client {:op      :CopyObject
                        :request {:CopySource src, :Bucket bucket, :Key new-k}})
    (delete store k)))

(defn connect
  "Initialize a new S3 storage client at the specified `bucket`."
  [bucket]
  {:bucket bucket
   :client (aws/client {:api :s3})})

(defn close
  "Stops the connected client"
  [{:keys [client]}]
  (aws/stop client))
