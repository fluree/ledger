(ns fluree.db.ledger.storage.s3store
  (:refer-clojure :exclude [read list list*])
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [fluree.db.ledger.storage :refer [key->unix-path]]
            [fluree.db.ledger.storage.crypto :as crypto]
            [fluree.db.util.log :as log])
  (:import (java.io ByteArrayOutputStream Closeable)))

(set! *warn-on-reflection* true)

(defn bucket->url [bucket]
  (str "https://" bucket ".s3.amazonaws.com/"))


(defn key->url [{:keys [bucket]} k]
  (str (bucket->url bucket) k))


(defn strip-path-prefix
  "Strip path prefix from key"
  [path key]
  (if (str/starts-with? key path)
    (-> key (str/replace-first path "") (str/replace #"^/" ""))
    key))


(defn read
  "Returns a byte array of the data under key `k` (converted to a UNIX-style
  path with key->unix-path) of this store's S3 bucket. Returns an exception if
  an error occurs."
  [{:keys [client bucket]} base-path k]
  (with-open [out (ByteArrayOutputStream.)]
    (let [s3-key (key->unix-path base-path k)
          resp   (aws/invoke client {:op      :GetObject
                                     :request {:Bucket bucket, :Key s3-key}})]
      (log/debug "Reading" s3-key "in bucket" bucket)
      (if (:cognitect.anomalies/category resp)
        (if (:cognitect.aws.client/throwable resp)
          resp
          (ex-info "S3 read failed" {:response resp}))
        (let [{in :Body} resp]
          (when in
            (io/copy in out)
            (.close ^Closeable in)
            (.toByteArray out)))))))


(defn connection-storage-read
  "Returns an async fn that closes over the AWS client and base bucket URL for
  read ops."
  ([conn base-path] (connection-storage-read conn base-path nil))
  ([conn base-path encryption-key]
   (if encryption-key
     (fn [key]
       (async/thread
         (let [data (read conn base-path key)]
           (when data (crypto/decrypt-bytes data encryption-key)))))
     (fn [key]
       (async/thread
         (read conn base-path key))))))


(defn write
  "Writes the byte array `v` under the key `k` (converted to a UNIX-style path
  with key->unix-path) in this store's S3 bucket.
  Returns (NB: doesn't throw) an exception if writing fails."
  [{:keys [client bucket]} base-path k v]
  (let [body   (bytes v)
        s3-key (key->unix-path base-path k)
        _      (log/debug "Writing" (count body) "bytes to" s3-key "in bucket" bucket)
        resp   (aws/invoke client {:op      :PutObject
                                   :request {:Bucket bucket,
                                             :Key    s3-key,
                                             :Body   body}})]
    (if (:cognitect.anomalies/category resp)
      (if-let [err-msg (get-in resp [:Error :Message])]
        (ex-info (str "S3 write failed: " err-msg) resp)
        (ex-info "S3 write failed" resp))
      resp)))


(defn connection-storage-write
  "Returns an async fn that closes over the AWS client and base bucket URL for
  write ops."
  ([conn base-path] (connection-storage-write conn base-path nil))
  ([conn base-path encryption-key]
   (if encryption-key
     (fn [key data]
       (async/thread
         (let [enc-data (crypto/encrypt-bytes data encryption-key)]
           (write conn base-path key enc-data))))
     (fn [key data]
       (async/thread
         (write conn base-path key data))))))


(defn- s3-list
  [{:keys [client bucket]} path & [continuation-token]]
  ;; handy for debugging but probably don't want it in production
  ;(aws/validate-requests client true)
  (let [base-req {:op      :ListObjectsV2
                  :request {:Bucket bucket}}
        req      (cond-> base-req

                         (not= path "/")
                         (assoc-in [:request :Prefix] path)

                         continuation-token
                         (assoc-in [:request :ContinuationToken]
                                   continuation-token))
        resp     (aws/invoke client req)]
    (if (:cognitect.anomalies/category resp)
      (if-let [err (:cognitect.aws.client/throwable resp)]
        (throw err)
        (throw (ex-info "S3 list failed" {:response resp})))
      resp)))


(defn- list*
  ([conn path]
   (list* conn path nil))
  ([{:keys [bucket] :as conn} path continuation-token]
   (let [path' (or path "/")]
     (print ". ") (flush)
     (log/debug "Listing files in bucket" bucket "at" path')
     (let [{objects :Contents :as s3-result} (s3-list conn path'
                                                      continuation-token)
           bucket-url (partial key->url conn)
           result     (map (fn [{key :Key, size :Size}]
                             {:name (strip-path-prefix path' key)
                              :url  (bucket-url key)
                              :size size})
                           objects)]
       (log/trace (format "Objects found in %s bucket at %s: %s"
                          bucket path' result))
       (with-meta result (dissoc s3-result :Contents))))))


(defn list
  "Returns a lazy seq of data maps with keys `#{:name :size :url}` representing
  the files in this store's S3 bucket."
  ([conn & [path continuation-token]]
   (lazy-seq
     (let [s3-objects (list* conn path continuation-token)
           {truncated?         :IsTruncated
            continuation-token :NextContinuationToken} (meta s3-objects)]
       (if truncated?
         (concat s3-objects (list conn path continuation-token))
         s3-objects)))))


;; NB: Currently this realizes the entire lazy seq for listing buckets
;; or prefixes with >1000 objects. This shouldn't be much of an issue in our
;; current usage, but be aware of this if those requirements ever change.
;; - WSM 2021-10-28
(defn connection-storage-list
  "Returns an async fn that closes over the AWS client and base bucket URL for
  list ops."
  [conn base-path]
  (fn [path]
    (async/thread
      (list conn (->> path (io/file base-path) .toString)))))


(defn exists?
  "Returns `true` if there is an object under key `k` (converted to a
  UNIX-style path with key->unix-path) of this conn's S3 bucket."
  [conn base-path k]
  (let [s3-key (key->unix-path base-path k)
        list   (s3-list conn s3-key)
        result (< 0 (:KeyCount list))]
    (log/debug "Checking for existence of" s3-key "in bucket" (:bucket conn) "-" result)
    result))


(defn connection-storage-exists?
  "Returns an async fn that closes over the AWS client and base bucket URL for
  exists? ops."
  [conn base-path]
  (fn [key]
    (async/thread
      (exists? conn base-path key))))


(defn delete
  "Removes the object under key `k` (converted to a UNIX-style path with
  key->unix-path) of this store's S3 bucket."
  [{:keys [client bucket]} base-path k]
  (let [s3-key (key->unix-path base-path k)]
    (log/debug "Deleting" s3-key "in bucket" bucket)
    (aws/invoke client {:op      :DeleteObject
                        :request {:Bucket bucket,
                                  :Key    s3-key}})))


(defn connection-storage-delete
  "Returns an async fn that closes over the AWS client and base bucket URL for
  delete ops."
  [conn base-path]
  (fn [key]
    (async/thread
      (delete conn base-path key))))


(defn rename
  "Moves the object under key `k` to key `new-k` (both converted to UNIX-style
  paths with key->unix-path) of this store's S3 bucket."
  [{:keys [client bucket] :as conn} base-path k new-k]
  (let [old-s3-key (key->unix-path base-path k)
        new-s3-key (key->unix-path new-k)
        src        (str/join "/" [bucket old-s3-key])]
    (log/debug "Renaming" old-s3-key "to" new-s3-key "in bucket" bucket)
    (aws/invoke client {:op      :CopyObject
                        :request {:CopySource src,
                                  :Bucket     bucket,
                                  :Key        new-s3-key}})
    (delete conn base-path old-s3-key)))


(defn connection-storage-rename
  "Returns an async fn that closes over the AWS client and base bucket URL for
  rename ops."
  [conn base-path]
  (fn [old-key new-key]
    (async/thread
      (rename conn base-path old-key new-key))))


(defn connect
  "Initialize a new S3 storage client at the specified `bucket`."
  [bucket]
  (log/info "Connecting to S3 bucket" bucket)
  {:bucket bucket
   :client (aws/client {:api :s3})})


(defn close
  "Stops the connected client"
  [{:keys [client]}]
  (log/info "Closing connection to S3")
  (aws/stop client))

