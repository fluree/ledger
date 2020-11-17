(ns fluree.db.ledger.storage
  (:require [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (java.io File)))

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
