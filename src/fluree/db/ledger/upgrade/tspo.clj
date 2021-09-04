(ns fluree.db.ledger.upgrade.tspo
  (:require [fluree.db.flake :as flake]
            [fluree.db.serde.avro :as serde]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.ledger.storage.filestore :as filestore]
            [clojure.core.async :as async :refer [<! go go-loop]]
            [abracad.avro :as avro]))

(def FdbRootDb-schema-v0
  (avro/parse-schema
    serde/avro-FdbChildNode
    {:type      :record
     :name      "FdbRoot"
     :namespace "fluree"
     :fields    [{:name "dbid", :type :string}
                 {:name "block", :type :long}
                 {:name "t", :type :long}
                 {:name "ecount", :type {:type "map", :values :long}}
                 {:name "stats", :type {:type "map", :values :long}}
                 {:name "fork", :type [:null :string]}
                 {:name "forkBlock", :type [:null :long]}
                 {:name "spot", :type "fluree.FdbChildNode"} ;; spot
                 {:name "psot" :type "fluree.FdbChildNode"} ;; psot
                 {:name "post" :type "fluree.FdbChildNode"} ;; post
                 {:name "opst" :type "fluree.FdbChildNode"} ;; opst
                 {:name "timestamp" :type [:null :long]}
                 {:name "prevIndex" :type [:null :long]}]}))

(defn deserialize-root-v0
  [db-root]
  (let [db-root* (avro/decode FdbRootDb-schema-v0 db-root)]
    (assoc db-root*
           :ecount (serde/convert-ecount-integer-keys (:ecount db-root*))
           :stats  (serde/convert-stats-keywords (:stats db-root*)))))

(defrecord LegacySerializer []
  serdeproto/StorageSerializer
  (-serialize-block [_ block]
    (serde/serialize-block block))
  (-deserialize-block [_ block]
    (serde/deserialize-block block))
  (-serialize-db-root [_ db-root]
    (serde/serialize-db-root db-root))
  (-deserialize-db-root [_ db-root]
    (deserialize-root-v0 db-root))
  (-serialize-branch [_ branch]
    (serde/serialize-branch branch))
  (-deserialize-branch [_ branch]
    (serde/deserialize-branch branch))
  (-serialize-leaf [_ leaf-data]
    (serde/serialize-leaf leaf-data))
  (-deserialize-leaf [_ leaf]
    (serde/deserialize-leaf leaf))
  (-serialize-garbage [_ garbage]
    (serde/serialize-garbage garbage))
  (-deserialize-garbage [_ garbage]
    (serde/deserialize-garbage garbage))
  (-serialize-db-pointer [_ pointer]
    (serde/serialize-db-pointer pointer))
  (-deserialize-db-pointer [_ pointer]
    (serde/deserialize-db-pointer pointer)))
