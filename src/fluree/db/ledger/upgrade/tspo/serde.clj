(ns fluree.db.ledger.upgrade.tspo.serde
  (:require [abracad.avro :as avro]
            [fluree.db.serde.avro :as serde]
            [fluree.db.serde.protocol :as serdeproto]))

(set! *warn-on-reflection* true)

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
                 {:name "spot", :type "fluree.FdbChildNode"}
                 {:name "psot", :type "fluree.FdbChildNode"}
                 {:name "post", :type "fluree.FdbChildNode"}
                 {:name "opst", :type "fluree.FdbChildNode"}
                 {:name "timestamp", :type [:null :long]}
                 {:name "prevIndex", :type [:null :long]}]}))

(defn deserialize-root-v0
  [db-root]
  (let [db-root* (avro/decode FdbRootDb-schema-v0 db-root)]
    (-> db-root*
        (assoc :ecount (serde/convert-ecount-integer-keys (:ecount db-root*))
               :stats  (serde/convert-stats-keywords (:stats db-root*))))))

(def FdbLeafNode-schema-v0
  (avro/parse-schema
   serde/avro-Flake
   {:type      :record
    :name      "FdbLeafNode"
    :namespace "fluree"
    :fields    [{:name "flakes", :type {:type  :array
                                        :items "fluree.Flake"}}
                {:name "his", :type [:null :string]}]}))

(defn deserialize-leaf-v0
  [leaf]
  (binding [avro/*avro-readers* serde/bindings]
    (avro/decode FdbLeafNode-schema-v0 leaf)))

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
    (deserialize-leaf-v0 leaf))
  (-serialize-garbage [_ garbage]
    (serde/serialize-garbage garbage))
  (-deserialize-garbage [_ garbage]
    (serde/deserialize-garbage garbage))
  (-serialize-db-pointer [_ pointer]
    (serde/serialize-db-pointer pointer))
  (-deserialize-db-pointer [_ pointer]
    (serde/deserialize-db-pointer pointer)))

(defrecord IntermediateSerializer []
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
