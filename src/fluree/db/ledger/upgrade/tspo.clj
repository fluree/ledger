(ns fluree.db.ledger.upgrade.tspo
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.api :as fdb]
            [fluree.db.flake :as flake]
            [fluree.db.ledger.upgrade.tspo.serde :as tspo-serde]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto :refer [TxGroup]]
            [fluree.db.ledger.consensus.raft :as raft]
            [fluree.db.serde.avro :as serde]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.query.range :as range]
            [fluree.db.ledger.storage.filestore :as filestore]
            [clojure.core.async :as async :refer [>! <! chan close! go go-loop]]
            [clojure.tools.logging :as log]
            [abracad.avro :as avro]))

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
                 {:name "psot" :type "fluree.FdbChildNode"}
                 {:name "post" :type "fluree.FdbChildNode"}
                 {:name "opst" :type "fluree.FdbChildNode"}
                 {:name "timestamp" :type [:null :long]}
                 {:name "prevIndex" :type [:null :long]}]}))

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

(defrecord LegacyRaft [state-atom event-chan command-chan server this-server port
                       close raft raft-initialized open-api private-keys]
  TxGroup
  (-add-server-async [group server] (raft/add-server-async group server))
  (-remove-server-async [group server] (raft/remove-server-async group server))
  (-new-entry-async [group entry] (raft/new-entry-async group entry))
  (-local-state [group] (raft/local-state group))
  (-state [group] (raft/state group))
  (-is-leader? [group] (raft/is-leader? group))
  (-active-servers [group] (let [server-map   (txproto/kv-get-in group [:leases :servers])
                                 current-time (System/currentTimeMillis)]
                             (reduce-kv (fn [acc server lease-data]
                                          (if (>= (:expire lease-data) current-time)
                                            (conj acc server)
                                            acc))
                                        #{} server-map)))
  (-start-up-activities [group conn system shutdown join?] (raft/raft-start-up group conn system shutdown join?)))

(defn index-chunks
  [{:keys [conn block t network dbid] :as db} idx chunk-ch]
  (let [out (async/chan)
        db* (select-keys db [:conn :block :t :network :dbid])]
    (go-loop [statuses []
              db*      db]
      (if-let [chunk (<! chunk-ch)]
        (do (println "Indexing chunk" (-> statuses count inc))
            (let [cmp      (get index/default-comparators idx)
                  novelty  (apply flake/sorted-set-by cmp chunk)
                  index-db (assoc-in db* [:novelty idx] novelty)

                  {:keys [root] :as status}
                  (<! (indexing/refresh-root index-db #{} idx))]
              (when (>! out status)
                (recur (conj statuses status)
                       (assoc db* idx root)))))
        (do (println "Indexed" (-> statuses count inc) "chunks")
            (async/close! out))))
    out))

(defn log-statuses
  [idx status-ch]
  (go-loop []
    (if-let [status (<! status-ch)]
      (do (log/info "Upgrade status for index idx" status)
          (recur))
      [idx ::done])))

(defn spot->tspo
  [db chunk-size]
  (let [chunk-ch (async/chan 1 (partition-all chunk-size))]
    (-> (range/index-flake-stream db :spot)
        (async/pipe chunk-ch)
        (->> (index-chunks db :tspo))
        (log-statuses :tspo))))

(defn index-flakes
  [db chunk-size idx flake-ch]
  (let [chunk-ch (async/pipe flake-ch
                             (chan 1 (partition-all chunk-size)))]
    (->> chunk-ch
         (index-chunks db idx)
         (log-statuses idx))))

(defn leaf?
  [id-map]
  (contains? id-map :leaf))

(defn branch?
  [id-map]
  (contains? id-map :branch))

(defn history?
  [id-map]
  (contains? id-map :history))

(defn child->id-map
  [{:keys [id leaf]}]
  (if leaf
    {:leaf id}
    {:branch id}))

(defn id->history-map
  [id]
  {:history id})

(defn read-flakes
  [{:keys [conn] :as db} idx]
  (let [out  (chan)
        root-map (-> db
                     (get idx)
                     child->id-map)]
    (go-loop [q [root-map]]
      (if (seq q)
        (let [nxt  (peek q)
              rst  (pop q)]
          (println "reading flakes for index:" idx "[ nxt:" nxt "]")
          (if (index/leaf? nxt)
            (let [{:keys [flakes his]} (<! (storage/read-leaf conn (:id nxt)))]
              (async/onto-chan! flakes)
              (recur (conj q (id->history-map his))))
            (let [{:keys [children]} (<! (storage/read-branch conn (:id nxt)))
                  child-xf           (comp (map val)
                                           (map child->id-map))]
              (recur (into q child-xf children)))))
        (do (println "done reading flakes for index:" idx)
            (close! out))))
    out))

(defn convert-idx
  [db chunk-size idx]
  (->> (read-flakes db idx)
       (index-flakes db chunk-size idx)))

(defn convert
  [db chunk-size]
  (->> [:spot :psot :post :opst]
       (mapv (partial convert-idx db chunk-size))
       async/merge
       (async/reduce conj [])))

(defn read-blocks
  [conn network dbid]
  (let [out (async/chan)]
    (go
      (let [{last-block :block, :as db} (<! (fdb/db conn [network dbid]))]
        (loop [current-block 1]
          (if-not (> current-block last-block)
            (let [block (<! (storage/read-block conn network dbid current-block))]
              (>! out block)
              (recur (inc current-block)))
            (async/close! out)))))
    out))

(defn empty-root-map
  [network dbid]
  (reduce-kv (fn [m idx cmp]
               (assoc m idx (index/empty-branch network dbid cmp)))
             {} index/default-comparators))

(defn index-flake?
  [db idx f]
  (case idx
    :spot true
    :psot true
    :tspo true
    :opst (dbproto/-p-prop db :ref? (flake/p f))
    :post (dbproto/-p-prop db :idx? (flake/p f))))

(defn index-flakes
  [conn network dbid idx root t flakes]
  (let [cmp     (get index/default-comparators idx)
        novelty (apply flake/sorted-set-by cmp flakes)]
    (indexing/refresh-root conn network dbid
                           #::indexing {:idx          idx
                                        :root         root
                                        :novelty      novelty
                                        :t            t
                                        :remove-preds #{}})))

(defn index-blocks
  [conn network dbid block-ch]
  (go-loop [root-map (reduce-kv (fn [m idx cmp]
                                  (assoc m idx (index/empty-branch network dbid cmp)))
                                {} index/default-comparators)]
    (if-let [{:keys [t flakes] :as next-block} (<! block-ch)]
      (let [idx-map-ch (->> root-map
                            (map (fn [[idx root]]
                                   (index-flakes conn network dbid
                                                 idx root t flakes)))
                            (async/map (fn [& tallies]
                                         (reduce (fn [m {:keys [idx root]}]
                                                   (assoc m idx root))
                                                 {} tallies))))]
        (recur (<! idx-map-ch)))
      root-map)))
