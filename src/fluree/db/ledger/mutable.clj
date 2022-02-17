(ns fluree.db.ledger.mutable
  (:require [fluree.db.api :as fdb]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.storage.core :as storage]
            [fluree.db.constants :as const]
            [fluree.db.util.log :as log]
            [fluree.db.ledger.reindex :as reindex])
  (:import (fluree.db.flake Flake)))

(set! *warn-on-reflection* true)

(defn next-version
  [conn storage-block-key]
  (go-try (loop [n 1]
            (let [version-key (str storage-block-key "--v" n)]
              (if (<? (storage/exists? conn version-key))
                (recur (inc n))
                n)))))

(defn filter-flakes-from-block
  [block flakes]
  (let [ts (-> (map #(.-t ^Flake %) flakes) set)]
    (assoc block
      :flakes (filterv #(let [^Flake f %]
                          (and (not ((set flakes) f))
                               (not (and (= const/$_tx:tx (.-p f))
                                         (ts (.-t f))))))
                       (:flakes block)))))

(defn hide-block
  [conn nw ledger block flakes]
  (go-try (let [current-block (<? (storage/read-block conn nw ledger block))
                new-block     (filter-flakes-from-block current-block flakes)
                old-block-key (storage/ledger-block-file-path nw ledger block)
                new-block-key (str old-block-key "--v" (<? (next-version conn old-block-key)))
                _             (<?? (storage/rename conn old-block-key new-block-key))
                _             (<?? (storage/write-block conn nw ledger new-block))]
            (log/debug (str "Flakes hidden in block " block))
            true)))

(defn purge-block
  [conn nw ledger block flakes]
  (go-try (let [conn-delete   (:storage-delete conn)
                block-key     (storage/ledger-block-key nw ledger block)
                current-block (<? (storage/read-block conn nw ledger block))
                _             (<?? (conn-delete block-key))
                new-block     (filter-flakes-from-block current-block flakes)
                _             (<?? (storage/write-block conn nw ledger new-block))
                numVersions   (-> (next-version conn block-key) <? dec)]
            (loop [version numVersions]
              (when (> 0 version)
                (let [versioned-block     (<? (storage/read-block-version conn nw ledger block version))
                      new-versioned-block (filter-flakes-from-block versioned-block flakes)]
                  (<?? (storage/write-block-version conn nw ledger new-versioned-block version)))
                (recur (dec version))))
            (log/warn (str "Flakes purged from block " block))
            true)))

(defn identify-hide-blocks-flakes
  [db {:keys [block hide purge] :as query-map}]
  (go-try (let [[pattern idx] (fdb/get-history-pattern (or hide purge))
                [block-start block-end] (when block (<? (fdb/resolve-block-range db query-map)))
                from-t    (if (and block-start (not= 1 block-start))
                            (dec (:t (<? (time-travel/as-of-block db (dec block-start))))) -1)
                to-t      (if block-end
                            (:t (<? (time-travel/as-of-block db block-end))) (:t db))
                flakes    (<? (query-range/time-range db idx = pattern {:from-t from-t :to-t to-t}))
                block-map (<? (go-try (loop [[^Flake flake & r] flakes
                                             t-map     {}
                                             block-map {}]
                                        (if flake
                                          (if-let [block (get t-map (.-t flake))]
                                            (recur r t-map (update block-map block conj flake))
                                            (let [t     (.-t flake)
                                                  block (<? (time-travel/non-border-t-to-block db t))]
                                              (if (get block-map block)
                                                (recur r (assoc t-map t block) (update block-map block conj flake))
                                                (recur r (assoc t-map t block) (assoc block-map block [flake])))))
                                          block-map))))]
            [block-map (count flakes)])))

;; Use a pattern [s p o] to declare flakes to hide.
;; Hide both additions and retractions, as well as the _tx/tx for that block
(defn hide-flakes
  [conn nw ledger query-map]
  ;; TODO - this can take some time. Need a good way to handle this.
  (go-try (let [db         (<? (fdb/db conn (str nw "/" ledger)))
                [block-map fuel] (<? (identify-hide-blocks-flakes db query-map))
                _          (when (not-empty block-map)
                             (loop [[[block flakes] & r] block-map]
                               (<?? (hide-block conn nw ledger block flakes))
                               (if r (recur r)
                                     true)))
                ; Pass in a custom ecount, so as not to have multiple items
                ; with same subject id
                old-ecount (:ecount db)
                _          (<? (reindex/reindex conn nw ledger {:ecount old-ecount}))]
            {:status 200
             :result {:flakes-hidden fuel
                      :blocks        (keys block-map)}
             :fuel   fuel})))

(defn purge-flakes
  [conn nw ledger query-map]
  ;; TODO - this can take some time. Need a good way to handle this.
  (go-try (let [db         (<? (fdb/db conn (str nw "/" ledger)))
                ;; TODO - this doesn't work if you've previously hidden the data.
                [block-map fuel] (<? (identify-hide-blocks-flakes db query-map))
                _          (when (not-empty block-map)
                             (loop [[[block flakes] & r] block-map]
                               (<?? (purge-block conn nw ledger block flakes))
                               (if r (recur r)
                                     true)))
                ;; Pass in a custom ecount, so as not to have multiple items
                ;; with same subject id
                old-ecount (:ecount db)
                _          (<? (reindex/reindex conn nw ledger {:ecount old-ecount}))]
            {:status 200
             :result {:flakes-purged fuel
                      :blocks        (keys block-map)}
             :fuel   fuel})))

(comment
  (require '[fluree.db.serde.protocol :as serdeproto])
  (def conn (:conn user/system))

  (<?? (hide-flakes conn "fluree" "test" {:hide [87960930223081]}))
  (<?? (purge-flakes conn "fluree" "test" {:purge [422212465065991]}))

  (->> (<?? (storage/read conn "fluree_test_block_000000000000004:v2"))
       (serdeproto/-deserialize-block (:serializer conn))))
