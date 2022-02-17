(ns fluree.db.ledger.mutable
  (:require [fluree.db.api :as fdb]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.storage.core :as storage]
            [fluree.db.constants :as const]
            [fluree.db.util.log :as log]
            [fluree.db.ledger.reindex :as reindex]
            [fluree.db.util.core :as util]
            [fluree.db.flake :as flake]))

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
  (let [ts (-> (map #(flake/t %) flakes) set)]
    (assoc block
      :flakes (filterv #(let [f %]
                          (and (not ((set flakes) f))
                               (not (and (= const/$_tx:tx (flake/p f))
                                         (ts (flake/t f))))))
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


(defn- create-block-map
  "Creates a block-map from a vector of flakes"
  [db flakes]
  (go-try
    (loop [[flake & r] flakes
           t-map     {}
           block-map {}]
      (if flake
        (if-let [block (get t-map (flake/t flake))]
          (recur r t-map (update block-map block conj flake))
          (let [t     (flake/t flake)
                block (<? (time-travel/non-border-t-to-block db t))]
            (if (get block-map block)
              (recur r (assoc t-map t block) (update block-map block conj flake))
              (recur r (assoc t-map t block) (assoc block-map block [flake])))))
        block-map))))


(defn identify-hide-blocks-flakes
  [db {:keys [block hide purge] :as query-map}]
  (go-try (let [[pattern idx] (fdb/get-history-pattern (or hide purge))
                [block-start block-end]
                  (when block (<? (fdb/resolve-block-range db query-map)))
                from-t    (if (and block-start (not= 1 block-start))
                            (dec (:t (<? (time-travel/as-of-block db (dec block-start)))))
                            -1)
                to-t      (if block-end
                            (:t (<? (time-travel/as-of-block db block-end)))
                            (:t db))
                flakes    (<? (query-range/time-range db idx = pattern {:from-t from-t :to-t to-t}))
                _ (log/debug "identify-hide-blocks-flakes " {:flakes flakes})
                block-map (<? (go-try (loop [[flake & r] flakes
                                             t-map     {}
                                             block-map {}]
                                        (if flake
                                          (if-let [block (get t-map (flake/t flake))]
                                            (recur r t-map (update block-map block conj flake))
                                            (let [t     (flake/t flake)
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

(defn in?
  "true if coll contains elm"
  [coll elm]
  (some #(= elm %) coll))

(defn validate-purge-params
  "Validate provided subject id exists"
  [param]
  (cond
    (util/subj-ident? param)
    [param]

    (sequential? param)
    (if-not (seq param)
      (throw (ex-info (str "Please specify a valid subject. Provided: " param)
                      {:status 400
                       :error  :db/invalid-ident}))
      param)

    :else
    (throw (ex-info (str "Invalid subject. Provided: " param)
                    {:status 400
                     :error  :db/invalid-ident}))))

(defn get-component-preds-from-schema
  "Return ids of predicates designated as a component"
  [db]
  (->> db
       :schema
       :pred
       (reduce-kv
         (fn [m k v]
           (if (and (number? k) (:component v))
             (into m [k])
             m))
         [])
       set))

(defn identify-component-sids
  "Given a set of flakes, traverse schema to follow components"
  [preds flakes]
  (->> flakes
       (filter #(some preds %))
       (reduce (fn [m v]
                 (conj  m (flake/o v)))
               [])))

(defn identify-purge-graph
  "Return a vector of vectors
   * 1st element - a vector of flakes where provided ID is either the subject or object reference
   * 2nd element - a vector of sids for component references (similar to a cascade delete)"
  [db preds sid]
  (go-try
    (let [s-flakes  (->> (flake/parts->Flake [sid nil nil nil])
                         (query-range/search db)
                         <?)
          o-flakes  (->> (flake/parts->Flake [nil nil sid nil])
                         (query-range/search db)
                         <?)
          comp-sids (identify-component-sids preds s-flakes)]
      [(concat s-flakes o-flakes) comp-sids])))

(defn identify-purge-flakes
  "Return a vector of flakes to be purged based on the provided sids"
  [db preds sids]
  (go-try
    (loop [[sid & r] sids ;; sids to be processed; this list can grow if components are found
           sid-list sids  ;; complete list of sids targeted for purge
           flakes []]     ;; vector of flakes to be purged; can be duplicates
      (if sid
        (let [[so-flakes comp-sids] (<? (identify-purge-graph db preds sid))
              new-sids              (filter #(not (in? sid-list %)) comp-sids)]
          (recur (concat r new-sids)         ;; add any new sids to the "to be processed" vector
                 (concat sid-list new-sids)  ;; add any new sids to the vector of targeted sids
                 (concat flakes so-flakes))) ;; add flakes to purge vector
        (into [] (distinct) flakes)          ;; get rid of duplicate flakes
        ))))

(defn identify-purge-map
  "Generate the map of blocks->flakes to be purged from the ledger"
  [db preds sids]
  (go-try
    (let [flakes    (<? (identify-purge-flakes db preds sids))
          block-map (<? (create-block-map db flakes))]
      [block-map (count flakes)])))

  ;; TODO - this can take some time. Need a good way to handle this.
(defn purge-flakes
  "Purge all flakes referencing a subject.
  Currently, only a single subject can be processed at a time"
  [conn nw ledger {:keys [purge]}]
  (go-try
    (let [sids             (validate-purge-params purge)
          db               (<? (fdb/db conn (str nw "/" ledger)))
          preds            (get-component-preds-from-schema db)

          ;; TODO - this doesn't work if you've previously hidden the data.
          [block-map fuel] (<? (identify-purge-map db preds sids))
          _                (when (seq block-map)
                             (loop [[[block flakes] & r] block-map]
                               (<?? (purge-block conn nw ledger block flakes))
                               (if r
                                 (recur r)
                                 true)))
          ;; Pass in a custom ecount, so as not to have multiple items
          ;; with same subject id
          old-ecount (:ecount db)
          _          (<? (reindex/reindex conn nw ledger {:ecount old-ecount}))
          ;; Kick-off process to re-index full-text
          ;; TODO - consider amount of fuel for full-text indexing
          indexer    (-> conn :full-text/indexer :process)
          ft-res     (if indexer
                       (<? (indexer {:action :reset, :db db}))
                       :none)]
      {:status 200
       :result {:flakes-purged fuel
                :blocks        (keys block-map)
                :full-text     ft-res}
       :fuel   fuel})))

(comment
  (require '[fluree.db.serde.protocol :as serdeproto])
  (def conn (:conn user/system))

  (<?? (hide-flakes conn "fluree" "test" {:hide [87960930223081]}))
  (<?? (purge-flakes conn "fluree" "test" {:purge [422212465065991]}))

  (->> (<?? (storage/read conn "fluree_test_block_000000000000004:v2"))
       (serdeproto/-deserialize-block (:serializer conn))))
