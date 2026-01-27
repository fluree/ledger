(ns ledger-toolkit.block-processor.metadata
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]))

(def block-metadata-predicates
  #{const/$_block:hash const/$_block:prevHash const/$_block:transactions const/$_block:ledgers const/$_block:instant const/$_block:number const/$_block:sigs})

(def transaction-metadata-predicates
  #{const/$_tx:hash const/$_tx:id const/$_tx:auth const/$_tx:authority const/$_tx:nonce const/$_tx:altId const/$_tx:doc const/$_tx:tx const/$_tx:sig const/$_tx:tempids const/$_tx:error})

(def all-metadata-predicates (into block-metadata-predicates transaction-metadata-predicates))

(defn is-metadata-flake? [flake]
  (< (.-s flake) 0))

(defn filter-user-data [flakes]
  (remove is-metadata-flake? flakes))

(defn generate-block-metadata [block-number source-metadata]
  (let [block-hash (or (:block-hash source-metadata) (str "consolidated-block-" block-number))
        timestamp (or (:timestamp source-metadata) (System/currentTimeMillis))
        tx-hash (or (:tx-hash source-metadata) (str "consolidated-tx-" block-number))
        ;; If provided, use explicit block-t and tx-t; else default to prior scheme
        block-t (or (:block-t source-metadata) (- (+ (* block-number 2) 1)))
        tx-t (or (:tx-t source-metadata) (inc block-t))
        block-subject block-t
        tx-subject tx-t
        base-flakes [(flake/->Flake block-subject const/$_block:hash block-hash block-t true nil)
                     (flake/->Flake block-subject const/$_block:number block-number block-t true nil)
                     (flake/->Flake block-subject const/$_block:instant timestamp block-t true nil)
                     (flake/->Flake block-subject const/$_block:transactions tx-subject block-t true nil)
                     (flake/->Flake tx-subject const/$_tx:hash tx-hash tx-t true nil)
                     (flake/->Flake tx-subject const/$_tx:id (str "consolidated-tx-id-" block-number) tx-t true nil)
                     (flake/->Flake tx-subject const/$_tx:nonce timestamp tx-t true nil)]]
    (if-let [previous-hash (:prev-hash source-metadata)]
      (cons (flake/->Flake block-subject const/$_block:prevHash previous-hash block-t true nil) base-flakes)
      base-flakes)))

(defn extract-block-metadata [flakes block-num]
  (let [metadata-flakes (filter is-metadata-flake? flakes)
        ;; We don’t know exact t mapping of source; try to recover hashes only
        block-hash (some #(when (= (.-p %) const/$_block:hash) (.-o %)) metadata-flakes)
        prev-hash (some #(when (= (.-p %) const/$_block:prevHash) (.-o %)) metadata-flakes)
        timestamp (some #(when (= (.-p %) const/$_block:instant) (.-o %)) metadata-flakes)
        tx-hash (some #(when (= (.-p %) const/$_tx:hash) (.-o %)) metadata-flakes)]
    {:block-hash block-hash :prev-hash prev-hash :timestamp timestamp :tx-hash tx-hash}))

(defn extract-sample-hashes [flakes]
  (let [metadata-flakes (filter is-metadata-flake? flakes)
        tx-hashes (keep #(when (= (.-p %) const/$_tx:hash) (.-o %)) metadata-flakes)
        block-hashes (keep #(when (= (.-p %) const/$_block:hash) (.-o %)) metadata-flakes)]
    {:tx-hashes tx-hashes :block-hashes block-hashes :sample-hash (or (first tx-hashes) (first block-hashes))}))

(defn process-flakes-with-metadata [flakes & {:keys [add-metadata? block-number t-value previous-hash] :or {add-metadata? false}}]
  (let [hash-info (extract-sample-hashes flakes)
        user-data (filter-user-data flakes)
        new-metadata (when add-metadata?
                       (generate-block-metadata block-number {:block-hash (:sample-hash hash-info) :prev-hash previous-hash}))]
    {:user-data user-data :metadata new-metadata :hash-info hash-info :combined (if new-metadata (concat user-data new-metadata) user-data)}))
