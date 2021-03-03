(ns fluree.db.ledger.transact.tx-meta
  (:require [fluree.db.util.async :refer [<? go-try merge-into? channel?]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.util.tx :as tx-util]))

;; to handle transaction-related flakes

(def ^:const system-predicates
  "List of _tx predicates that only Fluree can assign, any user attempt to modify these should throw."
  #{const/$_tx:id const/$_tx:tx const/$_tx:sig const/$_tx:hash
    const/$_tx:auth const/$_tx:authority const/$_tx:nonce
    const/$_tx:error const/$_tx:tempids
    const/$_block:number const/$_block:instant
    const/$_block:hash const/$_block:prevHash
    const/$_block:transactions const/$_block:ledgers
    const/$_block:sigs})


(defn tx-meta-flakes
  ([tx-state] (tx-meta-flakes tx-state nil))
  ([{:keys [auth authority txid tx-string signature nonce t] :as tx-state} error-str]
   (let [tx-flakes [(flake/->Flake t const/$_tx:id txid t true nil)
                    (flake/->Flake t const/$_tx:tx tx-string t true nil)
                    (flake/->Flake t const/$_tx:sig signature t true nil)]]
     (cond-> tx-flakes
             auth (conj (flake/->Flake t const/$_tx:auth auth t true nil)) ;; note an error transaction may not have a valid auth
             authority (conj (flake/->Flake t const/$_tx:authority authority t true nil))
             nonce (conj (flake/->Flake t const/$_tx:nonce nonce t true nil))
             error-str (conj (flake/->Flake t const/$_tx:error error-str t true nil))))))


(defn generate-hash-flake
  "Generates transaction hash, and returns the hash flake.
  Flakes must already be sorted in proper block order."
  [flakes {:keys [t] :as tx-state}]
  (let [tx-hash (tx-util/gen-tx-hash flakes true)]
    (flake/->Flake t const/$_tx:hash tx-hash t true nil)))

(defn add-tx-hash-flake
  "Adds tx-hash flake to db by adding directly into novelty.
  This assumes the tx-hash is not indexed - if that is modified it could create an issue
  but only within a block transaction - between blocks we get a full new db from raft state and
  drop the db-after we create inside a transaction."
  [db tx-hash-flake]
  (let [flake-bytes (flake/size-flake tx-hash-flake)]
    (-> db
        (update-in [:novelty :spot] conj tx-hash-flake)
        (update-in [:novelty :psot] conj tx-hash-flake)
        (update-in [:novelty :psot] conj tx-hash-flake)
        (update-in [:stats :size] + flake-bytes)
        (update-in [:stats :flakes] inc))))

(defn generate-tx-error-flakes
  "If an error occurs, returns a set of flakes for the 't' that represents error."
  [db t tx-map command error-str]
  (go-try
    (let [db         (dbproto/-rootdb db)
          {:keys [auth authority nonce txid]} tx-map
          tx-state   {:txid      txid
                      :auth      (<? (dbproto/-subid db ["_auth/id" auth] false))
                      :authority (when authority (<? (dbproto/-subid db ["_auth/id" authority] false)))
                      :tx-string (:cmd command)
                      :signature (:sig command)
                      :nonce     nonce}

          flakes     (->> (tx-meta-flakes tx-state error-str)
                          (flake/sorted-set-by flake/cmp-flakes-block))
          hash-flake (generate-hash-flake flakes tx-state)]
      {:t      t
       :hash   (.-o hash-flake)
       :flakes (conj flakes hash-flake)})))