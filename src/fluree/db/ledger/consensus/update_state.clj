(ns fluree.db.ledger.consensus.update-state
  (:require [fluree.db.constants :as constants]
            [clojure.string :as str]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.event-bus :as event-bus]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [clojure.set :as set]))

(set! *warn-on-reflection* true)

(defn get-pool
  "Returns the entry from the capped pool in the `state` map specified by
  `pool-path` with key `k`. Returns the entire pool if `k` is not provided."
  ([state pool-path]
   (get-in state pool-path))
  ([state pool-path k]
   (get-in state (conj pool-path k))))

(defn put-pool
  "Adds `v` to a capped command pool map within the `state` map specified by
  `pool-path` under the key `k` if and only if there are less than
  `max-pool-size` entries previously in the specified pool."
  [state max-size pool-path k v]
  (update-in state pool-path (fn [pool]
                               (if (< (count pool) max-size)
                                 (assoc pool k v)
                                 pool))))

(defn in-pool?
  "Returns a boolean indicating whether or not the key `k` has the value `v`
  within the command pool in the `state` map under the `pool-path`."
  [state pool-path k v]
  (-> state
      (get-pool pool-path k)
      (= v)))

(defn dissoc-ks
  "Dissoc, but with a key sequence."
  [map ks]
  (if (= 1 (count ks))
    (dissoc map (first ks))
    (update-in map (butlast ks) dissoc (last ks))))

(defn assoc-in*
  "Handles assoc-in commands. If assoc-in is called without a value to associate into,
  treats operation as a dissoc-in operation."
  [command state-atom]
  (let [[_ ks v] command]
    (if (nil? v)
      (swap! state-atom dissoc-ks ks)
      (swap! state-atom assoc-in ks v))
    true))

(defn get-in*
  [command state-atom]
  (let [[_ ks] command]
    (get-in @state-atom ks)))

(defn dissoc-in
  "Like Clojure's dissoc, but takes a key sequence to enable dissoc within a nested map."
  [map ks]
  (let [ks*        (butlast ks)
        dissoc-key (last ks)]
    (if ks*
      (update-in map ks* dissoc dissoc-key)
      (dissoc map dissoc-key))))

(defn dissoc-in*
  "Executes incoming :dissoc-in command against state atom.
  Like Clojure's dissoc, but takes a key sequence to enable
  dissoc within a nested map."
  [command state-atom]
  (let [[_ ks] command]
    (let [res (swap! state-atom dissoc-in ks)]
      res)))

(defn cas-in
  [command state-atom]
  (let [[_ ks swap-v compare-ks compare-v] command
        new-state (swap! state-atom (fn [state]
                                      (let [current-v (get-in state compare-ks)]
                                        (if (= current-v compare-v)
                                          (assoc-in state ks swap-v)
                                          state))))]
    (= swap-v (get-in new-state ks))))

(defn max-in
  [command state-atom]
  (let [[_ ks proposed-v] command
        new-state (swap! state-atom
                         update-in ks
                         (fn [current-val]
                           (if (or (nil? current-val)
                                   (> proposed-v current-val))
                             proposed-v
                             current-val)))]
    (= proposed-v (get-in new-state ks))))

(defn- extract-flake-object
  "Returns flake object (.-o flake) from block-map of the given type whose
  subject matches (:t tx-data). Returns nil if none is found."
  [{:keys [flakes] :as _block-map} {:keys [t] :as _tx-data} type]
  (some #(when (and (= type (flake/p %))
                    (= t (flake/s %)))
           (flake/o %))
        flakes))

(defn register-new-ledgers
  "Register new ledgers. Part of state-machine. Updates state-atom, and publishes out :new-ledger on event-bus"
  [txns state-atom block-map]
  (let [init-ledger-status
        (->> txns
             (filter #(and (= :new-ledger (:type (val %)))
                           (= 200 (:status (val %)))))
             (map (fn [[_ tx-data]]
                    (let [efo         (partial extract-flake-object block-map
                                               tx-data)
                          orig-cmd    (efo constants/$_tx:tx)
                          orig-sig    (efo constants/$_tx:sig)
                          orig-signed (efo constants/$_tx:signed)
                          {:keys [ledger fork forkBlock]} (when orig-cmd
                                                            (json/parse orig-cmd))
                          [network ledger-id] (when orig-cmd
                                                (if (sequential? ledger)
                                                  ledger
                                                  (str/split ledger #"/")))]
                      [network ledger-id (util/without-nils
                                           {:status    :initialize
                                            :command   (util/without-nils
                                                         {:cmd    orig-cmd
                                                          :sig    orig-sig
                                                          :signed orig-signed})
                                            :fork      fork
                                            :forkBlock forkBlock})]))))]
    (swap! state-atom (fn [s]
                        (reduce (fn [s* [network ledger-id db-status]]
                                  (assoc-in s* [:networks network :ledgers ledger-id] db-status))
                                s init-ledger-status)))
    ;; publish out new db
    (doseq [[network ledger-id db-status] init-ledger-status]
      ;; publish out new db events
      (event-bus/publish :new-ledger [network ledger-id] db-status))))


(defn stage-new-ledger
  "Stages new ledgers. Part of state-machine. Updates state-atom."
  [command state-atom]
  (let [[_ network ledger-id cmd-id new-ledger-command] command
        db-status {:status :initialize}]
    (if (get-in @state-atom [:networks network :ledgers ledger-id])
      false ; already exists
      (do
        (swap! state-atom
               (fn [s]
                 (-> s
                     (assoc-in [:networks network :ledgers ledger-id] db-status)
                     (assoc-in [:new-ledger-queue network cmd-id]
                               {:network   network
                                :ledger-id ledger-id
                                :command   new-ledger-command}))))
        cmd-id))))


(defn initialized-ledger
  [command state-atom]
  (let [[_ cmd-id network ledger-id status] command
        ok? (= :initialize (get-in @state-atom [:networks network :ledgers ledger-id :status]))]
    (if ok?
      (do (swap! state-atom (fn [s]
                              (-> s
                                  (update-in [:networks network :ledgers ledger-id]
                                             merge status)
                                  (assoc-in [:networks network :ledgers ledger-id
                                             :indexes (:index status)]
                                            (System/currentTimeMillis))
                                  (dissoc-in [:new-ledger-queue network cmd-id]))))
          true)
      (do
        (swap! state-atom (fn [s]
                            (-> s
                                (dissoc-in [:new-ledger-queue network cmd-id]))))
        false))))


(defn update-ledger-block
  [network ledger-id txids state block]
  (-> (reduce (fn [s txid] (dissoc-in s [:cmd-queue network txid])) state txids)
      (assoc-in [:networks network :ledgers ledger-id :block] block)))

(defn delete-db
  [command state-atom]
  (let [[_ old-network old-ledger] command
        ;; dissoc all other values, set status to :deleted
        _ (swap! state-atom assoc-in [:networks old-network :ledgers old-ledger] {:status :delete})]
    ;; If we eventually decide to allow renaming ledgers, we should ensure evenly distributed
    ;; networks after migration. For now, we don't delete network

    true))

(defn rename-keys-in-state
  [state-atom path]
  (let [networks     (-> (get-in @state-atom path) keys)
        new-networks (map str/lower-case networks)]
    (swap! state-atom update-in path set/rename-keys (zipmap networks new-networks))))


(defn lowercase-all-names
  [state-atom]
  (let [;; Rename :networks
        _        (rename-keys-in-state state-atom [:networks])
        networks (->> (get @state-atom :networks) keys (map str/lower-case))
        ;; Update :ledgers in :networks
        _        (mapv (fn [nw]
                         (let [ledgers     (-> (get-in @state-atom [:networks nw :ledgers]) keys)
                               new-ledgers (map str/lower-case ledgers)]
                           (swap! state-atom update-in [:networks nw :ledgers]
                                  set/rename-keys (zipmap ledgers new-ledgers))))
                       networks)
        ;; Rename :new-ledger-queue
        _        (rename-keys-in-state state-atom [:new-ledger-queue])

        ;; Rename :cmd-queue
        _        (rename-keys-in-state state-atom [:cmd-queue])

        ;; Rename :_work
        _        (rename-keys-in-state state-atom [:_work :networks])

        servers  (-> (get-in @state-atom [:_worker]) keys)

        ;; Rename :_worker
        _        (mapv #(rename-keys-in-state state-atom [:_worker % :networks])
                       servers)]
    true))

(defn new-index
  "Options include:
  :force? - force updated to index point even if point is earlier
  :ignore-submission-server? - Normally only the server registered to the ledger as the worker can update
                               index points. This allows this to be overridden.
  :status - all ledgers can have an associated status, typically 'ready' "
  [command state-atom]
  (let [[_ network ledger-id index submission-server opts] command
        {:keys [status force? ignore-submission-server?]} opts
        current-index    (get-in @state-atom [:networks network :ledgers ledger-id :index])
        is-more-current? (cond (true? force?) true ;; force override, set no matter what
                               current-index (>= index current-index)
                               :else true)
        server-allowed?  (cond (true? force?) true ;; force override, set no matter what
                               (true? ignore-submission-server?) true ;; ignore if came from assigned worker server
                               :else (= submission-server
                                        (get-in @state-atom [:_work :networks network])))]
    (if (and is-more-current? server-allowed?)
      (do
        (swap! state-atom update-in [:networks network :ledgers ledger-id]
               (fn [db-data]
                 (-> db-data
                     (assoc :index index)
                     (update :block max index)
                     (assoc-in [:indexes index] (System/currentTimeMillis))
                     (assoc :status (or status :ready)))))
        ;; publish new-block event
        (event-bus/publish :new-index [network ledger-id] index)
        true)
      (do
        (log/warn (str "Skipping index update (maybe reindexing?). Index must be more current and submission server must be currently assigned"
                       " Current index: " current-index
                       " Proposed index: " index
                       " Submission server: " submission-server
                       " Assigned network server: " (get-in @state-atom [:_work :networks network])))
        false))))
