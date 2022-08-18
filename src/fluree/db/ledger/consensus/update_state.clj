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

(def ^:const max-pool-size 100000)

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
  [state pool-path k v]
  (update-in state pool-path (fn [pool]
                               (if (< (count pool) max-pool-size)
                                 (assoc pool k v)
                                 pool))))

(defn clear-pool
  "Remove the entries associated with each id in `cmd-ids` from the command pool
  specified by `pool-path` in `state`."
  [state pool-path cmd-ids]
  (reduce (fn [s cmd-id]
            (update-in s pool-path dissoc cmd-id))
          state cmd-ids))

(defn dissoc-in
  "Like Clojure's dissoc, but takes a key sequence to enable dissoc within a
  nested map."
  [state ks]
  (let [[k & rst] ks]
    (if (empty? rst)
      (dissoc state k)
      (update state k dissoc-in rst))))

(defn assoc-in*
  "Associates the value `v` with the key sequence `ks` in `state` if `v` is
  non-nil. If the value `v` is nil, the key sequene `ks` is dissociated from
  state."
  [state ks v]
  (if (nil? v)
    (dissoc-in state ks)
    (assoc-in state ks v)))

(defn get-in*
  [state ks]
  (get-in state ks))

(defn cas-in
  [state ks swap-v compare-ks compare-v]
  (let [current-v (get-in state compare-ks)]
    (if (= current-v compare-v)
      (assoc-in state ks swap-v)
      state)))

(defn max-in
  [state ks proposed-v]
  (update-in state ks (fn [current-val]
                        (if (or (nil? current-val)
                                (> proposed-v current-val))
                          proposed-v
                          current-val))))

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

(defn ledger-staged?
  [state network new-ledger-cmd-id]
  (-> state
      (get-in [:new-ledger-queue network new-ledger-cmd-id])
      boolean))

(defn stage-new-ledger
  "Stages new ledgers. Part of state-machine. Updates state-atom."
  [state network ledger-id cmd-id new-ledger-command]
  (if (get-in state [:networks network :ledgers ledger-id])
    state
    (let [ledger-status {:status :initialize}]
      (-> state
          (assoc-in [:networks network :ledgers ledger-id] ledger-status)
          (assoc-in [:new-ledger-queue network cmd-id]
                    {:network   network
                     :ledger-id ledger-id
                     :command   new-ledger-command})))))

(defn ledger-initializing?
  [state network ledger-id]
  (-> state
      (get-in [:networks network :ledgers ledger-id :status])
      (= :initialize)))

(defn ledger-indexed-at
  [state network ledger-id index]
  (get-in state [:networks network
                 :ledgers ledger-id
                 :indexes index]))

(defn initialized-ledger
  [state network ledger-id cmd-id status ts]
  (let [idx (:index status)]
    (if (ledger-initializing? state network ledger-id)
      (-> state
          (update-in [:networks network :ledgers ledger-id]
                     merge status)
          (assoc-in [:networks network
                     :ledgers ledger-id
                     :indexes idx]
                    ts)
          (dissoc-in [:new-ledger-queue network cmd-id]))
      (dissoc-in state [:new-ledger-queue network cmd-id]))))


(defn update-ledger-block
  [state network ledger-id block txids]
  (-> state
      (clear-pool [:cmd-queue network] txids)
      (assoc-in [:networks network :ledgers ledger-id :block] block)))

(defn delete-ledger
  [state old-network old-ledger]
  ;; dissoc all other values, set status to :deleted
  ;; If we eventually decide to allow renaming ledgers, we should ensure evenly
  ;; distributed networks after migration. For now, we don't delete network
  (assoc-in state [:networks old-network :ledgers old-ledger] {:status :delete}))

(defn lowercase-keys-in
  [state path]
  (let [old-keys (-> state
                     (get-in path)
                     keys)
        new-keys (map str/lower-case old-keys)
        keymap   (zipmap old-keys new-keys)]
    (update-in state path set/rename-keys keymap)))

(defn lowercase-all-names
  "Convert string names to lowercase under the `:networks`, `:new-ledger-queue`,
  and `:cmd-queue` keys, and the `[:_work :networks]` path, as well as all
  string ledger names within the map associated with the `:networks` key and
  string server names within the map associated with the `:_worker` key within
  `state`."
  [state]
  (-> state
      (lowercase-keys-in [:networks])
      (update :networks (fn [nw-map]
                          (reduce-kv (fn [m k v]
                                       (assoc m k (lowercase-keys-in [:ledgers])))
                                     {} nw-map)))
      (lowercase-keys-in [:new-ledger-queue])
      (lowercase-keys-in [:cmd-queue])
      (lowercase-keys-in [:_work :networks])
      (update :_worker (fn [worker-map]
                         (reduce-kv (fn [m k v]
                                      (assoc m k (lowercase-keys-in [:networks]))))))))

(defn get-current-index
  [state network ledger-id]
  (get-in state [:networks network :ledgers ledger-id :index]))

(defn get-worker
  [state network]
  (get-in state [:_work :networks network]))

(defn newer-index?
  [proposed current]
  (or (not current)
      (>= proposed current)))

(defn new-index
  "Options include:
  :force? - force updated to index point even if point is earlier
  :ignore-submission-server? - Normally only the server registered to the ledger as the worker can update
                               index points. This allows this to be overridden.
  :status - all ledgers can have an associated status, typically 'ready' "
  [state network ledger-id index submission-server
   {:keys [timestamp status force? ignore-submission-server?] :as _opts}]
  (let [current-index    (get-current-index state network ledger-id)
        assigned-worker  (get-worker state network)
        server-allowed?  (or ignore-submission-server?  ;; ignore if came from assigned worker server
                             (= submission-server assigned-worker))]
    (if (or force?
            (and (newer-index? index current-index)
                 server-allowed?))
      (update-in state [:networks network :ledgers ledger-id]
                 (fn [db-data]
                   (-> db-data
                       (assoc :index index)
                       (update :block max index)
                       (assoc-in [:indexes index] timestamp)
                       (assoc :status (or status :ready)))))
      state)))
