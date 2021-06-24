(ns fluree.db.ledger.consensus.update-state
  (:require [fluree.db.constants :as constants]
            [clojure.string :as str]
            [fluree.db.util.core :as util]
            [fluree.db.event-bus :as event-bus]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [clojure.set :as set])
  (:import (fluree.db.flake Flake)))

(defn dissoc-ks
  "Dissoc, but with a key sequence."
  [map ks]
  (if (= 1 (count ks))
    (dissoc map (first ks))
    (update-in map (butlast ks) dissoc (last ks))))

(defn assoc-in*
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
  [map ks]
  (let [ks*        (butlast ks)
        dissoc-key (last ks)]
    (if ks*
      (update-in map ks* dissoc dissoc-key)
      (dissoc map dissoc-key))))

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

(defn register-new-dbs
  "Register new dbs. Part of state-machine. Updates state-atom, and publishes out :new-db on event-bus"
  [txns state-atom block-map]
  (let [init-db-status (->> txns
                            (filter #(and (= :new-db (:type (val %)))
                                          (= 200 (:status (val %)))))
                            (map (fn [[_ tx-data]]
                                   (let [t             (:t tx-data)
                                         orig-cmd      (some #(when (and (= constants/$_tx:tx (.-p ^Flake %))
                                                                         (= t (.-s ^Flake %)))
                                                                (.-o ^Flake %))
                                                             (:flakes block-map))
                                         orig-sig      (some #(when (and (= constants/$_tx:sig (.-p ^Flake %))
                                                                         (= t (.-s ^Flake %)))
                                                                (.-o ^Flake %))
                                                             (:flakes block-map))
                                         orig-cmd-data (when orig-cmd (json/parse orig-cmd))
                                         {:keys [db fork forkBlock]} orig-cmd-data
                                         [network dbid] (when orig-cmd (if (sequential? db) db (str/split db #"/")))]
                                     [network dbid (util/without-nils
                                                     {:status    :initialize
                                                      :command   {:cmd orig-cmd
                                                                  :sig orig-sig}
                                                      :fork      fork
                                                      :forkBlock forkBlock})]))))]

    (swap! state-atom (fn [s]
                        (reduce (fn [s* [network dbid db-status]]
                                  (assoc-in s* [:networks network :dbs dbid] db-status))
                                s init-db-status)))
    ;; publish out new db
    (doseq [[network dbid db-status] init-db-status]
      ;; publish out new db events
      (event-bus/publish :new-db [network dbid] db-status))))


(defn stage-new-db
  "Stages new dbs. Part of state-machine. Updates state-atom."
  [command state-atom]
  (let [[_ network dbid cmd-id new-db-command] command
        db-status {:status :initialize}]
    (if (get-in @state-atom [:networks network :dbs dbid])
      false                                                 ;; already exists
      (do
        (swap! state-atom (fn [s]
                            (-> s
                                (assoc-in [:networks network :dbs dbid] db-status)
                                (assoc-in [:new-db-queue network cmd-id] {:network network
                                                                          :dbid    dbid
                                                                          :command new-db-command}))))
        cmd-id))))


(defn initialized-db
  [command state-atom]
  (let [[_ cmd-id network dbid status] command
        ok? (= :initialize (get-in @state-atom [:networks network :dbs dbid :status]))]
    (if ok?
      (do (swap! state-atom (fn [s]
                              (-> s
                                  (update-in [:networks network :dbs dbid] merge status)
                                  (assoc-in [:networks network :dbs dbid :indexes (:index status)] (System/currentTimeMillis))
                                  (dissoc-in [:new-db-queue network cmd-id]))))
          true)
      (do
        (swap! state-atom (fn [s]
                            (-> s
                                (dissoc-in [:new-db-queue network cmd-id]))))
        false))))


(defn update-ledger-block
  [network dbid txids state block]
  (-> (reduce (fn [s txid] (dissoc-in s [:cmd-queue network txid])) state txids)
      (assoc-in [:networks network :dbs dbid :block] block)))

(defn delete-db
  [command state-atom]
  (let [[_ old-network old-db] command
        ;; dissoc all other values, set status to :deleted
        _ (swap! state-atom assoc-in [:networks old-network :dbs old-db] {:status :delete})
        ;; If we eventually decide to allow renaming dbs, we should ensure evenly distributed
        ;; networks after migration. For now, we don't delete network
        ]
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
        ;; Update :dbs in :networks
        _        (mapv (fn [nw]
                         (let [dbs     (-> (get-in @state-atom [:networks nw :dbs]) keys)
                               new-dbs (map str/lower-case dbs)]
                           (swap! state-atom update-in [:networks nw :dbs] set/rename-keys (zipmap dbs new-dbs))))
                       networks)
        ;; Rename :db-queue
        _        (rename-keys-in-state state-atom [:new-db-queue])

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
  (let [[_ network dbid index submission-server opts] command
        {:keys [status force? ignore-submission-server?]} opts
        current-index    (get-in @state-atom [:networks network :dbs dbid :index])
        is-more-current? (cond (true? force?) true          ;; force override, set no matter what
                               current-index (>= index current-index)
                               :else true)
        server-allowed?  (cond (true? force?) true          ;; force override, set no matter what
                               (true? ignore-submission-server?) true ;; ignore if came from assigned worker server
                               :else (= submission-server
                                        (get-in @state-atom [:_work :networks network])))]
    (if (and is-more-current? server-allowed?)
      (do
        (swap! state-atom update-in [:networks network :dbs dbid]
               (fn [db-data]
                 (-> db-data
                     (assoc :index index)
                     (update :block max index)
                     (assoc-in [:indexes index] (System/currentTimeMillis))
                     (assoc :status (or status :ready)))))
        ;; publish new-block event
        (event-bus/publish :new-index [network dbid] index)
        true)
      (do
        (log/warn (str "Skipping index update (maybe reindexing?). Index must be more current and submission server must be currently assigned"
                       " Current index: " current-index
                       " Proposed index: " index
                       " Submission server: " submission-server
                       " Assigned network server: " (get-in @state-atom [:_work :networks network])))
        false))))

