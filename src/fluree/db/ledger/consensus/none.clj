(ns fluree.db.ledger.consensus.none
  (:require [clojure.core.async :as async]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto :refer [TxGroup]]
            [fluree.db.util.log :as log]
            [fluree.crypto :as crypto]
            [fluree.db.storage.core :as storage]
            [fluree.db.event-bus :as event-bus]
            [fluree.db.ledger.consensus.update-state :as update-state]
            [fluree.db.ledger.storage.memorystore :as memorystore]
            [fluree.db.ledger.txgroup.monitor :as group-monitor])
  (:import (java.util UUID)))

(set! *warn-on-reflection* true)

(defrecord Command [entry id timeout callback])


(defn local-state
  "Returns local, current state from state machine"
  [group]
  @(:state-atom group))

(defn new-entry-async
  ([group entry] (new-entry-async group entry 5000))
  ([group entry timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp] (async/put! resp-chan resp))]
     (new-entry-async group entry timeout-ms callback)
     resp-chan))
  ([group entry timeout-ms callback]
   (assert (pos-int? timeout-ms))
   (let [id           (str (UUID/randomUUID))
         command      (map->Command {:entry    entry
                                     :id       id
                                     :timeout  timeout-ms
                                     :callback callback})
         command-chan (:command-chan group)]
     (async/put! command-chan [:new-command command callback]))))

;; Holds state change functions that are registered
(def state-change-fn-atom (atom {}))


(defn register-state-change-fn
  "Registers function to be called with every state monitor change. id provided is used to un-register function
  and is otherwise opaque to the system."
  [id f]
  (swap! state-change-fn-atom assoc id f))

(defn unregister-state-change-fn
  [id]
  (swap! state-change-fn-atom dissoc id))


(defn unregister-all-state-change-fn
  []
  (reset! state-change-fn-atom {}))


(defn state-machine
  [state-atom]
  (fn [command _ callback]
    (let [entry  (:entry command)
          op     (first entry)
          result (case op
                   :new-block (let [[_ network dbid block-map _] entry
                                    {:keys [block txns cmd-types]} block-map
                                    txids          (keys txns)
                                    file-key       (storage/ledger-block-key network dbid block)
                                    current-block  (get-in @state-atom [:networks network :dbs dbid :block])
                                    is-next-block? (if current-block
                                                     (= block (inc current-block))
                                                     (= 1 block))]
                                ;; if :new-db in cmd-types, then register new-db
                                (when (cmd-types :new-db)
                                  (update-state/register-new-dbs txns state-atom block-map))

                                (if is-next-block?
                                  (do
                                    ;; write out block data - todo: ensure raft shutdown happens successfully if write fails
                                    (memorystore/connection-storage-write file-key block-map)

                                    ;; update current block, and remove txids from queue
                                    (swap! state-atom
                                           (fn [state] (update-state/update-ledger-block network dbid txids state block)))

                                    ;; publish new-block event
                                    (event-bus/publish :block [network dbid] block-map)
                                    ;; return success!
                                    true)
                                  (do
                                    (log/warn " --------------- BLOCK REJECTED! "
                                              {:is-next-block? is-next-block?
                                               :state-dump     @state-atom}) false)))

                   ;; stages a new db to be created
                   :new-db (update-state/stage-new-db entry state-atom)

                   :initialized-db (update-state/initialized-db entry state-atom)

                   :new-index (update-state/new-index entry state-atom)

                   :assoc-in (update-state/assoc-in* entry state-atom)

                   ;; For no consensus, just returns true
                   :worker-assign true

                   :get-in (update-state/get-in* entry state-atom)

                   :dissoc-in (update-state/dissoc-in* entry state-atom)

                   ;; Will replace current val at key sequence only if existing val is = compare value at
                   ;; compare key sequence.
                   ;; Returns true if value updated.
                   :cas-in (update-state/cas-in entry state-atom)

                   ;; Will replace current val only if existing val is < proposed val. Returns true if value
                   ;; updated.
                   :max-in (update-state/max-in entry state-atom)

                   ;; Identical to local writes in no-consensus mode. Most writes should happen in local state
                   :storage-write
                   (let [[_ key bytes] entry]
                     (memorystore/connection-storage-write key bytes))


                   ;; Identical to local reads in no-consensus mode. Most reads should happen in local state
                   :storage-read
                   (let [[_ key] entry]
                     (memorystore/connection-storage-read key)))]
      (when-let [state-change-fns (vals @state-change-fn-atom)]
        (doseq [f state-change-fns]
          (try
            (f {:command entry :result result})
            (catch Exception e (log/error e "State change function error.")))))
      (callback result)
      result)))

;; start with a default state when no other state is present (initialization)
;; useful for setting a base 'version' in state
(def default-state {:version 3})

(defn event-loop
  "Launches an event loop where all state changes to the group state happen.

  This means all state changes are single-threaded.

  Maintains appropriate timeouts (heartbeat if leader, or election timeout if not leader)
  to trigger appropriate actions when no activity happens between timeouts.

  Events include:
  - new-command           - (leader) processes a new command, will return result of operation after applied to state
  - close                 - ends event-loop"
  [state]
  (let [event-chan   (:event-chan state)
        command-chan (:command-chan state)]
    (async/go-loop [state state]
      (let [[command _] (async/alts! [event-chan command-chan] :priority true)]
        (if (nil? command)
          (log/info :group-closed)
          (let [[_ data callback] command
                state-machine (:state-machine state)
                _             (state-machine data state callback)]
            (recur state)))))))

(defn in-memory-start-up
  [group conn system shutdown]
  (async/go
    (try
      (when (empty? (txproto/get-shared-private-key group))
        (log/info "Brand new Fluree instance, establishing default shared private key.")
        ;; TODO - check environment to see if a private key was supplied
        (let [private-key (or (:tx-private-key conn)
                              (:private (crypto/generate-key-pair)))]
          (txproto/set-shared-private-key (:group conn) private-key)))
      (register-state-change-fn (str (UUID/randomUUID))
                                (partial group-monitor/state-updates-monitor system))
      (catch Exception e
        (log/warn "2 -Error during raft initialization. Shutting down system")
        (log/error e)
        (shutdown system)
        (System/exit 1)))))

(defrecord InMemoryGroup [in-memory-server state-atom port this-server event-chan command-chan close open-api]
  TxGroup
  (-add-server-async [group server] false)
  (-remove-server-async [group server] false)
  (-new-entry-async [group entry] (new-entry-async group entry))
  (-local-state [group] (local-state group))
  (-state [group])
  (-is-leader? [group] true)
  (-active-servers [group] this-server)
  (-start-up-activities [group conn system shutdown join?] (in-memory-start-up group conn system shutdown)))

(defn launch-in-memory-server
  [{:keys [private-keys this-server port open-api] :as _group-settings}]
  (let [event-chan         (async/chan)
        command-chan       (async/chan)
        state-machine-atom (atom default-state)
        close-fn           (fn []
                             (async/close! event-chan)
                             (async/close! command-chan)
                             (group-monitor/close-db-queue)
                             (unregister-all-state-change-fn)
                             (memorystore/close)
                             (event-bus/reset-sub))
        config*            {:state-atom    (atom (assoc default-state
                                                   :private-key
                                                   (first private-keys)))
                            :event-chan    event-chan
                            :this-server   this-server
                            :port          port
                            :private-keys  private-keys
                            :command-chan  command-chan
                            :state-machine (state-machine state-machine-atom)}]
    (event-loop config*)
    (map->InMemoryGroup {:in-memory-server config*
                         :state-atom       state-machine-atom
                         :port             port
                         :this-server      this-server
                         :event-chan       event-chan
                         :command-chan     command-chan
                         :close            close-fn
                         :open-api         open-api})))
