(ns fluree.db.ledger.txgroup.txgroup-proto
  (:require [clojure.string :as str]
            [fluree.db.util.core :as util]
            [clojure.core.async :as async]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)


;; To allow for pluggable consensus, we have a TxGroup protocol.
;; In order to allow for a new consensus type, we need to create a record with all of the following methods.
;; Currently, we support a RaftGroup and SoloGroup.


(defprotocol TxGroup
  (-add-server-async [group server])
  (-remove-server-async [group server])
  (-new-entry-async [group entry] "Sends a command to the leader. If no callback provided, returns a core async promise channel that will eventually contain a response.")
  (-local-state [group])
  (-state [group])
  (-is-leader? [group])
  (-active-servers [group] "Returns list of active server-ids. If raft, servers with active leases.")
  (-start-up-activities [group conn system shutdown join?]))


;; STATE MACHINE COMMANDS - for generic state machine


;; get, assoc, dissoc functions

(defn kv-get-in
  "Performs a get-in using current local state. If you want a fully consistent
  get-in, use kv-get-in-async*."
  [group ks]
  (get-in (-local-state group) ks))

(defn kv-get-in-async*
  "Perform a fully consistent get-in operation (sends operation to all raft
  servers for sync)"
  [raft ks]
  (let [command [:get-in ks]]
    (-new-entry-async raft command)))

(defn kv-assoc-in-async
  "Writes value to specified key. Returns core async channel that will eventually
  have a response."
  [group ks v]
  (let [command [:assoc-in ks v]]
    (-new-entry-async group command)))

(defn kv-assoc-in
  "Writes value to specified key."
  [group ks v]
  (async/<!! (kv-assoc-in-async group ks v)))

(defn kv-dissoc-in-async
  "Remove value as key sequence. Returns true if a value was present and removed, false if no value existed.
  Returns core async channel that will eventually have a response."
  [group ks]
  (let [command [:dissoc-in ks]]
    (-new-entry-async group command)))

(defn kv-dissoc-in
  "Remove value as key sequence. Returns true if a value was present and removed,
  false if no value existed."
  [group ks]
  (async/<!! (kv-dissoc-in-async group ks)))

(defn kv-max-in-async
  "Writes value to specified key sequence. Will only write is proposed value is
  greater than the current value. Returns true if value is written, false if it
  is not written. Returns core async channel that will eventually have a
  response."
  [group ks v]
  (assert (number? v))
  (let [command [:max-in ks v]]
    (-new-entry-async group command)))

(defn kv-cas-in-async
  "Compare and swap. Compares current value and compare value at compare key sequence.
  If equal, sets swap value at swap key sequence. If compare key sequence is not
  provided, uses the swap key sequence for compare. Returns core async channel
  that will eventually have a response."
  ([raft ks swap-v compare-v]
   (kv-cas-in-async raft ks swap-v ks compare-v))
  ([raft ks swap-v compare-ks compare-v]
   (let [command [:cas-in ks swap-v compare-ks compare-v]]
     (-new-entry-async raft command))))

(defn kv-put-pool-async
  [group pool-path k v]
  (let [command [:put-pool pool-path k v]]
    (-new-entry-async group command)))

(defn kv-put-pool
  [group pool-path k v]
  (async/<!! (kv-put-pool-async group pool-path k v)))

;; version, this-server commands

(defn this-server
  [group]
  (:this-server group))

(defn data-version
  [group]
  (or (:version (-local-state group)) 1))

(defn set-data-version
  [group version]
  (assert (number? version)) (kv-assoc-in group [:version] version))

;; Index functions

(defn latest-index*
  "Returns latest index for given ledger given current state."
  [current-state network ledger-id]
  (log/debug "Getting latest index for" network ledger-id "in current-state:" current-state)
  (let [ledgers    (get-in current-state [:networks network :ledgers])
        ledger-id* (str/lower-case ledger-id)]
    (some (fn [key']
            (when (= ledger-id* (str/lower-case key'))
              (get-in ledgers [key' :index]))) (keys ledgers))))

(defn latest-index
  "Returns latest index for given ledger."
  [group network ledger-id]
  (latest-index* (-local-state group) network ledger-id))

(defn write-index-point-async
  "Attempts to register a new index point. If older than the previous index point,
  or this server is not responsible for this ledger, will return false. Else
  true upon success."
  ([group db] (write-index-point-async group (:network db) (:ledger-id db) (get-in db [:stats :indexed])
                                       (:this-server group) {}))
  ([group network ledger-id index-point submission-server opts]
   (let [command [:new-index network ledger-id index-point submission-server opts]]
     (-new-entry-async group command))))

(defn remove-current-index
  "Removes current index point from raft network."
  [group network ledger-id]
  (let [ks [:networks network :ledgers ledger-id :index]]
    (kv-dissoc-in group ks)))

(defn remove-index-point
  "Removes an index point. Returns true if index point existed and was
  successfully removed."
  [group network ledger-id idx-point]
  (let [ks [:networks network :ledgers ledger-id :indexes idx-point]]
    (kv-dissoc-in group ks)))

(defn remove-ledger
  "Removes current index point from raft network."
  [group network ledger-id]
  (let [ks [:networks network :ledgers ledger-id]]
    (kv-dissoc-in group ks)))


;; Network commands

(defn network-list
  "Returns a list of all networks using group."
  [group]
  (-> (-local-state group) :networks (keys)))

(defn initialize-network
  "Marks network as initialized"
  [group-raft network]
  (kv-assoc-in group-raft [:networks network :initialized?] true))


;; Ledger commands

(defn ledger-list*
  "Returns a list of all ready or initialized ledgers for all networks as a
  two-tuple, `[network ledger-id]`."
  [current-state]
  (let [networks (-> current-state :networks (keys))]
    (reduce
      (fn [acc network]
        (let [ledgers  (get-in current-state [:networks network :ledgers])
              ledgers' (filter #(#{:ready :initialize :reindex}
                                 (:status (get ledgers %))) (keys ledgers))]
          (reduce #(conj %1 [network %2]) acc ledgers')))
      [] networks)))

(defn all-ledger-list*
  "Returns a list of all ledgers for all networks as a two-tuple, `[network ledger-id]`."
  [current-state]
  (let [networks (-> current-state :networks (keys))]
    (reduce
      (fn [acc network]
        (let [ledgers (keys (get-in current-state [:networks network :ledgers]))]
          (reduce #(conj %1 [network %2]) acc ledgers)))
      [] networks)))

(defn all-ledger-list
  "Returns a list of all ledgers for all networks as a two-tuple, `[network ledger-id]`."
  [group]
  (all-ledger-list* (-local-state group)))


(defn ledger-list
  "Returns a list of ready ledgers for all networks as a two-tuple, `[network ledger-id]`."
  [group]
  (ledger-list* (-local-state group)))

(defn ledger-status
  "Returns current status for given ledger"
  [group network ledger-id]
  (kv-get-in group [:networks network :ledgers ledger-id :status]))

(defn ledger-exists?
  [group network ledger-id]
  (boolean (latest-index group network ledger-id)))

(defn ledger-info
  "Returns all info we have on given ledger."
  [group network ledger-id]
  (log/debug "Getting ledger-info at key-path"
             [:networks network :ledgers ledger-id]
             "from:" (-local-state group))
  (kv-get-in group [:networks network :ledgers ledger-id]))

(defn ledgers-info-map
  "Returns vector of maps with include 'ledger-info' data
  with :network :ledger keys added in."
  [conn]
  (let [group-raft    (:group conn)
        current-state @(:state-atom group-raft)
        ledger-list   (ledger-list* current-state)]
    (mapv (fn [[network ledger]]
            (-> (ledger-info group-raft network ledger)
                (assoc :network network :ledger ledger)))
          ledger-list)))

(defn update-ledger-status
  [group network ledger-id status-msg]
  (kv-assoc-in group [:networks network :ledgers ledger-id :status] status-msg))

(defn initialized-ledger-async
  "Registers first block of initialized db. Rejects if db already initialized.
  Always removes command-id from qeued new ledgers."
  [group cmd-id network ledger-id block fork index]
  (let [status  (util/without-nils {:status    :ready
                                    :block     block
                                    :fork      fork
                                    :forkBlock (when fork block)
                                    :index     index})
        command [:initialized-ledger cmd-id network ledger-id status]]
    (-new-entry-async group command)))

(defn lowercase-all-names
  [group]
  (-new-entry-async group [:lowercase-all-names]))

(defn new-ledger-async
  "Registers new network to be created by leader."
  [group network ledger-id cmd-id signed-cmd owners]
  (let [command [:new-ledger network ledger-id cmd-id signed-cmd owners]]
    (-new-entry-async group command)))

(defn find-all-ledgers-to-initialize
  "Finds all ledgers that need to be initialized in a given network.
  Returns a list of tuples: [network ledger-id command fork forkBlock]"
  [group]
  (let [initialize-cmds (-> (-local-state group)
                            (get :new-ledger-queue)
                            (vals)
                            (#(mapcat vals %)))]
    (reduce
      (fn [acc {:keys [network ledger-id command]}]
        (conj acc [network ledger-id command]))
      [] initialize-cmds)))

;; private key commands

(defn private-key
  [group]
  (get-in (-local-state group) [:private-keys]))

(defn set-shared-private-key
  "Sets a default public key to use for any network operations (creating a new
  network)"
  ([group private-key]
   (kv-assoc-in group [:private-key] private-key))
  ([group network private-key]
   (kv-assoc-in group [:networks network :private-key] private-key))
  ([group network ledger-id private-key]
   (kv-assoc-in group [:networks network :ledgers ledger-id :private-key] private-key)))

(defn get-shared-private-key
  "Both network and ledger are optional"
  ([group] (kv-get-in group [:private-key]))
  ([group network] (or (kv-get-in group [:networks network :private-key])
                       (kv-get-in group [:private-key])))
  ([group network ledger-id]
   (or (kv-get-in group [:networks network :ledgers ledger-id :private-key])
       (get-shared-private-key group network))))

;; Command queue commands

(defn command-queue
  "Returns command queue as a list of command entry maps containing the command
  and associated metadata. Queued command entry maps are stored in state machine
  under key sequence of `[:cmd-queue network command-id]`.

  Command entry map keys are:
    1. :id        - command id
    2. :command   - command data (map)
    3. :size      - size of command in bytes
    4. :network
    5. :ledger-id
    6. :instant   - time the entry was enqueued"
  ([group]
   (->> (kv-get-in group [:cmd-queue])
        vals
        (mapcat vals)))
  ([group network]
   (vals (kv-get-in group [:cmd-queue network])))
  ([group network ledger-id]
   (filter (fn [cmd-entry]
             (-> cmd-entry :ledger-id (= ledger-id)))
           (command-queue group network))))

(defn queue-command-async
  "Enqueues a new command processing"
  [group network ledger-id command-id command]
  (let [queue-time  (System/currentTimeMillis)
        size        (-> command :cmd count)
        queue-entry {:command   command
                     :size      size
                     :id        command-id
                     :network   network
                     :ledger-id ledger-id
                     :instant   queue-time}]
    (kv-put-pool-async group [:cmd-queue network] command-id queue-entry)))

;; Block commands


(defn block-height*
  "Returns block height given the group's state atom."
  [group-state network ledger-id]
  (get-in group-state [:networks network :ledgers ledger-id :block]))

(defn propose-new-block-async
  [group network ledger-id block-data]
  (let [command [:new-block network ledger-id block-data (this-server group)]]
    (-new-entry-async group command)))

(defn remove-command-from-queue
  "Removes a command that is in the queue so it will no longer attempt to be processed."
  [group network command-id]
  (kv-dissoc-in-async group [:cmd-queue network command-id]))

;; TODO - this should use a CAS, to ensure DB does not currently exist.
(defn register-genesis-block-async
  "Only for use by bootstrap or copy. Registers a new genesis block.
  Will reject if any value exists currently"
  ([group network ledger-id]
   (register-genesis-block-async group network ledger-id 1))
  ([group network ledger-id block]
   (kv-assoc-in-async group [:networks network :ledgers ledger-id :block] block)))


;; storage commands

(defn storage-write-async
  "Performs a fully consistent storage write."
  [raft k data]
  (let [command [:storage-write k data]]
    (-new-entry-async raft command)))
