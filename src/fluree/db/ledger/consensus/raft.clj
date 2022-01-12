(ns fluree.db.ledger.consensus.raft
  (:require [fluree.raft :as raft]
            [taoensso.nippy :as nippy]
            [clojure.core.async :as async :refer [<! <!! go-loop]]
            [clojure.pprint :as cprint]
            [fluree.db.util.log :as log]
            [clojure.string :as str]
            [fluree.db.storage.core :as storage]
            [fluree.db.serde.avro :as avro]
            [fluree.db.event-bus :as event-bus]
            [fluree.db.ledger.consensus.tcp :as ftcp]
            [fluree.db.util.async :refer [go-try <? <??]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto :refer [TxGroup]]
            [fluree.db.ledger.consensus.update-state :as update-state]
            [fluree.db.ledger.txgroup.monitor :as group-monitor]
            [fluree.db.ledger.consensus.dbsync2 :as dbsync2]
            [fluree.crypto :as crypto]
            [fluree.db.ledger.storage :as ledger-storage]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util :refer [exception?]])
  (:import (java.util UUID)
           (fluree.db.flake Flake)))

(set! *warn-on-reflection* true)


(defn snapshot-xfer
  "Transfers snapshot from this server as leader, to a follower.
  Will be called with two arguments, snapshot id and part number.
  Initial call will be for part 1, and subsequent calls, if necessary,
  will be for each successive part.

  Must return a snapshot with the following fields
  :parts - how many parts total
  :data - snapshot data

  If multiple parts are returned, additional requests for each part will be
  requested. A snapshot should be broken into multiple parts if it is larger than
  the amount of data you want to push across the network at once."
  [{:keys [path storage-read]}]
  (fn [id _]
    (let [file (str path id ".snapshot")
          ba   (<!! (storage-read file))]
      {:parts 1
       :data  ba})))


(defn snapshot-installer
  "Installs a new snapshot being sent from a different server.
  Blocking until write succeeds. An error will stop RAFT entirely.

  If snapshot-part = 1, should first delete any existing file if it exists (possible to have historic partial snapshot lingering).

  As soon as final part write succeeds, can safely garbage collect any old snapshots on disk except the most recent one."
  [{:keys [path storage-delete storage-write]}]
  (fn [snapshot-map]
    (let [{:keys [snapshot-index snapshot-part snapshot-data]} snapshot-map
          file (str path snapshot-index ".snapshot")]

      ;; NOTE: Currently snapshot-part is always 1 b/c we never send multi-part snapshots.
      ;;   See comment in `snapshot-xfer` for more details. - WSM 2020-09-01
      (when (= 1 snapshot-part)
        ;; delete any old file if exists
        (storage-delete file))

      ;; TODO: If multi-part snapshot transfers are implemented, need to figure out
      ;;   how & when to write out / upload the file here. For local FS, can just
      ;;   add a way to request appending. For something like S3, we will need to
      ;;   do something else like gather all parts locally and then upload or
      ;;   investigate streaming multipart uploads or similar.
      (<?? (storage-write file snapshot-data)))))


(defn snapshot-reify
  "Reifies a snapshot, should populate whatever data is needed into an initialized state machine
  that is used for raft.

  Called with snapshot-id to reify, which corresponds to the commit index the snapshot was taken.
  Should throw if snapshot not found, or unable to parse. This will stop raft."
  [{:keys [path state-atom storage-read]}]
  (fn [snapshot-id]
    (try
      (let [file  (str path snapshot-id ".snapshot")
            _     (log/debug "Reifying snapshot" file)
            data  (<?? (storage-read file))
            state (when data (nippy/thaw data))]
        (log/debug "Read snapshot data:" state)
        (reset! state-atom state))
      (catch Exception e (log/error e "Error reifying snapshot: " snapshot-id)))))


(defn- return-snapshot-id
  "Takes file map (from storage subsystem) and returns log id (typically same
  as start index) from the file name as a long integer."
  [file]
  (when-let [match (re-find #"^([0-9]+)\.snapshot$" (:name file))]
    (Long/parseLong (second match))))


(defn- purge-snapshots
  [{:keys [path storage-list storage-delete max-snapshots]}]
  (let [rm-snapshots (some->> (storage-list path)
                              <!!
                              (keep return-snapshot-id)
                              (sort >)
                              (drop max-snapshots))]
    (when (not-empty rm-snapshots)
      (log/info "Removing snapshots: " rm-snapshots))
    (doseq [snapshot rm-snapshots]
      (let [file (str/join "/" [(str/replace path #"/$" "")
                                (str snapshot ".snapshot")])]
        (storage-delete file)))))


(defn get-raft-state
  "Returns current raft state to callback."
  [raft callback]
  (raft/get-raft-state (:raft raft) callback))


(defn leader-async
  "Returns leader as a core async channel once available.
  Default timeout supplied, or specify one."
  ([raft] (leader-async raft 60000))
  ([raft timeout]
   (let [timeout-time (+ (System/currentTimeMillis) timeout)]
     (async/go-loop [retries 0]
       (let [resp-chan (async/promise-chan)
             _         (get-raft-state raft (fn [state]
                                              (if-let [leader (:leader state)]
                                                (async/put! resp-chan leader)
                                                (async/close! resp-chan))))
             resp      (async/<! resp-chan)]
         (cond
           resp resp

           (> (System/currentTimeMillis) timeout-time)
           (ex-info (format "Leader not yet established and timeout of %s reached. Polled raft state %s times." timeout retries)
                    {:status 400 :error :db/leader-timeout})

           :else
           (do
             (async/<! (async/timeout 100))
             (recur (inc retries)))))))))


(defn is-leader?-async
  [raft]
  (async/go
    (let [leader (async/<! (leader-async raft))]
      (if (instance? Throwable leader)
        leader
        (= (:this-server raft) leader)))))


(defn is-leader?
  [raft]
  (let [leader? (async/<!! (is-leader?-async raft))]
    (if (instance? Throwable leader?)
      (throw leader?)
      leader?)))


(defn snapshot-writer*
  "Blocking until write succeeds. An error will stop RAFT entirely."
  [{:keys [path state-atom storage-write] :as config}]
  (fn [index callback]
    (log/info "Ledger group snapshot write triggered for index:" index)
    (let [start-time    (System/currentTimeMillis)
          state         @state-atom
          file          (str path index ".snapshot")
          max-snapshots 6
          data          (nippy/freeze state)]
      (log/debug "Writing raft snapshot" file "with contents" state)
      (try
        (<?? (storage-write file data))
        (catch Exception e (log/error e "Error writing snapshot index:" index)))
      (log/info (format "Ledger group snapshot completed for index %s in %s milliseconds."
                        index (- (System/currentTimeMillis) start-time)))
      (callback)
      (purge-snapshots (assoc config :max-snapshots max-snapshots)))))


(defn snapshot-writer
  "Wraps snapshot-writer* in the logic that determines whether all nodes or
  only the leader should write snapshots."
  [{:keys [only-leader-snapshots] :as config}]
  (let [writer (snapshot-writer* config)]
    (fn [index callback]
      (if only-leader-snapshots
        (when (is-leader? config)
          (writer index callback))
        (writer index callback)))))


(defn snapshot-list-indexes
  "Lists all stored snapshot indexes, sorted ascending. Used for bootstrapping a
  raft network from a previously made snapshot."
  [{:keys [path storage-list]}]
  (log/debug "Initialized snapshot-list-indexes with path" path "and storage-list" storage-list)
  (fn []
    (log/debug "Listing snapshot indexes in" path)
    (let [files (try
                  (<?? (storage-list path))
                  (catch Exception e (log/error e "Error listing stored snapshots in" path)))]
      (log/debug "Got snapshot candidate files:" files)
      (->> files
           (map :name)
           (keep #(when-let [match (re-find #"^([0-9]+)\.snapshot$" %)]
                    (Long/parseLong (second match))))
           sort
           vec))))


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


(defn rejected-block-handler
  "If a block is rejected for some reason, handles exception and logging."
  [state-atom command error-msg]
  (let [[_ network dbid block-map submission-server] command
        {:keys [block txns cmd-types]} block-map
        txids (keys txns)]
    (swap! state-atom
           (fn [state]
             (reduce (fn [s txid] (update-state/dissoc-in s [:cmd-queue network txid])) state txids)))
    (ex-info (str " --------------- BLOCK REJECTED! " error-msg)
             {:error  :db/invalid-block
              :status 500})))


(defn state-machine
  [_ state-atom storage-read storage-write]
  (fn [command _]
    (let [op     (first command)
          result (case op

                   ;; new block is a special case as it requires a couple atomic transactions
                   ;; a new block is accepted only if
                   ;; (a) the submission-server is currently the worker for the network
                   ;; (b) the block-id is exactly one block increment from the previous block
                   ;; if it contains a command-type of :new-db, we also establish a new db record
                   :new-block (let [[_ network dbid block-map submission-server] command
                                    {:keys [block txns cmd-types]} block-map
                                    txids           (keys txns)
                                    file-key        (storage/ledger-block-key network dbid block)
                                    current-block   (get-in @state-atom [:networks network :dbs dbid :block])
                                    is-next-block?  (if current-block
                                                      (= block (inc current-block))
                                                      (= 1 block))
                                    server-allowed? (= submission-server
                                                       (get-in @state-atom [:_work :networks network]))]
                                ;; if :new-db in cmd-types, then register new-db
                                (when (cmd-types :new-db)
                                  (update-state/register-new-dbs txns state-atom block-map))

                                (if (and is-next-block? server-allowed?)
                                  (try
                                    ;; write out block data - todo: ensure raft shutdown happens successfully if write fails
                                    (storage-write file-key (avro/serialize-block block-map))

                                    ;; update current block, and remove txids from queue
                                    (swap! state-atom
                                           (fn [state] (update-state/update-ledger-block network dbid txids state block)))

                                    (log/info (str network "/" dbid " new-block " {:block         block
                                                                                   :txns          txids
                                                                                   :server        submission-server
                                                                                   :network-queue (count (get-in @state-atom [:cmd-queue network]))}))

                                    ;; publish new-block event
                                    (event-bus/publish :block [network dbid] block-map)
                                    ;; return success!
                                    true
                                    (catch Exception e
                                      (rejected-block-handler state-atom command
                                                              (str "Exception attempting to write block: " (.getMessage e)))))
                                  (rejected-block-handler state-atom command
                                                          (str (if (not is-next-block?)
                                                                 (str "Blocks out of order. Block " block " should be one more than current block: " current-block)
                                                                 (str "Server: " submission-server " is not registered as current worker for this network: " network
                                                                      ". That server is: " (get-in @state-atom [:_work :networks network])))
                                                               (pr-str {:server-allowed server-allowed?
                                                                        :is-next-block? is-next-block?
                                                                        :command        command
                                                                        :state-dump     @state-atom})))))


                   ;; stages a new db to be created
                   :new-db (update-state/stage-new-db command state-atom)

                   :delete-db (update-state/delete-db command state-atom)

                   :initialized-db (update-state/initialized-db command state-atom)

                   :new-index (update-state/new-index command state-atom)

                   :lowercase-all-names (update-state/lowercase-all-names state-atom)

                   :assoc-in (update-state/assoc-in* command state-atom)

                   ;; worker assignments are a little different in that they organize the key-seq
                   ;; both prepended by the server-id (for easy lookup of work based on server-id)
                   ;; and also at the end of the key-seq (for easy lookup of worker(s) for given resource(s))
                   ;; all worker data is stored under the :_worker key
                   :worker-assign (let [[_ ks worker-id] command
                                        unassign? (nil? worker-id)
                                        work-ks   (into [:_work] ks)
                                        worker-ks (into [:_worker worker-id] ks)]
                                    (swap! state-atom
                                           (fn [state]
                                             (let [existing-worker (get-in state work-ks)]
                                               (if unassign?
                                                 (-> state
                                                     (update-state/dissoc-in work-ks)
                                                     (update-state/dissoc-in worker-ks))
                                                 (-> (if existing-worker
                                                       (update-state/dissoc-in state (into [:_worker existing-worker] ks))
                                                       state)
                                                     (assoc-in work-ks worker-id)
                                                     (assoc-in worker-ks (System/currentTimeMillis)))))))
                                    true)

                   :get-in (update-state/get-in* command state-atom)

                   ;; Returns true if there was an existing value removed, else false.
                   :dissoc-in (update-state/dissoc-in* command state-atom)

                   ;; acquires lease, stored at specified ks (a more elaborate cas). Uses local clock
                   ;; to help combat clock skew. Will only allow a single lease at specified ks.
                   ;; returns true if provided id has the lease, false if other has the lease
                   :lease (let [[_ ks id expire-ms] command
                                epoch-ms     (System/currentTimeMillis)
                                expire-epoch (+ epoch-ms expire-ms)
                                new-lease    {:id id :expire expire-epoch}
                                new-state    (swap! state-atom update-in ks
                                                    (fn [current-lease]
                                                      (cond
                                                        ;; no lease, or renewal from current lease holder
                                                        (or (nil? current-lease) (= (:id current-lease) id))
                                                        new-lease

                                                        ;; a different id has the lease, not expired
                                                        (<= epoch-ms (:expire current-lease))
                                                        current-lease

                                                        ;; a different id has the lease, expired
                                                        :else
                                                        new-lease)))]
                            ;; true if have the lease
                            (= id (:id (get-in new-state ks))))

                   ;; releases lease if id is the current lease holder, or no lease exists. Returns true as operation always successful.
                   :lease-release (let [[_ ks id] command]
                                    (swap! state-atom
                                           (fn [state]
                                             (let [release? (or (nil? (get-in state ks))
                                                                (= id (:id (get-in state ks))))]
                                               (if release?
                                                 (update-state/dissoc-in state ks)
                                                 state))))
                                    true)

                   ;; Will replace current val at key sequence only if existing val is = compare value at compare key sequence.
                   ;; Returns true if value updated.
                   :cas-in (update-state/cas-in command state-atom)

                   ;; Will replace current val only if existing val is < proposed val. Returns true if value updated.
                   :max-in (update-state/max-in command state-atom)

                   ;; only used for local file-system storage (not centralized)
                   ;; does not block, always returns true.
                   ;; TODO - implement logic to retry writes if ultimately not successful (by re-requesting index ID from raft)
                   :storage-write (let [[_ key bytes] command]
                                    (future (storage-write key bytes))
                                    true)

                   ;; used only for a fully synchronized read - most reads should happen in local state
                   :storage-read (let [[_ key] command]
                                   (storage-read key)))]
      ;; call any registered state change functions
      (when-let [state-change-fns (vals @state-change-fn-atom)]
        (doseq [f state-change-fns]
          (try
            (f {:command command :result result})
            (catch Exception e (log/error e "State change function error.")))))
      result)))


;; map of request-ids to a response channel that will contain the response eventually
(def pending-responses (atom {}))


(defn send-rpc
  "Sends rpc call to specified server.
  Includes a resp-chan that will eventually contain a response.

  Returns true if successful, else will return an exception if
  connection doesn't exist (not established, or closed)."
  [raft server operation data callback]
  (let [this-server (:this-server raft)
        msg-id      (str (UUID/randomUUID))
        header      {:op     operation
                     :from   this-server
                     :to     server
                     :msg-id msg-id}]
    (log/trace "send-rpc start:" {:op operation :data data :header header})
    (when (fn? callback)
      (swap! pending-responses assoc msg-id callback))
    (let [success? (ftcp/send-rpc this-server server header data)]
      (if success?
        (log/trace "send-rpc success:" {:op operation :data data :header header})
        (do
          (swap! pending-responses dissoc msg-id)
          (log/debug "Connection to" server "is closed, unable to send rpc." header))))))


(defn message-consume
  "Function used to consume inbound server messages.

  client-id should be an approved client-id from the initial
  client negotiation process, can be can used to validate incoming
  messages are labeled as coming from the correct client."
  [raft key-storage-read-fn conn message]
  (try
    (let [message'  (nippy/thaw message)
          [header data] message'
          {:keys [op msg-id]} header
          response? (str/ends-with? (name op) "-response")
          {:keys [write-chan]} conn]
      (if response?
        (let [callback (get @pending-responses msg-id)]
          (when (fn? callback)
            (swap! pending-responses dissoc msg-id)
            (callback data)))
        (let [resp-header (assoc header :op (keyword (str (name op) "-response"))
                                        :to (:from header)
                                        :from (:to header))
              callback    (fn [x]
                            (ftcp/send-message write-chan resp-header x))]
          (case op
            :storage-read
            (let [file-key data]
              (log/debug "Storage read for key: " file-key)
              (async/go
                (-> (storage/read {:storage-read key-storage-read-fn} file-key)
                    (async/<!)
                    (callback))))

            :new-command
            (let [{:keys [id entry]} data
                  command (raft/map->RaftCommand {:entry entry
                                                  :id    id})]
              (log/debug "Raft - new command:" data)
              (raft/new-command raft command callback))

            ;; else
            (raft/invoke-rpc raft op data callback)))))
    (catch Exception e (log/error e "Error consuming new message! Ignoring."))))


;; start with a default state when no other state is present (initialization)
;; useful for setting a base 'version' in state
(def default-state {:version 3})

(defn start-instance
  [raft-config]
  (let [{:keys [port this-server log-directory entries-max log-history
                storage-ledger-read storage-group-read storage-ledger-write
                storage-group-write storage-group-exists storage-group-delete
                storage-group-list private-keys open-api]} raft-config
        event-chan             (async/chan)
        command-chan           (async/chan)
        state-machine-atom     (atom default-state)
        log-directory          (or log-directory (str "raftlog/" (name this-server) "/"))
        snapshot-config        {:path           "snapshots/"
                                :state-atom     state-machine-atom
                                :storage-read   storage-group-read
                                :storage-write  storage-group-write
                                :storage-exists storage-group-exists
                                :storage-delete storage-group-delete
                                :storage-list   storage-group-list}
        raft-config*           (assoc raft-config
                                 :event-chan event-chan
                                 :command-chan command-chan
                                 :send-rpc-fn send-rpc
                                 :log-history log-history
                                 :log-directory log-directory
                                 :entries-max (or entries-max 50)
                                 :state-machine (state-machine this-server
                                                               state-machine-atom
                                                               storage-ledger-read
                                                               storage-ledger-write)
                                 :snapshot-write (snapshot-writer snapshot-config)
                                 :snapshot-reify (snapshot-reify snapshot-config)
                                 :snapshot-xfer (snapshot-xfer snapshot-config)
                                 :snapshot-install (snapshot-installer snapshot-config)
                                 :snapshot-list-indexes (snapshot-list-indexes snapshot-config))
        _                      (log/debug "Starting Raft with config:" raft-config*)
        raft                   (raft/start raft-config*)
        client-message-handler (partial message-consume raft storage-ledger-read) ; Or should it be group?
        new-client-handler     (fn [client]
                                 (ftcp/monitor-remote-connection this-server client client-message-handler nil))
        ;; both starts server and returns a shutdown function
        server-shutdown-fn     (ftcp/start-tcp-server port new-client-handler)]

    ;; handy for debugging
    ;(add-watch state-machine-atom :logger
    ;           (fn [_ _ _ nv]
    ;             (log/debug "state atom changed to" nv))))

    {:raft            raft
     :state-atom      state-machine-atom
     :port            port
     :server-shutdown server-shutdown-fn
     :this-server     this-server
     :event-chan      event-chan
     :command-chan    command-chan
     :private-keys    private-keys
     :open-api        open-api}))


(defn get-raft-state-async
  "Returns current raft state as a core async channel."
  [raft]
  (let [resp-chan (async/promise-chan)]
    (raft/get-raft-state (:raft raft)
                         (fn [rs]
                           (async/put! resp-chan rs)
                           (async/close! resp-chan)))
    resp-chan))


(defn monitor-raft
  "Monitor raft events and state for debugging"
  ([raft] (monitor-raft raft (fn [x] (cprint/pprint x))))
  ([raft callback]
   (raft/monitor-raft (:raft raft) callback)))


(defn monitor-raft-stop
  "Stops current raft monitor"
  [raft]
  (raft/monitor-raft (:raft raft) nil))


(defn state
  [raft]
  (let [state (async/<!! (get-raft-state-async raft))]
    (if (instance? Throwable state)
      (throw state)
      state)))


;; TODO configurable timeout
(defn new-entry-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group entry] (new-entry-async group entry 5000))
  ([group entry timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (new-entry-async group entry timeout-ms callback)
     resp-chan))
  ([group entry timeout-ms callback]
   (go-try (let [raft'  (:raft group)
                 leader (<! (leader-async group))]
             (if (= (:this-server raft') leader)
               (raft/new-entry raft' entry callback timeout-ms)
               (let [id           (str (UUID/randomUUID))
                     command-data {:id id :entry entry}]
                 ;; since not leader, register entry id locally and will receive callback when committed to state machine
                 (raft/register-callback raft' id timeout-ms callback)
                 ;; send command to leader
                 (send-rpc raft' leader :new-command command-data nil)))))))


(defn add-server-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group newServer] (add-server-async group newServer 5000))
  ([group newServer timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (add-server-async group newServer timeout-ms callback)
     resp-chan))
  ([group newServer timeout-ms callback]
   (go-try (let [raft'  (:raft group)
                 leader (async/<! (leader-async group))
                 id     (str (UUID/randomUUID))]
             (if (= (:this-server raft') leader)
               (let [command-chan (-> group :command-chan)]
                 (async/put! command-chan [:add-server [id newServer] callback]))
               (do (raft/register-callback raft' id timeout-ms callback)
                   ;; send command to leader
                   (send-rpc raft' leader :add-server [id newServer] nil)))))))


(defn remove-server-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group server] (remove-server-async group server 5000))
  ([group server timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (remove-server-async group server timeout-ms callback)
     resp-chan))
  ([group server timeout-ms callback]
   (go-try (let [raft'  (:raft group)
                 leader (async/<! (leader-async group))
                 id     (str (UUID/randomUUID))]
             (if (= (:this-server raft') leader)
               (let [command-chan (-> group :command-chan)]
                 (async/put! command-chan [:remove-server [id server] callback]))
               (do (raft/register-callback raft' id timeout-ms callback)
                   ;; send command to leader
                   (send-rpc raft' leader :remove-server [id server] nil)))))))


(defn local-state
  "Returns local, current state from state machine"
  [raft]
  @(:state-atom raft))


(defn acquire-lease-async
  "Acquires a lease as specified key sequence in state map using provided id for specified ms.
  Must re-acquire before expiration. Lease is not automatically removed, but using 'leased?' function
  will look at lease time, and expired leases will allow new ids to acquire lease.

  Returns true if lease acquired, false otherwise."
  [raft ks id expire-ms]
  (let [command [:lease ks id expire-ms]]
    (new-entry-async raft command)))


(defn release-lease-async
  [raft ks id]
  (let [command [:lease-release ks id]]
    (new-entry-async raft command)))


(defn server-active?
  "Returns true if server has a currently active lease."
  [raft server-id]
  (let [lease (txproto/kv-get-in raft [:leases :servers server-id])]
    (if lease
      (>= (:expire lease) (System/currentTimeMillis))
      false)))



(defn index-fully-committed?
  "Returns a core async channel that will eventually return the index/commit once they are both equal.

  This helps when building state machine at startup if there is no pre-existing leader.
  In this case we can know when all of this historical entries that might have been committed
  previously are re-committed.

  If leader-only? boolean flag is true, will close the channel if not leader as opposed to returning result.
  Either way we still wait until commit is fully updated in case there is a leader change during that process.

  Note if massively high volume, could be that commit and index are never equal. Not likely but in
  this case we eventually return an exception if we retry 10000 times just to have an upper bounds.
  Exception doesn't throw, be sure to check for it."
  ([raft] (index-fully-committed? raft false))
  ([raft leader-only?]
   (async/go-loop [retries 0
                   last-status nil]
     (let [rs (async/<! (get-raft-state-async raft))
           {:keys [commit index status latest-index]} rs]
       (when (not= last-status [commit index status latest-index])
         (log/trace (str "index-fully-committed?: retry: " retries
                         " [commit index status latest-index] is: " [commit index status latest-index])))
       (cond

         (and (>= index latest-index) status)               ;; status will be 'nil' until leader or follower initiating
         (if (or (not leader-only?)
                 (= :leader status))
           commit
           nil)


         (> retries 10000)
         (ex-info (str "Raft index-fully-committed? loop tried 10000 times without a change. Latest state: " (pr-str rs))
                  {:status 500 :error :db/unexpected-error})

         :else
         (do
           (async/<! (async/timeout 100))
           (recur (inc retries) [commit index status latest-index])))))))


(defn register-server-lease-async
  "Registers a server as available with provided lease expiration.
  Leases need to be continuously renewed else they will expire.

  Don't register unless you are ready to become a ledger (i.e. have synced all index files).

  Returns true if lease secured, false otherwise."
  [raft expire-ms]
  (let [this-server (:this-server (:raft raft))]
    (acquire-lease-async raft [:leases :servers this-server] this-server expire-ms)))


(defn check-if-newer-blocks-on-disk
  "In the case of startup as a leader, but possibly old raft state, we check to see
  if there are newer blocks on disk that were added after the raft state we started with.

  If so, it will broadcast those blocks out."
  [{:keys [group] :as conn}]
  (go-try
    (let [current-state @(:state-atom group)]
      (when-let [ledgers (not-empty (txproto/ledger-list* current-state))]
        (doseq [[network dbid] ledgers]
          (let [latest-block (txproto/block-height* current-state network dbid)]

            (log/debug "Raft startup - latest block: " [network dbid] latest-block)
            (loop [next-block (inc latest-block)]
              (when (<? (ledger-storage/block-exists? conn network dbid next-block))
                (let [block-data  (<? (storage/read-block conn network dbid next-block))
                      ;; incoming raft event expects a map of txns in block, with txid being keys. and :cmd-types
                      ;; would error in raft if these are not included, recreate from block data
                      block-data* (assoc block-data :cmd-types #{:tx}
                                                    :txns (->> (:flakes block-data)
                                                               (keep #(let [^Flake f %]
                                                                        (when (= const/$_tx:id (.-p f))
                                                                          [(.-o f) nil])))
                                                               (into {})))]

                  (log/info (str "Ledger " network "/" dbid
                                 " has block file(s) beyond raft known block height of "
                                 latest-block ". Found block: " next-block))
                  (<? (txproto/propose-new-block-async group network dbid block-data*)))
                (recur (inc next-block))))))))))


(defn check-existing-ledgers-on-disk
  [{:keys [group] :as conn}]
  (go-try
    (try
      (let [ledgers (async/<! (ledger-storage/ledgers conn))
            time    (System/currentTimeMillis)]
        (when (util/exception? ledgers)
          (log/error ledgers (str "EXITING: No raft state, and error reading existing ledger files: " (ex-message ledgers)
                                  ". If you don't want ledgers included please move the ledger directory or the ledger files."))
          (System/exit 1))
        (when (seq ledgers)
          (log/warn "Found existing ledgers on disk, attempting to rebuild state.")
          (doseq [[[network ledger] {:keys [block index indexes]}] ledgers]
            (log/warn (str "--> " network "/" ledger " block: " block " last index: " index "."))
            (let [ledger-state {:block   block
                                :index   index
                                :indexes (into {} (map #(vector % time) indexes))
                                :status  :ready}
                  resp         (async/<! (new-entry-async group [:assoc-in
                                                                 [:networks network :dbs ledger]
                                                                 ledger-state]))]
              (when (util/exception? resp)
                (log/error resp (str "EXITING: Unexpected raft error syncing existing ledgers at startup. "
                                     "Error occurred when syncing ledger: " network "/" ledger
                                     " with ledger state: " ledger-state "."))
                (System/exit 1))))
          (log/warn (str "State for ledgers rebuilt. If you previously deleted ledgers still on disk, "
                         "verify they don't need to be removed again."))))
      (catch Exception e
        (log/error e (str "EXITING: Unexpected error trying to synchronize existing ledgers on disk with current "
                          "raft state: " (ex-message e)))
        (System/exit 1)))))

(defn filter-exception
  "Return a channel that will eventually contain the first exception passed
  through `ch` or closing without any values if no exceptions pass through
  `ch`."
  [ch]
  (go-loop []
    (when-let [x (<! ch)]
      (if (exception? x)
        x
        (recur)))))

(defn raft-start-up
  [group conn system* shutdown _]
  (async/go
    (try (let [fully-committed? (async/<! (index-fully-committed? group true))]
           (when (instance? Throwable fully-committed?)
             (log/error fully-committed? "Exception when initializing raft. Shutting down.")
             (shutdown system*)
             (System/exit 1))

           ;; do an initial file sync... the committed raft may contain blocks that end up leaving gaps
           (let [file-storage? (some? (-> conn :meta :file-storage-path))]
             (when file-storage?                            ; TODO: Support full-text indexes on s3 storage too
               (let [ledgers-info (txproto/ledgers-info-map conn)
                     others       (-> group :raft :other-servers)]
                 (when-let [exception (<! (filter-exception
                                           (dbsync2/consistency-full-check conn ledgers-info others)))]
                   (dbsync2/terminate! conn (str "Terminating due to file syncing error, "
                                                 "unable to sync required files with other servers.")
                                       exception))
                 (when-let [exception (<! (filter-exception
                                           (dbsync2/check-full-text-synced conn ledgers-info)))]
                   (dbsync2/terminate! conn (str "Terminating due to full text index syncing error, "
                                                 "unable to sync index with the current ledgers.")
                                       exception))
                 (log/debug "All database files synchronized."))))

           ;; register on the network
           (async/<! (register-server-lease-async group 5000))

           (when (async/<! (is-leader?-async group))
             (let [new-instance? (empty? (txproto/get-shared-private-key group))]
               (if new-instance?
                 (let [config-private-key (:tx-private-key conn)
                       generated-key      (when-not config-private-key
                                            (crypto/generate-key-pair))
                       private-key        (or config-private-key
                                              (:private generated-key))]
                   (log/info "Brand new Fluree instance.")
                   (if config-private-key
                     (log/info "Using default private key obtained from configuration settings.")
                     (log/info (str "Generating brand new default key pair, public key is: " (:public generated-key))))
                   (txproto/set-shared-private-key (:group conn) private-key)
                   (<? (check-existing-ledgers-on-disk conn)))
                 ;; not a new instance, but just started as leader - could have old
                 ;; raft files that don't have latest blocks. Check, and potentially add latest block
                 ;; files to network.
                 (<? (check-if-newer-blocks-on-disk conn)))))


           ;; monitor state changes to kick of transactions for any queues
           (register-state-change-fn (str (UUID/randomUUID))
                                     (partial group-monitor/state-updates-monitor system*))

           ;; in case we are responsible for networks but some exist in current queue, kick them off
           (group-monitor/kick-all-assigned-networks-with-queue conn)

           ;; create a loop to keep this server registered
           (loop []
             (let [;; pause 3 seconds
                   _           (async/<! (async/timeout 3000))
                   ;; TODO need to stop loop if server stopped
                   registered? (async/<! (register-server-lease-async group 5000))
                   leader?     (async/<! (is-leader?-async group))]

               ;; if leader, re-check worker distribute to ensure nothing is stuck
               (when (and leader? (true? registered?))
                 (group-monitor/redistribute-workers group)))

             (recur)))

         (catch Exception e
           (log/warn "Error during raft initialization. Shutting down system")
           (log/error e)
           (shutdown system*)
           (System/exit 1)))))


(defrecord RaftGroup [state-atom event-chan command-chan server this-server port
                      close raft raft-initialized open-api private-keys]
  TxGroup
  (-add-server-async [group server] (add-server-async group server))
  (-remove-server-async [group server] (remove-server-async group server))
  (-new-entry-async [group entry] (new-entry-async group entry))
  (-local-state [group] (local-state group))
  (-state [group] (state group))
  (-is-leader? [group] (is-leader? group))
  (-active-servers [group] (let [server-map   (txproto/kv-get-in group [:leases :servers])
                                 current-time (System/currentTimeMillis)]
                             (reduce-kv (fn [acc server lease-data]
                                          (if (>= (:expire lease-data) current-time)
                                            (conj acc server)
                                            acc))
                                        #{} server-map)))
  (-start-up-activities [group conn system shutdown join?] (raft-start-up group conn system shutdown join?)))


(defn launch-raft-server
  [server-configs this-server raft-configs]
  (let [join?                 (:join? raft-configs)
        server-duplicates?    (not= (count server-configs) (count (into #{} (map :server-id server-configs))))
        _                     (when server-duplicates?
                                (throw (ex-info (str "There appear to be duplicates in the group servers configuration: "
                                                     (pr-str server-configs))
                                                {:status 400 :error :db/invalid-configuration})))
        this-server-cfg       (some #(when (= this-server (:server-id %)) %) server-configs)
        _                     (when-not this-server-cfg
                                (throw (ex-info (str "This server: " (pr-str this-server) " has to be included in the group
                                server configuration." (pr-str server-configs))
                                                {:status 400 :error :db/invalid-configuration})))
        raft-servers          (->> server-configs           ;; ensure unique
                                   (mapv :server-id server-configs)
                                   (into #{})
                                   (into []))
        server-duplicates?    (not= (count server-configs) (count raft-servers))
        _                     (when server-duplicates?
                                (throw (ex-info (str "There appear to be duplicates in the group servers configuration: "
                                                     (pr-str server-configs))
                                                {:status 400 :error :db/invalid-configuration})))
        raft-initialized-chan (async/promise-chan)
        leader-change-fn      (:leader-change-fn raft-configs)
        leader-change-fn*     (fn [change-map]
                                (let [{:keys [new-raft-state old-raft-state]} change-map]
                                  (log/info "Ledger group leader change:"
                                            (dissoc change-map :key :new-raft-state :old-raft-state))
                                  (log/debug "Old raft state: \n" old-raft-state "\n"
                                             "New raft state: \n" new-raft-state)
                                  (when (not (nil? new-raft-state))
                                    (cond (and join? (not (nil? (:leader new-raft-state)))
                                               (not= this-server (:leader new-raft-state)))
                                          (async/put! raft-initialized-chan :follower)

                                          join?
                                          true

                                          (= this-server (:leader new-raft-state))
                                          (async/put! raft-initialized-chan :leader)

                                          :else
                                          (async/put! raft-initialized-chan :follower)))
                                  (when (fn? leader-change-fn)
                                    (leader-change-fn change-map))))
        raft-configs*         (merge raft-configs
                                     {:port             (:port this-server-cfg)
                                      :servers          raft-servers
                                      :this-server      this-server
                                      :leader-change-fn leader-change-fn*})
        _                     (log/debug "Starting raft instance with config: " raft-configs*)
        raft-instance         (start-instance raft-configs*)
        close-fn              (fn []
                                ;; close raft
                                (raft/close (:raft raft-instance))
                                ;; Unregister state-change-fns
                                (unregister-all-state-change-fn)
                                ;; close any open connections
                                (ftcp/close-all-connections this-server)
                                ;; close tcp server
                                ((:server-shutdown raft-instance)))]

    ;; we need a single duplex connection to each server.
    ;; TODO - Need slightly more complicated handling. If a server joins, close, and tries to restart with join = false, will fail
    (if join?
      ;; If joining an existing network, connects to all other servers
      (let [connect-servers (filter #(not= this-server (:server-id %)) server-configs)
            handler-fn      (partial message-consume (:raft raft-instance) (:storage-ledger-read raft-configs))]
        (log/debug "Raft: join? is true, joining all other servers: " connect-servers)
        (doseq [connect-to connect-servers]
          (ftcp/launch-client-connection this-server-cfg connect-to handler-fn)))

      ;; simple rule (for now) is we connect to servers whose id is > (lexical sort) than our own
      (let [connect-servers (filter #(> 0 (compare this-server (:server-id %))) server-configs)
            handler-fn      (partial message-consume (:raft raft-instance) (:storage-ledger-read raft-configs))]
        (doseq [connect-to connect-servers]
          (log/debug "Raft: connecting to server: " connect-to)
          (ftcp/launch-client-connection this-server-cfg connect-to handler-fn))))


    (-> (assoc raft-instance :raft-initialized raft-initialized-chan
                             :close close-fn)
        map->RaftGroup)))
