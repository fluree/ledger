(ns fluree.db.ledger.txgroup.monitor
  (:require [fluree.db.util.log :as log]
            [clojure.set :as set]
            [clojure.core.async :as async :refer [<! go]]
            [fluree.db.session :as session]
            [fluree.db.ledger.transact :as transact]
            [fluree.db.ledger.bootstrap :as bootstrap]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.api :as fdb]))

(set! *warn-on-reflection* true)

;; For now, just use this as a lock to ensure multiple processes are not trying to redistribute work simultaneously.
(def ^:private redistribute-workers-lock (atom nil))

;; Maximum size for a transaction, default is 2mb
(def ^:const default-max-txn-size 2e6)

(defn- acquire-lock
  "Returns true if acquired, false otherwise."
  []
  (let [myid (rand-int (Integer/MAX_VALUE))
        _    (swap! redistribute-workers-lock (fn [existing-id]
                                                (if existing-id
                                                  existing-id
                                                  myid)))]
    (= myid @redistribute-workers-lock)))


(defn- release-lock
  "Releases lock"
  []
  (reset! redistribute-workers-lock nil))


(defn get-worker
  "Returns map of all worker(s) as specified key-seq.

  Workers will always be the keys of the map at the 'leaf' of the key-seq.
  The map values will the be epoch-ms when that assignment was made."
  [raft ks]
  (txproto/kv-get-in raft (into [:_work] ks)))

(defn network-assigned-to
  "Returns truthy with assigned worker if network is assigned, else nil."
  [group network]
  (get-worker group [:networks network]))

(defn in-memory-db?
  [group]
  (= "class fluree.db.ledger.consensus.none.InMemoryGroup" (str (type group))))

(defn network-assigned-to?
  "Returns true if network is currently assigned to specified server-id.
  In non-server-id arity, uses :this-server from raft."
  ([group network] (network-assigned-to? group (:this-server group) network))
  ([group server-id network]
   (or (in-memory-db? group) (= server-id (network-assigned-to group network)))))

(defn worker-assign-async
  "Assigns a piece of work represented by provided key-sequence to the worker/server id specified.

  All work is stored in the map under both :_worker and :_work keys using slightly different key-seqs.

  :_work is intended to be a quick lookup of who is doing work specified.
  :_worker is intended to be able to quickly look up all work being done by the specified worker.

  Any new assigments automatically un-assign any existing worker.

  A worker-id of nil removes the work (dissoc's the last key in the key-seq)."
  [raft ks worker-id]
  (let [command [:worker-assign ks worker-id]]
    (txproto/-new-entry-async raft command)))


(defn assign-network-to-server-async
  "Uses the worker-assign feature."
  [raft server-id network]
  (worker-assign-async raft [:networks network] server-id))


(defn get-work
  "Returns all work for given worker-id as a map"
  [raft worker-id]
  (txproto/kv-get-in raft [:_worker worker-id]))


(defn assigned-networks-for-server
  "Returns assigned networks as a set based on server-id"
  ([raft] (assigned-networks-for-server raft (:this-server (:raft raft))))
  ([raft server-id]
   (-> (get-work raft server-id)
       :networks
       keys
       (set))))


(defn assigned-networks
  "Returns all networks that currently have assignments."
  [raft]
  (-> (get-worker raft [:networks])
      (keys)
      (set)))


(defn worker-status
  "Returns stats on current networks, unassigned networks, avail servers, underassigned servers and overassigned servers.

  :underassigned and :overassigned are sorted lists (in order of most over/under assigned servers)
  that are two-tuples of [server-id #{networks-assigned}]"
  [group]
  (let [servers-avail (txproto/-active-servers group)]
    (if (empty? servers-avail)
      nil
      (let [networks                (into #{} (txproto/network-list group))
            current-assignments     (reduce (fn [acc server] (conj acc [server (assigned-networks-for-server group server)])) [] servers-avail)
            assigned-networks       (->> current-assignments (mapcat second) (into #{}))
            unassigned-networks     (set/difference networks assigned-networks)
            servers-avail           (txproto/-active-servers group)
            networks-per-server     (/ (count networks) (count servers-avail))
            max-networks-per-server (int (Math/ceil networks-per-server))
            ;; when networks-per-server is a fraction, min will be one less than max. Else they will be the same.
            min-networks-per-server (if (= networks-per-server max-networks-per-server)
                                      networks-per-server
                                      (dec max-networks-per-server))
            overassigned            (->> current-assignments
                                         (filter #(> (count (second %)) max-networks-per-server))
                                         (sort-by #(count (second %)) >))
            underassigned           (->> current-assignments
                                         (filter #(<= (count (second %)) min-networks-per-server))
                                         (sort-by #(count (second %)) <))]
        {:networks            networks
         :unassigned          unassigned-networks
         :servers             servers-avail
         :current-assigned    current-assignments
         :networks-per-server max-networks-per-server
         :overassigned        overassigned
         :underassigned       underassigned}))))


(defn redistribute-workers
  "Finds any unassigned networks or 'over-assigned' workers and moves work to available workers.

  Uses a lock to ensure only a single process is redistributing at a time.

  Returns true if works was redistributed, false if another process was already redistributing based on lock
  or we are not currently the leader."
  [group]
  (cond
    (not (txproto/-is-leader? group))
    false

    (not (acquire-lock))
    false

    ;; we have a lock
    :else
    (if-let [worker-status (worker-status group)]
      (let [{:keys [unassigned networks-per-server overassigned underassigned]} worker-status
            to-assign (into unassigned (->> overassigned (mapcat #(drop networks-per-server (second %)))))]
        ;(log/warn " ------ networks that need assignment: " to-assign " underassigned servers: " underassigned)
        (loop [[next-server & r] underassigned
               to-assign to-assign]
          (let [[server-id server-assigned] next-server
                avail-n     (- networks-per-server (count server-assigned))
                assign-list (take avail-n to-assign)
                to-assign*  (drop avail-n to-assign)]
            (doseq [network assign-list]
              (assign-network-to-server-async group server-id network))
            (when (and (not-empty to-assign*)
                       (not-empty r))
              (recur r to-assign*))))
        (release-lock))
      (do
        (log/warn "Unable to assign work to servers, none are registered as active on the network.")
        (release-lock)))))


(defn queued-tx
  "If state change is a new tx that was queued
   returns three-tuple of [network ledger-id txid], else nil."
  [state-change]
  (let [{:keys [command result]} state-change
        [op arg1 arg2] command]
    (when (and (= :assoc-in op)
               (= :cmd-queue (first arg1))
               (true? result))
      (let [{:keys [ledger-id network id]} arg2]
        [network ledger-id id]))))


(defn work-changed?
  [server-id state-change]
  (let [{:keys [command]} state-change
        [op _ arg2] command]
    (and (= :worker-assign op)
         (= arg2 server-id))))


(defn get-multi-txns
  [cmd cmds multiTxns]
  (let [multiSet      (set multiTxns)
        filtered-cmds (filter #(multiSet (:id %)) cmds)
        all-cmds      (concat [cmd] filtered-cmds)]
    (if (= (count multiTxns) (count all-cmds))
      (let [size     (reduce #(+ (:size %2) %1) 0 cmds)
            ;; order the txns
            all-cmds (reduce (fn [acc txid]
                               (conj acc (some #(when (= txid (:id %)) %) all-cmds)))
                             [] multiTxns)]
        [size all-cmds]) nil)))


(defn select-block-commands
  "Given a db, and command queue, selects the next set of commands for a block

  cmd-queue is a list of commands, each with keys:
  - command
  - size
  - id
  - network
  - ledger-id
  - instant

 Commands can optionally contain multiTx, which allows for multiple transactions to be handle together in the
 same block. In order for multiTx to be allowed, all transactions need to be in the queue.  The order of txns
 listed in the last txn submitted is the order in which the transactions are processed."
  [cmd-queue tx-max]
  ;; TODO - need to check each command id to ensure it hasn't yet been processed
  (let [sorted-queue (->> cmd-queue
                          (sort-by :instant))]
    (loop [[next-cmd & r] sorted-queue
           total-size 0
           queued     []]
      (let [size        (:size next-cmd)
            multiTxs    (-> next-cmd :command :multiTxs)
            [size next-cmds] (if multiTxs
                               (let [multi (and (<= size tx-max)
                                                (get-multi-txns next-cmd r multiTxs))]
                                 ;; If multiTxs, but not all have come through, skip
                                 (if multi multi [0 nil]))
                               [size [next-cmd]])
            total-size* (if (> size tx-max)
                          ;; if command is larger than max-size, we will automatically reject - include tx to process error.
                          total-size
                          (long (+ total-size size)))       ; long keeps the recur arg primitive
            queued*     (if (or (> total-size* tx-max) (nil? next-cmds))
                          queued
                          (concat queued next-cmds))]
        (if (and r (< total-size* tx-max))
          (recur r total-size* queued*)
          queued*)))))


;; holds ledger-queues. Creates them on the fly if needed.
(def ledger-queues-atom (atom {}))


(defn close-ledger-queue
  "Closes queue for ledger by closing channel and removing from queue atom"
  ([]
   (reset! ledger-queues-atom {})
   true)
  ([network ledger-id]
   (swap! ledger-queues-atom (fn [x]
                               (when-let [chan (get-in x [network ledger-id])]
                                 (async/close! chan)
                                 (update x network dissoc ledger-id)))) true))


(defn clear-stale-commands
  [recent-cmds threshold]
  (->> recent-cmds
       (remove (fn [[cmd-id time]]
                 (< time threshold)))
       (into {})))

(defn add-new-commands
  [recent-cmds new-cmds time]
  (reduce (fn [m {:keys [id]}]
            (assoc m id time))
          recent-cmds new-cmds))

(defn get-tx-max
  "Returns max transaction size from db, or utilizes default"
  [db]
  (or (get-in db [:settings :txMax]) default-max-txn-size))


;; TODO - need to detect and propagate errors
;; TODO - need way for leader to reject new block if we changed who is
;;        responsible for a network in-between, and detect + close here
(defn ledger-queue-loop
  "Runs a continuous loop for a db to process new blocks"
  [{:keys [group] :as conn} kick-chan network ledger-id queue-id]
  (go
    (let [this-server (txproto/this-server group)
          session     (doto (session/session conn [network ledger-id])
                        session/reload-db!) ; always reload the session DB to
                                            ; ensure latest when starting loop
          db          (<? (session/current-db session))
          tx-max      (get-tx-max db)]
      (loop [prev-block  (transact/new-block-map db)
             recent-cmds {}]
        (when (some? (<! kick-chan))
          (if-not (network-assigned-to? group this-server network)
            (do
              (log/info "Network" network
                        "is no longer assigned to this server. Stopping transaction processing.")
              (close-ledger-queue network ledger-id)
              (session/close session))
            (let [db              (:db-after prev-block)
                  time            (System/currentTimeMillis)
                  clear-threshold (- time 100000) ; 100 seconds ago
                  recent-cmds*    (clear-stale-commands recent-cmds clear-threshold)
                  queue           (remove (fn [{:keys [id]}]
                                            (contains? recent-cmds id))
                                          (txproto/command-queue group network ledger-id))]
              (if (seq queue)
                (let [new-cmds      (select-block-commands queue tx-max)
                      next-block    (try
                                      (<? (transact/build-block session db new-cmds))
                                      (catch Exception e
                                        (log/error e
                                                   "Error processing new block. Ignoring last block and transaction(s):"
                                                   (map :id new-cmds))
                                        prev-block))
                      recent-cmds** (add-new-commands recent-cmds* new-cmds time)]
                  (async/put! kick-chan ::kick)
                  (recur next-block recent-cmds**))
                (recur prev-block recent-cmds*)))))))))


(defn random-queue-id
  [network ledger-id]
  (str network "/" ledger-id ":" (rand-int 100000)))

(defn ledger-queue
  [conn network ledger-id]
  (or (get-in @ledger-queues-atom [network ledger-id])
      (-> ledger-queues-atom
          (swap! (fn [queues]
                   (if-not (get-in queues [network ledger-id])
                     (let [queue-id (random-queue-id network ledger-id)
                           queue-ch (async/chan (async/dropping-buffer 1))]
                       (ledger-queue-loop conn queue-ch network ledger-id queue-id)
                       (assoc-in queues [network ledger-id] queue-ch))
                     queues))) ; race condition - queue was just created so don't create new one
          (get-in [network ledger-id]))))


(defn kick-all-assigned-networks-with-queue
  "Looks at all assigned networks, then for any with a queue gives the
  processor a 'kick' to ensure it is running and to process a block.

  Used at start-up, and with worker changes. Otherwise we will get notifications
  of any new queued tx and kick automatically.

  Returns true if it found anything to kick."
  [conn]
  (let [group             (:group conn)
        assigned-networks (if (in-memory-db? group)
                            (txproto/network-list group)
                            (assigned-networks-for-server group))]
    (doseq [network assigned-networks]
      (when-let [network-queue (not-empty (txproto/command-queue group network))]
        (let [queued-ledger-ids (->> network-queue
                                     (map :ledger-id)
                                     (into #{}))]
          (doseq [ledger-id queued-ledger-ids]
            (async/put! (ledger-queue conn network ledger-id) ::kick))
          true)))))


(defn state-updates-monitor
  "Function to be called with every state change, to possibly kick of an action.

  State-change function is called with state-change arg, a map with keys:
  - command - the command data/payload which is a vector of [op & args]
  - result - the state-machines result response after applying this command"
  [system state-change]
  (log/trace "State change in tx-group:" state-change)
  (let [op   (get-in state-change [:command 0])
        conn (:conn system)]
    (case op
      :new-ledger
      (future
        (when (txproto/-is-leader? (:group conn))
          (let [initialize-ledgers (txproto/find-all-ledgers-to-initialize (:group conn))]
            (doseq [[network ledger-id command] initialize-ledgers]
              (log/info "Initializing new ledger:" (str network "/" ledger-id) "-" command)
              (let [db      (try
                              (<?? (bootstrap/bootstrap-ledger system command))
                              (catch Exception e
                                (log/error e "Failed to bootstrap new ledger:" (str network "/" ledger-id))))
                    _       (log/trace "bootstrap-ledger returned:" db)
                    session (session/session conn [network ledger-id])]
                ;; force session close, so next request will cause session to keep in sync
                (session/close session))))))

      :initialized-ledger
      (future
        (when-not (txproto/-is-leader? (:group conn))
          (let [command (:command state-change)
                session (session/session conn [(get command 2) (get command 3)])]
            (session/close session))))

      :assoc-in
      (when-let [[network ledger-id _] (queued-tx state-change)]
        (when (network-assigned-to? (:group conn) network)
          (let [queue-chan (ledger-queue conn network ledger-id)]
            (async/put! queue-chan ::kick))))

      :worker-assign
      (when (and (not (in-memory-db? (:group system)))
                 (work-changed? (-> conn :group :this-server) state-change))
        ;; something about our work changed, re-run checks and do some work!
        (kick-all-assigned-networks-with-queue conn))

      ;; Currently only used for fullText search indexing
      :new-block
      (go-try
        (let [[_ network ledger-id block-data] (:command state-change)
              db      (<? (fdb/db (:conn system) [network ledger-id]))
              indexer (-> conn :full-text/indexer :process)]
          ;; TODO: Support full-text indexes on s3 too
          (<? (indexer {:action :block, :db db, :block block-data}))))

      ;;else
      nil)))
