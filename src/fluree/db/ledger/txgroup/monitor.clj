(ns fluree.db.ledger.txgroup.monitor
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.core.async :as async]
            [fluree.db.session :as session]
            [fluree.db.ledger.transact :as transact]
            [fluree.db.ledger.bootstrap :as bootstrap]
            [fluree.db.util.json :as json]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.api :as fdb]))

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
   returns three-tuple of [network dbid txid], else nil."
  [state-change]
  (let [{:keys [command result]} state-change
        [op arg1 arg2] command]
    (when (and (= :assoc-in op)
               (= :cmd-queue (first arg1))
               (true? result))
      (let [{:keys [dbid network id]} arg2]
        [network dbid id]))))


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
  - dbid
  - instant

 Commands can optionally contain multiTx, which allows for multiple transactions to be handle together in the
 same block. In order for multiTx to be allowed, all transactions need to be in the queue.  The order of txns
 listed in the last txn submitted is the order in which the transactions are processed."
  [db cmd-queue]
  ;; TODO - need to check each command id to ensure it hasn't yet been processed
  (let [max-size     (or (get-in db [:settings :txMax]) default-max-txn-size)
        sorted-queue (->> cmd-queue
                          (sort-by :instant))]
    (loop [[next-cmd & r] sorted-queue
           total-size 0
           queued     []]
      (let [size        (:size next-cmd)
            multiTxs    (-> next-cmd :command :multiTxs)
            [size next-cmds] (if multiTxs
                               (let [multi (and (<= size max-size)
                                                (get-multi-txns next-cmd r multiTxs))]
                                 ;; If multiTxs, but not all have come through, skip
                                 (if multi multi [0 nil]))
                               [size [next-cmd]])
            total-size* (if (> size max-size)
                          ;; if command is larger than max-size, we will automatically reject - include tx to process error.
                          total-size
                          (+ total-size size))
            queued*     (if (or (> total-size* max-size) (nil? next-cmds))
                          queued
                          (concat queued next-cmds))]
        (if (and r (< total-size* max-size))
          (recur r total-size* queued*)
          queued*)))))


;; holds db-queues. Creates them on the fly if needed.
(def db-queues-atom (atom {}))

(defn close-db-queue
  "Closes queue for db by closing channel and removing from queue atom"
  ([]
   (reset! db-queues-atom {})
   true)
  ([network dbid]
   (swap! db-queues-atom (fn [x]
                           (when-let [chan (get-in x [network dbid])]
                             (async/close! chan)
                             (update x network dissoc dbid)))) true))


;; TODO - need to detect and propagate errors
;; TODO - need way for leader to reject new block if we changed who is responsible for a network in-between, and detect + close here
(defn db-queue-loop
  "Runs a continuous loop for a db to process new blocks"
  [conn chan network dbid]
  (let [group       (:group conn)
        this-server (txproto/this-server group)]
    (async/go-loop [last-t nil]
      (let [kick (async/<! chan)]
        (when-not (nil? kick)
          (if-not (network-assigned-to? group this-server network)
            (do
              (log/info (str "Network " network " is no longer assigned to this server. Stopping to process transactions."))
              (close-db-queue network dbid))
            (let [queue     (txproto/command-queue group network dbid)
                  new-block (try (when (not-empty queue)
                                   (let [session    (session/session conn [network dbid])
                                         db         (<? (session/current-db session))
                                         at-last-t? (or (nil? last-t)
                                                        (= last-t (:t db)))]
                                     (if at-last-t?
                                       (let [cmds (select-block-commands db queue)]
                                         (when-not (empty? cmds)
                                           (->> cmds
                                                (transact/build-block session)
                                                <?)))
                                       ;; db isn't up to date, shouldn't happen but before abandoning, dump and reload db to see if we can get current
                                       (let [_           (session/clear-db! session)
                                             db*         (<? (session/current-db session))
                                             at-last-t?* (= last-t (:t db*))]
                                         (if at-last-t?*
                                           (do
                                             (log/info (format "Ledger %s/%s is not automatically updating internally, session may be lost." network dbid))
                                             (let [cmds (select-block-commands db queue)]
                                               (when-not (empty? cmds)
                                                 (->> cmds
                                                      (transact/build-block session)
                                                      <?))))
                                           (log/warn (format "Ledger skipping new transactions because our last processed 't' is %s, but the latest db we can retrieve is at %s" last-t (:t db))))))))
                                 (catch Exception e
                                   (log/error e "Error processing new block. Exiting tx monitor loop.")))]
              (when (not-empty queue)                       ;; in case we still have a queue to process, kick again until finished
                (async/put! chan ::kick))
              (recur (if new-block
                       (:t new-block)
                       last-t)))))))))


(defn db-queue
  [conn network dbid]
  (or (get-in @db-queues-atom [network dbid])
      (do
        (swap! db-queues-atom (fn [x]
                                (if (get-in x [network dbid])
                                  x
                                  (let [chan (async/chan (async/dropping-buffer 1))]
                                    ;; kick off loop
                                    (db-queue-loop conn chan network dbid)
                                    (assoc-in x [network dbid] chan)))))
        (get-in @db-queues-atom [network dbid]))))


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
        (let [queued-dbids (->> network-queue
                                (map :dbid)
                                (into #{}))]
          (doseq [dbid queued-dbids]
            (async/put! (db-queue conn network dbid) ::kick))
          true)))))

(defn new-db?
  "Returns [network dbid] if state update involves a new db.

  Monitors for :new-block commands, which look like this:
  [op network dbid block-map submission-server]"
  [state-change]
  (let [{:keys [command result]} state-change]
    (when (and (contains? (:cmd-types (nth command 3)) :new-db)
               (true? result))
      ;; we have a new DB!
      ;; return network db is within
      (second command))))

(defn state-updates-monitor
  "Function to be called with every state change, to possibly kick of an action.

  State-change function is called with state-change arg, a map with keys:
  - command - the command data/payload which is a vector of [op & args]
  - result - the state-machines result response after applying this command"
  [system state-change]
  (log/trace "State change in tx-group: " (pr-str state-change))
  (let [op   (get-in state-change [:command 0])
        conn (:conn system)]
    (case op
      :new-db
      (future
        (when (txproto/-is-leader? (:group conn))
          (let [initialize-dbs (txproto/find-all-dbs-to-initialize (:group conn))]
            (doseq [[network dbid command] initialize-dbs]
              (json/parse (:cmd command))
              (async/<!! (bootstrap/bootstrap-db system command))
              (let [session  (session/session conn [network dbid])]
                ;; force session close, so next request will cause session to keep in sync
                (session/close session))))))

      :initialized-db
      (future
        (when-not (txproto/-is-leader? (:group conn))
          (let [command (:command state-change)
                session (session/session conn [(get command 2) (get command 3)])]
            (session/close session))))

      :assoc-in
      (when-let [queued-tx (queued-tx state-change)]
        (let [[network dbid _] queued-tx]
          (when (network-assigned-to? (:group conn) network)
            (let [queue-chan (db-queue conn network dbid)]
              (async/put! queue-chan ::kick)))))

      :worker-assign
      (when (and (not (in-memory-db? (:group system)))
                 (work-changed? (-> conn :group :this-server) state-change))
        ;; something about our work changed, re-run checks and do some work!
        (kick-all-assigned-networks-with-queue conn))

      ;; Currently only used for fullText search indexing
      :new-block
      (go-try
       (let [[_ network dbid block-data] (:command state-change)
             db          (<? (fdb/db (:conn system) [network dbid]))
             indexer     (-> conn :full-text/indexer :process)]
         ;; TODO: Support full-text indexes on s3 too
         (<? (indexer {:action :block, :db db, :block block-data}))))

      ;;else
      nil)))
