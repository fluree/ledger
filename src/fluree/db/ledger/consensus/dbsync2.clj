(ns fluree.db.ledger.consensus.dbsync2
  (:require [fluree.db.storage.core :as storage]
            [fluree.db.ledger.storage :as ledger-storage]
            [clojure.core.async :as async :refer [go <! >!]]
            [fluree.db.util.log :as log]
            [fluree.db.ledger.storage.filestore :as filestore]
            [fluree.db.ledger.util :as util :refer [go-try <?]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [clojure.string :as str]
            [fluree.db.api :as fdb]))

(set! *warn-on-reflection* true)


(defn terminate!
  "Shuts down entire server.
  Reports message, logs exception and exists."
  [conn message exception]
  (log/error exception message)
  (let [group (:group conn)]
    (when-let [group-close (:close group)]
      (when (fn? group-close)
        (group-close)))
    (when-let [conn-close (:close conn)]
      (when (fn? conn-close)
        (conn-close))))
  (System/exit 1))


(def remote-servers-alive (atom {:last-check    nil
                                 :all-servers   []
                                 :alive-servers []}))


(defn- get-alive-servers
  "Gets alive servers - every 15 seconds will retry all servers in case new ones became alive, or existing
  ones went offline."
  []
  (swap! remote-servers-alive
         (fn [server-map]
           (let [{:keys [last-check alive-servers]} @remote-servers-alive]
             (if (or (empty? alive-servers)
                     (and last-check (< (+ last-check 15000) (System/currentTimeMillis))))
               ;; reset all servers
               (assoc server-map :last-check (System/currentTimeMillis)
                                 :alive-servers (:all-servers server-map))
               ;; keep map same, not time to adjust yet
               server-map))))
  ;; always return alive servers - above will force all servers to alive state if first time called
  (:alive-servers @remote-servers-alive))


(defn- remove-alive-server
  "Removes a server that timed out, so it won't be tried again."
  [server]
  (swap! remote-servers-alive
         (fn [server-map]
           (let [servers (filterv #(not= server %) (:alive-servers server-map))]
             (assoc server-map :alive-servers servers)))))


(defn remote-copy-fn*
  "Creates remote copy function that only requires file key.
  Should return exception if exhausts all options for remote copying."
  [conn remote-sync-servers server-timeout]
  (let [{:keys [group meta]} conn
        {:keys [file-storage-path encryption-secret]} meta
        ;; Note - do not use storage-write on the connection, as that will write through consensus layer - we just want to write locally here
        storage-write (filestore/connection-storage-write file-storage-path encryption-secret)
        raft          (:raft group)
        ;; send-rpc has args: [raft server operation data callback]
        send-rpc-fn   (get-in raft [:config :send-rpc-fn])]
    (swap! remote-servers-alive assoc :all-servers remote-sync-servers)
    ;; optionally, we can pass in an extra finished? channel - used for syncing indexes
    (fn [file-key-or-vec result-ch]
      (let [server-list (shuffle (get-alive-servers))
            [file-key finished?-port] (if (sequential? file-key-or-vec)
                                        [(first file-key-or-vec) (second file-key-or-vec)]
                                        [file-key-or-vec nil])
            raise       (fn [e message server]
                          (let [ex (ex-info
                                     (or message
                                         (str "Fatal error raised attempting to copy file: " file-key))
                                     {:status       500
                                      :error        :db/storage-error
                                      :file         file-key
                                      :server       server
                                      :server-order server-list}
                                     e)]
                            (when finished?-port
                              (async/put! finished?-port ex)
                              (async/close! finished?-port))
                            (async/put! result-ch ex)
                            (async/close! result-ch)))]
        (log/debug (str "Remote file copy request outgoing for: " file-key))
        (go
          (loop [[server & r] server-list]
            (let [resp-chan    (async/chan 1)
                  callback     (fn [resp] (if (nil? resp)
                                            (async/close! resp-chan)
                                            (async/put! resp-chan resp)))
                  _            (try (send-rpc-fn raft server :storage-read file-key callback)
                                    (catch Exception e (raise e "File copy send-rpc error" server)))
                  timeout-chan (async/timeout server-timeout)
                  result       (async/alt! timeout-chan :timeout
                                           resp-chan ([data] data))]
              (cond
                (= :timeout result)
                (if r
                  (do
                    (remove-alive-server server)
                    (recur r))
                  (raise nil
                         (format "Unable to retrieve file: %s after attempting servers: %s" file-key server-list)
                         nil))

                ;; some error, but try a different server if available
                (or (instance? Exception result) (nil? result))
                (if r
                  (recur r)                                 ;; more servers to try
                  (raise result
                         (format "Something went wrong. Trying to copy %s. Attempted all servers: %s" file-key server-list)
                         server))

                ;; we have a result!
                (not (nil? result))
                (let [write-res (<! (storage-write file-key result))]
                  (when (instance? Exception write-res)
                    (raise write-res
                           (str "Unexpected error, unable to write file " file-key " to local storage.")
                           server))
                  (log/debug (str "Remote file copy request complete for: " file-key))
                  (when finished?-port
                    (async/put! finished?-port file-key)
                    (async/close! finished?-port))
                  (async/put! result-ch file-key)
                  (async/close! result-ch))))))))))


(defn read-file-local
  "Returns core async channel with file's contents.

  If file is not yet local, tries to retrieve it and then returns."
  [conn sync-chan stats-atom file-key]
  (go-try
    (if-let [data (<? (storage/read-branch conn file-key))]
      data
      (let [result-ch (async/chan)]
        (swap! stats-atom update :missing inc)
        ;; queue request for file
        (>! sync-chan [file-key result-ch])
        ;; wait until we have confirmation it is in place.
        (<? result-ch)
        ;; once in place, read
        (or (<? (storage/read-branch conn file-key))
            (terminate! conn (str "Retrieved index file " file-key
                                  " however unable to read file from disk.")
                        (ex-info (str "Cannot read file from disk: " file-key)
                                 {:status 500 :error :db/invalid-file})))))))


(defn ensure-all-leaf-nodes-on-disk
  [{:keys [storage-exists] :as conn} sync-chan stats-atom leaf-keys]
  (go-try
    (loop [[leaf-key & r] leaf-keys]
      (if (nil? leaf-key)
        ::done
        (do
          (swap! stats-atom update :files inc)
          (when-not (<? (storage-exists leaf-key))
            (swap! stats-atom update :missing inc)
            (>! sync-chan leaf-key))
          (recur r))))))


(defn sync-index-branch
  "Starts an index branch, and synchronizes all the way to the data leafs,
  ensuring they are all on disk. If a leaf is not on disk, adds it to the port for
  retrieval."
  ([conn sync-chan stats-atom branch-id]
   (sync-index-branch conn sync-chan stats-atom branch-id []))
  ([conn sync-chan stats-atom branch-id parent]
   (log/debug "Start index branch node: " (conj parent branch-id))
   (go-try
     ;; first get file local if not already here. Will throw if an error occurs
     (swap! stats-atom update :files inc)                   ;; branch file
     (let [branch      (<? (read-file-local conn sync-chan stats-atom branch-id))
           children    (:children branch)
           child-leaf? (true? (-> children first :leaf))
           child-ids   (map :id children)]
       (if child-leaf?
         (<? (ensure-all-leaf-nodes-on-disk conn sync-chan stats-atom child-ids))
         (loop [[child-id & r] child-ids]
           (if (nil? child-id)
             ::done
             (let [done? (<? (sync-index-branch conn sync-chan stats-atom child-id))]
               (log/debug "Index branch node complete: " (conj parent branch-id child-id))
               (recur r)))))))))


(defn sync-ledger-index
  "Does a 100% sync of a db to a given index point.

  Returns core async channel with either ::done, or an exception if
  an error occurs during sync."
  [{:keys [storage-exists] :as conn} network dbid index-point sync-chan]
  (go-try
    (let [start-time       (System/currentTimeMillis)
          db-root          (<? (storage/read-db-root conn network dbid index-point))
          _                (when-not db-root
                             (terminate! conn (str "Unable to read index root for ledger: " network "/" dbid
                                                   " at index point: " index-point ".")
                                         (ex-info "Unable to read index root file"
                                                  {:network network :ledger dbid :index index-point})))
          stats-atom       (atom {:files   1
                                  :missing 0})
          {:keys [spot psot post opst tspo]} db-root

          sync-spot-ch     (sync-index-branch conn sync-chan stats-atom (:id spot))
          sync-psot-ch     (sync-index-branch conn sync-chan stats-atom (:id psot))
          sync-post-ch     (sync-index-branch conn sync-chan stats-atom (:id post))
          sync-opst-ch     (sync-index-branch conn sync-chan stats-atom (:id opst))
          sync-tspo-ch     (sync-index-branch conn sync-chan stats-atom (:id tspo))
          garbage-file-key (storage/ledger-garbage-key network dbid index-point)
          garbage-exists?  (<? (storage-exists garbage-file-key))]
      (when-not garbage-exists?
        (swap! stats-atom update :missing inc)
        (>! sync-chan garbage-file-key))

      ;; kick off 5 indexes in parallel...  will throw if an error occurs
      (<? sync-spot-ch)
      (<? sync-psot-ch)
      (<? sync-post-ch)
      (<? sync-opst-ch)
      (<? sync-tspo-ch)
      (let [total-missing (:missing @stats-atom)]
        (when (> total-missing 0)
          (log/info (str "-- missing index files for ledger: " network "/" dbid ". "
                         "Retrieved " total-missing " missing files of "
                         (:files @stats-atom) " total in "
                         (- (System/currentTimeMillis) start-time) " milliseconds.")))
        total-missing))))


(defn check-all-blocks-consistency
  "Checks actual file directory for any missing blocks through provided 'check through' block.
  Puts block file keys (filenames) onto provided port if they are missing."
  [conn network dbid check-through port]
  (go-try
    (log/debug (str "check-all-blocks-consistency for: " network "/" dbid "."))
    (let [blocks (into #{} (<? (ledger-storage/blocks conn network dbid)))]
      (loop [block-n check-through]
        (if (< block-n 1)
          ::finished
          (do
            (when-not (contains? blocks block-n)
              (log/warn (str "Block " block-n " missing for ledger: " network "/" dbid
                             ". Attempting to retrieve."))
              ;; block is missing, or file is empty... add to files we need to sync
              (>! port (storage/ledger-block-key network dbid block-n)))
            (recur (dec block-n))))))))


(defn missing-blocks
  "Given a list of blocks for a ledger and block-height, returns a
  list of missing blocks if any."
  [block-list block-height]
  (let [sorted-blocks (sort block-list)]
    (loop [[block & r] (range 1 (inc block-height))
           next-existing (first sorted-blocks)
           rest-existing (rest sorted-blocks)
           missing       []]
      (cond
        (nil? block)
        missing

        (nil? next-existing)
        (into missing (range block (inc block-height)))

        (= block next-existing)
        (recur r (first rest-existing) (rest rest-existing) missing)

        :else
        (recur r next-existing rest-existing (conj missing block))))))


(defn monitor-responses
  [conn responses-ch]
  (go
    (loop []
      (if-let [res-ch-or-msg (<! responses-ch)]
        (do
          (if (sequential? res-ch-or-msg)
            ;; a message passed through
            (let [[msg [network ledger] data] res-ch-or-msg]
              (case msg
                :complete-blocks (log/info (str "-- fully synchronized ledger blocks for "
                                                network "/" ledger " in "
                                                (- (System/currentTimeMillis) (:start data)) " milliseconds. "
                                                "Retrieved " (:missing data) " missing of " (:blocks data)
                                                " total blocks."))
                :complete-root (log/debug (str "Fully synchronized ledger index roots in "
                                               (- (System/currentTimeMillis) data) " milliseconds."))))
            ;; else an async chan
            (let [res (<! res-ch-or-msg)]
              (when (instance? Exception res)
                (let [ex-msg (ex-message res)
                      {:keys [file server server-list]} (ex-data res)
                      msg    (str "EXITING: Unable to retrieve block file: " file
                                  " from server: " server " of servers: " server-list
                                  " with error: " ex-msg ". "
                                  "Either find missing file and place on server,
                                  delete ledger if no longer used, and then restart.")]
                  (terminate! conn msg res)))))
          (recur))
        (do
          (log/debug "-------- All files synchronized.")
          ::done)))))


(defn verify-all-blocks
  "Prioritize all block files for all ledgers."
  [conn ledgers-info sync-chan]
  (go-try
    (let [start-time            (System/currentTimeMillis)
          responses             (async/chan)
          responses-complete-ch (monitor-responses conn responses)]
      (loop [[{:keys [network ledger] :as ledger-info} & r] ledgers-info
             total-missing 0]
        (if (nil? network)
          (do
            (async/close! responses)
            (<? responses-complete-ch)                      ;; wait for all block responses before proceeding
            (if (> total-missing 0)
              (log/info (str "Fetched total of " total-missing " missing block files across "
                             (count ledgers-info) " ledgers in "
                             (- (System/currentTimeMillis) start-time) " milliseconds."))
              (log/debug (str "Block consistency check completed in "
                              (- (System/currentTimeMillis) start-time) " milliseconds.")))
            ::done)
          (let [block-height (:block ledger-info)
                all-blocks   (<? (ledger-storage/blocks conn network ledger))
                missing      (missing-blocks all-blocks block-height)]
            (if (seq missing)
              (let [ledger-start-time (System/currentTimeMillis)]

                (log/debug (str (count missing) " of " block-height " blocks missing for ledger: "
                                network "/" ledger ", attempting to retrieve."
                                (when (not= (count missing) block-height) "Missing blocks: " missing)))
                (doseq [missing-block missing]
                  (let [res-chan (async/chan)]
                    (>! sync-chan [(storage/ledger-block-key network ledger missing-block) res-chan])
                    ;; put eventual response on responses channel to monitor for completeness.
                    (>! responses res-chan)))
                (>! responses [:complete-blocks [network ledger] {:start   ledger-start-time
                                                                  :blocks  block-height
                                                                  :missing (count missing)}]))
              (log/debug (str "All block files on disk for ledger: " network "/" ledger)))
            (recur r (+ total-missing (count missing)))))))))


(defn verify-all-index-roots
  [conn ledgers-info sync-chan]
  (go-try
    (let [start-time            (System/currentTimeMillis)
          responses             (async/chan)
          responses-complete-ch (monitor-responses conn responses)]
      (loop [[{:keys [network ledger index] :as ledger-info} & r] ledgers-info
             total-missing 0]
        (if (nil? network)
          (do
            (>! responses [:complete-root nil start-time])
            (async/close! responses)
            (<? responses-complete-ch)                      ;; wait for all block responses before proceeding
            (if (> total-missing 0)
              (log/info (str "Fetched total of " total-missing " missing index root files across "
                             (count ledgers-info) " ledgers in "
                             (- (System/currentTimeMillis) start-time) " milliseconds."))
              (log/debug (str "Index root consistency check completed in "
                              (- (System/currentTimeMillis) start-time) " milliseconds.")))
            ::done)
          (cond
            (nil? index)                                    ;; ledger might not yet be created
            (recur r total-missing)

            (<? (ledger-storage/index-root-exists? conn network ledger index))
            (recur r total-missing)

            :else
            (let [res-ch (async/chan)]
              (>! sync-chan [(storage/ledger-root-key network ledger index) res-ch])
              (>! responses res-ch)
              (recur r (inc total-missing)))))))))


(defn verify-all-index-data
  [conn ledgers-info sync-chan]
  (go-try
    (let [start-time (System/currentTimeMillis)]
      (loop [[{:keys [network ledger index] :as ledger-info} & r] ledgers-info
             missing-files 0]
        (if (nil? network)
          (do
            (when (> missing-files 0)
              (log/info "Retrieved a total of" missing-files "index files across all ledgers."))
            (log/info  "All ledger indexes verified in"
                      (- (System/currentTimeMillis) start-time) "milliseconds.")
            ::done)
          (let [ledger-start-time (System/currentTimeMillis)]
            (log/debug (str "Index syncing starting for: " network "/" ledger
                            " @ index point: " index "."))
            (if (nil? index)                                ;; ledger might not yet be created
              (recur r missing-files)
              (let [missing (<? (sync-ledger-index conn network ledger index sync-chan))]
                (log/debug (str "Index syncing complete for: " network "/" ledger
                                " @ index point: " index " in "
                                (- (System/currentTimeMillis) ledger-start-time) " milliseconds."))
                (recur r (+ missing-files missing))))))))))


(defn check-all-ledgers-consistency
  [{:keys [group] :as conn} ledgers-info sync-chan]
  (go-try
    ;; prioritize retrieving all block files for all ledgers
    (<? (verify-all-blocks conn ledgers-info sync-chan))

    ;; second priority is all index root files
    (<? (verify-all-index-roots conn ledgers-info sync-chan))

    ;; third priority is actual index files
    (<? (verify-all-index-data conn ledgers-info sync-chan))

    (async/close! sync-chan)))


(defn consistency-full-check
  [conn ledgers-info remote-sync-servers]
  (let [sync-chan     (async/chan)                          ;; files to sync are placed on this channel
        res-chan      (async/chan)                          ;; results file sync (error/success) are placed on this channel
        parallelism   8]
    (if (empty? ledgers-info)
      (go ::done)
      (let [remote-copy-fn (remote-copy-fn* conn remote-sync-servers 3000)]
        (check-all-ledgers-consistency conn ledgers-info sync-chan)

        ;; kick off pipeline of file copying, results of every operation will be placed on res-chan
        (async/pipeline-async parallelism res-chan remote-copy-fn sync-chan)

        ;; the file retrieval process queues up, and may also have an error... throw if we have a problem
        (go
          (try
            (loop [i 0]
              (let [next-result (util/<? res-chan)]
                (cond (nil? next-result)
                      (do
                        (when (> i 0)
                          (log/info "Successfully copied" i "files from other servers that were missing."))
                        ::finished)

                      (instance? Exception next-result)
                      (terminate! conn "Fatal error synchronizing ledger files (next-result)." next-result)

                      :else
                      (recur (inc i)))))
            (catch Exception e
              (terminate! conn "Fatal error synchronizing ledger files." e))))))))


(defn check-full-text-synced
  "Takes ledger-info array that includes keys:
  :network, :ledger, :block, :index ...."
  [conn ledgers-info]
  (go-try
    (loop [[{:keys [network ledger block]} & r] ledgers-info]
      (if ledger
        (do (when (> block 1)
              (let [db      (<? (fdb/db conn (str network "/" ledger)))
                    indexer (-> conn :full-text/indexer :process)]
                (<? (indexer {:action :sync, :db db}))))
            (recur r))
        true))))
