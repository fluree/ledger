(ns fluree.db.ledger.consensus.dbsync2
  (:require [fluree.db.storage.core :as storage]
            [clojure.core.async :as async :refer [go go-loop <! >!]]
            [clojure.tools.logging :as log]
            [fluree.db.ledger.storage.filestore :as filestore]
            [fluree.db.ledger.util :as util :refer [go-try <?]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [clojure.string :as str]
            [fluree.db.api :as fdb]))


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
        {:keys [storage-path encryption-secret]} meta
        storage-write (filestore/connection-storage-write storage-path encryption-secret)
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
                         (format "Something went wrong. Trying to copy %s. Attempted all servers: " file-key server-list)
                         server))

                ;; we have a result!
                (not (nil? result))
                (do
                  (try
                    (storage-write file-key result)
                    (catch Exception e (raise e nil server)))
                  (when finished?-port
                    (async/put! finished?-port file-key)
                    (async/close! finished?-port))
                  (async/put! result-ch file-key)
                  (async/close! result-ch))))))))))


(defn get-file-local
  "Returns core async channel, will return true (or exception)
  when file is already in local storage (i.e. local to this node; could be an S3 bucket)"
  [conn port file-key]
  (util/go-try
    (let [file-exists? (:storage-exists conn)
          exists?      (<? (file-exists? file-key))]
      (if exists?
        true
        (let [result-ch (async/chan)]
          ;; queue request for file
          (>! port [file-key result-ch])
          ;; wait until we have confirmation it is in place.
          (<? result-ch))))))


(defn get-index-leaf-if-needed
  [conn port child-key]
  (go-try
    (let [child-his-key        (str child-key "-his")
          storage-exists?      (:storage-exists conn)
          child-exists?-ch     (storage-exists? child-key)
          child-his-exists?-ch (storage-exists? child-his-key)
          ;; pull both files in parallel to speed things up
          child-exists?        (<? child-exists?-ch)
          child-his-exists?    (<? child-his-exists?-ch)]
      (when-not child-exists?
        (>! port child-key))
      (when-not child-his-exists?
        (>! port child-his-key))
      :done)))


(defn sync-index-branch
  "Starts an index branch, and synchronizes all the way to the data leafs,
  ensuring they are all on disk. If a leaf is not on disk, adds it to the port for
  retrieval."
  [conn port branch-id]
  (util/go-try
    ;; first get file local if not already here. Will throw if an error occurs
    (util/<? (get-file-local conn port branch-id))
    ;; with file local, we can load and check children
    (let [branch      (<? (storage/read-branch conn branch-id))
          children    (:children branch)
          child-leaf? (true? (-> children first :leaf))]

      (loop [[c & r] children]
        (cond

          (and c child-leaf?)
          (do (<? (get-index-leaf-if-needed conn port (:id c)))
              (recur r))

          ;; child is another branch node
          c
          (do (<? (sync-index-branch conn port (:id c)))
              (recur r))

          ;; no more children, return
          (nil? c)
          ::done)))))


(defn sync-index-point
  "Does a 100% sync of a db to a given index point.

  Returns core async channel with either ::done, or an exception if
  an error occurs during sync."
  [conn network dbid index-point port]
  (go-try
    ;; will get index root local if not there already... throws on error
    (->> (storage/ledger-root-key network dbid index-point)
         (get-file-local conn port)
         <?)
    (let [db-root          (storage/read-db-root conn network dbid index-point)
          {:keys [spot psot post opst]} (<? db-root)
          sync-spot-ch     (sync-index-branch conn port (:id spot))
          sync-psot-ch     (sync-index-branch conn port (:id psot))
          sync-post-ch     (sync-index-branch conn port (:id post))
          sync-opst-ch     (sync-index-branch conn port (:id opst))
          garbage-file-key (storage/ledger-garbage-key network dbid index-point)
          storage-exists?  (:storage-exists conn)
          garbage-exists?  (<? (storage-exists? garbage-file-key))]

      ;; kick off 4 indexes in parallel...  will throw if an error occurs
      (<? sync-spot-ch)
      (<? sync-psot-ch)
      (<? sync-post-ch)
      (<? sync-opst-ch)
      (when-not garbage-exists?
        (>! port garbage-file-key))
      ::done)))


(defn check-all-blocks-consistency
  "Checks actual file directory for any missing blocks through provided 'check through' block.
  Puts block file keys (filenames) onto provided port if they are missing."
  [conn network dbid check-through port]
  (go-try
    (let [file-path    (storage/block-storage-path conn network dbid)
          _            (log/debug "check-all-blocks-consistency block-storage-path:" file-path)
          storage-list (:storage-list conn)
          all-files    (<? (storage-list file-path))
          last-element (fn [path] (-> path (str/split #"/") last))
          block-files  (filter #(->> % :name last-element (re-matches #"^[0-9]+\.fdbd"))
                               all-files)
          blocks       (reduce (fn [acc block-file]
                                 (let [block (some->> (:name block-file)
                                                      last-element
                                                      ^String (re-find #"^[0-9]+")
                                                      Long/parseLong)]
                                   (if (> (:size block-file) 0)
                                     (conj acc block)
                                     acc)))
                               #{} block-files)]
      (loop [block-n check-through]
        (if (< block-n 1)
          ::finished
          (do
            (when-not (contains? blocks block-n)
              ;; block is missing, or file is empty... add to files we need to sync
              (>! port (storage/ledger-block-key network dbid block-n)))
            (recur (dec block-n))))))))


(defn check-db-full-consistency
  "First checks every block, then checks all DB indexes."
  [conn current-state port network dbid]
  (go-try
    (let [latest-block (txproto/block-height* current-state network dbid)
          last-index   (txproto/latest-index* current-state network dbid)
          ;; wait to sync all blocks until we start checking latest index file
          block-result (<? (check-all-blocks-consistency conn network dbid latest-block port))
          index-result (when last-index
                         (<? (sync-index-point conn network dbid last-index port)))]
      (log/debug (str network "/" dbid ": block-sync complete to: " latest-block
                      ": index-sync complete for: " last-index ". "
                      "Block-sync result: " block-result ", Index-sync result: " index-result "."))
      ::done)))


(defn consistency-full-check
  [conn remote-sync-servers]
  (let [group-raft         (:group conn)
        current-state      @(:state-atom group-raft)
        db-list            (txproto/ledger-list* current-state)
        sync-chan          (async/chan)                     ;; files to sync are placed on this channel
        res-chan           (async/chan)                     ;; results file sync (error/success) are placed on this channel
        parallelism        8
        ;; kick off all db syncs in parallel. will put all missing files onto sync-chan
        find-files-results (mapv (fn [[network dbid]]
                                   (check-db-full-consistency conn current-state sync-chan network dbid))
                                 db-list)]
    (if (empty? db-list)
      (go ::done)
      (let [remote-copy-fn (remote-copy-fn* conn remote-sync-servers 3000)]

        ;; kick off pipeline of file copying, results of every operation will be placed on res-chan
        (async/pipeline-async parallelism res-chan remote-copy-fn sync-chan)

        ;; each db sync may have errors, check and throw/exit if we hit any
        (go
          (try
            (loop [[c & r] find-files-results]
              (if (nil? c)
                (do
                  (async/close! sync-chan)                  ;; close sync-chan so pipeline will close
                  ::done)
                (let [next-result (<? c)]
                  (recur r))))
            (catch Exception e
              (async/close! sync-chan)
              (terminate! conn "Error synchronizing files, fatal error - exiting." e))))

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
  "Takes an array of arrays.
  [ [nw/ledger block] [nw/ledger block] [nw/ledger block] ]"
  [conn storage-dir ledger-block-arr]
  (go
    (loop [[[ledger block] & r] ledger-block-arr]
      (if ledger
        (do (when (> block 1)
              (let [[network dbid] (str/split ledger #"/")
                    db             (util/<? (fdb/db conn ledger))
                    indexer        (:full-text/indexer conn)]
                (<! (indexer {:action :sync, :db db}))))

            (recur r))

        true))))
