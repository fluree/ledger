(ns fluree.db.server
  (:gen-class)
  (:require [environ.core :as environ]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]

            [fluree.db.util.core :as util]
            [fluree.crypto :as crypto]
            [fluree.db.connection :as connection]

            [fluree.db.server-settings :as settings]

            [fluree.db.peer.http-api :as http-api]
            [fluree.db.peer.messages :as messages]

            [fluree.db.ledger.stats :as stats]
            [fluree.db.ledger.storage.filestore :as filestore]
            [fluree.db.ledger.storage.memorystore :as memorystore]
            [fluree.db.ledger.txgroup.core :as txgroup]
            [fluree.db.ledger.consensus.raft :as raft]
            [fluree.db.ledger.txgroup.monitor :as group-monitor]
            [fluree.db.ledger.consensus.dbsync2 :as dbsync2]
            [fluree.db.ledger.upgrade :as upgrade]
            [fluree.raft :as fraft]
            [fluree.db.ledger.consensus.tcp :as ftcp]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.constants :as const]
            [clojure.pprint :as pprint]))

;; instantiates server operations

(defn local-message-process
  "Handles any local incoming messages, eventually producing a result
  that is passed to the producer-chan.

  The response on the producer chan will use the same req-id as the incoming message,
  allowing the response to be passed along to downstream client waiting.

  A message at this stage looks like:
  [operation req-id arg]"
  [system producer-chan]
  (fn [conn message]
    (async/thread
      (messages/message-handler (assoc system :conn conn) producer-chan message)
      true)))


(defn local-message-response
  "Monitors producer channel, and will respond to requests that are waiting."
  [conn producer-chan]
  (async/go-loop []
    (let [msg (async/<! producer-chan)]
      (when-not (nil? msg)
        (connection/process-events conn msg)
        (recur)))))


(defn shutdown
  "Perform a shutdown of system created with 'startup'"
  [system]
  (let [{:keys [conn webserver group stats]} system
        try-continue (fn [f]
                       (try (f)
                            (catch Exception e
                              (log/error e "Exception executing close function: " (pr-str f)))))]
    (when (fn? (:close webserver))
      (try-continue (:close webserver)))
    (try-continue (fn [] (async/close! stats)))
    (when (fn? (:close group))
      (try-continue (:close group)))
    (when (fn? (:close conn))
      (try-continue (:close conn)))
    (ftcp/shutdown-client-event-loop)))


(defn check-version-upgrade-fn
  "Called whenever server newly becomes leader to upgrade raft data if needed."
  [conn system]
  ;; return a fluree/raft leader-watch 4-arg fn
  (fn [& _]
    (log/info "This server just became leader of the raft group.")
    ;; upgrade if needed
    (let [group           (:group conn)
          data-version    (txproto/data-version group)
          current-version const/data_version]
      (cond (= current-version data-version)
            nil

            ;; Current version > data-version, shutdown
            (< current-version data-version)
            (do (log/warn (str "Current data version: " current-version " is greater than the data version of Fluree currently running: " data-version ". Please retry this data with a more recent FlureeDB."))
                (shutdown system)
                (System/exit 1))

            ;; Can't hold up RAFT as it is used when upgrading - launch asynchronously
            (> current-version data-version)
            (future
              (upgrade/upgrade conn data-version current-version))))))

(defn startup
  ([] (startup (settings/build-env environ/env)))
  ([settings]
   (log/info (str "Starting Fluree in mode: " (:fdb-mode settings)))
   (log/info "Starting with config:\n" (with-out-str
                                         (pprint/pprint
                                           (cond-> (into (sorted-map) settings) ;; hide encryption secret from logs
                                                   (:fdb-encryption-secret settings) (assoc :fdb-encryption-secret "prying eyes want to know...")))))
   (log/info "JVM arguments: " (str (stats/jvm-arguments)))
   (log/info "Memory Info: " (stats/memory-stats))
   (let [config         (settings/build-settings settings)
         {:keys [transactor? mode consensus conn join?]} config
         consensus-type (:type consensus)
         storage-type   (:storage-type conn)
         memory?        (= :memory storage-type)
         group          (let [group-opts (:group config)]
                          (if transactor?
                            ;; TODO - currently if query-peer, we use a dummy group obj. Change this?
                            (txgroup/start group-opts consensus-type join?)
                            group-opts))

         remote-writer  (fn [k data]
                          (txproto/storage-write-async group k data))
         conn           (let [conn-opts        (get-in config [:conn :options])
                              storage-write-fn (case storage-type
                                                 :file remote-writer
                                                 :s3 remote-writer
                                                 :memory memorystore/connection-storage-write)
                              producer-chan    (async/chan (async/sliding-buffer 100))
                              publish-fn       (local-message-process {:config config :group group} producer-chan)
                              conn-impl        (if transactor?
                                                 (connection/connect nil (assoc conn-opts :storage-write storage-write-fn :publish publish-fn :memory? memory?))
                                                 (connection/connect (:fdb-group-servers-ports settings) (assoc conn-opts :memory? memory?)))]
                          ;; launch message consumer, handles messages back from ledger
                          (local-message-response conn-impl producer-chan)
                          (-> conn-impl
                              (assoc :group group)))
         system         {:config    config
                         :conn      conn
                         :webserver nil
                         :group     group}

         ;; add a leader-watch function to upgrade data if required
         _              (when (and (= :raft consensus-type) transactor?)
                          (fraft/add-leader-watch (:raft group) ::upgrade (check-version-upgrade-fn conn system) :become-leader))

         webserver      (let [webserver-opts (-> (:webserver config)
                                                 (assoc :system system))]
                          (http-api/webserver-factory webserver-opts))
         stats          (stats/initiate-stats-reporting system (-> config :stats :interval))
         system*        (assoc system :webserver webserver
                                      :stats stats)
         _              (when (and (or memory? (= consensus-type :in-memory))
                                   (not (and memory? (= consensus-type :in-memory))))
                          (do (log/warn "Error during start-up. Currently if storage-type is 'memory', then consensus-type has to be 'in-memory' and vice versa.")
                              (shutdown system*)
                              (System/exit 1)))]

     ;; wait for initialization, and kick off some startup activities
     (when transactor?
       (txproto/-start-up-activities group conn system* shutdown join?)) system*)))

(defn- execute-command
  "Execute some arbitrary commands on FlureeDB (then exit)"
  [command]
  (case (util/str->keyword command)
    ;; generate public/private keys and also show corresponding account id
    :keygen
    (let [acct-keys  (crypto/generate-key-pair)
          account-id (crypto/account-id-from-private (:private acct-keys))]
      (println "Private:" (:private acct-keys))
      (println "Public:" (:public acct-keys))
      (println "Account id:" account-id))

    ;; else
    (println (str "Unknown command: " command)))
  (System/exit 0))


(defn -main [& args]
  (if-let [command (:fdb-command environ/env)]
    (execute-command command)
    (let [system (startup)]
      (.addShutdownHook
        (Runtime/getRuntime)
        (Thread. ^Runnable
                 (fn []
                   (log/info "SHUTDOWN Start")
                   (shutdown system)
                   (log/info "SHUTDOWN Complete")))))))
