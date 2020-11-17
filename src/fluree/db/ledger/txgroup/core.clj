(ns fluree.db.ledger.txgroup.core
  (:require [fluree.db.ledger.consensus.raft :as raft]
            [fluree.db.ledger.consensus.none :as none]
            [clojure.tools.logging :as log]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]))

;; ledger group consensus

{:keys         {"DEF345" "private-key"}                     ;; keys used across tx group
 :networks     {"networka" {:initialized? true              ;; marks network as initialized
                            :private-key  "ABC123SDFSDFSDDFSF"
                            :storage      {:type :filesystem ;; ledger group storage options
                                           :opts {}}
                            :dbs          {"mydb1" {:private-key "ABC123SDFSDFSDDFSF" ;; default private key if db is open
                                                    :block       152
                                                    :index       123 ;; last index point
                                                    :indexes     {123 234234234} ;; each index point and timestamp on when completed - to bring new server in sync and useful for centralized garbage collection



                                                    ;; blocks we are processing
                                                    :next-block  {:block  124
                                                                  :txids  [:tx1 :tx2 :tx3]
                                                                  :server :server-a ;; who internally is producing result
                                                                  :result {}}
                                                    :indexing    {:block  150 ;; reindexing job in progress
                                                                  :server :server-b ;; who assigned to create index
                                                                  ;; files are tracked to allow garbage collection in case index does not complete
                                                                  ;; we use timestamps as a 'heartbeat' on indexing process. If too long of time progresses
                                                                  ;; without next file, or completion, then we can kill the process and reassign to a different server
                                                                  :files  {"filename" 1234}} ;; for new index, new filenames and timestamps when leader receives



                                                    :keys        #{"ABC123"}}} ;; keys to use as signing keys for open-api

                            :tx-stats     {}}}              ;; stats on other (external) tx participants


 ;; Leases allow other transactors in group to claim leadership over an activity, leases must be renewed before expiration
 ;; server-lease, registers server on network as available to take work. Servers need to renew leases frequently (depends on expiration ms used)
 ;; The leader will review server leases, and based on that distributed work (divided by network today) to each of the servers.
 :leases       {:servers {:server-id {:id     :ABC
                                      :expire 512343234344}}}

 ;; here we register the servers and what they are responsible for. The leader keeps this up to date based on the 'active' server leases registered.
 ;; workers poll this data to know what work they are responsible for.
 :_worker      {:server-id-a {:networks {"network-a" 512343234344}}
                :server-id-b {:networks {"network-b" 512343234344}}}
 :_work        {:networks {"network-a" :server-id-a
                           "network-b" :server-id-b}}

 ;; pending transactions
 :cmd-queue    {"network" {"txid" {:data    {:cmd "" :sig ""}
                                   :size    400             ;; number of bytes
                                   :txid    "txid"
                                   :network "network"
                                   :dbid    "dbid"
                                   :instant 512343234344}}}
 :new-db-queue {"network" {"id" {:network "network"
                                 :dbid    "dbid"
                                 :command {:cmd "command-json"
                                           :sig "sig"}}}}}



(defn start
  [group-settings consensus-type join?]
  (let [{:keys [server-configs this-server port timeout-ms heartbeat-ms
                log-history snapshot-threshold log-directory storage-type
                storage-ledger-read storage-group-read storage-ledger-write
                storage-group-write storage-group-exists storage-group-delete
                storage-group-list catch-up-rounds private-keys
                open-api]} group-settings
        group (condp = consensus-type
                :raft (raft/launch-raft-server
                        server-configs
                        this-server
                        {:port                  port
                         :log-directory         log-directory
                         :storage-ledger-read   storage-ledger-read
                         :storage-ledger-write  storage-ledger-write
                         :storage-group-read    storage-group-read
                         :storage-group-write   storage-group-write
                         :storage-group-exists  storage-group-exists
                         :storage-group-delete  storage-group-delete
                         :storage-group-list    storage-group-list
                         :timeout-ms            timeout-ms
                         :heartbeat-ms          heartbeat-ms
                         :log-history           log-history
                         :snapshot-threshold    snapshot-threshold
                         :only-leader-snapshots (not= :file storage-type)
                         :join?                 join?
                         :catch-up-rounds       catch-up-rounds
                         :private-keys          private-keys
                         :open-api              open-api})
                :in-memory (none/launch-in-memory-server group-settings))]


    (log/debug "Start-group. Settings are: " (pr-str group-settings))
    group))

(defn data-version
  [group]
  (or (:version (raft/local-state group)) 1))

(defn set-data-version
  [group version]
  (assert (number? version))
  (txproto/kv-assoc-in group [:version] version))

