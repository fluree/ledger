(ns dev
  (:require [fluree.db.server-settings :as settings]
            [environ.core :as environ]
            [fluree.db.server :as server]
            [fluree.db.api :as fdb]
            [fluree.db.test-helpers :as test-helpers]))

(comment
  (def ledger-peer (test-helpers/start-server {:fdb-api-port 8090
                                               :fdb-mode "ledger"
                                               :fdb-group-servers "ledger-server@localhost:11001"
                                               :fdb-group-this-server "ledger-server"
                                               :fdb-group-log-directory "./dev/data/group"
                                               :fdb-storage-file-root "./dev/data/ledger"}))
  (test-helpers/stop-server ledger-peer)

  (def conn (fdb/connect "http://localhost:8090"))

  @(fdb/new-ledger conn "dan/test0")



  ,)
