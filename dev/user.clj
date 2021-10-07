(ns user
  (:require [clojure.tools.logging :as log]
            [clojure.tools.namespace.repl :as tn :refer [refresh refresh-all]]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [criterium.core :as criterium]
            [environ.core :as environ]
            [fluree.db.api :as fdb]
            [fluree.db.flake :as flake]
            [fluree.db.permissions :as permissions]
            [fluree.db.dbfunctions.fns :as dbfunctions]
            [fluree.db.storage.core :as storage]
            [fluree.db.test-helpers :as test-helpers]
            [fluree.crypto :as crypto]
            [fluree.db.session :as session]
            [fluree.db.event-bus :as event-bus]
            [fluree.db.util.json :as json]
            [fluree.db.server :as server]
            [fluree.db.server-settings :as settings]
            [fluree.db.ledger.bootstrap :as bootstrap]
            [fluree.db.ledger.consensus.raft :as raft]
            [fluree.db.ledger.txgroup.core :as txgroup]
            [fluree.db.peer.http-api :as http-api]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.peer.password-auth :as pw-auth]
            [fluree.db.query.sql :as sql])
  (:import (java.io PushbackReader)))

(set! *warn-on-reflection* true)

;; TODO
;; Turned off graphql handler of ledger

;; TODO - ledger
;; Track last transaction/db-request time, and if too long, close core async channel
;; If a unique predicate that doesn't start with _block is added to a block, then the block could get updated. Verify no resolved idents end up with a negative number (a block eid)
;; split history leaf nodes at max kb size
;; Index in background... need to be careful about -resolve-history in that novelty is bound to a block. Need to re-calculate novelty afterwards.
;; combine block storage files to speed things up at a certain point??
;; Track old unused index segments when reindexing


(def system nil)
(def settings nil)

(defn init []
  (let [new-sys (server/startup)]
    (alter-var-root #'system (constantly new-sys))))

(defn start
  ([] (start {}))
  ([override-settings]
   (let [settings (-> (settings/build-env environ/env)
                      (merge override-settings))]
     (alter-var-root #'settings (constantly settings))
     (alter-var-root #'system (constantly (server/startup settings)))
     :started)))

(defn re-start
  "Used for refresh. Uses last used settings to start again."
  []
  (start settings))

(defn stop []
  (alter-var-root #'system (fn [s] (when s (server/shutdown s))))
  :stopped)

(defn go []
  (start))

(defn reset []
  (stop)
  (refresh-all :after 'user/re-start))

(defn read-edn-resource
  [resource-path]
  (with-open [r (-> resource-path io/resource io/reader PushbackReader.)]
    (edn/read r)))

(defn read-json-resource
  [resource-path]
  (-> resource-path io/resource slurp json/parse))

(defn create-db [db-name]
  @(-> system
       :conn
       (fdb/new-ledger db-name)))

(defn transact-db [db-name txns]
  @(-> system
       :conn
       (fdb/transact db-name txns)))

(defn load-ledger-resources [db-name loader resource-paths]
  (loop [[p & rst] resource-paths]
    (when p
      (log/info "Loading resource " p " into ledger " db-name)
      (->> p
           loader
           (transact-db db-name))
      (Thread/sleep 1000)
      (recur rst))))

(defn load-sample-db [db-name loader resource-paths]
  (create-db db-name)
  (Thread/sleep 1000)
  (load-ledger-resources db-name loader resource-paths))

(defn load-chat-ledger
  []
  (let [collection-txn  [{:_id  "_collection"
                          :name "person"}
                         {:_id  "_collection"
                          :name "chat"}
                         {:_id  "_collection"
                          :name "comment"}
                         {:_id  "_collection"
                          :name "artist"}
                         {:_id  "_collection"
                          :name "movie"}]

        pred-filename   "../test/fluree/db/ledger/Resources/ChatApp/chatPreds.edn"
        predicate-txn   (edn/read-string (slurp (io/resource pred-filename)))

        data-filename   "../test/fluree/db/ledger/Resources/ChatApp/chatAppData.edn"
        data-txn        (edn/read-string (slurp (io/resource data-filename)))]
    (log/info "Creating new ledger")
    @(fdb/new-ledger (:conn system) test-helpers/ledger-chat)
    (Thread/sleep 250)

    (log/info "Adding collections")
    @(fdb/transact (:conn system) test-helpers/ledger-chat collection-txn)

    (log/info "Adding predicates")
    @(fdb/transact (:conn system) test-helpers/ledger-chat predicate-txn)

    (log/info "Adding data")
    @(fdb/transact (:conn system) test-helpers/ledger-chat data-txn)

    :loaded))

(defn query-db [db-name query]
  @(-> system
       :conn
       (fdb/db db-name)
       (fdb/query query)))

(defn sql-query-db [db-name sql-query]
  (->> sql-query
       sql/parse
       (query-db db-name)))

(comment
  (async/<!! (http-api/action-handler :ledger-stats system {} {} :test/chat {}))
  (async/<!! (http-api/action-handler :ledger-info system {} {} :test/chat {}))

  (as-> (async/<!! (http-api/db-handler* system :dev/_bptest63 :query {:select [:*] :from "testcoll"} :signature-X nil)) res
        (assoc res :body (json/parse (:body res))))


  (-> (fdb/transact-async (:conn system) :dev/_bptest63
                          [{:_id "testcoll" :p1 "f-hi1" :p2 1}
                           {:_id "testcoll" :p1 "f-hi2" :p2 2}
                           {:_id "testcoll" :p1 "f-hi3" :p2 3}]
                          {:auth "TfHHPV5SocwXTAQ9vwRpELZp8AfAkspmtc9"})
      (async/<!!))

  (async/close! (:stats system))

  (-> (:conn system)
      :meta)

  @(fdb/ledger-list (:conn system))

  @(fdb/query (fdb/db (:conn system) "bp/pw")
              {:select ["*"] :from "_auth"})

  @(fdb/query (fdb/db (:conn system) "bp/pw")
              {:select ["*" {"_user/auth" ["*"]}] :from "_user"})


  @(fdb/transact (:conn system) "bp/pw"
                 [{:_id  ["_user/username" "bplatz"]
                   :auth [["_auth/id" "TfAsSWnmjgvtZaEPum6VpWoUkXRN4txo6K3"]]}])


  @(fdb/query (fdb/db (:conn system) "bp/pw")
              {:selectOne ["_auth/salt"]
               :from      ["_auth/id" "TfKh7PBLUHkCXEtbNd5Crrq9fhAcVHGVBsd"]})

  @(fdb/query (fdb/db (:conn system) "bp/pw")
              {:selectOne "?salt"
               :where     [["?id" "_auth/id" "TfKh7PBLUHkCXEtbNd5Crrq9fhAcVHGVBsd"]
                           ["?id" "_auth/salt" "?salt"]]})


  @(fdb/query (fdb/db (:conn system) "bp/pw")
              {:select ["?auth-ids" "?salt"]
               :where  [["?id" "_user/username" "bplatz"]
                        ["?id" "_user/auth" "?auth"]
                        ["?auth" "_auth/id" "?auth-ids"]
                        ["?auth" "_auth/salt" "?salt"]]})


  @(fdb/new-ledger (:conn system) "bp/pw")

  (def res (pw-auth/fluree-new-pw-auth (:conn system) "bp/pw" "fluree" nil))

  (async/poll! res)

  (def vres (pw-auth/fluree-decode-jwt
              (:conn system)
              "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJicC9wdyIsInN1YiI6IlRmRHoyU1lIOHNxRnRncTk1NnIzSDNndlZjUVh5SmlQZEdiIiwiZXhwIjoxNTczODQ4ODAxMzU2LCJpYXQiOjE1NzM4NDg1MDEzNTYsInByaSI6ImQ3MmNmOTJkZGUyNjIzMDg3YWFkMmU3YjE5ZDYxN2ZhNGY5NmQ4ZTE0YTI0YTNkMTUxNTkzMDUwOGU1YzZiYTYzMDVjZDU1ZGRlNDllNzgzNzA2NWE1MzNjMjFjY2ZlMDIwZGE2MTAxM2NhNmU1MTkzNzc4NDdiNmY1MzRmZGQ1YmRhYWU0ZTAzZjViMjI2MmY2ZWEyNjljYjRhYmU4ZDgifQ.WRnnGKki2lEC3D-EeTs-5boyoAkaDdC3eDBFyTPhk1k"))

  (async/poll! vres))




(comment

  ;; Standalone - On Disk
  (start {:fdb-mode                   "dev"
          :fdb-group-servers          "myserver@localhost:9790"
          :fdb-group-this-server      "myserver"
          :fdb-group-log-directory    "./data/raft"
          :fdb-storage-file-root      "./data"
          :fdb-consensus-type         "raft"
          :fdb-api-open               true
          :fdb-api-port               8090
          :fdb-stats-report-frequency "30m"})

  ;; Standalone - In Memory
  (start {:fdb-group-servers     "DEF@localhost:11001"
          :fdb-group-this-server "DEF"
          :fdb-storage-type      "memory"
          :fdb-api-port          8091
          :fdb-consensus-type    "in-memory"})

  (stop)


  ;; Query Peer
  (start {:fdb-group-servers       "myserver@localhost:9790"
          :fdb-group-servers-ports "localhost:8090"
          :fdb-api-port            8080
          :fdb-mode                "query"})



  ;; Three servers
  (start {:fdb-group-servers       "ABC@localhost:9790,DEF@localhost:9791,GHI@localhost:9792"
          :fdb-group-this-server   "ABC"
          :fdb-group-log-directory "./data/ABC/raft/"
          :fdb-storage-file-root   "./data/ABC/"
          :fdb-api-port            8090})
  (start {:fdb-group-servers       "ABC@localhost:9790,DEF@localhost:9791,GHI@localhost:9792"
          :fdb-group-this-server   "DEF"
          :fdb-group-log-directory "./data/DEF/raft/"
          :fdb-storage-file-root   "./data/DEF/"
          :fdb-api-port            8091})

  (start {:fdb-group-servers       "ABC@localhost:9790,DEF@localhost:9791,GHI@localhost:9792"
          :fdb-group-this-server   "GHI"
          :fdb-group-log-directory "./data/GHI/raft/"
          :fdb-storage-file-root   "./data/GHI/"
          :fdb-api-port            8092})

  ;; Three servers dynamic changes


  ;; Start first
  (start {:fdb-group-servers       "ABC@localhost:9790"
          :fdb-group-this-server   "ABC"
          :fdb-group-log-directory "./data/ABC/raft/"
          :fdb-storage-file-root   "./data/ABC/"
          :fdb-api-port            8090
          :fdb-join?               false})

  (start {:fdb-group-servers       "ABC@localhost:9790,DEF@localhost:9791"
          :fdb-group-this-server   "DEF"
          :fdb-group-log-directory "./data/DEF/raft/"
          :fdb-storage-file-root   "./data/DEF/"
          :fdb-api-port            8091
          :fdb-join?               true})

  ;; Add server two
  (txproto/-add-server-async (:group system) "DEF")

  (start {:fdb-group-servers       "ABC@localhost:9790,DEF@localhost:9791,GHI@localhost:9792"
          :fdb-group-this-server   "GHI"
          :fdb-group-log-directory "./data/GHI/raft/"
          :fdb-storage-file-root   "./data/GHI/"
          :fdb-api-port            8092
          :fdb-join?               true})

  ;; Add/ remove server three
  (txproto/-add-server-async (:group system) "GHI")
  (txproto/-remove-server-async (:group system) "GHI")


  (stop)

  ;; monitor raft
  (raft/monitor-raft (-> system :group)
                     (fn [x] (let [{:keys [time event]} x
                                   [op data] event
                                   elapsed-t (some-> (:request data) :instant (#(- (System/currentTimeMillis) %)))]
                               (when
                                 (or (not= :append-entries op)
                                     (> (count (-> x :event second :entries)) 0))
                                 (clojure.pprint/pprint {:op      op
                                                         :time    time
                                                         :data    data
                                                         :elapsed elapsed-t})))))



  (txproto/-local-state (:group system))

  (async/<!! (raft/leader-async (:group system)))
  (raft/get-raft-state (:group system))


  (raft/monitor-raft (:group system))
  (raft/monitor-raft (:group system) (fn [x] (let [op (first (:event x))]
                                               (when
                                                 (or (not= :append-entries op)
                                                     (> (count (-> x :event second :entries)) 0))
                                                 (let [response-time (some-> x :event second :request :instant (#(- (:instant x) %)))]
                                                   (clojure.pprint/pprint (-> x
                                                                              (select-keys [:time :event])
                                                                              (assoc :response-ms response-time))))))))
  (raft/monitor-raft-stop (:group system))

  (raft/is-leader? (:group system))

  (fluree.raft.log/read-log-file (io/file "data/ABC/raft/0.raft"))
  (fluree.raft.log/read-log-file (io/file "data/DEF/raft/5604.raft"))


  (raft/register-state-change-fn "myfnid" (fn [state-change] (log/warn "------------ State change!!!:" state-change)))

  ;(txgroup/ledger-list (:group system))

  (crypto/account-id-from-private "9c33a5c3ab49bd5255dd165006eb648d056e6d7c09daaec229c67312e8c0dc64")

  (-> system
      :group
      :state-atom
      deref
      :networks
      (get "bptest01"))



  (def async-resp (-> system
                      :group
                      (raft/kv-assoc-in-async ["test5" "me"] "from ABC2")))

  (async/poll! async-resp)


  (time (raft/kv-assoc-in (:group system) ["test2" "me"] "from GHI2"))



  (settings/build-env environ/env))


(comment

  (def conn (:conn system))

  @(fdb/transact conn "dev/$network" [{:_id "_tag" :id "test/tag3"}])

  @(fdb/new-ledger conn "dev/movie2")



  (-> (:conn system)
      :state
      deref
      :listeners
      clojure.pprint/pprint)


  (storage/read-db-pointer conn "dev" "bptestfork5")

  (storage/read-db-root conn "dev" "bptestfork5" 1)

  (async/<!! (storage/read-block conn "dev" "$network" 2))

  (session/blank-db conn "dev/_there"))




(comment

  (-> (fdb/session (:conn system) "dev/1")
      :state
      deref
      :db/db
      :conn)


  (-> system :conn :meta)

  @(fdb/transact (:conn system) "dev/1" (:tx master-db-tx-data))


  @(fdb/transact (:conn system) "dev/1" [{:_id  "account"
                                          :name "temp5"}]))










(comment)

;; db-identfiers
;; https://network.flur.ee/fdb/db/network/dbid-or-name
;; https://network.flur.ee/fdb/db/network/dbid-or-name/time

;; explore data
;; https://network.flur.ee/fdb/db/network/dbid-or-name/entity-subject

;; storage
;; https://network.flur.ee/fdb/storage/network/dbid-or-name/root
;; https://network.flur.ee/fdb/storage/network/dbid-or-name/root/time
;; https://network.flur.ee/fdb/storage/network/dbid-or-name/garbage/time
;; https://network.flur.ee/fdb/storage/network/dbid-or-name/block/time
;; https://network.flur.ee/fdb/storage/network/dbid-or-name/block/time-start/time-end
;; https://network.flur.ee/fdb/storage/network/dbid-or-name/idx-type/id
