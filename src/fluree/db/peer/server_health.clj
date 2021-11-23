(ns fluree.db.peer.server-health
  (:require [clojure.core.async :as async]
            [clojure.walk :as walk]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.util.json :as json]))

(def ^:const default-nwstate-timeout-ms 60000)              ;; TODO - What should be the default setting?

;200 OK. The client request has succeeded
(def ^:const http-ok 200)

; 408 Request Timeout. The server did not complete the request before the client expiration timeout.
(def ^:const http-timeout 408)

;500 Internal Server Error. An unknown error has occurred.
(def ^:const http-internal-server-error 500)


(defn remove-deep
  "Walks a nested map, removing key(s)/value(s) based on keys provided.

  key-set - identifies keys (with values) to be removed
  data    - a nested map"
  [key-set data]
  (walk/prewalk (fn [node] (if (map? node)
                             (apply dissoc node key-set)
                             node))
                data))

(defn extract-state-from-leases
  "Extracts current state of server network based on the current server's view.

  Inputs:
  group - map of server leases from the consensus (e.g., raft) state

  Returns: a vector of maps containing
     :id      - server identifier (e.g., myserver)
     :active? - indicator whether or not the servers are in active communication

  Should never return nil; but...
  "
  [leases instant]
  (when-let [servers (into [] (:servers leases))]
    (loop [[server & r] servers
           acc []]
      (if-let [item (second server)]
        (recur r (into acc [{:id      (:id item)
                             :active? (>= (:expire item) instant)}]))
        acc))))

(defn parse-command-queue
  "Retrieves current backlog from consensus state based on the current server's view.

  Inputs:
  cmd-queue - a map of outstanding transactions/commands from the consensus state

  returns: a vector of maps, each map with
     * the network as a keyword (e.g., given a ledger test/one; the id becomes :test)
     * the count of pending transactions as the value of the keyword
     * the keyword
     * the instant of oldest transaction"
  [cmd-queue]
  (loop [[cq & r] cmd-queue
         acc []]
    (if cq
      (let [[k v] cq
            acc* (into acc [{(keyword k) (count v)
                             :txn-count  (count v)
                             :txn-oldest-instant (some->> v vals (map :instant) (apply min))}])]

        (recur r acc*))
      acc)))

(defn parse-new-db-queue
  "Retrieves current backlog from consensus state based on the current server's view.

  Inputs:
  new-db-queue - a map of outstanding 'new ledger' requests from the consensus state

  returns: a vector of maps, each map with
     * the network as a keyword (e.g., given a new ledger test/one; the id becomes :test)
     * the count of pending new ledger requests as the value of the keyword"
  [new-db-queue]
  (loop [[nq & r] new-db-queue
         acc []]
    (if nq
      (let [[k v] nq
            acc* (into acc [{(keyword k) (count v)}])]
        (recur r acc*))
      acc)))

(defn get-consensus-state
  "Returns a nested map documenting the current state of the txproto group

  Expects system as input, should contain the raft state"
  [{:keys [group]}]
  (let [instant    (System/currentTimeMillis)
        raft       (-> group :state-atom deref (dissoc :private-key))
        {:keys [cmd-queue new-db-queue networks leases]} raft
        cmd-queue  (parse-command-queue cmd-queue)
        oldest-txn (some->> cmd-queue
                            (mapv (fn [m] (:txn-oldest-instant m)))
                            (remove nil?)
                            seq
                            (apply min))
        svr-state  (when leases
                     (extract-state-from-leases leases instant))
        raft'      (-> raft
                       (assoc :cmd-queue    cmd-queue
                              :new-db-queue (parse-new-db-queue new-db-queue)
                              :networks     (some->> networks (remove-deep [:private-key]) vector)))]
    (-> (txproto/-state group)
        (select-keys [:snapshot-term
                      :latest-index
                      :snapshot-index
                      :other-servers
                      :index
                      :snapshot-pending
                      :term
                      :leader
                      :timeout-at
                      :this-server
                      :status
                      :id
                      :commit
                      :servers
                      :voted-for
                      :timeout-ms])
        (assoc :open-api (:open-api group))
        (assoc :raft raft')
        (assoc :svr-state svr-state)
        (assoc :oldest-pending-txn-instant oldest-txn))))

(defn get-request-timeout
  "Retrieves the request-timeout from headers and returns the integer value.

  If the request-timeout header is not defined or the provided value cannot be coerced to an integer,
  the default-value is assigned."
  [{:keys [headers]} default-value]
  (if-let [t (:request-timeout headers)]
    (if (integer? t)
      t
      (try (Integer/parseInt t)
           (catch Exception _ default-value)))
    default-value))

(defn health-handler
  [{:keys [config group]} _]
  (let [state (cond
                group
                (-> group txproto/-state :status)

                (:transactor? config)
                "ledger"

                :else
                "query")]
    {:status  http-ok
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 {:ready       true
                                    :status      state
                                    :utilization 0.5})}))

(defn nw-state-handler
  [{:keys [config group] :as system} request]
  (let [timeout (get-request-timeout request default-nwstate-timeout-ms)
        attempt (async/go
                  (try
                    (let [body (cond
                                 group
                                 (get-consensus-state system)

                                 (:transactor? config)
                                 {:status "ledger"}

                                 :else
                                 {:status "query"})]
                      {:status  http-ok
                       :headers {"Content-Type" "application/json; charset=utf-8"}
                       :body    (json/stringify-UTF8 body)})
                    (catch Exception e
                      {:status  (or (-> e ex-data :status) http-internal-server-error)
                       :headers {"Content-Type" "application/json; charset=utf-8"}
                       :body    (json/stringify-UTF8 e)})))
        [resp ch] (async/alts!! [attempt (async/timeout timeout)])]
    (if (= ch attempt)
      resp
      {:status  http-timeout
       :headers {"Content-Type" "text/plain"}
       :body    (->> timeout
                     (format "Client Timeout. Request did not complete in %d ms")
                     json/stringify-UTF8)})))