(ns fluree.db.peer.http-api
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [aleph.http :as http]
            [aleph.netty :as netty]
            [compojure.core :as compojure]
            [compojure.route :as route]
            [manifold.deferred :as d]
            [ring.middleware.params :as params]
            [aleph.middleware.cors :as cors]

            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [fluree.db.api :as fdb]
            [fluree.db.flake :as flake]
            [fluree.db.query.fql :as fql]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.session :as session]
            [fluree.db.graphdb :as graphdb]
            [fluree.db.ledger.transact :as transact]
            [fluree.db.ledger.snapshot :as snapshot]
            [fluree.db.ledger.export :as export]
            [fluree.db.query.http-signatures :as http-signatures]
            [fluree.db.token-auth :as token-auth]

            [fluree.db.peer.websocket :as websocket]
            [ring.util.response :as resp]
            [clojure.string :as str]
            [fluree.db.util.async :refer [<?? <? go-try]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.permissions-validate :as permissions-validate]
            [fluree.db.peer.password-auth :as pw-auth]
            [fluree.db.ledger.reindex :as reindex]
            [fluree.db.ledger.mutable :as mutable]
            [fluree.db.auth :as auth]
            [fluree.db.ledger.delete :as delete])


  (:import (java.io Closeable)
           (java.time Instant)
           (java.net BindException)
           (fluree.db.flake Flake)))

(defn s
  [^Flake f]
  (.-s f))

(defn- count-graphql-resp
  [res fuel]
  (let [res' (if (coll? res)
               (reduce + (map (fn [n]
                                (cond

                                  (map? n)
                                  (let [work (map
                                               (fn [n]
                                                 (if (= (key n) :_id) 0
                                                                      (count-graphql-resp (val n) fuel))) n)
                                        sum  (reduce + work)]
                                    sum)

                                  (vector? n)
                                  (let [work (map #(count-graphql-resp % fuel) n)
                                        sum  (reduce + work)]
                                    sum)

                                  :else
                                  1)) res)) 1)] res'))

(defn- collect-errors
  [e debug-mode?]
  (let [base-resp (merge (ex-data e) {:message (.getMessage e)})
        error     (cond->
                    base-resp
                    debug-mode? (assoc :stack (mapv str (.getStackTrace e))))]
    (if-let [cause (.getCause e)]
      (assoc error :cause (collect-errors cause debug-mode?))
      error)))


(defn- wrap-errors
  [debug-mode? handler]
  (fn [req]
    (->
      (d/future
        (handler req))
      (d/catch
        (fn [e]
          (let [{:keys [status headers body]} (ex-data e)
                body         (if (instance? java.lang.AssertionError e)
                               {:status  400
                                :message (.getMessage e)
                                :error   :db/assert-failed}
                               (or body (collect-errors e debug-mode?)))
                status       (or status (:status body) 500)
                json-encode? (or (map? body) (sequential? body) (boolean? body) (number? body))]
            (if (>= status 500)
              (log/error e "Server exception:" body)
              (log/info "Request exception:" body))
            (cond-> {:status status :headers headers :body body}
                    json-encode?
                    (assoc :headers (merge headers {"content-type" "application/json; charset=UTF-8"})
                           :body (json/stringify-UTF8 (select-keys body [:status :message :error]))))))))))


(def not-found
  {:status  404
   :headers {"Content-Type" "text/plain"}
   :body    "Not found"})

(defn- json-response
  [status body & [headers]]
  {:status  status
   :headers (merge {"Content-Type" "application/json; charset=utf-8"} headers)
   :body    (json/stringify-UTF8 body)})


(defn decode-body
  [body type]
  (case type
    :json (json/parse body)))

(defn- return-token
  [req]
  (some->>
    (get-in req [:headers "authorization"])
    (re-find #"^Bearer (.+)$")
    (second)))

(defn- return-signature
  [req]
  (get-in req [:headers "signature"]))


(defn delete-ledger
  "Deletes ledger and snapshots, without creating an S3 bucket. "
  [{:keys [conn] :as system} params auth-sid]
  ;(let [{:keys [dbname]} params
  ;      master-conn    (db/master-connection backend)
  ;      conn           (db/connect backend dbname)
  ;      delete-db-resp @(db/delete-db conn)]
  ;  (if (= 200 (:status delete-db-resp))
  ;    ;; db is deleting, remove master DB record of DB
  ;    (let [res @(db/transact master-conn {:tx (json/write [{:_id     ["db/name" dbname]
  ;                                                           :deleted true}])})]
  ;      (if (= 200 (:status res))
  ;        {:status  200
  ;         :message (str "Success. Deleting " dbname ".")}
  ;        res))
  ;    delete-db-resp))

  nil)


(defn verify-auth
  [db auth authority]
  (go-try (if (or (not auth) (= auth authority))
            auth
            (let [auth_id      (<? (dbproto/-subid db ["_auth/id" auth] true))
                  _            (when (util/exception? auth_id)
                                 (throw (ex-info (str "Auth id for transaction does not exist
                                                    in the database: " auth) {:status 403 :error
                                                                                      :db/invalid-auth})))
                  authority_id (<? (dbproto/-subid db ["_auth/id" authority] true))
                  _            (when (util/exception? authority_id)
                                 (throw (ex-info (str "Authority " authority " does not exist.")
                                                 {:status 403 :error :db/invalid-auth})))
                  _            (<? (transact/valid-authority? db auth_id authority_id))]
              auth))))


(defn- auth-map
  "Attempts to extract an auth map (contains at least an :auth key) from
  JWT first if available, then http-signature."
  [system ledger request body]
  (let [jwt (return-token request)]
    ;; TODO - requests from admin UI coming in with "undefined", get rid of that and can remove this condition
    (if (or (not jwt) (= "undefined" jwt))
      (let [request (assoc request :body body)]
        (-> (http-signatures/verify-request request)
            (assoc :ledger ledger)))
      (pw-auth/fluree-auth-map (:conn system) ledger jwt))))

(defn open-api?
  [system]
  (-> system :group :open-api))

(defn- is-ledger?
  "Checks if running as a transaction server (true) or a query edge server (false)"
  [system]
  (-> system :config :is-ledger?))

(defn authenticated?
  "Returns truthy if either open-api is enable or user is authenticated"
  [system auth-map]
  (or (open-api? system)
      (:auth auth-map)))

(defn- require-authentication
  "Will throw if request if not authenticated"
  [system auth-map]
  (when-not (authenticated? system auth-map)
    (throw (ex-info (str "Request requires authentication.")
                    {:status 401
                     :error  :db/invalid-auth}))))

(defn- strict-authentication
  "Will throw if request if not authenticated, and if the auth-id isn't
  currently within the system. Returns a core async channel."
  [system auth-map]
  (go-try
    (require-authentication system auth-map)
    (let [{:keys [auth ledger]} auth-map
          db    (fdb/db (:conn system) ledger)
          subid (<? (fdb/subid-async db ["_auth/id" auth]))]
      (if-not subid
        (throw (ex-info (str "Request auth id is not valid: " auth)
                        {:status 401
                         :error  :db/invalid-auth}))
        true))))


(defmulti action-handler (fn [action _ _ _ _ _] action))

(defmethod action-handler :transact
  [_ system param auth-map ledger timeout]
  (go-try
    (require-authentication system auth-map)
    (let [conn        (:conn system)
          private-key (when (= :jwt (:type auth-map))
                        (<? (pw-auth/fluree-decode-jwt conn (:jwt auth-map))))
          _           (when-not (sequential? param)
                        (throw (ex-info (str "A transaction submitted to the 'transact' endpoint must be a list/vector/array.")
                                        {:status 400 :error :db/invalid-transaction})))
          auth-id     (:auth auth-map)
          result      (<? (fdb/transact-async conn ledger param {:auth        auth-id
                                                                 :private-key private-key
                                                                 :wait        true
                                                                 :timeout     timeout}))]
      [{:status (or (:status result) 200)
        :fuel   (or (:fuel result) 0)}
       result])))

(defmethod action-handler :hide
  [_ system param _ ledger _]
  (go-try
    (or (open-api? system)
        (throw (ex-info "Hiding flakes in a closed API is not currently supported"
                        {:status  400
                         :message :db/invalid-command})))
    (let [conn   (:conn system)
          {:keys [local]} param
          _      (when (false? local)
                   (throw (ex-info "Hiding flakes is not currently supported across a network. local must be set to true (defaults to true)."
                                   {:status  400
                                    :message :db/invalid-command})))
          [nw ledger] (str/split (util/keyword->str ledger) #"/")
          result (<? (mutable/hide-flakes conn nw ledger param))]
      [{:status (or (:status result) 500)
        :fuel   (or (:fuel result) 0)}
       (:result result)])))

(defmethod action-handler :purge
  [_ system param _ ledger _]
  (go-try
    (or (open-api? system)
        (throw (ex-info "Hiding flakes in a closed API is not currently supported"
                        {:status  400
                         :message :db/invalid-command})))
    (let [conn   (:conn system)
          {:keys [local]} param
          _      (when (false? local)
                   (throw (ex-info "Purging flakes is not currently supported across a network. local must be set to true (defaults to true)."
                                   {:status  400
                                    :message :db/invalid-command})))
          [nw ledger] (str/split (util/keyword->str ledger) #"/")
          result (<? (mutable/purge-flakes conn nw ledger param))]
      [{:status (or (:status result) 500)
        :fuel   (or (:fuel result) 0)}
       (:result result)])))


(defmethod action-handler :command
  [_ system param _ _ _]
  (go-try
    (let [_      (when-not (and (map? param) (:cmd param))
                   (throw (ex-info (str "Api endpoint for 'command' must contain a map/object with cmd keys.")
                                   {:status 400 :error :db/invalid-command})))
          conn   (:conn system)
          result (cond (and (:cmd param) (:sig param))
                       (<? (fdb/submit-command-async conn param))

                       (not (open-api? system))
                       (throw (ex-info (str "Api endpoint for 'command' must contain a map/object with cmd and sig keys when using a closed Api.")
                                       {:status 400 :error :db/invalid-command}))

                       :else
                       (let [cmd  (-> param :cmd json/parse)
                             opts (-> (dissoc cmd :tx)
                                      (assoc :txid-only true))
                             {:keys [tx db]} cmd]
                         (<? (fdb/transact-async conn db tx opts))))]
      [{:status (or (:status result) 200)
        :fuel   (or (:fuel result) 0)}
       result])))

(defmethod action-handler :test-transact-with
  [_ system param auth-map ledger _]
  (go-try
    (require-authentication system auth-map)
    (let [{:keys [tx flakes auth]} param
          conn          (:conn system)
          auth-id       (:auth auth-map)
          db            (fdb/db conn ledger {:auth (when auth-id ["_auth/id" auth-id])})
          private-key   (or (txproto/get-shared-private-key (:group system))
                            (throw (ex-info (str "There is no shared private key in this group. The test-transact-with endpoint is not currently supported in databases where fdb-api-open is false")
                                            {:status 400})))

          cmd-data      (fdb/tx->command ledger tx private-key {:auth auth})
          flakes'       (map flake/parts->Flake flakes)
          block-instant (Instant/now)
          db-with       (<? (dbproto/-forward-time-travel db flakes'))
          next-t        (- (:t db-with) 1)
          session       (session/session conn ledger)
          res           (<? (transact/build-transaction session db-with {:command cmd-data} next-t block-instant))
          {:keys [flakes fuel status error]} res
          _             (session/close session)]
      [{:status status}
       (if error {:error error} {:flakes flakes :fuel fuel})])))


(defmethod action-handler :gen-flakes
  [_ system param auth-map ledger _]
  (go-try
    (require-authentication system auth-map)
    (let [conn        (:conn system)
          session     (session/session conn ledger)
          auth-id     (:auth auth-map)
          db          (fdb/db conn ledger {:auth (when auth-id ["_auth/id" auth-id])})
          private-key (or (txproto/get-shared-private-key (:group system))
                          (throw (ex-info (str "There is no shared private key in this group. The gen-flakes endpoint is not currently supported in databases where fdb-api-open is false")
                                          {:status 400
                                           :error  :db/invalid-transaction})))]
      (loop [fuel-tot   0
             txn        [(first param)]
             txs        (rest param)
             db         (<? db)
             flakes-all []]
        (let [cmd-data      (fdb/tx->command ledger txn private-key)
              next-t        (dec (:t db))
              block-instant (Instant/now)
              res           (<? (transact/build-transaction session db {:command cmd-data} next-t block-instant))
              {:keys [flakes fuel status error db-after]} res
              fuel-tot      (+ fuel-tot fuel)
              _             (if (not= status 200)
                              (throw (ex-info error
                                              {:status status
                                               :error  error
                                               :fuel   fuel-tot})))
              flakes'       (concat flakes-all flakes)]
          (if (empty? txs)
            (let [_                 (session/close session)
                  flakes-by-subject (group-by s flakes')
                  res-map           (async/go-loop [vals' (vals flakes-by-subject)
                                                    acc []]
                                      (let [val' (first vals')
                                            res  (<? (fql/flakes->res db-after
                                                                      (volatile! {})
                                                                      (volatile! fuel-tot)
                                                                      1000000 {:wildcard? true, :select {}} val'))
                                            acc' (conj acc res)]
                                        (if (not-empty (rest vals'))
                                          (recur (rest vals') acc')
                                          acc')))]
              [{:status status} {:res    (<? res-map)
                                 :flakes flakes'
                                 :fuel   fuel-tot}])
            (recur fuel-tot [(first txs)] (rest txs) db-after flakes')))))))


(defmethod action-handler :ledger-stats
  [_ system _ auth-map ledger _]
  (go-try
    (let [conn    (:conn system)
          session (session/session conn ledger)
          db-info (<? (fdb/ledger-info-async conn ledger))
          db-stat (-> (<? (session/db conn ledger {:connect? false}))
                      (get-in [:stats]))
          _       (session/close session)]
      [{:status 200} {:status 200 :data (merge db-info db-stat)}])))


(defmethod action-handler :snapshot
  [_ system param auth-map ledger _]
  (go-try
    (<? (strict-authentication system auth-map))
    (let [conn (:conn system)
          {:keys [no-history]} param
          [network dbid] (graphdb/validate-ledger-ident ledger)]
      (if no-history
        [{:status 200} (<? (snapshot/create-snapshot-no-history conn network dbid))]
        [{:status 200} (<? (snapshot/create-snapshot conn network dbid))]))))

(defmethod action-handler :list-snapshots
  [_ system _ auth-map ledger _]
  (go-try
    (<? (strict-authentication system auth-map))
    (let [conn      (:conn system)
          [network dbid] (graphdb/validate-ledger-ident ledger)
          snapshots (snapshot/list-snapshots conn network dbid)]
      [{:status 200} snapshots])))



(defmethod action-handler :reindex
  [_ system _ _ ledger _]
  ;; For now, does not require authentication
  (go-try
    (let [conn      (:conn system)
          [network dbid] (graphdb/validate-ledger-ident ledger)
          reindexed (<? (reindex/reindex conn network dbid))]
      [{:status 200} {:block (:block reindexed)
                      :t     (:t reindexed)
                      :stats (:stats reindexed)}])))

(defmethod action-handler :export
  [_ system param auth-map ledger _]
  (go-try
    (<? (strict-authentication system auth-map))
    (let [conn (:conn system)
          {:keys [format block]} param
          db   (<? (fdb/db conn ledger))
          file (<? (export/db->export db format block))]
      [{:status 200} file])))

;; sync-to is option of db (or maybe a different db function). So doesn't return till

(defmethod action-handler :default
  [action system param auth-map ledger _]
  (go-try
    (require-authentication system auth-map)
    (let [conn       (:conn system)
          auth-id    (:auth auth-map)
          open-api   (open-api? system)
          sync-block (get-in param [:opts :syncTo])
          db         (if sync-block
                       (fdb/sync-to-db conn ledger sync-block {:auth        (when auth-id ["_auth/id" auth-id])
                                                               :syncTimeout (get-in param [:opts :syncTimeout])})
                       (fdb/db conn ledger {:auth (when auth-id ["_auth/id" auth-id])}))]
      (case action
        :query
        (let [query (assoc param :opts (merge (:opts param) {:meta true :open-api open-api}))
              res   (<? (fdb/query-async db query))]
          [(dissoc res :result) (:result res)])

        :multi-query
        (let [query (assoc param :opts (merge (:opts param) {:meta true :open-api open-api}))
              res   (<? (fdb/multi-query-async db query))]
          [(dissoc res :result) (:result res)])

        :block
        (let [query (assoc param :opts (merge (:opts param) {:meta true :open-api open-api}))
              res   (<? (fdb/block-query-async conn ledger query))]
          [(dissoc res :result) (:result res)])

        :block-range-with-txn
        (let [query (assoc param :opts (merge (:opts param) {:meta true :open-api open-api}))
              res   (<? (fdb/block-range-with-txn-async conn ledger query))]
          [{:status 200} {:status 200
                          :data   res}])

        :history
        (let [res (<? (fdb/history-query-async db (assoc-in param [:opts :meta] true)))]
          [(dissoc res :result) (:result res)])

        :graphql
        (let [result (<? (fdb/graphql-async conn ledger param))]
          [{:status 200} {:status 200
                          :data   result}])

        :sparql
        [{:status 200} (<? (fdb/sparql-async db param {:open-api open-api}))]

        :ledger-info
        (let [res (<? (fdb/ledger-info-async conn ledger))]
          [{:status 200} {:status 200 :data res}])

        ; Test endpoints
        :query-with
        (let [res (<? (fdb/query-with-async db param))]
          [(dissoc res :result) (:result res)])

        ;; else
        (throw (ex-info (str "Invalid action:" action)
                        {:status 400
                         :error  :db/invalid-action}))))))


(defn wrap-action-handler
  "Wraps a db request to facilitate proper response format"
  [system {:keys [headers body params remote-addr] :as request}]
  (let [deferred (d/deferred)]
    (async/go
      (try
        (let [{:keys [action network db]} params
              start           (System/nanoTime)
              ledger          (keyword network db)
              action*         (keyword action)
              action-param    (when body (decode-body body :json))
              auth-map        (auth-map system ledger request action-param)
              request-timeout (if-let [timeout (:request-timeout headers)]
                                (try (Integer/parseInt timeout)
                                     (catch Exception e 60000))
                                60000)
              [header body] (<? (action-handler action* system action-param auth-map ledger request-timeout))
              request-time    (- (System/nanoTime) start)
              resp-body       (json/stringify-UTF8 body)
              resp-headers    (reduce-kv (fn [acc k v]
                                           (assoc acc (str "x-fdb-" (util/keyword->str k)) v))
                                         {"Content-Type" "application/json; charset=utf-8"
                                          "x-fdb-time"   (format "%.2fms" (float (/ request-time 1000000)))
                                          "x-fdb-fuel"   (or (get header :fuel) (get body :fuel) 0)}
                                         header)
              resp            {:status  (or (:status header) 200)
                               :headers resp-headers
                               :body    resp-body}]
          (log/info (str ledger ":" action " [" (:status header) "] " remote-addr) header)
          (d/success! deferred resp))
        (catch Exception e
          (d/error! deferred e))))
    deferred))


;; TODO - need to include some good logging here of activity
(defn password-login
  "Returns a JWT token if successful.
  Must supply ledger, password and either user or auth identifier.
  Expire is optional
  - ledger   - ledger identifier
  - password - plain-text password
  - user     - _user/username (TODO: should allow any _user ident in the future)
  - auth     - _auth/id (TODO: should allow any _auth ident in the future)
  - expire   - requested time to expire in milliseconds"
  [system ledger {:keys [body] :as request}]
  (let [deferred (d/deferred)]
    (async/go
      (try
        (let [{:keys [password user auth expire]} (decode-body body :json)
              _       (when-not password
                        (throw (ex-info "A password must be supplied in the provided JSON."
                                        {:status 400
                                         :error  :db/invalid-request})))
              _       (when-not (or user auth)
                        (throw (ex-info "A user identity or auth identity must be supplied."
                                        {:status 400
                                         :error  :db/invalid-request})))

              options (util/without-nils {:expire expire})

              jwt     (<? (pw-auth/fluree-login-user (:conn system) ledger password user auth options))]
          (d/success! deferred
                      {:headers {"Content-Type" "application/json"}
                       :status  200
                       :body    (json/stringify-UTF8 jwt)}))
        (catch Exception e
          (d/error! deferred e))))
    deferred))


;; TODO - ensure 'expire' is epoch ms, or coerce if a string
(defn password-generate
  [system ledger {:keys [body] :as request}]
  (let [deferred (d/deferred)]
    (async/go
      (try
        (let [{:keys [password user roles expire create-user?]} (decode-body body :json)
              _       (when-not password
                        (throw (ex-info "A password must be supplied in the provided JSON."
                                        {:status 400
                                         :error  :db/invalid-request})))
              conn    (:conn system)
              {:keys [signing-key]} (-> conn :meta :password-auth)
              options (util/without-nils
                        {:private-key  signing-key
                         :create-user? create-user?
                         :expire       expire
                         :user         user
                         :roles        roles})
              {:keys [jwt]} (<? (pw-auth/fluree-new-pw-auth conn ledger password options))]
          (d/success! deferred
                      {:headers {"Content-Type" "application/json"}
                       :status  200
                       :body    (json/stringify-UTF8 jwt)}))
        (catch Exception e
          (d/error! deferred e))))
    deferred))


(defn password-renew
  "Renews JWT token, returning renewed JWT in body."
  [system ledger {:keys [body] :as request}]
  (let [conn        (:conn system)
        jwt-options (-> conn :meta :password-auth)
        {:keys [secret]} jwt-options
        jwt         (some->> (return-token request)
                             ;; returns map, or will throw if token invalid or expired
                             (token-auth/verify-jwt secret))
        _           (when-not jwt
                      (throw (ex-info "A valid JWT token must be supplied in the header for a token renewal."
                                      {:status 401
                                       :error  :db/invalid-auth})))
        _           (when-not (= (:iss jwt) ledger)
                      (throw (ex-info (str "JWT is issued from a different ledger than the request specifies: " ledger)
                                      {:status 401
                                       :error  :db/invalid-auth})))
        {:keys [expire]} (when body
                           (decode-body body :json))
        options     (util/without-nils {:expire expire})
        new-jwt     (pw-auth/fluree-renew-jwt jwt-options jwt options)]
    {:headers {"Content-Type" "application/json"}
     :status  200
     :body    (json/stringify-UTF8 new-jwt)}))


(defn password-handler
  [system {:keys [headers body params remote-addr] :as request}]
  (when-not (pw-auth/password-enabled? (:conn system))
    (throw (ex-info "Password authentication is not enabled."
                    {:status 401
                     :error  :db/no-password-auth})))
  (let [{:keys [action network db]} params
        ledger (str network "/" db)]
    (case (keyword action)
      :renew (password-renew system ledger request)
      :login (password-login system ledger request)
      :generate (password-generate system ledger request))))

(defn nw-state
  [system request]
  (let [open-api? (open-api? system)
        raft      (-> system :group :state-atom deref (dissoc :private-key))
        {:keys [cmd-queue new-db-queue networks leases]} raft
        instant   (System/currentTimeMillis)
        cmd-q     (reduce-kv #(conj %1 {(keyword %2) (count %3)}) [] cmd-queue)
        new-db-q  (reduce-kv #(conj %1 {(keyword %2) (count %3)}) [] new-db-queue)
        nw-data   (reduce-kv #(conj %1 {(keyword %2) (dissoc %3 :private-key)}) [] networks)
        svr-state (reduce-kv #(conj %1 {:id (:id %3) :active? (> (:expire %3) instant)}) [] (:servers leases))
        raft'     (assoc raft :cmd-queue cmd-q
                              :new-db-queue new-db-q
                              :networks nw-data)
        state     (-> (txproto/-state (:group system))
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
                      (assoc :open-api open-api?)
                      (assoc :raft raft')
                      (assoc :svr-state svr-state))]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 state)}))

(defn add-server
  [system {:keys [headers body params remote-addr] :as request}]
  (let [{:keys [server]} (decode-body body :json)
        add-server (<?? (txproto/-add-server-async (:group system) server))]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 add-server)}))

(defn remove-server
  [system {:keys [headers body params remote-addr] :as request}]
  (let [{:keys [server]} (decode-body body :json)
        remove-server (<?? (txproto/-remove-server-async (:group system) server))]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 remove-server)}))

(defn health-handler
  [system request]
  (let [state (-> (txproto/-state (:group system))
                  :status)]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 {:ready       true
                                    :status      state
                                    :utilization 0.5})}))

(defn keys-handler
  [system request]
  (let [{:keys [public private]} (crypto/generate-key-pair)
        account-id (crypto/account-id-from-public public)]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 {:private    private
                                    :public     public
                                    :account-id account-id})}))

(defn get-ledgers
  [system request]
  (let [ledgers @(fdb/ledger-list (:conn system))]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 ledgers)}))

(defn new-ledger
  [{:keys [conn] :as system} {:keys [body] :as request}]
  (let [body         (decode-body body :json)
        ledger-ident (:db/id body)
        opts         (dissoc body :db/id)
        result       @(fdb/new-ledger conn ledger-ident opts)
        _            (when (is-ledger? system) (session/session conn ledger-ident))
        _            (if (= clojure.lang.ExceptionInfo (type result))
                       (throw result))]
    ;; create session so tx-monitors will work
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 result)}))

(defn delete-ledger
  [{:keys [conn] :as system} {:keys [headers body params remote-addr] :as request}]
  (let [deferred (d/deferred)]
    (async/go
      (try
        (let [body         (when body (decode-body body :json))
              ledger-ident (:db/id body)
              [nw db] (str/split ledger-ident #"/")
              ledger       (keyword nw db)
              auth-map     (auth-map system ledger request body)
              session      (session/session conn [nw db])
              db*          (<? (session/current-db session))
              ;; TODO - root role just checks if the auth has a role with id 'root' this can
              ;; be manipulated, so we need a better way of handling this.
              _            (when-not (or (open-api? system)
                                         (<? (auth/root-role? db* (:auth auth-map))))
                             (throw (ex-info (str "To delete a ledger, must be using an open API or an auth record with a root role.") {:status 401 :error :db/invalid-auth})))
              res          (<? (delete/process conn nw db))
              resp         {:status  200
                            :headers {"Content-Type" "application/json; charset=utf-8"}
                            :body    (json/stringify-UTF8 {"deleted" (str nw "/" db)})}]
          (log/info (str ledger ":deleted" " [" (or resp 400) "] " remote-addr))
          (d/success! deferred resp))
        (catch Exception e
          (d/error! deferred e)))) deferred))



(defn- promise-chan->deferred
  "Takes a channel and delivers the first result available into a manifold deferred"
  [chan]
  (let [d (d/deferred)]
    (async/go
      (let [res (async/<! chan)]
        (if (instance? Throwable res)
          (d/error! d res)
          (d/success! d res))))
    d))

(defn deserialize
  [serializer key data]
  (cond
    (str/includes? key "_block_")
    (serdeproto/-deserialize-block serializer data)

    (str/includes? key "_root_")
    (serdeproto/-deserialize-db-root serializer data)

    (str/ends-with? key "-b")
    (serdeproto/-deserialize-branch serializer data)

    (str/ends-with? key "-l")
    (serdeproto/-deserialize-leaf serializer data)

    (str/ends-with? key "-l-his")
    (serdeproto/-deserialize-leaf serializer data)))



;; TODO - check for content in the cache might be quicker? cache is already deserialized though.
;; TODO - if file store, consider streaming contents back - currently sends it all in one big chunk.
(defn storage-handler
  "Handler for key-value store requests.
  Used if query engine uses ledger as storage."
  [system {:keys [headers params] :as request}]
  (let [deferred (d/deferred)]
    (async/go
      (try (let [conn             (:conn system)
                 storage-read-fn  (:storage-read conn)
                 accept-encodings (or (get headers "accept")
                                      "application/json")
                 signature        (return-signature request)
                 open-api?        (open-api? system)
                 _                (if (and (not open-api?) (not signature))
                                    (throw (ex-info (str "To request an item from storage, open-api must be true or your request must be signed.") {:status 401
                                                                                                                                                    :error  :db/invalid-transaction})))
                 response-type    (if (str/includes? accept-encodings "application/json")
                                    :json
                                    :avro)
                 _                (when (and (not open-api?) (= :avro response-type))
                                    (throw (ex-info (str "If using a closed api, a storage request must be returned as json.") {:status 401
                                                                                                                                :error  :db/invalid-transaction})))
                 {:keys [network db type key]} params
                 db-name          (keyword network db)
                 _                (when-not (and network db type)
                                    (throw (ex-info (str "Incomplete request. At least a network, db and type are required. Provided network: " network " db: " db " type: " type " key: " key)
                                                    {:status 400 :error :db/invalid-request})))
                 auth-id          (when signature
                                    (let [this-server (get-in system [:group :this-server])
                                          servers     (get-in system [:config :group :server-configs])
                                          host        (-> (filter (fn [n] (= (:server-id n) this-server)) servers)
                                                          first :host)

                                          {:keys [auth authority]} (http-signatures/verify-request* {:headers headers} :get
                                                                                                    (str "/fdb/storage/" network "/" db
                                                                                                         (if type (str "/" type))
                                                                                                         (if key (str "/" key))) host)
                                          db          (<? (fdb/db (:conn system) db-name))]
                                      (<? (verify-auth db auth authority))))
                 formatted-key    (cond-> (str network "_" db "_" type)
                                          key (str "_" key))
                 avro-data        (<? (storage-read-fn formatted-key))
                 status           (if avro-data 200 404)
                 headers          (if avro-data
                                    {"Content-Type" (if (= :json response-type)
                                                      "application/json"
                                                      "avro/binary")}
                                    {"Content-Type" "text/plain"})
                 body             (cond
                                    (and avro-data (= :json response-type))
                                    (let [serializer (fluree.db.storage.core/serde conn)
                                          data       (deserialize serializer formatted-key avro-data)]
                                      (if auth-id
                                        (let [auth            (if (string? auth-id) ["_auth/id" auth-id] auth-id)
                                              permissioned-db (<? (fdb/db conn db-name {:auth auth}))
                                              flakes          (when-let [flakes (:flakes data)]
                                                                (<? (permissions-validate/allow-flakes? permissioned-db flakes)))
                                              data'           (if flakes
                                                                (assoc data :flakes flakes)
                                                                data)]
                                          (json/stringify data'))
                                        (json/stringify data)))

                                    avro-data avro-data

                                    :else "Not Found")]
             (d/success! deferred {:status  status
                                   :headers headers
                                   :body    body}))
           (catch Exception e
             (d/error! deferred e)))
      (d/timeout! deferred 3000 {:status  504
                                 :headers {"Content-Type" "text/plain"}
                                 :body    "Gateway Timeout"}))
    deferred))



(defn subscription-handler
  [system request])



; From https://gist.github.com/dannypurcell/8215411
(defn ignore-trailing-slash
  "Modifies the request uri before calling the handler.
  Removes a single trailing slash from the end of the uri if present.

  Useful for handling optional trailing slashes until Compojure's route matching syntax supports regex.
  Adapted from http://stackoverflow.com/questions/8380468/compojure-regex-for-matching-a-trailing-slash"
  [handler]
  (fn [request]
    (let [uri (:uri request)]
      (handler (assoc request :uri (if (and (not (= "/" uri))
                                            (.endsWith uri "/"))
                                     (subs uri 0 (dec (count uri)))
                                     uri))))))


;; TODO - handle CORS more reasonably for production
(defn- make-handler
  [system]
  (ignore-trailing-slash (cors/wrap-cors
                           (wrap-errors
                             (:debug-mode? system)
                             (params/wrap-params
                               (compojure/routes
                                 (compojure/GET "/fdb/storage/:network/:db/:type" request (storage-handler system request))
                                 (compojure/GET "/fdb/storage/:network/:db/:type/:key" request (storage-handler system request))
                                 (compojure/GET "/fdb/ws" request (websocket/handler system request))
                                 (compojure/ANY "/fdb/health" request (health-handler system request))
                                 (compojure/ANY "/fdb/nw-state" request (nw-state system request))
                                 (compojure/POST "/fdb/add-server" request (add-server system request))
                                 (compojure/POST "/fdb/remove-server" request (remove-server system request))
                                 (compojure/POST "/fdb/sub" request (subscription-handler system request))
                                 (compojure/GET "/fdb/new-keys" request (keys-handler system request))
                                 (compojure/POST "/fdb/new-keys" request (keys-handler system request))
                                 (compojure/POST "/fdb/dbs" request (get-ledgers system request))
                                 (compojure/GET "/fdb/dbs" request (get-ledgers system request))
                                 (compojure/POST "/fdb/new-db" request (new-ledger system request))
                                 (compojure/POST "/fdb/delete-db" request (delete-ledger system request))
                                 (compojure/POST "/fdb/:network/:db/pw/:action" request (password-handler system request))
                                 (compojure/POST "/fdb/:network/:db/:action" request (wrap-action-handler system request))
                                 (compojure/GET "/" [] (resp/resource-response "index.html" {:root "adminUI"}))
                                 (route/resources "/" {:root "adminUI"})
                                 (compojure/GET "/account" [] (resp/resource-response "index.html" {:root "adminUI"}))
                                 (compojure/GET "/flureeql" [] (resp/resource-response "index.html" {:root "adminUI"}))
                                 (compojure/GET "/graphql" [] (resp/resource-response "index.html" {:root "adminUI"}))
                                 (compojure/GET "/sparql" [] (resp/resource-response "index.html" {:root "adminUI"}))
                                 (compojure/GET "/schema" [] (resp/resource-response "index.html" {:root "adminUI"}))
                                 (compojure/GET "/import" [] (resp/resource-response "index.html" {:root "adminUI"}))
                                 (compojure/GET "/permissions" [] (resp/resource-response "index.html" {:root "adminUI"}))
                                 (constantly not-found))))

                           :access-control-allow-origin [#".+"]
                           :access-control-expose-headers ["X-Fdb-Block" "X-Fdb-Fuel" "X-Fdb-Status" "X-Fdb-Time"]
                           :access-control-allow-methods [:get :put :post :delete])))

(defrecord WebServer [close])


(defn webserver-factory
  [opts]
  (if-not (:enabled opts)
    (do (log/info "Web server disabled, not starting.")
        nil)
    (let [{:keys [port open-api system debug-mode?]} opts
          _           (log/info (str "Starting web server on port: " port (if open-api " with an open API." "with a closed API.")))
          _           (log/info "")
          _           (log/info (str "http://localhost:" port))
          _           (log/info "")
          clients     (atom {})
          system*     (assoc system :clients clients
                                    :debug-mode? debug-mode?
                                    :open-api open-api)
          server-proc (try
                        (http/start-server (make-handler system*) {:port port})
                        (catch BindException _
                          (log/error (str "Cannot start. Port binding failed, address already in use. Port: " port "."))
                          (log/error "FlureeDB Exiting. Adjust your config, or shut down other service using port.")
                          (System/exit 1))
                        (catch Exception e
                          (log/error "Unable to start http API: " (.getMessage e))
                          (log/error "FlureeDB Exiting.")
                          (System/exit 1)))
          close-fn    (fn [] (do (.close ^Closeable server-proc)
                                 (netty/wait-for-close server-proc)))]
      (map->WebServer {:close close-fn}))))



(comment
  (def opts {:enabled true :system user/system :port 8080})

  (webserver-factory opts)


  (defn handler [req]
    {:status  200
     :headers {"content-type" "text/plain"}
     :body    "hello!"})

  (def server5 (http/start-server handler {:port 8080}))

  (type server)

  (do
    ;(.close server4)
    (aleph.netty/wait-for-close server5))


  (.close server5))


