(ns fluree.db.peer.http-api
  (:require [fluree.db.util.log :as log]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [org.httpkit.server :as http]
            [compojure.core :as compojure :refer [defroutes]]
            [compojure.route :as route]
            [ring.middleware.params :as params]
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
            [fluree.db.ledger.export :as export]
            [fluree.db.query.http-signatures :as http-signatures]
            [fluree.db.token-auth :as token-auth]
            [fluree.db.peer.websocket :as websocket]
            [ring.util.response :as resp]
            [ring.middleware.cors :as cors]
            [fluree.db.util.async :refer [<?? <? go-try alts??]]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.permissions-validate :as permissions-validate]
            [fluree.db.peer.server-health :as server-health]
            [fluree.db.peer.password-auth :as pw-auth]
            [fluree.db.ledger.reindex :as reindex]
            [fluree.db.ledger.mutable :as mutable]
            [fluree.db.auth :as auth]
            [fluree.db.ledger.delete :as delete]
            [fluree.db.meta :as meta]
            [fluree.db.storage.core :as storage-core]
            [fluree.db.ledger.transact.core :as tx-core]
            [fluree.db.util.tx :as tx-util])
  (:import (java.time Instant)
           (java.net BindException URL)
           (fluree.db.flake Flake)
           (clojure.lang ExceptionInfo)
           (org.httpkit BytesInputStream)))

(set! *warn-on-reflection* true)

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
  (let [base-resp (merge (ex-data e) {:message (ex-message e)})
        error     (cond->
                    base-resp
                    debug-mode? (assoc :stack (mapv str (.getStackTrace ^Throwable e))))]
    (if-let [cause (ex-cause e)]
      (assoc error :cause (collect-errors cause debug-mode?))
      error)))


(defn- wrap-errors
  [debug-mode? handler]
  (fn [req]
    (try
      (handler req)
      (catch Exception e
        (let [{:keys [status headers body]} (ex-data e)
              body         (if (instance? AssertionError e)
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
                         :body (json/stringify-UTF8 (select-keys body [:status :message :error])))))))))


(def not-found
  {:status  404
   :headers {"Content-Type" "text/plain"}
   :body    "Not found"})


(defn decode-body
  [body type]
  (let [body' (-> ^BytesInputStream body .bytes (String. "UTF-8"))]
    (case type
      :string body'
      :json (json/parse body'))))


(defn- return-token
  [req]
  (some->>
    (get-in req [:headers "authorization"])
    (re-find #"^Bearer (.+)$")
    (second)))


(defn- return-signature
  [req]
  (get-in req [:headers "signature"]))


(defn verify-auth
  [db auth authority]
  (go-try
    (if (or (not auth) (= auth authority))
      auth
      (let [auth_id      (<? (dbproto/-subid db ["_auth/id" auth] true))
            _            (when (util/exception? auth_id)
                           (throw (ex-info (str "Auth id for transaction does not exist
                                                    in the database: " auth)
                                           {:status 403 :error :db/invalid-auth})))
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
  (-> system :config :webserver :open-api))


(defn authenticated?
  "Returns truthy if either open-api is enabled or user is authenticated"
  [system auth-map]
  (or (open-api? system)
      (:auth auth-map)))


(defn- require-authentication
  "Will throw if request is not authenticated"
  [system auth-map]
  (when-not (authenticated? system auth-map)
    (throw (ex-info (str "Request requires authentication.")
                    {:status 401
                     :error  :db/invalid-auth}))))


(defn- strict-authentication
  "Will throw if request is not authenticated, and if the auth-id isn't
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
  [_ {:keys [conn] :as system} param auth-map ledger
   {:keys [timeout] :as opts}]
  (go-try
    (require-authentication system auth-map)
    (let [private-key   (when (= :jwt (:type auth-map))
                          (<? (pw-auth/fluree-decode-jwt conn (:jwt auth-map))))
          verified-auth (when (= :http-signature (:type auth-map))
                          (-> auth-map
                              (select-keys [:signed :signature])
                              ;; :authority is guaranteed to be the verified auth-id
                              ;; derived from the private key that signed the
                              ;; original request
                              (assoc :auth (:authority auth-map))))
          _             (when-not (sequential? param)
                          (throw (ex-info (str "A transaction submitted to the "
                                               "'transact' endpoint must be a "
                                               "list/vector/array.")
                                          {:status 400
                                           :error  :db/invalid-transaction})))
          txid-only?    (some-> opts (get "txid-only") str/lower-case (= "true"))
          auth-id       (:auth auth-map)
          result        (<? (fdb/transact-async conn ledger param
                                                (util/without-nils
                                                  {:auth          auth-id
                                                   :verified-auth verified-auth
                                                   :private-key   private-key
                                                   :txid-only     txid-only?
                                                   :timeout       timeout})))]
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
  [_ system param _ ledger {:keys [timeout] :as opts}]
  (go-try
    (let [_      (when-not (and (map? param) (:cmd param))
                   (throw (ex-info (str "Api endpoint for 'command' must contain a map/object with cmd keys.")
                                   {:status 400 :error :db/invalid-command})))
          conn   (:conn system)
          result (cond
                   (and (:cmd param) (:sig param))
                   (let [persist-resp (<? (fdb/submit-command-async conn param))
                         result       (if (and (string? persist-resp) (-> param :txid-only false?))
                                        (<? (fdb/monitor-tx-async conn ledger persist-resp timeout))
                                        persist-resp)]
                     result)

                   (not (open-api? system))
                   (throw (ex-info (str "Api endpoint for 'command' must contain a map/object with cmd and sig keys when using a closed Api.")
                                   {:status 400 :error :db/invalid-command}))

                   :else
                   (let [cmd  (-> param :cmd json/parse)
                         opts (-> (dissoc cmd :tx)
                                  (assoc :txid-only false))
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
          conn         (:conn system)
          auth-id      (:auth auth-map)
          db           (fdb/db conn ledger {:auth (when auth-id ["_auth/id" auth-id])})
          private-key  (or (txproto/get-shared-private-key (:group system))
                           (throw (ex-info (str "There is no shared private key in this group. The test-transact-with endpoint is not currently supported in databases where fdb-api-open is false")
                                           {:status 400})))

          cmd-data     (fdb/tx->command ledger tx private-key {:auth auth})
          internal-cmd {:command cmd-data
                        :id      (:id cmd-data)}
          flakes'      (map flake/parts->Flake flakes)
          db-with      (<? (dbproto/-forward-time-travel db flakes'))
          session      (session/session conn ledger)
          res          (->> (tx-util/validate-command internal-cmd)
                            (tx-core/transact {:db-before db-with :instant (Instant/now)})
                            <?)
          {:keys [flakes fuel status error]} res
          _            (session/close session)]
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
                          (throw
                            (ex-info
                              (str "There is no shared private key in this group. The gen-flakes endpoint is not currently supported in databases where fdb-api-open is false")
                              {:status 400
                               :error  :db/invalid-transaction})))]
      (loop [fuel-tot   0
             txn        [(first param)]
             txs        (rest param)
             db         (<? db)
             flakes-all []]
        (let [cmd-data     (fdb/tx->command ledger txn private-key)
              internal-cmd {:command cmd-data
                            :id      (:id cmd-data)}

              {:keys [flakes fuel status error db-after]}
              (->> internal-cmd
                   tx-util/validate-command
                   (tx-core/transact {:db-before db :instant (Instant/now)})
                   <?)

              fuel-tot     (+ fuel-tot fuel)
              _            (when (not= status 200)
                             (throw (ex-info error
                                             {:status status
                                              :error  error
                                              :fuel   fuel-tot})))
              flakes'      (concat flakes-all flakes)]
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
              [{:status status}
               {:res    (<? res-map)
                :flakes flakes'
                :fuel   fuel-tot}])
            (recur fuel-tot [(first txs)] (rest txs) db-after flakes')))))))


(defmethod action-handler :ledger-stats
  [_ system _ _ ledger _]
  (log/trace "Getting ledger-info for" ledger)
  (go-try
    (let [conn    (:conn system)
          db-info (<? (fdb/ledger-info-async conn ledger))
          db-stat (-> (<? (session/db conn ledger {:connect? false}))
                      (get-in [:stats]))]
      [{:status 200} {:status 200 :data (merge db-info db-stat)}])))


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

(defmethod action-handler :reindex-fulltext
  [_ system _ _ ledger _]
  ;; For now, does not require authentication
  (go-try
    (let [conn           (:conn system)
          indexer        (-> conn :full-text/indexer :process)
          _              (graphdb/validate-ledger-ident ledger) ;validates, throws if not valid
          db             (<? (fdb/db conn ledger))
          reindex-status (<? (indexer {:action :reset, :db db}))]
      [{:status 200} reindex-status])))

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
    (let [conn     (:conn system)
          auth-id  (:auth auth-map)
          open-api (open-api? system)
          db-opts  (cond-> (select-keys (:opts param) [:syncTo :syncTimeout :roles :auth])
                           (not open-api) (dissoc :roles :auth) ;; open-api can specify auth id or roles to query as
                           auth-id (assoc :auth ["_auth/id" auth-id]))
          db       (fdb/db conn ledger db-opts)]
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

        :sql
        [{:status 200} (<? (fdb/sql-async db param {:open-api open-api}))]

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


(defn command-response-headers->http-headers [cmd-response-headers]
  (reduce-kv (fn [acc k v]
               (assoc acc (str "x-fdb-" (util/keyword->str k)) v))
             {"Content-Type" "application/json; charset=utf-8"}
             cmd-response-headers))


(defn wrap-action-handler
  "Wraps a db request to facilitate proper response format"
  [system {:keys [headers body params remote-addr] :as request}]
  (log/trace "wrap-action-handler received:" request)
  (let [{:keys [action network db]} params
        start           (System/nanoTime)
        ledger          (keyword network db)
        action*         (keyword action)
        body'           (when body (decode-body body :string))
        action-param    (some-> body' json/parse)
        _               (log/trace "wrap-action-handler decoded body:"
                                   action-param)
        auth-map        (auth-map system ledger request body')
        request-timeout (if-let [timeout (:request-timeout headers)]
                          (try (Integer/parseInt timeout)
                               (catch Exception _ 60000))
                          60000)
        opts            (assoc params :timeout request-timeout)
        [header body] (<?? (action-handler action* system action-param auth-map
                                           ledger opts))
        request-time    (- (System/nanoTime) start)
        resp-body       (json/stringify-UTF8 body)
        resp-headers    (-> header
                            (assoc :time (format "%.2fms"
                                                 (float
                                                   (/ request-time 1000000)))
                                   :fuel (or (:fuel header) (:fuel body) 0))
                            command-response-headers->http-headers)
        resp            {:status  (or (:status header) 200)
                         :headers resp-headers
                         :body    resp-body}]
    (log/info (str ledger ":" action " [" (:status header) "] " remote-addr)
              header)
    resp))


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
  [system ledger {:keys [body]}]
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

        jwt     (<?? (pw-auth/fluree-login-user (:conn system) ledger password user auth options))]
    {:headers {"Content-Type" "application/json"}
     :status  200
     :body    (json/stringify-UTF8 jwt)}))


;; TODO - ensure 'expire' is epoch ms, or coerce if a string
(defn password-generate
  [system ledger {:keys [body]}]
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
        {:keys [jwt]} (<?? (pw-auth/fluree-new-pw-auth conn ledger password options))]
    {:headers {"Content-Type" "application/json"}
     :status  200
     :body    (json/stringify-UTF8 jwt)}))


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
  [system {:keys [params] :as request}]
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


(defn version-handler [_ _]
  {:headers {"Content-Type" "text/plain"}
   :status  200
   :body    (meta/version)})


(defn add-server
  [system {:keys [body]}]
  (let [{:keys [server]} (decode-body body :json)
        add-server (<?? (txproto/-add-server-async (:group system) server))]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 add-server)}))


(defn remove-server
  [system {:keys [body]}]
  (let [{:keys [server]} (decode-body body :json)
        remove-server (<?? (txproto/-remove-server-async (:group system) server))]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 remove-server)}))


(defn keys-handler
  [_ _]
  (let [{:keys [public private]} (crypto/generate-key-pair)
        account-id (crypto/account-id-from-public public)]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 {:private    private
                                    :public     public
                                    :account-id account-id})}))


(defn get-ledgers
  [system _]
  (let [ledgers @(fdb/ledger-list (:conn system))]
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 ledgers)}))


(defn new-ledger
  [{:keys [conn] :as system} {:keys [body] :as request}]
  (let [body         (decode-body body :json)
        transactor?  (-> system :config :transactor?)
        ledger-ident (:db/id body)
        opts         (dissoc body :db/id :auth) ; dissoc auth so it can't be spoofed
        {sig-auth :auth} (http-signatures/verify-signature-header request)
        opts'        (if sig-auth
                       (update opts :owners (comp set conj) sig-auth)
                       opts)
        result       @(fdb/new-ledger conn ledger-ident opts')]
    (log/debug "new-ledger result:" result)
    ;; create session so tx-monitors will work
    (when transactor? (session/session conn ledger-ident))
    (when (= ExceptionInfo (type result))
      (throw result))
    {:status  200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body    (json/stringify-UTF8 result)}))


(defn delete-ledger
  [{:keys [conn] :as system} {:keys [body remote-addr] :as request}]
  (let [body-str     (when body (decode-body body :string))
        body'        (some-> body-str json/parse)
        ledger-ident (:db/id body')
        [nw db] (str/split ledger-ident #"/")
        ledger       (keyword nw db)
        auth-map     (auth-map system ledger request body-str)
        session      (session/session conn [nw db])
        db*          (<?? (session/current-db session))
        ;; TODO - root role just checks if the auth has a role with id 'root' this can
        ;; be manipulated, so we need a better way of handling this.
        _            (when-not (or (open-api? system)
                                   (<?? (auth/root-role? db* (:auth auth-map))))
                       (throw (ex-info (str "To delete a ledger, must be using an open API or an auth record with a root role.") {:status 401 :error :db/invalid-auth})))
        _            (<?? (delete/process conn nw db))
        resp         {:status  200
                      :headers {"Content-Type" "application/json; charset=utf-8"}
                      :body    (json/stringify-UTF8 {"deleted" (str nw "/" db)})}]
    (log/info (str ledger ":deleted" " [" (or resp 400) "] " remote-addr))
    resp))


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
    (serdeproto/-deserialize-leaf serializer data)))


(def storage-timeout 3000) ;; TODO - Should this be configurable?

;; TODO - check for content in the cache might be quicker? cache is already deserialized though.
;; TODO - if file store (or other store that supports it), consider streaming contents back - currently sends it all in one big chunk.
(defn storage-handler
  "Handler for key-value store requests.
  Used if query engine uses ledger as storage."
  [system {:keys [headers params] :as request}]
  (let [attempt (go-try
                  (let [conn             (:conn system)
                        storage-read-fn  (:storage-read conn)
                        accept-encodings (or (get headers "accept")
                                             "application/json")
                        signature        (return-signature request)
                        jwt              (return-token request) ;may not be signed if client is using password auth
                        open-api?        (open-api? system)
                        _                (when (and (not open-api?) (not signature) (not jwt))
                                           (throw (ex-info (str "To request an item from storage, open-api must be true or your request must be signed.")
                                                           {:status 401
                                                            :error  :db/invalid-transaction})))
                        response-type    (if (str/includes? accept-encodings "application/json")
                                           :json
                                           :avro)
                        _                (when (and (not open-api?) (= :avro response-type))
                                           (throw (ex-info (str "If using a closed api, a storage request must be returned as json.")
                                                           {:status 401
                                                            :error  :db/invalid-transaction})))
                        {:keys [network db type key]} params
                        db-name          (keyword network db)
                        _                (when-not (and network db type)
                                           (throw (ex-info (str "Incomplete request. At least a network, db and type are required. Provided network: " network " db: " db " type: " type " key: " key)
                                                           {:status 400 :error :db/invalid-request})))
                        auth-id          (cond

                                           signature
                                           (let [{:keys [auth authority]} (http-signatures/verify-request*
                                                                            {:headers headers} :get
                                                                            (str "/fdb/storage/" network "/" db
                                                                                 (when type (str "/" type))
                                                                                 (when key (str "/" key))))
                                                 db (<? (fdb/db (:conn system) db-name))]
                                             (<? (verify-auth db auth authority)))

                                           jwt
                                           (let [jwt-auth (-> system
                                                              :conn
                                                              (pw-auth/fluree-auth-map jwt)
                                                              :auth)
                                                 db'      (<? (fdb/db (:conn system) db-name))
                                                 auth-id' (<? (dbproto/-subid db' ["_auth/id" jwt-auth] true))
                                                 _        (when (util/exception? auth-id')
                                                            (throw (ex-info (str "Auth id for request does not exist in the database: " jwt-auth)
                                                                            {:status 403 :error :db/invalid-auth})))]
                                             jwt-auth))
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
                                           (let [serializer (storage-core/serde conn)
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
                    {:status  status
                     :headers headers
                     :body    body}))]
    (let [[resp ch] (alts?? [attempt (async/timeout storage-timeout)])]
      (if (= ch attempt)
        resp
        {:status  504
         :headers {"Content-Type" "text/plain"}
         :body    "Gateway Timeout"}))))


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
                                            (str/ends-with? uri "/"))
                                     (subs uri 0 (dec (count uri)))
                                     uri))))))


(defn wrap-response-headers [handler & headers]
  (fn [request]
    (let [header-map (reduce (fn [m [k v]] (assoc m (name k) v))
                             {} (partition 2 headers))
          response   (handler request)]
      (update response :headers merge header-map))))


(defn command-handler [system {:keys [headers body] :as _request}]
  (let [start        (System/nanoTime)
        cmd          (decode-body body :json)
        ledger       (some-> cmd :ledger keyword)
        timeout      (or (:request-timeout headers) 60000)
        opts         {:timeout timeout}
        _            (log/debug "Sending command:" (pr-str cmd))
        [headers body] (<?? (action-handler :command system cmd nil ledger
                                            opts))
        request-time (- (System/nanoTime) start)]
    (log/debug "Command result:" (pr-str body))
    (log/debug "Command response headers:" (pr-str headers))
    {:status  (or (:status headers) 200)
     :headers (-> headers
                  (assoc :time (format "%.2fms" (float (/ request-time 1000000)))
                         :fuel (or (:fuel headers) (:fuel body) 0))
                  command-response-headers->http-headers)
     :body    (json/stringify-UTF8 body)}))


(defn- api-routes
  [system]
  (compojure/routes
    (compojure/GET "/fdb/storage/:network/:db/:type" request (storage-handler system request))
    (compojure/GET "/fdb/storage/:network/:db/:type/:key" request (storage-handler system request))
    (compojure/GET "/fdb/ws" request (websocket/handler system request))
    (compojure/ANY "/fdb/health" request (server-health/health-handler system request))
    (compojure/ANY "/fdb/nw-state" request (server-health/nw-state-handler system request))
    (compojure/GET "/fdb/version" request (version-handler system request))
    (compojure/POST "/fdb/add-server" request (add-server system request))
    (compojure/POST "/fdb/remove-server" request (remove-server system request))
    (compojure/GET "/fdb/new-keys" request (keys-handler system request))
    (compojure/POST "/fdb/new-keys" request (keys-handler system request))
    (compojure/POST "/fdb/dbs" request (get-ledgers system request))
    (compojure/GET "/fdb/dbs" request (get-ledgers system request))
    (compojure/POST "/fdb/new-db" request (new-ledger system request))
    (compojure/POST "/fdb/delete-db" request (delete-ledger system request))
    (compojure/POST "/fdb/:network/:db/pw/:action" request (password-handler system request))
    (compojure/POST "/fdb/:network/:db/:action" request (wrap-action-handler system request))
    (compojure/GET "/fdb/:network/:db/:action" request (wrap-action-handler system request))
    (compojure/POST "/fdb/command" request (command-handler system request))

    ;; fallback 404 for unknown API paths
    (compojure/ANY "/fdb/*" [] not-found)))

;; Teach ring how to handle resources under GraalVM
;; From https://github.com/ring-clojure/ring/issues/370
(defmethod resp/resource-data :resource
  [^URL url]
  (let [conn (.openConnection url)]
    {:content        (.getInputStream conn)
     :content-length (let [len (.getContentLength conn)] (when-not (pos? len) len))}))

(defroutes admin-ui-routes
           (compojure/GET "/" [] (resp/resource-response "index.html" {:root "adminUI"}))
           (route/resources "/" {:root "adminUI"})
           (compojure/GET "/:page" [] (resp/resource-response "index.html" {:root "adminUI"})))


;; TODO - handle CORS more reasonably for production
(defn- make-handler
  [system]
  (-> (compojure/routes
        (api-routes system)

        admin-ui-routes

        ;; final 404 fallback
        (constantly not-found))

      (->> (wrap-errors (:debug-mode? system)))
      (wrap-response-headers "X-Fdb-Version" (meta/version))
      params/wrap-params
      (cors/wrap-cors
        :access-control-allow-origin [#".+"]
        :access-control-expose-headers ["X-Fdb-Block" "X-Fdb-Fuel" "X-Fdb-Status" "X-Fdb-Time" "X-Fdb-Version"]
        :access-control-allow-methods [:get :put :post :delete])
      ignore-trailing-slash))


(defonce web-server (atom {}))

(defrecord WebServer [close])

(defn webserver-factory
  [opts]
  (if-not (:enabled opts)
    (do (log/info "Web server disabled, not starting.")
        nil)
    (let [{:keys [port open-api system debug-mode? json-bigdec-string]} opts
          _        (log/info (str "Starting web server on port: " port (if open-api " with an open API." "with a closed API.")))
          _        (log/info "")
          _        (log/info (str "http://localhost:" port))
          _        (log/info "")
          clients  (atom {})
          system*  (assoc system :clients clients
                                 :debug-mode? debug-mode?
                                 :open-api open-api)
          _        (try
                     (json/encode-BigDecimal-as-string json-bigdec-string)
                     (swap! web-server assoc port (http/run-server
                                                    (make-handler system*)
                                                    {:port port}))
                     (catch BindException _
                       (log/error (str "Cannot start. Port binding failed, address already in use. Port: " port "."))
                       (log/error "FlureeDB Exiting. Adjust your config, or shut down other service using port.")
                       (System/exit 1))
                     (catch Exception e
                       (log/error "Unable to start http API: " (.getMessage e))
                       (log/error "FlureeDB Exiting.")
                       (System/exit 1)))
          close-fn (fn []
                     ;; shut down the web server but give existing connections 1s to finish
                     (let [server-close-fn (get @web-server port)]
                       (server-close-fn :timeout 1000))
                     (swap! web-server dissoc port))]
      (map->WebServer {:close close-fn}))))

