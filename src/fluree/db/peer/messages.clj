(ns fluree.db.peer.messages
  (:require [alphabase.core :as ab-core]
            [fluree.db.util.log :as log]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [fluree.db.auth :as auth]
            [fluree.db.session :as session]
            [fluree.db.api :as fdb]
            [fluree.db.util.core :as util]
            [fluree.db.event-bus :as event-bus]
            [fluree.db.ledger.delete :as ledger-delete]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.peer.password-auth :as pw-auth]
            [fluree.db.token-auth :as token-auth]
            [fluree.db.ledger.consensus.raft :as raft]
            [fluree.db.dbproto :as dbproto]))

(set! *warn-on-reflection* true)

(defn- throw-invalid-command
  [message]
  (throw (ex-info message {:status 400 :error :db/invalid-command})))

(defn- process-command
  "Does sanity checks for a new command and if valid, propagates it.
  Returns command-id/txid upon successful persistence to network, else
  throws."
  [{:keys [conn group] :as _system}  {:keys [cmd sig signed] :as signed-cmd} timestamp]
  (when-not (and (string? cmd) (string? sig))
    (throw-invalid-command (str "Command map requires keys of 'cmd' and 'sig', with a json string command map and signature of the command map respectively. Provided: "
                                (pr-str signed-cmd))))
  (when (> (count cmd) 10000000)
    (throw-invalid-command (format "Command is %s bytes and exceeds the configured max size." (count cmd))))
  (let [id       (crypto/sha3-256 cmd)
        cmd-data (try (json/parse cmd)
                      (catch Exception _
                        (throw-invalid-command "Invalid command serialization, could not decode JSON.")))
        cmd-type (keyword (:type cmd-data))
        _        (when-not cmd-type (throw-invalid-command "No 'type' key in command, cannot process."))
        ;; verify signature before passing along
        auth-id  (try
                   (crypto/account-id-from-message (or signed cmd) sig)
                   (catch Exception _ (throw-invalid-command "Invalid signature on command.")))]
    (log/debug "Processing signed command:" (pr-str signed-cmd))
    (case cmd-type
      :tx (let [{:keys [ledger tx deps expire nonce]} cmd-data
                _ (log/debug "tx command:" cmd-data)
                _ (when-not ledger (throw-invalid-command "No ledger specified for transaction."))
                [network ledger-id] (session/resolve-ledger conn ledger)]
            (when-not tx
              (throw-invalid-command "No tx specified for transaction."))
            (when (and deps (or (not (sequential? deps)) (not (every? string? deps))))
              (throw-invalid-command (format "Transaction 'deps', when provided, must be a sequence of txid(s). Provided: %s" deps)))
            (when (and expire (or (not (pos-int? expire)) (< expire timestamp)))
              (throw-invalid-command (format "Transaction 'expire', when provided, must be epoch millis and be later than now. expire: %s current time: %s" expire timestamp)))
            (when-not (txproto/ledger-exists? group network ledger-id)
              (throw-invalid-command (str "Ledger does not exist: " ledger)))
            (when (and nonce (not (int? nonce)))
              (throw-invalid-command (format "Nonce, if provided, must be an integer. Provided: %s" nonce)))
            (let [queued? (async/<!! (txproto/queue-command-async group network ledger-id id signed-cmd))]
              (when-not queued?
                (throw (ex-info "Command pool full" {:status 503, :error :db/pool-error})))
              id))

      :signed-qry
      (let [{:keys [ledger qry expire nonce meta]} cmd-data
            _      (when-not ledger (throw-invalid-command "No ledger specified for signed query."))
            _      (when-not qry (throw-invalid-command "No qry specified for signed query."))
            _      (when (and expire (or (not (pos-int? expire)) (< expire timestamp)))
                     (throw-invalid-command (format "Signed query 'expire', when provided, must be epoch millis and be later than now. expire: %s current time: %s" expire timestamp)))
            _      (when (and nonce (not (int? nonce)))
                     (throw-invalid-command (format "Nonce, if provided, must be an integer. Provided: %s" nonce)))
            [network ledger-id] (session/resolve-ledger conn ledger)
            _      (when-not (txproto/ledger-exists? group network ledger-id)
                     (throw-invalid-command (str "The ledger does not exist: " ledger)))
            action (keyword (:action cmd-data))
            meta   (if (nil? meta) false meta)
            db     (if (= action :block)
                     nil
                     (fdb/db conn ledger {:auth (when auth-id ["_auth/id" auth-id])}))]

                                        ; 1) execute the query or 2) queue the execution of the signed query?
        (case action
          :query
          (let [_ (log/debug ":signed-qry w/ :query db:" db "\nquery:" qry "\nmeta:" meta)
                result (async/<!! (fdb/query-async db (assoc-in qry [:opts :meta] meta)))
                _      (when (instance? clojure.lang.ExceptionInfo result)
                         (throw result))]
            result)

          :multi-query
          (let [result (async/<!! (fdb/multi-query-async db (assoc-in qry [:opts :meta] meta)))
                _      (when (instance? clojure.lang.ExceptionInfo result)
                         (throw result))]
            result)

          :block
          (let [query  (update qry :opts merge {:meta meta, :auth auth-id})
                result (async/<!! (fdb/block-query-async conn ledger query))
                _      (when (instance? clojure.lang.ExceptionInfo result)
                         (throw result))]
            result)

          :history
          (let [query  (update qry :opts merge {:meta meta})
                result (async/<!! (fdb/history-query-async db query))
                _      (when (instance? clojure.lang.ExceptionInfo result)
                         (throw result))]
            result)

          ;; else
          (throw (ex-info (str "Invalid action:" action " for a signed query")
                          {:status 400
                           :error  :db/invalid-action}))))

      :new-ledger (let [{:keys [ledger snapshot auth expire nonce owners]} cmd-data
                        [network ledger-id] (if (sequential? ledger) ledger (str/split ledger #"/"))]
                    (when (and auth auth-id (not= auth auth-id))
                      (throw-invalid-command (str "New-ledger command was signed by auth: " auth-id
                                                  " but the command specifies auth: " auth
                                                  ". They must be the same if auth is provided.")))
                    (when-not (re-matches #"^[a-z0-9-]+$" network)
                      (throw-invalid-command (str "Invalid network name: " network)))
                    (when-not (re-matches #"^[a-z0-9-]+$" ledger-id)
                      (throw-invalid-command (str "Invalid ledger name: " ledger-id)))
                    (when (and expire (or (not (pos-int? expire)) (< expire timestamp)))
                      (throw-invalid-command (format "Transaction 'expire', when provided, must be epoch millis and be later than now. expire: %s current time: %s"
                                                     expire timestamp)))
                    (when (and nonce (not (int? nonce)))
                      (throw-invalid-command (format "Nonce, if provided, must be an integer. Provided: %s" nonce)))
                    (when ((set (txproto/all-ledger-list group)) [network ledger-id])
                      (throw-invalid-command (format "Cannot create a new ledger, it already exists or existed: %s" ledger)))
                    (when snapshot
                      (let [storage-exists? (:storage-exists conn)
                            exists?         (storage-exists? (str snapshot))]
                        (when-not exists?
                          (throw-invalid-command
                           (format "Cannot create a new ledger, snapshot file %s does not exist in storage %s"
                                   snapshot (case (:storage-type conn)
                                              :s3 (-> conn :meta :s3-storage)
                                              :file (-> conn :meta :file-storage-path)
                                              (:storage-type conn)))))))

                    ;; TODO - do more validation, reconcile with "unsigned-cmd" validation before this

                    (async/<!! (txproto/new-ledger-async group network ledger-id id signed-cmd owners))

                    id)
      :delete-ledger (let [{:keys [ledger]} cmd-data
                           [network ledger-id] (if (sequential? ledger) ledger (str/split ledger #"/"))
                           old-session (session/session conn ledger)
                           db          (async/<!! (session/current-db old-session))
                           _           (when-not (or (:open-api group)
                                                     (async/<!! (auth/root-role? db ["_auth/id" auth-id])))
                                         (throw (ex-info (str "To delete a ledger, must be using an open API or an auth record with a root role.")
                                                         {:status 401 :error :db/invalid-auth})))]
                       (async/<!! (ledger-delete/process conn network ledger-id))
                       (session/close old-session))
      :default-key (let [{:keys [expire nonce network ledger-id private-key]} cmd-data
                         default-auth-id (some-> (txproto/get-shared-private-key group)
                                                 (crypto/account-id-from-private))
                         network-auth-id (some->> network
                                                  (txproto/get-shared-private-key group)
                                                  (crypto/account-id-from-private))]
                     ;; signed auth-id must be either the network or txgroup default key to succeed
                     (when-not (or (= auth-id default-auth-id)
                                   (= auth-id network-auth-id))
                       (throw-invalid-command (str "Command signed with unknown auth id: " auth-id)))
                     (when (not (string? private-key))
                       (throw-invalid-command "A string private-key must be provided."))
                     (when (and expire (or (not (pos-int? expire)) (< expire timestamp)))
                       (throw-invalid-command (format "Transaction 'expire', when provided, must be epoch millis and be later than now. expire: %s current time: %s" expire timestamp)))
                     (when (and nonce (not (int? nonce)))
                       (throw-invalid-command (format "Nonce, if provided, must be an integer. Provided: %s" nonce)))
                     (when (and ledger-id (not (string? ledger-id)))
                       (throw-invalid-command (str "Ledger-id must be a string if provided. Provided: " (pr-str ledger-id))))
                     (when (and network (not (string? network)))
                       (throw-invalid-command (str "Network must be a string if provided. Provided: " (pr-str network))))
                     (cond
                       (and network ledger-id)
                       (txproto/set-shared-private-key group network ledger-id private-key)

                       network
                       (txproto/set-shared-private-key group network private-key)

                       :else
                       (txproto/set-shared-private-key group private-key))))))


(def subscription-auth (atom {}))

(defn ledger-info
  "Returns basic ledger information for incoming requests."
  [system network ledger-id]
  (if (and network ledger-id)
    (do
      (log/debug "Get ledger-info request for" (str network "/" ledger-id))
      (-> (txproto/ledger-info (:group system) network ledger-id)
          (select-keys [:indexes :block :index :status])))
    {}))

(defn ledger-stats
  "Returns more detailed statistics about ledger than base ledger-info"
  [system ledger success! error!]
  (async/go
    (log/debug "Got ledger-stats req for" ledger)
    (let [[network ledger-id] (session/resolve-ledger (:conn system) ledger)]
      (log/debug "ledger-stats resolved ledger" ledger)
      (if-not (and network ledger-id)
        (error! (ex-info (str "Invalid ledger: " ledger)
                         {:status 400 :error :db/invalid-ledger}))
        (let [ledger-info (ledger-info system network ledger-id)
              _           (log/debug "Ledger info for" ledger "-" ledger-info)
              db-stat     (when (and (seq ledger-info) ; skip stats if db is still initializing
                                     (not= :initialize (:status ledger-info)))
                            (let [session-db (async/<! (session/db (:conn system) ledger {}))]
                              (if (util/exception? session-db)
                                session-db
                                (get session-db :stats))))]
          (if (util/exception? db-stat)
            (error! db-stat)
            (success! (merge ledger-info db-stat))))))))

(defn message-handler
  "Response messages are [operation subject data opts]"
  ([system producer-chan msg]
   (message-handler system producer-chan (str (random-uuid)) msg))
  ([system producer-chan ws-id msg]
   (let [[operation req-id arg] msg
         now      (System/currentTimeMillis)
         success! (fn [resp] (async/put! producer-chan [:response req-id resp nil]))
         error!   (fn [e]
                    (let [exdata     (ex-data e)
                          status     (or (:status exdata) 500)
                          error      (or (:error exdata) :db/unexpected-error)
                          error-resp {:message (ex-message e)
                                      :status  status
                                      :error   error}]
                      ;; log any unexpected errors locally
                      (when (>= status 500)
                        (log/error e "Unexpected error processing message:" msg))
                      (async/put! producer-chan [:response req-id nil error-resp])))]
     (log/debug "Incoming message:" msg)
     (try
       (case (keyword operation)
         :close (do ;; close will trigger the on-closed callback and clean up all session info.
                  ;; send a confirmation message first
                  (success! true)
                  ;(s/put! ws (json/write [(assoc header :status 200) true]))
                  (async/close! producer-chan))
         :ping (async/put! producer-chan [:pong req-id true nil])

         :settings (let [open-api? (-> system :group :open-api)
                         has-auth? (-> (get-in @subscription-auth [ws-id])
                                       (as-> wsm (reduce-kv (fn [res _ val] (if (map? val) (into res (vals val)) res)) [] wsm))
                                       seq)]
                     (cond-> {:open-api?         open-api?
                              :password-enabled? (-> system :conn pw-auth/password-enabled?)}
                             (or open-api? has-auth?) (assoc :jwt-secret (-> system :conn :meta :password-auth :secret
                                                                             (ab-core/byte-array-to-base :hex)))
                             true success!))

         :cmd (success! (process-command system arg now))

         :subscribe (let [pw-enabled?     (pw-auth/password-enabled? (:conn system))
                          open-api?       (-> system :group :open-api)
                          transactor?     (-> system :conn :transactor?)
                          _               (when (and (sequential? arg) (not (or pw-enabled? (not open-api?))))
                                            (throw (ex-info (str "Supplying an auth/jwt is not allowed.")
                                                            {:status 400 :error :db/invalid-request})))
                          [ledger auth-or-jwt] (cond
                                                 (sequential? (first arg)) ;; [ [network, ledger-id], auth ]
                                                 arg

                                                 :else ;; network/ledger-id or [network, ledger-id]
                                                 [arg])

                          auth            (cond
                                            (and (nil? auth-or-jwt)
                                                 (or open-api? transactor?)) ;; open, give root access
                                            0

                                            (and (int? auth-or-jwt) open-api?) ;; open, allow them to select any auth
                                            auth-or-jwt

                                            (and (string? auth-or-jwt) pw-enabled?) ;; jwt, figure out auth
                                            (-> (pw-auth/fluree-auth-map (:conn system) auth-or-jwt)
                                                :id)

                                            (or (string? auth-or-jwt) (sequential? auth-or-jwt)) ;; ["_auth/id", "TfG81..."]  or "TfG81..."
                                            (let [auth-id (if (string? auth-or-jwt)
                                                            ["_auth/id" auth-or-jwt]
                                                            auth-or-jwt)
                                                  root-db (async/<!! (fdb/db (:conn system) ledger))]
                                              (async/<!! (dbproto/-subid root-db auth-id))))
                          _               (when-not (or auth open-api?)
                                            (throw (ex-info "To access the server, either open-api must be true or a valid auth must be provided."
                                                            {:status 401
                                                             :error  :db/invalid-request})))

                          resolved-ledger (session/resolve-ledger (:conn system) ledger)
                          [network ledger-id] resolved-ledger
                          _               (when-not (txproto/ledger-exists? (:group system) network ledger-id)
                                            (throw (ex-info (str "Ledger " ledger " does not exist on this server.")
                                                            {:status 400 :error :db/invalid-ledger})))
                          _               (swap! subscription-auth assoc-in [ws-id network ledger-id] auth)]
                      (event-bus/subscribe-db resolved-ledger producer-chan)
                      (success! true))

         :unsubscribe (let [ledger          (if (sequential? (first arg))
                                              ;; Expect [ [network, ledger-id], auth ] or [network, ledger-id] or network/ledger-id
                                              (first arg)
                                              arg)
                            resolved-ledger (session/resolve-ledger (:conn system) ledger)
                            [network ledger-id] resolved-ledger
                            _               (when-not (txproto/ledger-exists? (:group system) network ledger-id)
                                              (throw (ex-info (str "Ledger " resolved-ledger " does not exist.")
                                                              {:status 400 :error :db/invalid-ledger})))
                            _               (swap! subscription-auth update-in [ws-id network] ledger-id)]
                        (event-bus/unsubscribe-db resolved-ledger producer-chan)
                        (success! true))

         :nw-subscribe (if (-> system :group :open-api)
                         (raft/monitor-raft (-> system :group) (fn [x] (let [{:keys [time event]} x
                                                                             [op data] event
                                                                             elapsed-t (some-> (:request data) :instant (#(- now %)))]
                                                                         (when
                                                                           (or (not (#{:append-entries
                                                                                       :append-entries-response} op))
                                                                               (> (count (-> x :event second :entries)) 0))
                                                                           (async/put! producer-chan
                                                                                       [:nw-log req-id {:op      op
                                                                                                        :time    time
                                                                                                        :data    (str data)
                                                                                                        :elapsed elapsed-t}])))))

                         (throw-invalid-command (str "Can only subscribe to network if using an open API.")))

         :nw-unsubscribe (raft/monitor-raft-stop (-> system :group))

         :unsigned-cmd (let [{:keys [type ledger jwt] :as cmd-data}
                             arg

                             cmd-type    (keyword type)
                             _           (when-not (#{:tx :new-ledger :default-key :delete-ledger} cmd-type)
                                           (throw-invalid-command
                                             (str "Invalid command type (:type) provided in unsigned command: "
                                                  type)))
                             [network ledger-id] (cond
                                                   (= :new-ledger cmd-type)
                                                   (cond
                                                     (string? ledger) (str/split ledger #"/")
                                                     (sequential? ledger) ledger
                                                     :else (throw (ex-info (str "Invalid ledger provided for new-ledger: " (pr-str ledger))
                                                                           {:status 400 :error :db/invalid-command})))

                                                   (= :delete-ledger cmd-type)
                                                   (session/resolve-ledger (:conn system) ledger)

                                                   (= :tx cmd-type)
                                                   (session/resolve-ledger (:conn system) ledger)

                                                   (= :default-key cmd-type)
                                                   (let [{:keys [network ledger-id]} cmd-data]
                                                     [network ledger-id]))
                             private-key (if jwt
                                           (let [secret (get-in system [:conn :meta :password-auth :secret])
                                                 jwt    (token-auth/verify-jwt secret jwt)]
                                             (async/<!! (pw-auth/fluree-decode-jwt (:conn system) jwt)))
                                           (if-let [pk (txproto/get-shared-private-key (:group system)
                                                                                    network ledger-id)]
                                             (do (log/debug "Signing unsigned cmd with default private key")
                                                 pk)
                                             (do (log/error "No private key found to sign unsigned cmd")
                                                 nil)))
                             {:keys [expire nonce] :or {nonce now}} cmd-data
                             expire      (or expire (+ 60000 nonce))
                             cmd-data*   (assoc cmd-data :expire expire :nonce nonce)]
                         (when (< expire now)
                           (throw-invalid-command (format "Command expired. Expiration: %s. Current time: %s."
                                                          expire now)))
                         (when (and (= :new-ledger cmd-type)
                                    (txproto/ledger-exists? (:group system) network ledger-id))
                           (throw-invalid-command (str "The ledger already exists or existed: " ledger)))
                         (when (and (= :tx cmd-type)
                                    (not (txproto/ledger-exists? (:group system) network ledger-id)))
                           (throw-invalid-command (str "Ledger does not exist: " ledger)))
                         (when-not private-key
                           (throw-invalid-command (str "The ledger group is not configured with a default private "
                                                       "key for use with ledger: " ledger ". Unable to process an unsigned "
                                                       "transaction.")))
                         (let [cmd        (-> cmd-data*
                                              (util/without-nils)
                                              (json/stringify))
                               sig        (crypto/sign-message cmd private-key)
                               id         (crypto/sha3-256 cmd)
                               signed-cmd {:cmd    cmd
                                           :sig    sig
                                           :id     id
                                           :ledger ledger}]
                           (success! (process-command system signed-cmd now))))

         :ledger-info (let [[network ledger-id] (session/resolve-ledger (:conn system) arg)]
                        (success! (ledger-info system network ledger-id)))

         :ledger-stats (future ; as thread/future - otherwise if this needs to load new db will have new requests and will permanently block
                         (ledger-stats system arg success! error!))

         :ledger-list (let [response (txproto/ledger-list (:group system))]
                        (success! response))

         ;; TODO - unsigned-cmd should cover a 'tx', remove below
         :tx (let [tx-map      arg
                   _           (log/debug "tx-map:" tx-map)
                   {:keys [ledger tx]} tx-map
                   [network ledger-id] (session/resolve-ledger (:conn system) ledger)
                   _           (when-not (txproto/ledger-exists? (:group system) network ledger-id)
                                 (throw-invalid-command (str "Ledger does not exist: " ledger)))
                   private-key (txproto/get-shared-private-key (:group system) network ledger-id)
                   _           (when-not private-key
                                 (throw-invalid-command (str "The ledger group is not configured with a default private "
                                                             "key for use with ledger: " ledger ". Unable to process an unsigned "
                                                             "transaction.")))
                   cmd         (fdb/tx->command ledger tx private-key tx-map)]
               (success! (process-command system cmd now)))

         :pw-login (let [{:keys [ledger password user auth]} arg]
                     (when-not (pw-auth/password-enabled? (:conn system))
                       (throw (ex-info "Password authentication is not enabled."
                                       {:status 401 :error :db/no-password-auth})))
                     (when-not ledger
                       (throw (ex-info "A ledger must be supplied."
                                       {:status 400 :error :db/invalid-request})))
                     (when-not password
                       (throw (ex-info "A password must be supplied."
                                       {:status 400 :error :db/invalid-request})))
                     (when-not (or user auth)
                       (throw (ex-info "A user identity or auth identity must be supplied."
                                       {:status 400 :error :db/invalid-request})))
                     (async/go
                       (let [jwt (async/<! (pw-auth/fluree-login-user (:conn system) ledger password user auth arg))]
                         (if (util/exception? jwt)
                           (error! jwt)
                           (success! jwt)))))

         :pw-renew (let [{:keys [jwt expire]} arg
                         _           (when-not (pw-auth/password-enabled? (:conn system))
                                       (throw (ex-info "Password authentication is not enabled."
                                                       {:status 401 :error :db/no-password-auth})))
                         _           (when-not jwt
                                       (throw (ex-info "A token must be supplied."
                                                       {:status 400 :error :db/invalid-request})))
                         jwt-options (-> system :conn :meta :password-auth)
                         {:keys [secret]} jwt-options
                         jwt'        (token-auth/verify-jwt secret jwt)]
                     (when-not jwt'
                       (throw (ex-info "A valid JWT token must be supplied for a token renewal."
                                       {:status 401 :error :db/invalid-auth})))
                     (-> (pw-auth/fluree-renew-jwt jwt-options jwt' (util/without-nils {:expire expire}))
                         (success!)))

         :pw-generate (let [{:keys [ledger password roles user
                                    create-user? createUser
                                    private-key privateKey
                                    expire]} arg
                            _       (when-not (pw-auth/password-enabled? (:conn system))
                                      (throw (ex-info "Password authentication is not enabled."
                                                      {:status 401 :error :db/no-password-auth})))
                            _       (when-not ledger
                                      (throw (ex-info "A ledger must be supplied."
                                                      {:status 400 :error :db/invalid-request})))
                            _       (when-not password
                                      (throw (ex-info "A password must be supplied."
                                                      {:status 400 :error :db/invalid-request})))
                            conn    (:conn system)
                            options (util/without-nils
                                      {:roles        roles
                                       :user         user
                                       :private-key  (or private-key privateKey)
                                       :create-user? (or create-user? createUser)
                                       :expire       expire})]
                        (async/go
                          (let [resp (async/<! (pw-auth/fluree-new-pw-auth conn ledger password options))
                                {:keys [jwt]} resp]
                            (if (util/exception? resp)
                              (error! resp)
                              (success! jwt))))))

       (catch Exception e
         (log/error {:error   e
                     :message msg}
                   "Error caught in incoming message handler:")
         (error! e))))))
