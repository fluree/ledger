(ns fluree.db.test-helpers
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [<!!]]
            [fluree.db.server :as server]
            [fluree.db.api :as fdb]
            [fluree.db.server-settings :as setting]
            [fluree.db.util.log :as log]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [fluree.db.util.json :as json]
            [org.httpkit.client :as http]
            [fluree.db.api.auth :as fdb-auth])
  (:import (java.net ServerSocket)
           (java.util UUID)))

(def ^:constant init-timeout-ms 120000)

(defn get-free-port []
  (let [socket (ServerSocket. 0)]
    (.close socket)
    (.getLocalPort socket)))

(def port (delay (get-free-port)))
(def alt-port (delay (get-free-port)))
(def config (delay (setting/build-env
                     {:fdb-mode              "dev"
                      :fdb-group-servers     "DEF@localhost:11001"
                      :fdb-group-this-server "DEF"
                      :fdb-storage-type      "memory"
                      :fdb-api-port          @port
                      :fdb-consensus-type    "in-memory"})))

(def system nil)


(def ledger-endpoints "fluree/api")
(def ledger-query+transact "fluree/querytransact")
(def ledger-chat "fluree/chat")
(def ledger-crypto "fluree/crypto")
(def ledger-voting "fluree/voting")
(def ledger-supplychain "fluree/supplychain")
(def ledger-todo "fluree/todo")
(def ledger-invoice "fluree/invoice")

(def all-ledgers
  #{ledger-endpoints ledger-query+transact ledger-chat ledger-crypto
    ledger-voting ledger-supplychain ledger-todo ledger-invoice})


(defn print-banner [msg]
  (println "\n*************************************\n\n"
           msg
           "\n\n*************************************"))


(defn start*
  [& [opts]]
  (alter-var-root #'system (constantly (server/startup (merge @config opts)))))


(defn start
  [& [opts]]
  (print-banner "STARTING")
  (start* opts)
  :started)


(defn stop* []
  (alter-var-root #'system (fn [s] (when s (server/shutdown s)))))


(defn stop []
  (print-banner "STOPPING")
  (stop*)
  :stopped)


(defn check-if-ready
  "Kicks off simultaneous asynchronous ledger-ready? checks for every ledger in
  ledgers. Returns a core.async channel that will be filled with vectors like
  [ledger ready?] as they become available and then closed when all have been
  returned."
  [conn ledgers]
  (let [res-ch (async/chan (count ledgers))
        puts   (atom 0)]
    (dorun
      (for [ledger ledgers]
        (async/take! (fdb/ledger-ready?-async conn ledger)
                     #(async/put! res-ch [ledger %]
                                  (fn [_]
                                    (swap! puts inc)
                                    (when (= @puts (count ledgers))
                                      (async/close! res-ch)))))))
    res-ch))


(defn wait-for-init
  ([conn ledgers] (wait-for-init conn ledgers init-timeout-ms))
  ([conn ledgers timeout]
   (loop [elapsed 0
          ledgers ledgers]
     (let [start         (System/currentTimeMillis)
           ready-checks  (check-if-ready conn ledgers)
           ready-ledgers (<!! (async/reduce
                                (fn [rls [ledger ready?]]
                                  (assoc rls ledger ready?))
                                {} ready-checks))]
       (when (not-every? second ready-ledgers)
         (Thread/sleep 1000)
         (let [split   (- (System/currentTimeMillis) start)
               elapsed (+ elapsed split)]
           (if (>= elapsed timeout)
             (throw (RuntimeException.
                      (str "Waited " elapsed
                           "ms for test ledgers to initialize. Max is "
                           timeout "ms.")))
             (recur elapsed (map first (remove second ready-ledgers))))))))))


(defn init-ledgers!
  ([] (init-ledgers! system all-ledgers))
  ([ledgers] (init-ledgers! system ledgers))
  ([{:keys [conn] :as _system} ledgers]
   (dorun
     (for [ledger ledgers]
       (fdb/new-ledger-async conn ledger)))
   (wait-for-init conn ledgers)))


(defn rand-ledger
  "Generate a random, new, empty ledger with base-name prefix. Waits for it to
  be ready and then returns its name as a string."
  [base-name]
  (let [name (str base-name "-" (UUID/randomUUID))]
    (init-ledgers! [name])
    name))


(defn test-system
  "This fixture is intended to be used like this:
  (use-fixture :once test-system)
  It starts up an in-memory ledger server for testing. It does not create any
  ledgers. You might find the rand-ledger fn useful for that."
  ([tests] (test-system {} tests))
  ([opts tests]
   (try
     (start* opts)
     (tests)
     (catch Throwable e
       (log/error e "Caught test exception")
       e)
     (finally (stop*)))))


(defn safe-update
  "Like update but takes a predicate fn p that is first run on the current
  value for key k in map m. Iff p returns truthy does the update take place."
  [m k p f]
  (let [v (get m k)]
    (if (p v)
      (update m k f)
      m)))


(defn standard-request
  ([body]
   (standard-request body {}))
  ([body opts]
   {:headers (cond-> {"content-type" "application/json"}
                     (:token opts) (assoc "Authorization" (str "Bearer " (:token opts))))
    :body    (json/stringify body)}))


(def endpoint-url-short (str "http://localhost:" @port "/fdb/"))


(defn transact-resource
  "Transacts the type (keyword form of test-resources subdirectory) of resource
  with filename file. Optional api arg can be either :http (default) or :clj to
  indicate which API to use for the transaction. :clj can be useful under
  closed-api mode since this doesn't sign the HTTP requests."
  ([type ledger file] (transact-resource type ledger file :http))
  ([type ledger file api]
   (let [tx (->> file (str (name type) "/") io/resource slurp edn/read-string)]
     (case api
       :clj
       @(fdb/transact (:conn system) ledger tx)

       :http
       (let [endpoint (str endpoint-url-short ledger "/transact")]
         (-> tx
             standard-request
             (->> (http/post endpoint))
             deref
             (safe-update :body string? json/parse)))))))

(def ^{:arglists '([ledger file] [ledger file api])
       :doc      "Like transact-resource but bakes in :schemas as the first arg."}
  transact-schema
  (partial transact-resource :schemas))

(def ^{:arglists '([ledger file] [ledger file api])
       :doc      "Like transact-resource but bakes in :data as the first arg."}
  transact-data
  (partial transact-resource :data))


(defn create-auths
  "Creates 3 auths in the given ledger: root, all persons, all persons no
  handles. Returns of vector of [key-maps create-txn-result]."
  ([ledger] (create-auths ledger (:conn system)))
  ([ledger conn]
   (let [keys     (vec (repeatedly 3 fdb-auth/new-private-key))
         add-auth [{:_id   "_auth"
                    :id    (get-in keys [0 :id])
                    :roles [["_role/id" "root"]]}
                   {:_id   "_auth"
                    :id    (get-in keys [1 :id])
                    :roles ["_role$allPersons"]}
                   {:_id   "_auth"
                    :id    (get-in keys [2 :id])
                    :roles ["_role$noHandles"]}
                   {:_id   "_role$allPersons"
                    :id    "allPersons"
                    :rules ["_rule$allPersons"]}
                   {:_id   "_role$noHandles"
                    :id    "noHandles"
                    :rules ["_rule$allPersons" "_rule$noHandles"]}
                   {:_id               "_rule$allPersons"
                    :id                "role$allPersons"
                    :collection        "person"
                    :collectionDefault true
                    :fns               [["_fn/name" "true"]]
                    :ops               ["all"]}
                   {:_id        "_rule$noHandles"
                    :id         "noHandles"
                    :collection "person"
                    :predicates ["person/handle"]
                    :fns        [["_fn/name" "false"]]
                    :ops        ["all"]}]]
     [keys (->> add-auth
                (fdb/transact-async conn ledger)
                <!!)])))


;; ======================== DEPRECATED ===============================

(defn ^:deprecated test-system-deprecated
  "This fixture is deprecated. As tests are converted to the more idiomatic
  approach, use the new test-system :once fixture w/ rand-ledger calls in each
  test instead."
  ([f]
   (test-system-deprecated {} f))
  ([opts f]
   (try
     (do (start opts)
         (init-ledgers!)
         (f))
     :success
     (catch Exception e
       (log/error e "Caught test exception")
       e)
     (finally (stop)))))


(defn safe-Throwable->map [v]
  (if (isa? (class v) Throwable)
    (Throwable->map v)
    (do
      (println "Not a throwable:" (pr-str v))
      v)))


(defn extract-errors [v]
  (if (isa? (class v) Throwable)
    (or (some-> (ex-data v) (assoc :message (ex-message v)))
        (Throwable->map v))
    (do
      (println "Not a throwable:" (pr-str v))
      v)))


(defn get-tempid-count
  "Returns count of tempids within a collection, given the tempid map from the returned transaction
  and a collection name."
  [tempids collection]
  (let [collection-tempids (get tempids collection)]
    (when-not (sequential? collection-tempids)
      (throw (ex-info (str "Unable to get collection range from tempid map for: " collection)
                      {:tempids    tempids
                       :collection collection})))
    (let [[start-sid end-sid] collection-tempids]
      (inc (- end-sid start-sid)))))


(defn contains-every?
  "Returns true if and only if map m contains every key supplied in subsequent
  args ks. Uses (contains? m k) so the same semantics apply (e.g. checks for
  map keys not values).

  NB: This is NOT the same as set equality. It checks that the set contents of
  m is a (non-strict) superset of ks. In other words, m can have more than ks,
  but must have all of ks."
  [m & ks]
  (every? #(contains? m %) ks))
