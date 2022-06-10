(ns fluree.db.test-helpers
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [fluree.db.server :as server]
            [fluree.db.api :as fdb]
            [fluree.db.server-settings :as setting]
            [fluree.db.util.log :as log]
            [fluree.db.server-settings :as settings]
            [environ.core :as environ])
  (:import (java.net ServerSocket)))

(defn get-free-port []
  (let [socket (ServerSocket. 0)]
    (.close socket)
    (.getLocalPort socket)))

(def port (delay (get-free-port)))
(def alt-port (delay (get-free-port)))
(def config (delay (setting/build-env
                     {:fdb-mode                "dev"
                      :fdb-group-servers       "DEF@localhost:11001"
                      :fdb-group-this-server   "DEF"
                      :fdb-storage-type        "memory"
                      :fdb-api-port            @port
                      :fdb-consensus-type      "in-memory"})))

(def system nil)


(def ledger-endpoints "fluree/api")
(def ledger-query+transact "fluree/querytransact")
(def ledger-chat "fluree/chat")
(def ledger-crypto "fluree/crypto")
(def ledger-voting "fluree/voting")
(def ledger-supplychain "fluree/supplychain")
(def ledger-todo "fluree/todo")
(def ledger-invoice "fluree/invoice")

(defn print-banner [msg]
  (println "\n*************************************\n\n"
           msg
           "\n\n*************************************"))

(defn start
  [opts]
  (print-banner "STARTING")
  (alter-var-root #'system (constantly (server/startup (merge @config opts))))
  :started)


(defn stop []
  (print-banner "STOPPING")
  (alter-var-root #'system (fn [s] (when s (server/shutdown s))))
  :stopped)


(defn test-system
  ([f]
   (test-system f {}))
  ([f opts]
   (try
     (do (start opts)
         @(fdb/new-ledger (:conn system) ledger-endpoints)
         @(fdb/new-ledger (:conn system) ledger-query+transact)
         @(fdb/new-ledger (:conn system) ledger-chat)
         @(fdb/new-ledger (:conn system) ledger-crypto)
         @(fdb/new-ledger (:conn system) ledger-voting)
         @(fdb/new-ledger (:conn system) ledger-supplychain)
         @(fdb/new-ledger (:conn system) ledger-todo)
         @(fdb/new-ledger (:conn system) ledger-invoice)
         (async/<!! (async/timeout 15000))
         (f))
     :success
     (catch Exception e (log/error "Caught test exception" e)
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


(defn contains-many? [m & ks]
  (every? #(contains? m %) ks))


(defn start-server
  "Start a single server with the specified settings, returning the server."
  [settings]
  (let [server-settings (-> (settings/build-env environ/env)
                            (merge settings))]
    (server/startup server-settings)))


(defn stop-server
  "Stop the supplied server (from `fluree.db.test-helpers/start`)."
  [s]
  (when s (server/shutdown s))
  :stopped)
