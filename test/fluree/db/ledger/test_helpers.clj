(ns fluree.db.ledger.test-helpers
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [fluree.db.server :as server]
            [fluree.db.api :as fdb]
            [fluree.db.server-settings :as setting]
            [fluree.db.util.log :as log])
  (:import (java.net ServerSocket)))

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

; https://github.com/clojure/core.async/blob/master/src/test/clojure/clojure/core/async/exceptions_test.clj
; Following helper function copied from clojure.core unit tests.
;----------------------------------------------------------------------------------------------------------
; clojure.core.async.exceptions-test
;;   Copyright (c) Rich Hickey and contributors. All rights reserved.
;;   The use and distribution terms for this software are covered by the
;;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;;   which can be found in the file epl-v10.html at the root of this distribution.
;;   By using this software in any fashion, you are agreeing to be bound by
;;   the terms of this license.
;;   You must not remove this notice, or any other, from this software.
(defn clojure-core-with-default-uncaught-exception-handler [handler f]
  (let [old-handler (Thread/getDefaultUncaughtExceptionHandler)]
    (Thread/setDefaultUncaughtExceptionHandler
      (reify Thread$UncaughtExceptionHandler
        (uncaughtException [_ thread throwable]
          (handler thread throwable))))
    (f)
    (Thread/setDefaultUncaughtExceptionHandler old-handler)))
;----------------------------------------------------------------------------------------------------------

