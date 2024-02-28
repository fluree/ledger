(ns fluree.db.ledger.stats
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [clojure.walk :as walk]
            [clojure.string :as str]
            [cheshire.core :as json])
  (:import (java.lang.management ManagementFactory)
           (java.time Instant)))

(set! *warn-on-reflection* true)


;;; ---------------------------------
;;;
;;; System Stats Reporting
;;;
;;; ---------------------------------


(defn set-interval
  "Periodically execute function f using core.async.

  Returns a channel that can be used to cancel. Use like:
  (def myinterval (set-interval #(println \"hi\") 2000))
  (async/close! myinterval)"
  [f time-in-ms]
  (let [stop (async/chan)]
    (async/go-loop []
      (async/alt!
        (async/timeout time-in-ms)
        (do (async/<! (async/thread (f)))
            (recur))
        stop :stop))
    stop))



(defn memory-stats
  []
  (let [memory-mxbean (-> (ManagementFactory/getMemoryMXBean)
                          (.getHeapMemoryUsage))
        gb-format     #(format "%.1f GB" (/ % 1073741824.0))]
    {:used      (-> (.getUsed memory-mxbean) gb-format)
     :committed (-> (.getCommitted memory-mxbean) gb-format)
     :max       (-> (.getMax memory-mxbean) gb-format)
     :init      (-> (.getInit memory-mxbean) gb-format)
     :time      (str (Instant/now))}))


(defn jvm-arguments
  "Returns list of passed JVM arguments.
  Will not contain the '-server' argument though."
  []
  (let [jvm-name   (System/getProperty "java.vm.name")
        input-args (-> (ManagementFactory/getRuntimeMXBean) (.getInputArguments))]

    {:jvm jvm-name :input input-args}))


(defn jvm-args->map
  [input]
  (-> (reduce (fn [acc setting]
                (if (str/starts-with? setting "-D")
                  (let [[k v] (-> setting
                                  (str/replace-first "-D" "")
                                  (str/split #"="))]
                    (assoc acc k v))
                  acc)) {} input) walk/keywordize-keys))

(defn compact-group-state
  "Report out a smaller version of the raft group state to save logging"
  [{:keys [networks] :as group-state}]
  (let [networks* (reduce-kv
                    (fn [acc network network-map]
                      (let [dbs*         (reduce-kv (fn [acc dbid db-data]
                                                      (assoc acc dbid (dissoc db-data :indexes)))
                                                    {} (:dbs network-map))
                            network-map* (assoc network-map :dbs dbs*)]
                        (assoc acc network network-map*)))
                    {}
                    networks)]
    (-> group-state
        (select-keys [:version :leases :_work])
        (assoc :networks networks*))))


(defn report-stats
  [system]
  (log/info "Memory: " (-> (memory-stats) (json/encode)))
  (let [group-state  (txproto/-local-state (:group system))]
    (log/info "Group state: " (json/encode (compact-group-state group-state)))
    (log/trace "Full group state: " (json/encode group-state))))



(defn initiate-stats-reporting
  "Returns closable core async chan that will stop loop."
  [system interval]
  (set-interval
    (fn [] (report-stats system))
    (or interval 1000000)))
