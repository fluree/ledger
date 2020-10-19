(ns fluree.db.ledger.load.load
  (:require [clojure.test :refer :all]
            [aleph.http :as http]
            ;[clj-gatling.core :as gatling]
            [criterium.core :as criterium]
            [fluree.db.util.json :as json]))

(defn standard-request
  ([body]
   (standard-request body {}))
  ([body opts]
   {:content-type     :json
    :throw-exceptions (if (contains? opts :throw-exceptions)
                        (:throw-exceptions opts) true)
    :headers          (-> {:content-type :application/json}
                          (#(if (:token opts)
                              (assoc % :authorization (str "Bearer " (:token opts))) %)))
    :body             (json/stringify body)}))


(defn localhost-request []
  (let [{:keys [status]} @(http/post "http://localhost:8090/fdb/plane/demo/query" (standard-request {:select ["*"] :from "_collection"}))]
    (= status 200)))

(comment

  @(http/post "http://localhost:8090/fdb/plane/demo/query"

              (standard-request {:select ["*"] :from "_collection"}))
  (localhost-request)

(criterium/quick-bench (localhost-request))

  ;Evaluation count : 318 in 6 samples of 53 calls.
  ;             Execution time mean : 2.071853 ms
  ;    Execution time std-deviation : 249.290136 µs
  ;   Execution time lower quantile : 1.892151 ms ( 2.5%)
  ;   Execution time upper quantile : 2.495999 ms (97.5%)
  ;                   Overhead used : 2.101910 ns
  ;
  ;Found 1 outliers in 6 samples (16.6667 %)
  ;	low-severe	 1 (16.6667 %)
  ; Variance from outliers : 31.3347 % Variance is moderately inflated by outliers

  ;; This is faster than our tests???


  )

;(clj-gatling/run
;  {:name "Simulation"
;   :scenarios [{:name "Localhost test scenario"
;                :steps [{:name "Root"
;                         :request localhost-request}]}]}
;  {:concurrency 100})