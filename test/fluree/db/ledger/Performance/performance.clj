(ns fluree.db.ledger.Performance.performance
  (:require [clojure.java.io :as io]
            [criterium.core :as criterium]
            [fluree.db.api :as fdb]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.util.core :as util]))

;; UTILITY FUNCTIONS - Time and Results Formatting

(defn time-return-data
  [f & args]
  (let [start-time (System/nanoTime)
        _          (apply f args)
        end-time   (System/nanoTime)]
    (float (/ (- end-time start-time) 1000000))))

(defn abs
  [n]
  (if (<= 0 n) n (* -1 n)))

(defn average
  [numbers]
  (if (empty? numbers)
    0
    (/ (reduce + numbers) (count numbers))))

(defn format-res
  [res type]
  (let [mean (-> res :mean first)
        [scale unit] (criterium/scale-time (abs mean))]
    {:sample    (-> res :sample-count)
     :mean      mean
     :mean-time (criterium/format-value mean scale unit)
     :type      type}))

(defn format-mean-time
  [mean]
  (let [[scale unit] (criterium/scale-time (abs mean))]
    (criterium/format-value mean scale unit)))


;; TEST TRANSACTIONS
;; TODO - not working with 0.11.0

(defn add-and-delete-data
  [conn dbid]
  (let [txn       [{:_id "person" :favNums [1]}]
        res       (async/<!! (fdb/transact-async conn dbid txn))
        _id       (-> res :tempids (get "person$1"))
        deleteTxn [{:_id _id :_action "delete"}]
        deleteRes (async/<!! (fdb/transact-async conn dbid deleteTxn))]
    deleteRes))



(defn add-and-update-data
  [conn dbid]
  (let [txn       [{:_id "person" :favNums [1]}]
        res       (async/<!! (fdb/transact-async conn dbid txn))
        _id       (-> res :tempids (get "person$1"))
        updateTxn [{:_id _id :favNums [2]}]
        updateRes (async/<!! (fdb/transact-async conn dbid updateTxn))]
    updateRes))


;; TEST QUERIES

(def queryTxnRanges
  {:schemaTxns       [1 3]
   :txns             [4 50]
   :basic-query      [51 100]
   :analytical-query [101 150]
   :block-query      [151 200]
   :history-query    [201 250]
   :graphql-query    [251 300]
   :multi-query      [301 350]
   :sparql-query     [351 400]})

(defn get-query-type
  "Offset is used for special query lists, i.e. PlaneQueryTxn. Rather than"
  ([queryTxns type]
    (get-query-type queryTxns type 0))
  ([queryTxns type offset]
   (select-keys queryTxns (range (+ offset (first (get queryTxnRanges type)))
                                 (+ 1 offset (second (get queryTxnRanges type)))))))

(defn test-queries
  "'query-map is a map in the format:
  { 1 [:query-type, QUERY ]
    2 [:query-type, QUERY ] }

  i.e  { 1 [:basic-query, {:select [\"*\"], \"from\": \"_collection\" } ]
         2 [:basic-query, {:select [\"*\"], \"from\": \"_predicate\" } ] }"
  [db f query-map]
  (reduce (fn [acc [q-num [type q]]]
            (try (let [res (criterium/benchmark (f db q) nil)]
                   (assoc acc q-num (format-res res type)))
                 (catch Exception e {:issued q :error true}))) {} query-map))

(defn add-schema-performance-check
  [conn dbid]
  (let [collections (-> "../test/fluree/db/ledger/Resources/ChatApp/collections.edn" io/resource slurp read-string)
        coll-txn    (time-return-data (fn [conn dbid collections]
                                        (async/<!! (fdb/transact-async conn dbid collections)))
                                      conn dbid collections)
        predicates  (-> "../test/fluree/db/ledger/Resources/ChatApp/chatPreds.edn" io/resource slurp read-string)
        pred-txn    (time-return-data (fn [conn dbid collections]
                                        (async/<!! (fdb/transact-async conn dbid collections))) conn dbid predicates)
        data        (-> "../test/fluree/db/ledger/Resources/ChatApp/chatAppData.edn" io/resource slurp read-string)
        data-txn    (time-return-data (fn [conn dbid collections]
                                        (async/<!! (fdb/transact-async conn dbid collections))) conn dbid data)
        ;; For now, these are hard-coded
        keyCollTxn  1
        keyPredTxn  2
        keyDataTxn  3]
    {keyCollTxn {:mean (str coll-txn " ms") :mean-time (/ coll-txn 1000)}
     keyPredTxn {:mean (str pred-txn " ms") :mean-time (/ pred-txn 1000)}
     keyDataTxn {:mean (str data-txn " ms") :mean-time (/ data-txn 1000)}}))


;(add-schema-performance-check conn dbid)

;; TODO - recommend turning off transact and block-range logging beforehand
;; I didn't turn off either. IDK if results affected.
(defn performance-check
  "NOTE: This performance check will take more than an hour."
  ([conn dbid]
    (performance-check conn dbid "../test/fluree/db/ledger/Performance/QueryTxnList.edn" 0 true))
  ([conn dbid queryTxnFile offset schema?]
    (let [add-schema-res         (when schema? (add-schema-performance-check conn dbid))
          _                      (log/info "Schema timing results: " add-schema-res)
          queries                (-> queryTxnFile io/resource slurp read-string)
          myDb                   (fdb/db conn dbid)
          basic-query-coll       (get-query-type queries :basic-query offset)
          query-bench            (test-queries myDb (fn [db q]
                                                      (async/<!! (fdb/query-async db q))) basic-query-coll)
          _                      (log/info "Basic query bench results: " query-bench)
          analytical-query-coll  (get-query-type queries :analytical-query offset)
          analytical-query-bench (test-queries myDb (fn [db q]
                                                      (async/<!! (fdb/query-async db q))) analytical-query-coll)
          _                      (log/info "Analytical query bench results: " analytical-query-bench)
          ;block-query-coll       (get-query-type queries :block-query offset)
          ;block-query-bench      (test-queries myDb (fn [db q]
          ;                                            (async/<!!
          ;                                              (fdb/block-query-async conn dbid q))) block-query-coll)
          ;_                      (log/info "Block query bench results: " block-query-bench)
          ;history-query-bench  (test-queries myDb (fn [db q]
          ;                                          (async/<!!
          ;                                            (fdb/history-query-async db q))) history-query-coll
          ;                                   :history-query)
          ;_ (log/info "History query bench results"  history-query-bench)
          ;sparql-query-bench  (test-queries myDb (fn [db q]
          ;                                         (async/<!!
          ;                                           (fdb/sparql-async db q))) sparql-query-coll :sparql-query)
          ;_ (log/info "SPARQL query bench results: " sparql-query-bench)
          ;graphql-query-bench  (test-queries myDb (fn [db q]
          ;                                          (async/<!! (fdb/graphql-async conn dbid q nil))) :graphql-query graphql-query-coll)
          ;_ (log/info "GraphQL query bench results:" graphql-query-bench)
          ;multi-query-bench  (test-queries myDb (fn [db q]
          ;                                        (async/<!!
          ;                                          (fdb/multi-query-async db q))) multi-query-coll :multi-query)
          ;_ (log/info "Multi-query bench results: " multi-query-bench)
          ;add-data-bench     (->> (criterium/benchmark
          ;                          (async/<!! (fdb/transact-async conn dbid [{:_id "person" :favNums [1]}])) nil)
          ;                        (format-res :addData :txn))
          ;_ (log/info "Add data bench: " add-data-bench)
          ;add-update-bench (->> (criterium/benchmark (add-and-update-data conn dbid) nil)
          ;                      (format-res :addUpdateData :txn))
          ;_ (log/info "Add and update data bench: " add-update-bench)
          ;add-delete-bench (->> (criterium/benchmark (add-and-delete-data conn dbid) nil)
          ;                      (format-res :addDeleteData :txn))
          ;_ (log/info "Add and delete data bench: " add-delete-bench)

          ] (merge
              ;add-schema-res
              query-bench analytical-query-bench
              ;block-query-bench
              ;                       history-query-bench sparql-query-bench graphql-query-bench
              ;                        multi-query-bench add-data-bench
              ;add-update-bench add-delete-bench
              ))))



;; COMPARE TWO SETS OF RESULTS, i.e 0.10.4 and 0.11.0
;; TODO - reconfigure for new results formats.

;(defn compare-results
;  ([res1 res2]
;   (compare-results res1 res2 0.5))
;  ([res1 res2 percentChange]
;   (reduce (fn [acc [res2Key res2Val]]
;             (if-let [res1Time (-> (get res1 res2Key) :mean)]
;               (let [res2Time (-> res2Val :mean)
;                     diff     (- res2Time res1Time)
;                     percent  (/ diff res1Time)]
;                 (if (>= (abs percent) percentChange)
;                   (let [[scale unit] (criterium/scale-time (abs diff))
;                         diff-formatted (criterium/format-value diff scale unit)
;                         txn?           (= :txn (:type res2Val))
;                         key            (if (neg? percent)
;                                          (if txn? :decreased-txn :decreased-query)
;                                          (if txn? :increased-txn :increased-query))
;                         typeV          (:type res2Val)]
;                     (update acc key conj {:query          res2Key :oldTime res1Time :newTime res2Time
;                                           :diff           diff :percentDiff percent
;                                           :diff-formatted diff-formatted :type typeV}))
;                   acc))
;               (update acc :no-match conj res2Key)))
;           {} res2)))
;
;(defn format-results
;  [compare-res diff-label diff-key]
;  (let [diff         (map #(-> % :diff abs) (diff-key compare-res))
;        percent-diff (map #(-> % :percentDiff abs) (diff-key compare-res))
;
;        old-time     (map #(-> % :oldTime abs) (diff-key compare-res))
;        new-time     (map #(-> % :newTime abs) (diff-key compare-res))
;
;        cnt          (count (diff-key compare-res))]
;    (if (= 0 cnt)
;      [(str "There are no results for  " diff-label)]
;      (let [avg    (format-mean-time (average diff))
;            mx     (format-mean-time (apply max diff))
;            mn     (format-mean-time (apply min diff))
;            pDAvg  (str (* (average percent-diff) 100) " %")
;            oldAvg (format-mean-time (average old-time))
;            newAvg (format-mean-time (average new-time))]
;        [(str "Results for " diff-label)
;         (str "There are: " cnt " samples.")
;         (str "The average difference is: " avg)
;         (str "The max difference is: " mx)
;         (str "The average old time is: " oldAvg)
;         (str "The average new time is: " newAvg)
;         (str "The percent different is: " pDAvg)
;         (str "--------------------------------")]))))
;
;(defn compare-res-formatted
;  ([compare-res]
;   (compare-res-formatted compare-res nil))
;  ([compare-res type]
;   (let [type-vec  [[(str "increased " (if type (str type " ")) "queries") :increased-query]
;                    [(str "decreased " (if type (str type " ")) "queries") :decreased-query]]
;         type-vec' (if type type-vec
;                            (concat type-vec [["increased transactions" :increased-txn]
;                                              ["decreased transactions" :decreased-txn]]))
;         res       (map (fn [[label res-key]]
;                          (format-results compare-res label res-key))
;                        type-vec')
;         res'      (if type res
;                            (concat res
;                                    [(str "There are " (count (:no-match compare-res)) " no matches.")]))]
;     (into [] res'))))
;
;(defn filter-comparisons
;  [compare-res type]
;  (let [inc-q (filter #(= (:type %) type) (:increased-query compare-res))
;        dec-q (filter #(= (:type %) type) (:decreased-query compare-res))]
;    {:increased-query inc-q :decreased-query dec-q}))
;
;(defn generate-full-report
;  ([res1 res2]
;   (generate-full-report res1 res2 0))
;  ([res1 res2 percent-change]
;   (let [compare-res              (compare-results res1 res2 percent-change)
;         all-res-formatted        (-> compare-res
;                                      compare-res-formatted)
;         basic-res-formatted      (-> (filter-comparisons compare-res :basic-query)
;                                      (compare-res-formatted "basic"))
;         block-res-formatted      (-> (filter-comparisons compare-res :block-query)
;                                      (compare-res-formatted "block"))
;         analytical-res-formatted (-> (filter-comparisons compare-res :analytical-query)
;                                      (compare-res-formatted "analytical"))
;         history-res-formatted    (-> (filter-comparisons compare-res :history-query)
;                                      (compare-res-formatted "history"))
;
;         grapqhl-res-formatted    (-> (filter-comparisons compare-res :graphql-query)
;                                      (compare-res-formatted "graphql"))
;
;         sparql-res-formatted     (-> (filter-comparisons compare-res :sparql-query)
;                                      (compare-res-formatted "sparql"))
;
;         multi-res-formatted      (-> (filter-comparisons compare-res :multi-query)
;                                      (compare-res-formatted "multi"))]
;     [all-res-formatted basic-res-formatted block-res-formatted
;      analytical-res-formatted history-res-formatted grapqhl-res-formatted
;      sparql-res-formatted multi-res-formatted])))


(comment

  (def conn (:conn user/system))
  (def q {:select ["handle" {"person/follows" ["handle"]}], :from "person"})
  (def db (fdb/db conn "fluree/test"))

  (def queries (-> "../test/fluree/db/ledger/Performance/QueryTxnList.edn" io/resource slurp read-string))

  (def query-test {53 (get (get-query-type queries :basic-query) 53)
                   54 (get (get-query-type queries :basic-query) 54)
                   55 (get (get-query-type queries :basic-query) 55)
                   56 (get (get-query-type queries :basic-query) 56)})

  query-test

  (test-queries db (fn [db q]
                     (async/<!! (fdb/query-async db q))) query-test)

  (def res (criterium/bench (+ 1 1)))

  (criterium/-bench ((fn [db q]
                            (async/<!! (fdb/query-async db q))) db q))


  51      {:sample 60, :mean 9.138601788989441E-5, :mean-time " 91.386018 µs", :type :basic-query},


  (criterium/report-result (criterium/quick-bench (+ 1 1)))


  (test-queries myDb (fn [db q]
                       (async/<!! (fdb/query-async db q))) )


  (def myPerformanceCheck (performance-check conn "fluree/test"))


  (def myPerformanceCheck (performance-check conn "plane/demo" "../test/fluree/db/ledger/Performance/PlaneQueryList.edn" 1000
                                             false))


  (into (sorted-map) myPerformanceCheck)

  (+ 1 1)



  (def compare-res (compare-results res1 res2))

  compare-res

  (def res1 (-> "performanceMetrics/0-10-4.edn" io/resource slurp read-string))
  (def res2 (-> "performanceMetrics/0-11-0-old-version.edn" io/resource slurp read-string))

  (generate-full-report res1 res2)





  (add-schema-performance-check conn "fluree/new")


  (count (keys (-> "../test/fluree/db/ledger/Performance/QueryTxnList.edn" io/resource slurp read-string)))

  (def queryTxns (-> "../test/fluree/db/ledger/Performance/QueryTxnList.edn" io/resource slurp read-string))

  (select-keys queryTxns (range 1 50))

  )
