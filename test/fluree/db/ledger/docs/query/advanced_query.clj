(ns fluree.db.ledger.docs.query.advanced-query
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [fluree.db.util.log :as log]))

(use-fixtures :once test/test-system-deprecated)

(deftest crawl-graph
  (testing "Crawl the graph")
  (let [crawl-query     {:select ["*" {"chat/person" ["*"]}]
                           :from   "chat"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]

    (is (= 3 (count res)))

    (is (= #{"zsmith" "dsanchez" "jdoe"}
           (-> (map #(get-in % ["chat/person" "person/handle"]) res) set)))))


(deftest crawl-graph-reverse
  (testing "Crawl the graph with a reverse ref")
  (let [crawl-query       {:select ["*" {"chat/_person" ["*"]}]
                           :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= 7 (count res)))))


(deftest crawl-graph-reverse-add
  (testing "Crawl the graph with a reverse ref and regular ref")
  (let [crawl-query {:select ["*" {"chat/_person" ["*" {"chat/person" ["*"]}]}]
                     :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]

    (is (= 7 (count res)))

    (is (= #{nil "zsmith" "dsanchez" "jdoe"}
           (-> (map #(get-in % ["chat/_person" 0 "chat/person" "person/handle"]) res) set)))))


(deftest select-no-ns-preds
  (testing "Select predicates with no namespace")
  (let [crawl-query   {:select ["handle" "fullName"]
                       :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= 7 (count res)))

    (is (= #{"dsanchez" "anguyen" "zsmith" "jdoe" "aSmith" "aVargas"
             "jakethesnake"}
           (-> (map #(get % "handle") res) set)))

    (is (= #{"Amy Nguyen" "Zach Smith" "Jane Doe" "Diana Sanchez" "Alex Vargas"
             "Alice Smith" "Jake Parsell"}
           (-> (map #(get % "fullName") res) set)))))

(deftest select-with-as
  (testing "Select predicates with _as specified")
  (let [crawl-query   {:select ["handle" {"fullName" [{"_as" "name"}]}]
                       :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= 7 (count res)))

    (is (= #{"dsanchez" "anguyen" "zsmith" "jdoe" "aSmith" "aVargas"
             "jakethesnake"}
           (-> (map #(get % "handle") res) set)))

    (is (= #{"Amy Nguyen" "Zach Smith" "Jane Doe" "Diana Sanchez" "Alex Vargas"
             "Alice Smith" "Jake Parsell"}
           (-> (map #(get % "name") res) set)))))


;; TODO - find user where limit would change results
(deftest select-with-as-and-limit
  (testing "Select predicates with _as and _limit specified")
  (let [crawl-query {:select ["handle" {"comment/_person"
                                        ["*" {"_as" "comment" "_limit" 1}]}]
                     :from ["person/handle", "anguyen"]}
        db  (basic/get-db test/ledger-chat)
        res (-> (async/<!! (fdb/query-async db crawl-query))
                first)]
    (is (= "Welcome Diana! This is Amy." (get-in res ["comment" 0 "comment/message"])))))


(deftest graphql-with-reverse-ref
  (testing "Graphl with reverse ref")
  (let [graphql-query {:query "{ graph {\n  person {\n    _id\n    handle\n    chat_Via_person (limit: 10) {\n      instant\n      message\n      comments {\n        message\n      }\n    }\n  }\n}}"}
        res  (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query))]

    (is (= 7 (-> (:person res) count)))

    (is (= #{nil "Hi! I'm a chat from Diana." "Hi! I'm chat from Jane." "Hi! I'm a chat from Zach."}
           (-> (map #(get-in % ["chat/_person" 0 "message"]) (:person res)) set)))

    (is (= #{nil "Zsmith is responding!" "Welcome Diana!"}
           (-> (map #(get-in % ["chat/_person" 0 "comments" 0 "message"]) (:person res)) set)))))

(deftest crawl-graph-two
  (testing "Crawl the graph")
  (let [crawl-query     {:select ["handle" {"person/follows" ["handle"]}]
                         :from "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= 7 (count res)))

    (is (= #{nil "jdoe" "zsmith" "anguyen"}
           (-> (map #(get-in % ["person/follows" "handle"]) res) set)))))


;; TODO - recur not working?
(deftest crawl-graph-two-with-recur
  (testing "Crawl the graph with recur")
  (let [crawl-query       {:select ["handle" {"person/follows" ["handle" {"_recur" 10}]}] :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]

    (is (= 6 (count res)))

    (is (=  #{nil 351843720888321 351843720888322}
           (-> (map #(get-in % ["person/follows" "person/follows" "person/follows" :_id]) res) set)))))

(deftest aggregate-binding
  (testing "Aggregate function using two-tuple in where clause"
    (let [query      {:select ["?e" "?maxNum"]
                      :where  [["?e" "person/favNums" "?favNums"]
                               ["?maxNum" "#(max ?favNums)"]]}
          db         (basic/get-db test/ledger-chat)
          res        (async/<!! (fdb/query-async db query))
          maxNumVals (->> res
                          (map second)
                          (into #{}))]

      ;; all two-tuple results should have the identical ?maxNum
      (is (= 1 (count maxNumVals)))

      ;; sample data set has max favNum as 1950
      (is (= 1950 (first maxNumVals))))))


(deftest group-by-with-limit-offset
  (testing "Group By query with limit returns first two full results"
    (let [query-all {:select "?favNums"
                     :where  [["?e" "person/favNums" "?favNums"]]
                     :groupBy "?e"}
          query-limit {:select "?favNums"
                       :where  [["?e" "person/favNums" "?favNums"]]
                       :groupBy "?e"
                       :limit 2}
          query-offset {:select "?favNums"
                        :where  [["?e" "person/favNums" "?favNums"]]
                        :groupBy "?e"
                        :offset 2
                        :limit 2}
          db  (basic/get-db test/ledger-chat)
          res-all  (async/<!! (fdb/query-async db query-all))
          res-limit (async/<!! (fdb/query-async db query-limit))
          res-offset (async/<!! (fdb/query-async db query-offset))]

      (is (= res-limit (->> res-all (take 2) (into {})))
          "limit 2 query should be same as first two of full results")

      (is (= res-offset (->> res-all (drop 2) (take 2) (into {})))
          "offset 2, limit 2 query should be same as drop 2 take 2"))))


(deftest group-by-with-having
  (testing "Group By query with 'having' statement defined for filter"
    (let [q-all          {:select "(sum ?favNums)"
                          :where   [["?e" "person/favNums" "?favNums"]]
                          :groupBy "?e"}
          q-having       {:select  "(sum ?favNums)"
                          :where   [["?e" "person/favNums" "?favNums"]]
                          :groupBy "?e"
                          :having  (str '(< 200 (sum ?favNums)))}
          q-having+and   {:select  "(sum ?favNums)"
                          :where   [["?e" "person/favNums" "?favNums"]]
                          :groupBy "?e"
                          :having  (str '(and (< 200 (sum ?favNums)) (> 1900 (sum ?favNums))))}
          db             (basic/get-db test/ledger-chat)
          res-all        (async/<!! (fdb/query-async db q-all))
          res-having     (async/<!! (fdb/query-async db q-having))
          res-having+and (async/<!! (fdb/query-async db q-having+and))
          ;; get a vector of all ?favNums sums
          all-sums (vals res-all)]

      (is (= (set (vals res-having))
             (set (filter #(< 200 %) all-sums)))
          "having results should be same as all results filtered for < 200")

      (is (= (set (vals res-having+and))
             (set (filter #(and (< 200 %) (> 1900 %)) all-sums)))
          "having+and results should be same as all results filtered for < 200 'and' > 1900"))))


(deftest multi-query
  (testing "Multi query")
  (let [multi-query    { :chatQuery { :select ["*"] :from "chat"}
                        :personQuery  { :select ["*"] :from  "person"}}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/multi-query-async db multi-query))]

    (is (= 3 (count (:chatQuery res))))

    (is (= #{351843720888320 351843720888321 351843720888323}
           (-> (map #(get-in % ["chat/person" :_id]) (:chatQuery res)) set)))

    (is (= 7 (count (:personQuery res))))

    (is (= #{"dsanchez" "anguyen" "zsmith" "jdoe" "aSmith" "aVargas"
             "jakethesnake"}
           (-> (map #(get % "person/handle") (:personQuery res)) set)))))

;; TODO - multi-query is not supposed to throw error, but return it
(deftest multi-query-with-error
  (testing "Multi query with incorrect query")
  (let [multi-query      {:incorrectQuery { :select ["*"] :from "apples"}
                          :personQuery  { :select ["*"] :from  "person"}}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/multi-query-async db multi-query))]

    (is (= (:status (:incorrectQuery res)) nil))

    (is (=  6 (count (:personQuery res))))

    (is (= #{"dsanchez" "anguyen" "zsmith" "jdoe" "aSmith" "aVargas"
             "jakethesnake"}
           (-> (map #(get % "person/handle") (:personQuery res)) set)))))

(deftest advanced-query-test
  (crawl-graph)
  (crawl-graph-reverse)
  (crawl-graph-reverse-add)
  (select-no-ns-preds)
  (select-with-as)
  (select-with-as-and-limit)
  (graphql-with-reverse-ref)
  (crawl-graph-two)
  ;(crawl-graph-two-with-recur)
  (aggregate-binding)
  (group-by-with-limit-offset)
  (group-by-with-having)
  (multi-query))
  ;(multi-query-with-error)


(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (advanced-query-test))
