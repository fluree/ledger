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
           (-> (map #(get-in % ["chat/person" "person/handle"]) res) set) ))))


(deftest crawl-graph-reverse
  (testing "Crawl the graph with a reverse ref")
  (let [crawl-query       {:select ["*" {"chat/_person" ["*"]}]
                           :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= 6 (count res)))))


(deftest crawl-graph-reverse-add
  (testing "Crawl the graph with a reverse ref and regular ref")
  (let [crawl-query {:select ["*" {"chat/_person" ["*" {"chat/person" ["*"]}]}]
              :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]

    (is (= 6 (count res)))

    (is (= #{nil "zsmith" "dsanchez" "jdoe"} (-> (map #(get-in % ["chat/_person" 0 "chat/person" "person/handle"]) res) set)))))


(deftest select-no-ns-preds
  (testing "Select predicates with no namespace")
  (let [crawl-query   {:select ["handle" "fullName"]
                       :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= 6 (count res)))

    (is (= #{"dsanchez" "anguyen" "zsmith" "jdoe" "aSmith" "aVargas"}
           (-> (map #(get % "handle") res) set)))

    (is (= #{"Amy Nguyen" "Zach Smith" "Jane Doe" "Diana Sanchez" "Alex Vargas" "Alice Smith"}
           (-> (map #(get % "fullName") res) set)))))

(deftest select-with-as
  (testing "Select predicates with _as specified")
  (let [crawl-query   {:select ["handle" {"fullName" [{"_as" "name"}]}]
                       :from   "person"}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= 6 (count res)))

    (is (= #{"dsanchez" "anguyen" "zsmith" "jdoe" "aSmith" "aVargas"}
           (-> (map #(get % "handle") res) set)))

    (is (= #{"Amy Nguyen" "Zach Smith" "Jane Doe" "Diana Sanchez" "Alex Vargas" "Alice Smith"}
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
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/graphql-async (basic/get-conn) test/ledger-chat graphql-query))]

    (is (= 6 (-> (:person res) count)))

    (is (= #{nil "Hi! I'm a chat from Diana." "Hi! I'm chat from Jane." "Hi! I'm a chat from Zach."}
           (-> (map #(get-in % ["chat/_person" 0 "message"]) (:person res)) set)))

    (is (= #{nil "Zsmith is responding!" "Welcome Diana!"}
           (-> (map #(get-in % ["chat/_person" 0 "comments" 0 "message"]) (:person res)) set)))))

(deftest crawl-graph-two
  (testing "Crawl the graph")
  (let [crawl-query     {:select ["handle" {"person/follows" ["handle"]}]
                         :from "person" }
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= 6(count res)))

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


(deftest multi-query
  (testing "Multi query")
  (let [multi-query    { :chatQuery { :select ["*"] :from "chat" }
                        :personQuery  { :select ["*"] :from  "person" }}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/multi-query-async db multi-query))]

    (is (= 3 (count (:chatQuery res))))

    (is (= #{351843720888320 351843720888321 351843720888323}
           (-> (map #(get-in % ["chat/person" :_id]) (:chatQuery res)) set)))

    (is (= 6 (count (:personQuery res))))

    (is (= #{"dsanchez" "anguyen" "zsmith" "jdoe" "aSmith" "aVargas"}
           (-> (map #(get % "person/handle") (:personQuery res)) set)))))

;; TODO - multi-query is not supposed to throw error, but return it
(deftest multi-query-with-error
  (testing "Multi query with incorrect query")
  (let [multi-query      { :incorrectQuery { :select ["*"] :from "apples" }
                          :personQuery  { :select ["*"] :from  "person" }}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/multi-query-async db multi-query))]

    (is (= (:status (:incorrectQuery res)) nil))

    (is (=  6 (count (:personQuery res))))

    (is (= #{"dsanchez" "anguyen" "zsmith" "jdoe" "aSmith" "aVargas"}
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
  (multi-query)
  ;(multi-query-with-error)
  )

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (advanced-query-test))