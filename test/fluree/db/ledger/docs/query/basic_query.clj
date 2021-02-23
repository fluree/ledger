(ns fluree.db.ledger.docs.query.basic-query
  (:require [clojure.test :refer :all]
            [clojure.stacktrace :refer [root-cause]]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [fluree.db.util.core :as utils]
            [clojure.core.async :as async]
            [clojure.set :refer [subset?]]))

(use-fixtures :once test/test-system)

(defn average
  [numbers]
  (if (empty? numbers)
    0
    (/ (reduce + numbers) (count numbers))))

(defn average-decimal
  [numbers]
  (if (empty? numbers)
    0
    (/ (reduce + 0.0 numbers) (count numbers))))

(defn issue-consecutive-queries
  [ledger-name queries]
  (let [db (basic/get-db ledger-name)]
    (async/go-loop [results []
                    [q & r] queries]
      (let [resp    (async/<!! (fdb/query-async db q))
            results (conj results resp)]
        (if r
          (recur results r)
          results)))))

(deftest select-chats
  (testing "Select all chats")
  (let [chat-query {:select ["*"] :from "chat"}
        db  (basic/get-db test/ledger-chat)
        chats (async/<!! (fdb/query-async db chat-query))]

    ;; should be 3 chats
    (is (= 3 (count chats)))

    ;; the keys for every chat should be _id, message, person, instant, or comments
    (is (every? (fn [chat]
                  (every? #(boolean (#{"_id" "chat/message" :_id "chat/person"
                                          "chat/instant" "chat/comments"} %)) (keys chat))) chats))))


(deftest select-from-subject-id
  (testing "Select from chat, subject-id")
  (let [chat-base  {:selectOne {"?chats" ["*"]},
                    :where [["?chats", "chat/person", "?person"],
                            ["?person", "person/handle", "jdoe"]]} ; get ids of interest
        db         (basic/get-db test/ledger-chat)
        base-res   (async/<!! (fdb/query-async db chat-base))
        person-id  (-> base-res (get "chat/person") (get "_id"))
        chat       (async/<!!
                     (fdb/query-async
                       db {:selectOne ["person/handle"] :from person-id}))]

    (is (= (get chat "person/handle") "jdoe"))))

(deftest select-from-two-tuple
  (testing "Select from two tuple, [\"person/handle\" \"jdoe\"]")
  (let [chat-query {:select ["*"] :from ["person/handle" "jdoe"]}
        db  (basic/get-db test/ledger-chat)
        res (-> (async/<!! (fdb/query-async db chat-query))
                first)]

    (is (= (get res "person/handle") "jdoe"))

    (is (= (get res "person/fullName") "Jane Doe"))

    (is (= (get res "person/age") 25))

    (is (= (count (get res "person/favNums")) 5))

    (is (= (count (get res "person/favArtists")) 3))

    (is (= (count (get res "person/favMovies")) 3))

    ; check ids
    (let [id   (get res "_id")
          res' (async/<!! (fdb/query-async db {:selectOne ["*"]
                                               :from id}))]
      (is (= (get res "person/handle") (get res' "person/handle")))
      (is (= (get res "person/follows") (get res' "person/follows"))))))

(deftest select-from-group-of-subjects
  (testing "Select from a group of subjects")
  (let [chat-query {:select ["*"]
                    :from [369435906932737, ["person/handle", "jdoe"],
                           387028092977153,  ["person/handle", "zsmith"]]}
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db chat-query))]

    (is (subset? #{369435906932737 387028092977153} (-> (map #(get % "_id") res) set)))

    (is (= (-> (map #(get % "person/handle") res) set) #{nil "jdoe" "zsmith"}))))


(deftest select-from-predicate
  (testing "Select from a particular predicate")
  (let [chat-query {:select ["*"]
                    :from "person/handle"}
        db  (basic/get-db test/ledger-chat)
        res        (async/<!! (fdb/query-async db chat-query))]

    (is (= (count res) 6))

    (is (every? (fn [person]
                  (-> (get person "person/handle") boolean)) res))))


(deftest select-certain-predicates
  (testing "Select certain predicates")
  (let [chat-query {:select ["chat/message", "chat/person"]
                    :from "chat"
                    :limit 100}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db chat-query))]

    (is (every? (fn [chat]
                  (map #(-> (#{"chat/message" "chat/person"} %) boolean) (keys chat))) res))))


(deftest select-with-where
  (testing "Select with where")
  (let [chat-query {:select ["chat/message" "chat/instant"]
                    :where "chat/instant > 1517437000000"}
        db  (basic/get-db test/ledger-chat)
        res        (async/<!! (fdb/query-async db chat-query))]

    (is (= 3 (count res)))

    (is (= #{"chat/message" :_id "chat/instant"}
           (-> (apply merge res) keys set)))))



(deftest select-as-of-block
  (testing "Select as of block")
  (let [chat-query {:select ["*"]
                    :from "chat"
                    :block 2}
        db  (basic/get-db test/ledger-chat)
        res        (async/<!! (fdb/query-async db chat-query))
        chat-query-4 {:select ["*"]
                      :from "chat"
                      :block 4}
        res-4        (async/<!! (fdb/query-async db chat-query-4))]


    (is (empty? res))

    (is (= 3 (count res-4)))))


(deftest select-with-limit-and-offset
  (testing "Select with limit and offset")
  (let [limit-query {:select ["*"]
                     :from "chat"
                     :limit 2}
        offset-query {:select ["*"]
                      :from "chat"
                      :offset 1}
        limit-and-offset-query {:select ["*"]
                                :from "chat" :limit 1
                                :offset 1}
        all-res (async/<!! (issue-consecutive-queries test/ledger-chat [limit-query offset-query limit-and-offset-query]))]

    (is (= (-> (map count all-res) sort vec) [1 2 2]))))


(deftest select-with-groupBy
  (testing "Select with groupBy")
  (let [base-query {:select {"?person" ["handle", "favNums"]}
                    :where  [["?person" "person/handle" "?handle"]]
                    :orderBy ["ASC" "?handle"]}
        groupBy-query {:select ["?handle", "(as (avg ?nums) avg)"],
                       :where [["?person", "person/handle", "?handle"],
                               ["?person", "person/favNums", "?nums"]],
                       :groupBy "?handle",
                       :opts {:prettyPrint true}}
        groupBy-query' {:select ["?handle", "(as (avg ?nums) avg)"],
                        :where [["?person", "person/handle", "?handle"],
                                ["?person", "person/favNums", "?nums"]],
                        :groupBy ["?handle"],
                        :opts {:prettyPrint true}}
        db         (basic/get-db test/ledger-chat)
        ; groupBy on favNums excludes any entries without a favNum
        ; exclude these from base set for validation purposes
        base-res   (->> (async/<!! (fdb/query-async db base-query))
                        (reduce-kv (fn [m k v]
                                     (if-let [favNums (get v "favNums")]
                                       (assoc m (get v "handle")
                                                (merge v
                                                       {:index k
                                                        :count (count favNums)
                                                        :avg (average favNums)
                                                        :avg-dec (average-decimal favNums)}))
                                       m))
                                   {}))
        groupBy-res  (async/<!! (fdb/query-async db groupBy-query))
        groupBy-res' (async/<!! (fdb/query-async db groupBy-query'))]

    ; validate string groupBy
    (is (not= clojure.lang.ExceptionInfo (type groupBy-res)))
    (is (= (-> groupBy-res keys count) (-> base-res keys count))) ;validate # of handles - groupBy is a subset
    (is (empty?
          (utils/without-nils
            (loop [[k & r-k] (keys base-res)
                     [v & r-v] (vals base-res)
                     errs {}]
                (if (or (nil? k) (empty k))
                  errs
                  (let [exp-vals (-> v (select-keys [:avg :avg-dec]) vals set)
                        gbe      (get groupBy-res k)
                        obs-vals (-> gbe
                                     (as-> z (map val (filter (comp #{"avg"} key) (apply concat z))))
                                     set)]
                    (recur
                      r-k
                      r-v
                      (assoc errs k
                         (cond-> []
                                 (not= (get v :count) (count gbe))
                                 (conj (str "Invalid number of entries"
                                            "; expected: " (get v :count)
                                            "; observed: " (count gbe)))
                                 (not (subset? obs-vals exp-vals))
                                 (conj (str "One or more observed average(s) did not match acceptable values"
                                            "; accepted: " exp-vals
                                            "; observed: " obs-vals)))))))))))


    ;validate vector groupBy
    (is (not= clojure.lang.ExceptionInfo (type groupBy-res')))
    (is (= (-> groupBy-res' keys count) (-> base-res keys count))) ;validate # of handles - groupBy is a subset
    (is (empty?
          (utils/without-nils
            (loop [[k & r-k] (keys base-res)
                   [v & r-v] (vals base-res)
                   errs {}]
              (if (or (nil? k) (empty k))
                errs
                (let [exp-vals (-> v (select-keys [:avg :avg-dec]) vals set)
                      gbe      (get groupBy-res' [k])
                      obs-vals (-> gbe
                                   (as-> z (map val (filter (comp #{"avg"} key) (apply concat z))))
                                   set)]
                  (recur
                    r-k
                    r-v
                    (assoc errs k
                       (cond-> []
                               (not= (get v :count) (count gbe))
                               (conj (str "Invalid number of entries"
                                          "; expected: " (get v :count)
                                          "; observed: " (count gbe)))
                               (not (subset? obs-vals exp-vals))
                               (conj (str "One or more observed average(s) did not match acceptable values"
                                          "; accepted: " exp-vals
                                          "; observed: " obs-vals)))))))))))))


(deftest select-boolean-predicates
  (testing "with true predicate"
    (let [query {:select ["?person"] :from "person"
                 :where  [["?person" "person/active" true]]}
          db (basic/get-db test/ledger-chat)
          res (async/<!! (fdb/query-async db query))]
      (is (= 3 (count res)))))
  (testing "with false predicate"
    (let [query {:select ["?person"] :from "person"
                 :where  [["?person" "person/active" false]]}
          db (basic/get-db test/ledger-chat)
          res (async/<!! (fdb/query-async db query))]
      (is (= 1 (count res))))))


;; TODO: Make this work like typical test suites w/ only one later of deftests.
(deftest basic-query-test
  (select-chats)
  (select-from-subject-id)
  (select-from-two-tuple)
  (select-from-group-of-subjects)
  (select-from-predicate)
  (select-certain-predicates)
  (select-with-where)
  (select-as-of-block)
  (select-with-limit-and-offset)
  (select-with-groupBy)
  (select-boolean-predicates))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (basic-query-test))
