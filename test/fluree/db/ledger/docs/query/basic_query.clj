(ns fluree.db.ledger.docs.query.basic-query
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.string :as str]))

(use-fixtures :once test/test-system)


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
  (testing "Select from chat, 369435906932737")
  (let [chat-query {:select ["*"] :from 369435906932737}
        db         (basic/get-db test/ledger-chat)
        chat       (-> (async/<!! (fdb/query-async db chat-query))
                       first)
        comments   (get chat "chat/comments")]

    (is (= (get chat "_id") 369435906932737))

    (is (= (get chat "chat/message") "Hi! I'm chat from Jane."))

    (is (= (get chat "chat/person") {"_id" 351843720888321}))

    (is (= 2 (count comments)))))

(deftest select-from-two-tuple
  (testing "Select from two tuple, [\"person/handle\" \"jdoe\"]")
  (let [chat-query {:select ["*"] :from ["person/handle" "jdoe"]}
        db  (basic/get-db test/ledger-chat)
        res (-> (async/<!! (fdb/query-async db chat-query))
                first)]

    (is (= (get res "_id") 351843720888321))

    (is (= (get res "person/handle") "jdoe"))

    (is (= (get res "person/fullName") "Jane Doe"))

    (is (= (get res "person/age") 25))

    (is (= (get res "person/follows") {"_id" 351843720888322}))

    (is (= (count (get res "person/favNums")) 5))

    (is (= (count (get res "person/favArtists")) 3))

    (is (= (count (get res "person/favMovies")) 3))))

(deftest select-from-group-of-subjects
  (testing "Select from a group of subjects")
  (let [chat-query {:select ["*"]
                    :from [369435906932737, ["person/handle", "jdoe"],
                           387028092977153,  ["person/handle", "zsmith"] ]}
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db chat-query))]

    (is (= (-> (map #(get % "_id") res) set) #{369435906932737 351843720888321
                                               351843720888322 387028092977153}))

    (is (= (-> (map #(get % "person/handle") res) set) #{nil "jdoe" "zsmith"}))

    (is (= (-> (map #(get % "comment/message") res) set) #{nil "Zsmith is responding!"}))))


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
                    :limit 100 }
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db chat-query))]

    (is (every? (fn [chat]
                  (map #(-> (#{"chat/message" "chat/person"} %) boolean) (keys chat))) res))))


(deftest select-with-where
  (testing "Select with where")
  (let [chat-query {:select ["chat/message" "chat/instant"]
                    :where "chat/instant > 1517437000000" }
        db  (basic/get-db test/ledger-chat)
        res        (async/<!! (fdb/query-async db chat-query))]

    (is (= 3 (count res)))

    (is (= #{"chat/message" :_id "chat/instant"}
           (-> (apply merge res) keys set)))))



(deftest select-as-of-block
  (testing "Select as of block")
  (let [chat-query {:select ["*"]
                    :from "chat"
                    :block 2 }
        db  (basic/get-db test/ledger-chat)
        res        (async/<!! (fdb/query-async db chat-query))
        chat-query-4 {:select ["*"]
                    :from "chat"
                    :block 4 }
        res-4        (async/<!! (fdb/query-async db chat-query-4 ))]


    (is (empty? res))

    (is (= 3 (count res-4)))))


(deftest select-as-of-ISO-string
  (testing "Select as of an ISO-8601 formatted string")
  (let [chat-query {:select ["*"]
                    :from "chat"
                    :block "2017-11-14T20:59:36.097Z"}
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db chat-query))]

    (is (= clojure.lang.ExceptionInfo (type res)))

    (is (= "There is no data as of 1510693176097" (.getMessage res)))))

(deftest select-as-of-1-minute-ago
  (testing "Select as of 10 seconds ago")
  (let [chat-query {:select ["*"]
                    :from "chat"
                    :block "PT1M" }
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db chat-query))]

    (is (= clojure.lang.ExceptionInfo (type res)))


    (is (str/includes? (.getMessage res) "There is no data as of"))))

(deftest select-with-limit-and-offset
  (testing "Select with limit and offset")
  (let [limit-query {:select ["*"]
                    :from "chat"
                    :limit 2 }
        offset-query {:select ["*"]
                     :from "chat"
                     :offset 1 }
        limit-and-offset-query {:select ["*"]
                      :from "chat" :limit 1
                      :offset 1 }
        all-res (async/<!! (issue-consecutive-queries test/ledger-chat [limit-query offset-query limit-and-offset-query]))]

    (is (= (-> (map count all-res) sort vec) [1 2 2]))

    (is (= (mapv (fn [results] (-> (map #(get % "_id") results) sort vec)) all-res) [[369435906932738 369435906932739] [369435906932737 369435906932738] [369435906932738]]))))

(deftest basic-query-test
  (select-chats)
  (select-from-subject-id)
  (select-from-two-tuple)
  (select-from-group-of-subjects)
  (select-from-predicate)
  (select-certain-predicates)
  (select-with-where)
  (select-as-of-block)
  (select-as-of-ISO-string)
  (select-as-of-1-minute-ago)
  (select-with-limit-and-offset))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (basic-query-test))
