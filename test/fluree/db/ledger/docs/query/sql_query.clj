(ns fluree.db.ledger.docs.query.sql-query
  (:require [clojure.test :refer :all]
            [clojure.stacktrace :refer [root-cause]]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [fluree.db.util.core :as utils]
            [clojure.core.async :as async]
            [clojure.set :refer [subset?]]))

(use-fixtures :once test/test-system)

(deftest query-tests

  (testing "Select all chats"
    (let [query "select * from chat"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 3 chats
      (is (= 3 (count data)))

      ;; the keys for every chat should be _id, message, person, instant, or comments
      (is (every? (fn [chat]
                    (every? #(boolean (#{"_id" "chat/message" :_id "chat/person"
                                         "chat/instant" "chat/comments"} %)) (keys chat))) data))))

  (testing "Select all persons of age 25"
    (let [query "select * from person where age = 25"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 1 person
      (is (= 1 (count data)))

      ;; the keys for every person should be _id, follows, age, active, favMovies,
      ;; favNums, handle, fullName
      (is (every? (fn [item]
                    (every? #(boolean (#{"_id" "person/follows" :_id "person/age"
                                         "person/active" "person/favMovies"
                                         "person/favNums" "person/favArtists"
                                         "person/handle" "person/fullName"} %))
                            (keys item))) data))))

  (testing "Select all persons older or younger than 34"
    (let [query "select * from person where age < 34 OR age > 34"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 3 people
      (is (= 3 (count data)))))

  (testing "Select all persons younger than 34"
    (let [query "select * from person where age < 34"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 1 person
      (is (= 1 (count data)))))

  (testing "Select all persons 34 years of age or less"
    (let [query "select * from person where age <= 34"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 2 people
      (is (= 2 (count data)))))

  (testing "Select all persons older than 34"
    (let [query "select * from person where age > 34"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 2 people
      (is (= 2 (count data)))))

  (testing "Select all persons 34 years of age or older"
    (let [query "select * from person where age >= 34"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 3 people
      (is (= 3 (count data)))))

  (testing "Select all persons older than 34 with a favNum less than 50"
    (let [query "select * from person where age > 34 and favNums < 50"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 4 items returned
      (is (= 4 (count data)))))

  (testing "Select all persons older than 34 or who have a favNum less than 50"
    (let [query "select * from person where age > 34 or favNums < 50"
          data  (-> (basic/get-db test/ledger-chat)
                    (fdb/sql-async query)
                    (async/<!!))]

      ;; should be 10 items returned
      (is (= 10 (count data))))))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (query-tests))
