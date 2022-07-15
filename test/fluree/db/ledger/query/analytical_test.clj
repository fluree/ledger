(ns fluree.db.ledger.query.analytical-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as fdb]
            [fluree.db.util.async :refer [<??]]))

(use-fixtures :once test/test-system)

(deftest analytical-with-prefix-two-tuple-subject
  (testing "Analytical query with prefix, with two-tuple subject"
    (let [crawl-query {:select "?nums"
                       :where  [["$fdb", ["person/handle", "zsmith"], "person/favNums", "?nums"]]}
          ledger      (test/rand-ledger test/ledger-chat
                                        {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db          (fdb/db (:conn test/system) ledger {:syncTo block})
          res         (<?? (fdb/query-async db crawl-query))]
      (is (= #{5 645 28 -1 1223} (set res))
          (str "Unexpected query result: " (pr-str res))))))

(deftest analytical-with-two-tuple-subject
  (testing "Analytical query with two-tuple subject")
  (let [crawl-query {:select "?nums"
                     :where  [[["person/handle", "zsmith"], "person/favNums", "?nums"]]}
        ledger      (test/rand-ledger test/ledger-chat
                                      {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db          (fdb/db (:conn test/system) ledger {:syncTo block})
        res         (<?? (fdb/query-async db crawl-query))]
    (is (= #{5 645 28 -1 1223} (set res)))))

(deftest analytical-with-two-clauses
  (testing "Analytical query with two clauses")
  (let [crawl-query {:select "?nums"
                     :where  [["?person", "person/handle", "zsmith"],
                              ["?person", "person/favNums", "?nums"]]}
        ledger      (test/rand-ledger test/ledger-chat
                                      {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db          (fdb/db (:conn test/system) ledger {:syncTo block})
        res         (<?? (fdb/query-async db crawl-query))]
    (is (= #{5 645 28 -1 1223} (set res)))))

(deftest analytical-with-nil-subject
  (testing "Analytical query with nil subject")
  (let [crawl-query {:select "?nums",
                     :where  [["$fdb", nil, "person/favNums", "?nums"]]}
        ledger      (test/rand-ledger test/ledger-chat
                                      {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db          (fdb/db (:conn test/system) ledger {:syncTo block})
        res         (<?? (fdb/query-async db crawl-query))]
    (is (= #{7 1223 -2 28 12 9 0 1950 5 645 -1 2 98} (set res)))))

(deftest analytical-with-prefix-two-clauses-two-tuple-subject
  (testing "Analytical query with two clauses, two bound variables, two-tuple subjects")
  (let [crawl-query {:select ["?nums1", "?nums2"]
                     :where  [[["person/handle", "zsmith"],
                               "person/favNums", "?nums1"],
                              [["person/handle", "jdoe"],
                               "person/favNums", "?nums2"]]}
        ledger      (test/rand-ledger test/ledger-chat
                                      {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db          (fdb/db (:conn test/system) ledger {:syncTo block})
        res         (<?? (fdb/query-async db crawl-query))]
    (is (= #{-1 645 1223 28 5} (set (map first res))))
    (is (= #{0 -2 1223 12 98} (set (map second res))))))

(deftest analytical-with-prefix-two-clauses-two-tuple-subject-matching
  (testing "Analytical query with two clauses, one bound variable, filtered between two two-tuple subjects")
  (let [crawl-query {:select "?nums"
                     :where  [["$fdb", ["person/handle", "zsmith"], "person/favNums", "?nums"],
                              ["$fdb", ["person/handle", "jdoe"], "person/favNums", "?nums"]]}
        ledger      (test/rand-ledger test/ledger-chat
                                      {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db          (fdb/db (:conn test/system) ledger {:syncTo block})
        res         (<?? (fdb/query-async db crawl-query))]
    (is (= [1223] res))))

(deftest analytical-select-one-with-aggregate-sum
  (testing "Analytical query with Select One clause using Aggregate sum operand")
  (let [crawl-query {:select "(sum ?nums)",
                     :where  [[["person/handle" "zsmith"] "person/favNums" "?nums"]]}
        ledger      (test/rand-ledger test/ledger-chat
                                      {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db          (fdb/db (:conn test/system) ledger {:syncTo block})
        res         (<?? (fdb/query-async db crawl-query))]
    (is (= (reduce + [5 645 28 -1 1223]) res))))

(deftest analytical-select-one-with-aggregate-sample
  (testing "Analytical query with select clause using Aggregate sample operand")
  (let [sample-query {:select "(sample 10 ?nums)",
                      :where  [[nil "person/favNums" "?nums"]]}
        total-query  {:select "?nums",
                      :where  [[nil "person/favNums" "?nums"]]}
        ledger       (test/rand-ledger test/ledger-chat
                                       {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db           (fdb/db (:conn test/system) ledger {:syncTo block})
        sample-res   (<?? (fdb/query-async db sample-query))
        total-res    (<?? (fdb/query-async db total-query))]
    (is (= 10 (count sample-res)))
    (is (every? (set total-res) sample-res))))

(deftest analytical-reverse-crawl-from-bound-variable
  (testing "Analytical query that reverse-crawls from bound-variable predicate to all subjects with that predicate")
  (let [crawl-query {:select {:?artist ["*" {:person/_favArtists ["*"]}]},
                     :where  [[nil "person/favArtists" "?artist"]]}
        ledger      (test/rand-ledger test/ledger-chat
                                      {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db          (fdb/db (:conn test/system) ledger {:syncTo block})
        res         (<?? (fdb/query-async db crawl-query))]
    (is (vector? res))
    (is (contains? (first res) :_id))
    (is (contains? (first res) "artist/name"))
    (is (contains? (first res) "person/_favArtists"))
    (is (vector? (get (first res) "person/_favArtists")))))


(deftest analytical-where-clause-filter-option
  (testing "Analytical query Filter option in Where clause")
  (let [analytical-query {:select {:?person ["person/handle" "person/favNums"]},
                          :where
                          [["$fdb"
                            "?person"
                            "person/favNums"
                            "(> ?nums 1000)"]]}
        ledger           (test/rand-ledger test/ledger-chat
                                           {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db               (fdb/db (:conn test/system) ledger {:syncTo block})
        res              (<?? (fdb/query-async db analytical-query))]

    (is (not (contains? (set (map #(get (first %) "person/handle") res)) "anguyen")))
    ;because ["person/handle", "anguyen"] only has nums < 1000

    (is (every? number? (get (first (first res)) "person/favNums")))))

;; TODO: build out similar tests for other filter operators

(deftest analytical-across-sources-db-blocks
  (testing "Analytical query in which Where clause queries across db blocks")
  (let [analytical-query {:select "?nums",
                          :where
                          [["$fdb4" ["person/handle" "zsmith"] "person/favNums" "?nums"]
                           ["$fdb4" ["person/handle" "jdoe"] "person/favNums" "?nums"]]}
        ledger           (test/rand-ledger test/ledger-chat
                                           {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db               (fdb/db (:conn test/system) ledger {:syncTo block})

        res              (<?? (fdb/query-async db analytical-query))]

    (is (every? number? res))))

;; TODO: Write wikidata test w/ :online metadata
