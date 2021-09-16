(ns fluree.db.ledger.docs.query.analytical-query
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [fluree.db.util.log :as log]))

(use-fixtures :once test/test-system)

(deftest analytical-with-prefix-two-tuple-subject
  (testing "Analytical query with prefix, with two-tuple subject")
  (let [crawl-query      {:select "?nums"
                          :where [["$fdb", ["person/handle", "zsmith"], "person/favNums", "?nums"]]}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= (set res) (set [5 645 28 -1 1223])))))

(deftest analytical-with-two-tuple-subject
  (testing "Analytical query with two-tuple subject")
  (let [crawl-query      {:select "?nums"
                          :where [[["person/handle", "zsmith"], "person/favNums", "?nums"]]}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= (set res) (set [5 645 28 -1 1223])))))


(deftest analytical-with-two-clauses
  (testing "Analytical query with two clauses")
  (let [crawl-query      {:select "?nums"
                          :where [["?person", "person/handle", "zsmith"],
                                   ["?person", "person/favNums", "?nums"]]}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= (set res) (set [5 645 28 -1 1223])))))

(deftest analytical-with-nil-subject
  (testing "Analytical query with nil subject")
  (let [crawl-query      {:select "?nums",
                          :where [ ["$fdb", nil, "person/favNums", "?nums"] ]}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= (set res) (set [7 1223 -2 28 12 9 0 1950 5 645 -1 2 98])))))

(deftest analytical-with-prefix-two-clauses-two-tuple-subject
  (testing "Analytical query with two clauses, two bound variables, two-tuple subjects")
  (let [crawl-query       {:select  ["?nums1", "?nums2"]
                           :where [[["person/handle", "zsmith"],
                                    "person/favNums", "?nums1"],
                                   [["person/handle", "jdoe"],
                                    "person/favNums", "?nums2"] ]}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= (first (map set (apply (partial map vector) res))) #{-1 645 1223 28 5}))
    (is (= (last (map set (apply (partial map vector) res))) #{0 -2 1223 12 98}))))


(deftest analytical-with-prefix-two-clauses-two-tuple-subject-matching
  (testing "Analytical query with two clauses, one bound variable, filtered between two two-tuple subjects")
  (let [crawl-query      { :select "?nums"
                          :where [ ["$fdb", ["person/handle", "zsmith"], "person/favNums", "?nums"],
                                  ["$fdb", ["person/handle", "jdoe"], "person/favNums", "?nums"]]}
        db  (basic/get-db test/ledger-chat)
        res  (async/<!! (fdb/query-async db crawl-query))]
    (is (= res [1223]))))

(deftest analytical-select-one-with-aggregate-sum
  (testing "Analytical query with Select One clause using Aggregate sum operand")
  (let [crawl-query     {:select "(sum ?nums)",
                         :where [[["person/handle" "zsmith"] "person/favNums" "?nums"]]}
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db crawl-query))]
    (is (= res (reduce + [5 645 28 -1 1223])))))

;;TODO - Fix why sample 10 only returning 9
(deftest analytical-select-one-with-aggregate-sample
  (testing "Analytical query with Select One clause using Aggregate sample operand")
  (let [sample-query     {:select "(sample 10 ?nums)",
                         :where [[nil "person/favNums" "?nums"]]}
        total-query      {:select "?nums",
                          :where [[nil "person/favNums" "?nums"]]}
        db  (basic/get-db test/ledger-chat)
        sample-res (async/<!! (fdb/query-async db sample-query))
        total-res (async/<!! (fdb/query-async db total-query))]
    (is (= 10 (count sample-res)))
    (is (= (every? #(contains? (set total-res) %) sample-res) true))))

(deftest analytical-reverse-crawl-from-bound-variable
  (testing "Analytical query that reverse-crawls from bound-variable predicate to all subjects with that predicate")
  (let [crawl-query     {:select {:?artist ["*" {:person/_favArtists ["*"]}]},
                         :where [[nil "person/favArtists" "?artist"]]}
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db crawl-query))]
    (is (vector? res))
    (is (contains? (first res) :_id))
    (is (contains? (first res) "artist/name"))
    (is (contains? (first res) "person/_favArtists"))
    (is (vector? (get (first res) "person/_favArtists")))))

(deftest analytical-where-clause-optional-option
  (testing "Analytical query Optional option in Where clause")
  (let [analytical-query     {:select "?person",
                         :where [["$fdb" "?person" "person/fullName" "?fullName" {:optional true}]
                                 ]}
        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db analytical-query))]
    (is (coll? res))
    (is (integer? (first res)))))

(deftest analytical-where-clause-filter-option
  (testing "Analytical query Filter option in Where clause")
  (let [analytical-query     {:select {:?person ["person/handle" "person/favNums"]},
                              :where
                                      [["$fdb"
                                        "?person"
                                        "person/favNums"
                                        "?nums"
                                        {:filter "(> ?nums 1000)"}]]}

        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db analytical-query))]

    (is (not (contains? (set (map #(get (first %) "person/handle") res)) "anguyen")))
    ;because ["person/handle", "anguyen"] only has nums < 1000

    (is (every? number? (get (first (first res)) "person/favNums")))))

;TODO: build out similar tests for other filter operators

(deftest analytical-across-sources-db-blocks
  (Thread/sleep 5000)
  (testing "Analytical query in which Where clause queries across db blocks")
  (let [analytical-query     {:select "?nums",
                              :where
                                      [["$fdb4" ["person/handle" "zsmith"] "person/favNums" "?nums"]
                                       ["$fdb4" ["person/handle" "jdoe"] "person/favNums" "?nums"]]}


        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db analytical-query))]

    (is (every? number? res))))

(deftest analytical-across-sources-wikidata
  (testing "Analytical query in which Where clause queries across sources that include wikidata")
  (let [analytical-query     {:select ["?name" "?artist" "?artwork" "?artworkLabel"],
                              :where
                                      [[["person/handle" "jdoe"] "person/favArtists" "?artist"]
                                       ["?artist" "artist/name" "?name"]
                                       ["$wd" "?artwork" "wdt:P170" "?creator"]
                                       ["$wd" "?creator" "?label" "?name"]]}


        db  (basic/get-db test/ledger-chat)
        res (async/<!! (fdb/query-async db analytical-query))]

    (log/info "WIKIDATA: " res)
    ;TODO: FINISH TEST!!!
    ))


(deftest analytical-query-test
  (analytical-with-prefix-two-tuple-subject)
  (analytical-with-two-tuple-subject)
  (analytical-with-two-clauses)
  (analytical-with-nil-subject)
  (analytical-with-prefix-two-clauses-two-tuple-subject)
  (analytical-with-prefix-two-clauses-two-tuple-subject-matching)
  ;(analytical-select-one-with-aggregate-sum)
  ;(analytical-select-one-with-aggregate-sample)
  (analytical-reverse-crawl-from-bound-variable)
  (analytical-where-clause-optional-option)
  (analytical-where-clause-filter-option)
  (analytical-across-sources-db-blocks)
  ;(analytical-across-sources-wikidata)
  )

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (analytical-query-test))


(comment





  { :selectOne "(sum ?nums)"
   :where [[["person/handle", "zsmith"], "person/favNums", "?nums"]] }

  { :selectOne "(sample 10 ?nums)",
   :where [[nil, "person/favNums", "?nums"]]}

  { :select {"?artist" ["*", {"person/_favArtists" ["*"]}]},
   :where [ [nil, "person/favArtists", "?artist"]]}

  ; tx first

  [{ :_id ["person/handle", "zsmith"]
    :favNums [100] }]

  ; then query
  { :select "?nums"
   :where [ ["$fdb4", ["person/handle", "zsmith"], "person/favNums", "?nums"], ["$fdb5", ["person/handle", "zsmith"], "person/favNums", "?nums"] ] }

  { :select  ["?name", "?artist", "?artwork", "?artworkLabel"],
   :where [[["person/handle", "jdoe"], "person/favArtists", "?artist"],
                     ["?artist", "artist/name", "?name"],
                     ["$wd", "?artwork", "wdt:P170", "?creator"],
                     ["$wd", "?creator", "?label", "?name"]] }


  { :select ["?handle", "?title", "?narrative_location"],
   :where [ ["?user", "user/favMovies", "?movie"],
                     ["?movie", "movie/title", "?title"],
                     ["$wd", "?wdMovie", "?label", "?title"],
                     ["$wd", "?wdMovie", "wdt:P840", "?narrative_location"],
                     ["$wd", "?wdMovie", "wdt:P31", "wd:Q11424"],
                     ["?user", "user/handle", "?handle"]]}

  {:select ["?name", "?artist", "?artwork"],
   :where [[["person/handle", "jdoe"], "person/favArtists", "?artist"],
           ["?artist", "artist/name", "?name"],
           ["$wd", "?artwork", "wdt:P170", "?creator", {"limit" 5, "distinct" false}],
           ["$wd", "?creator", "?label", "?name"]]}


  )
