(ns fluree.db.ledger.query.analytical-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as fdb]
            [fluree.db.util.async :refer [<??]]
            [clojure.core.async :as async]
            [clojure.set :as set]))

(use-fixtures :once test/test-system)

(deftest with-prefix-two-tuple-subject
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

(deftest with-two-tuple-subject
  (testing "Analytical query with two-tuple subject"
    (let [crawl-query {:select "?nums"
                       :where  [[["person/handle", "zsmith"], "person/favNums", "?nums"]]}
          ledger      (test/rand-ledger test/ledger-chat
                                        {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db          (fdb/db (:conn test/system) ledger {:syncTo block})
          res         (<?? (fdb/query-async db crawl-query))]
      (is (= #{5 645 28 -1 1223} (set res))))))

(deftest with-two-clauses
  (testing "Analytical query with two clauses"
    (let [crawl-query {:select "?nums"
                       :where  [["?person", "person/handle", "zsmith"],
                                ["?person", "person/favNums", "?nums"]]}
          ledger      (test/rand-ledger test/ledger-chat
                                        {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db          (fdb/db (:conn test/system) ledger {:syncTo block})
          res         (<?? (fdb/query-async db crawl-query))]
      (is (= #{5 645 28 -1 1223} (set res))))))

(deftest with-nil-subject
  (testing "Analytical query with nil subject"
    (let [crawl-query  {:select "?nums",
                        :where  [["$fdb", nil, "person/favNums", "?nums"]]}
          ledger       (test/rand-ledger test/ledger-chat
                                         {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db           (fdb/db (:conn test/system) ledger {:syncTo block})
          res          (<?? (fdb/query-async db crawl-query))
          expected-set #{7 1223 -2 28 12 9 0 1950 5 645 -1 2 98 42}
          actual-set   (set res)]
      (is (= expected-set actual-set)
          (if (set/subset? expected-set actual-set)
            (str "Additional favNum(s) in query results: "
                 (set/difference actual-set expected-set))
            (str "Missing favNum(s) in query results: "
                 (set/difference expected-set actual-set)))))))

(deftest with-prefix-two-clauses-two-tuple-subject
  (testing "Analytical query with two clauses, two bound variables, two-tuple subjects"
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
      (is (= #{0 -2 1223 12 98} (set (map second res)))))))

(deftest with-prefix-two-clauses-two-tuple-subject-matching
  (testing "Analytical query with two clauses, one bound variable, filtered between two two-tuple subjects"
    (let [crawl-query {:select "?nums"
                       :where  [["$fdb", ["person/handle", "zsmith"], "person/favNums", "?nums"],
                                ["$fdb", ["person/handle", "jdoe"], "person/favNums", "?nums"]]}
          ledger      (test/rand-ledger test/ledger-chat
                                        {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db          (fdb/db (:conn test/system) ledger {:syncTo block})
          res         (<?? (fdb/query-async db crawl-query))]
      (is (= [1223] res)))))

(deftest select-one-with-aggregate-sum
  (testing "Analytical query with Select One clause using Aggregate sum operand"
    (let [crawl-query {:select "(sum ?nums)",
                       :where  [[["person/handle" "zsmith"] "person/favNums" "?nums"]]}
          ledger      (test/rand-ledger test/ledger-chat
                                        {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db          (fdb/db (:conn test/system) ledger {:syncTo block})
          res         (<?? (fdb/query-async db crawl-query))]
      (is (= (reduce + [5 645 28 -1 1223]) res)))))

(deftest select-one-with-aggregate-sample
  (testing "Analytical query with select clause using Aggregate sample operand"
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
      (is (every? (set total-res) sample-res)))))

(deftest reverse-crawl-from-bound-variable
  (testing "Analytical query that reverse-crawls from bound-variable predicate to all subjects with that predicate"
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
      (is (vector? (get (first res) "person/_favArtists"))))))


(deftest where-clause-filter-option
  (testing "Analytical query Filter option in Where clause"
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

      (is (every? number? (get (first (first res)) "person/favNums"))))))

;; TODO: build out similar tests for other filter operators

(deftest across-sources-db-blocks
  (testing "Analytical query in which Where clause queries across db blocks"
    (let [analytical-query {:select "?nums",
                            :where
                            [["$fdb4" ["person/handle" "zsmith"] "person/favNums" "?nums"]
                             ["$fdb4" ["person/handle" "jdoe"] "person/favNums" "?nums"]]}
          ledger           (test/rand-ledger test/ledger-chat
                                             {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db               (fdb/db (:conn test/system) ledger {:syncTo block})

          res              (<?? (fdb/query-async db analytical-query))]

      (is (every? number? res)))))

;; TODO: Write wikidata test w/ :online metadata

(deftest with-supplied-vars
  (testing "query with supplied vars should resolve correctly"
    (let [query  {:where  [["?p" "person/handle" "?handle"]
                           ["?p" "person/favNums" "?favNums"]
                           ["?p" "person/user" "?username"]]
                  :select {"?p" ["handle" "favNums" "user"]}
                  :vars   {"?username" ["_user/username" "jake"]}}
          ledger (test/rand-ledger test/ledger-chat
                                   {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db     (fdb/db (:conn test/system) ledger {:syncTo block})
          res    (<?? (fdb/query-async db query))]
      (is (= [{"handle"  "jakethesnake", :_id 351843720888324
               "favNums" [7 42], "user" {:_id 87960930223081}}]
             res)
          (str "Unexpected query result: " (pr-str res))))))

(deftest with-order-by-variable
  (testing "ordering by a variable should work"
    (let [query  {:where  [["?p" "person/handle" "?handle"]
                           ["?p" "person/age" "?age"]]
                  :select {"?p" ["handle" "age"]}
                  :opts   {:orderBy ["DESC" "?age"]}}
          ledger (test/rand-ledger test/ledger-chat
                                   {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db     (fdb/db (:conn test/system) ledger {:syncTo block})
          res    (<?? (fdb/query-async db query))]
      (is (= [{"handle" "dsanchez", :_id 351843720888323, "age" 70}
              {"handle" "zsmith", :_id 351843720888321, "age" 63}
              {"handle" "anguyen", :_id 351843720888322, "age" 34}
              {"handle" "jakethesnake", :_id 351843720888324, "age" 29}
              {"handle" "jdoe", :_id 351843720888320, "age" 25}]
             res)
          (str "Unexpected query result: " (pr-str res)))))

  (testing "orderBy variable in optional clause should work"
    (let [query       {:where  [["?p" "rdf:type" "_predicate"]
                                {:optional [["?p" "_predicate/doc" "?doc"]]}]
                       :select ["?p" "?doc"]
                       :opts   {:orderBy "?doc"}}
          ledger      (test/rand-ledger "test/order-by-var-optional")
          db          (fdb/db (:conn test/system) ledger)
          res         (<?? (fdb/query-async db query))
          docs        (map second res)
          sorted-docs (sort docs)]
      (is (= sorted-docs docs)
          (str "Results were not ordered by ?doc: " (pr-str res)))))

  (testing "orderBy variable in binding should work"
    ;; This isn't a very meaningful test b/c ?maxFavNums is global and thus
    ;; will be the same value for every tuple. Not a very good value to order
    ;; by. Mostly we're just testing that this doesn't throw an exception.
    (let [query           {:where  [["?p" "person/handle" "?handle"]
                                    ["?p" "person/favNums" "?favNums"]
                                    ["?maxFavNums" "#(max ?favNums)"]]
                           :select ["?handle" "?maxFavNums"]
                           :opts   {:orderBy "?maxFavNums"}}
          ledger          (test/rand-ledger "test/order-by-var-binding"
                                            {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db              (fdb/db (:conn test/system) ledger {:syncTo block})
          res             (<?? (fdb/query-async db query))
          max-fav-nums    (map second res)
          sorted-fav-nums (sort max-fav-nums)]
      (is (= sorted-fav-nums max-fav-nums)
          (str "Results were not ordered by ?maxFavNums: " (pr-str res)))))

  (testing "orderBy variable in union should work"
    (let [query      {:where  [{:union [[["?p" "_predicate/type" "string"]]
                                        [["?p" "_predicate/type" "tag"]]]}]
                      :select "?p"
                      :opts   {:orderBy "?p"}}
          ledger     (test/rand-ledger "test/order-by-union")
          db         (fdb/db (:conn test/system) ledger)
          res        (<?? (fdb/query-async db query))
          sorted-res (sort res)]
      (is (= sorted-res res)
          (str "Results were not ordered by ?p" (pr-str res))))))

(deftest order-by-limit-offset
  (testing "orderBy query"
    (let [ledger          (test/rand-ledger test/ledger-chat
                                            {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db              (fdb/db (:conn test/system) ledger {:syncTo block})
          query-all       {:select  {"?e" ["person/handle"
                                           {"comment/_person" {"_as" "comments"}}]}
                           :where   [["?e" "person/favNums" "?favNums"]]
                           :orderBy "person/handle"}
          all-results     (async/<!! (fdb/query-async db query-all))
          total-count     (count all-results)]

      (testing "with limit"
        (let [limit       1
              query-limit (assoc query-all :limit limit)
              subject     (async/<!! (fdb/query-async db query-limit))]

          (is (= (count subject) limit)
              "returns subjects only up to the limit")

          (testing "and with offset"
            (let [offset       1
                  query-offset (assoc query-limit :offset offset)
                  subject      (async/<!! (fdb/query-async db query-offset))]

              (is (= (count subject) limit)
                  "returns subjects only up to the limit")

              (is (= (first subject) (second all-results))
                  "returns only subjects after the offset"))))))))

(deftest order-by-group-by
  (testing "orderBy works with groupBy on a different value"
    (let [ledger  (test/rand-ledger test/ledger-chat)
          _       (<?? (fdb/transact-async (:conn test/system) ledger
                                           [{:_id                "_predicate"
                                             :name               "_user/type"
                                             :type               "string"
                                             :restrictCollection "_user"}]))
          {:keys [block]} (<?? (fdb/transact-async (:conn test/system) ledger
                                                   [{:_id            "_user$user2"
                                                     :_user/username "delta"
                                                     :_user/type     "dog"}
                                                    {:_id            "_user$user1"
                                                     :_user/username "zeta"
                                                     :_user/type     "dog"}
                                                    {:_id            "_user$user3"
                                                     :_user/username "beta"
                                                     :_user/type     "dog"}
                                                    {:_id            "_user$user4"
                                                     :_user/username "epsilon"
                                                     :_user/type     "person"}
                                                    {:_id            "_user$user5"
                                                     :_user/username "gamma"
                                                     :_user/type     "person"}
                                                    {:_id            "_user$user6"
                                                     :_user/username "alpha"
                                                     :_user/type     "person"}]))
          db      (fdb/db (:conn test/system) ledger {:syncTo block})
          query   {:select  ["?user" "?username" "?type"]
                   :where   [["?user" "_user/username" "?username"]
                             ["?user" "_user/type" "?type"]]
                   :orderBy "?username"
                   :groupBy "?type"}
          results (<?? (fdb/query-async db query))]
      (is (= {"person"
              [[87960930223086 "alpha" "person"]
               [87960930223084 "epsilon" "person"]
               [87960930223085 "gamma" "person"]]
              "dog"
              [[87960930223083 "beta" "dog"]
               [87960930223081 "delta" "dog"]
               [87960930223082 "zeta" "dog"]]}
             results)))))

(deftest with-filter-variable
  (testing "filter with variable in where clause works"
    (let [query  {:select ["?name" "?isIndexed"]
                  :where  [["?predicate" "_predicate/name" "?name"]
                           ["?predicate" "_predicate/index" "?isIndexed"]
                           {:filter ["(nil? ?isIndexed)"]}]}
          ledger (test/rand-ledger "test/filter-optional-var")
          db     (fdb/db (:conn test/system) ledger)
          res    (<?? (fdb/query-async db query))]
      (is (= [] res))))
  (testing "filter with variable in optional clause works"
    (let [query  {:select ["?name" "?isIndexed"]
                  :where  [["?predicate" "_predicate/name" "?name"]
                           {:optional [["?predicate" "_predicate/index" "?isIndexed"]]}
                           {:filter ["(nil? ?isIndexed)"]}]}
          ledger (test/rand-ledger "test/filter-optional-var")
          db     (fdb/db (:conn test/system) ledger)
          res    (<?? (fdb/query-async db query))]
      (is (vector? res))
      (is (every? #(-> % second nil?) res)))))
