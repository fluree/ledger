(ns fluree.db.ledger.query.basic-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as fdb]
            [fluree.db.util.core :as utils]
            [fluree.db.util.async :refer [<?? <? go-try]]
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
  [ledger-name block queries]
  (let [db (fdb/db (:conn test/system) ledger-name {:syncTo block})]
    (go-try
      (loop [results []
             [q & r] queries]
        (let [resp    (<? (fdb/query-async db q))
              results (conj results resp)]
          (if r
            (recur results r)
            results))))))

(deftest select-chats
  (testing "Select all chats"
    (let [chat-query {:select ["*"] :from "chat"}
          ledger     (test/rand-ledger
                       test/ledger-chat
                       {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db         (fdb/db (:conn test/system) ledger {:syncTo block})
          chats      (<?? (fdb/query-async db chat-query))]

      ;; should be 3 chats
      (is (= 3 (count chats)))

      ;; the keys for every chat should be _id, message, person, instant, or comments
      (is (every?
            (fn [chat]
              (every?
                #{:_id :chat/message :chat/person :chat/instant :chat/comments}
                (->> chat keys (map keyword))))
            chats)))))


(deftest select-from-subject-id
  (testing "Select from chat, subject-id"
    (let [chat-base {:selectOne {"?chats" ["*"]},
                     :where     [["?chats", "chat/person", "?person"],
                                 ["?person", "person/handle", "jdoe"]]} ; get ids of interest
          ledger    (test/rand-ledger
                      test/ledger-chat
                      {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db        (fdb/db (:conn test/system) ledger {:syncTo block})
          base-res  (<?? (fdb/query-async db chat-base))
          person-id (-> base-res (get "chat/person") :_id)
          chat      (<??
                      (fdb/query-async
                        db {:selectOne ["person/handle"] :from person-id}))]

      (is (= (get chat "person/handle") "jdoe")))))

(deftest select-from-two-tuple
  (testing "Select from two tuple, [\"person/handle\" \"jdoe\"]"
    (let [chat-query {:select ["*"] :from ["person/handle" "jdoe"]}
          ledger     (test/rand-ledger test/ledger-chat
                                       {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db         (fdb/db (:conn test/system) ledger {:syncTo block})
          res        (->> chat-query
                          (fdb/query-async db)
                          <??
                          first)]

      (is (= (get res "person/handle") "jdoe"))

      (is (= (get res "person/fullName") "Jane Doe"))

      (is (= (get res "person/age") 25))

      (is (= (count (get res "person/favNums")) 5))

      (is (= (count (get res "person/favArtists")) 3))

      (is (= (count (get res "person/favMovies")) 3))

      ; check ids
      (let [id   (:_id res)
            res' (<?? (fdb/query-async db {:selectOne ["*"]
                                           :from      id}))]
        (is (= (get res "person/handle") (get res' "person/handle")))
        (is (= (get res "person/follows") (get res' "person/follows")))))))

(deftest select-from-group-of-subjects
  (testing "Select from a group of subjects"
    (let [subj1-id 369435906932737
          subj2-id 387028092977153
          chat-query  {:select ["*"]
                       :from [subj1-id ["person/handle" "jdoe"]
                              subj2-id ["person/handle" "zsmith"]]}
          ledger      (test/rand-ledger test/ledger-chat
                                        {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db          (fdb/db (:conn test/system) ledger {:syncTo block})
          res         (<?? (fdb/query-async db chat-query))]

      (is (subset? #{subj1-id subj2-id} (set (map :_id res)))
          (str "Unexpected query result: " (pr-str res)))

      (is (= (-> (map #(get % "person/handle") res) set) #{nil "jdoe" "zsmith"})
          (str "Unexpected query result: " (pr-str res))))))


(deftest select-from-predicate
  (testing "Select from a particular predicate"
    (let [chat-query {:select ["*"]
                      :from   "person/handle"}
          ledger     (test/rand-ledger
                       test/ledger-chat
                       {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db         (fdb/db (:conn test/system) ledger {:syncTo block})
          res        (<?? (fdb/query-async db chat-query))]

      (is (= 5 (count res)))

      (is (every? #(contains? % "person/handle") res)))))


(deftest select-certain-predicates
  (testing "Select certain predicates"
    (let [chat-query {:select ["chat/message", "chat/person"]
                      :from   "chat"
                      :limit  100}
          ledger     (test/rand-ledger
                       test/ledger-chat
                       {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db         (fdb/db (:conn test/system) ledger {:syncTo block})
          res        (<?? (fdb/query-async db chat-query))]

      (is (every? (fn [chat]
                    (map #(-> (#{"chat/message" "chat/person"} %) boolean) (keys chat))) res)))))


(deftest select-with-where
  (testing "Select with where"
    (let [chat-query {:select ["chat/message" "chat/instant"]
                      :where  "chat/instant > 1517437000000"}
          ledger     (test/rand-ledger
                       test/ledger-chat
                       {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db         (fdb/db (:conn test/system) ledger {:syncTo block})
          res        (<?? (fdb/query-async db chat-query))]

      (is (= 3 (count res)))

      (is (= #{"chat/message" :_id "chat/instant"}
             (-> (apply merge res) keys set))))))


(deftest select-as-of-block
  (testing "Select as of block"
    (let [chat-query   {:select ["*"]
                        :from   "chat"
                        :block  2}
          ledger       (test/rand-ledger
                         test/ledger-chat
                         {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db           (fdb/db (:conn test/system) ledger {:syncTo block})
          res          (<?? (fdb/query-async db chat-query))
          chat-query-4 {:select ["*"]
                        :from   "chat"
                        :block  4}
          res-4        (<?? (fdb/query-async db chat-query-4))]


      (is (empty? res))

      (is (= 3 (count res-4))))))


(deftest select-with-limit-and-offset
  (testing "Select with limit and offset"
    (let [limit-query            {:select ["*"]
                                  :from   "chat"
                                  :limit  2}
          offset-query           {:select ["*"]
                                  :from   "chat"
                                  :offset 1}
          limit-and-offset-query {:select ["*"]
                                  :from   "chat"
                                  :limit 1, :offset 1}
          ledger                 (test/rand-ledger
                                   test/ledger-chat
                                   {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          all-res                (<?? (issue-consecutive-queries
                                        ledger
                                        block
                                        [limit-query offset-query limit-and-offset-query]))]

      (is (= (-> (map count all-res) sort vec) [1 2 2])))))


(deftest select-with-groupBy
  (testing "Select with groupBy"
    (let [base-query     {:select  {"?person" ["handle", "favNums"]}
                          :where   [["?person" "person/handle" "?handle"]]
                          :orderBy ["ASC" "handle"]}
          groupBy-query  {:select  ["?handle", "(as (avg ?nums) avg)"],
                          :where   [["?person", "person/handle", "?handle"],
                                    ["?person", "person/favNums", "?nums"]],
                          :groupBy "?handle",
                          :opts    {:prettyPrint true}}
          groupBy-query' {:select  ["?handle", "(as (avg ?nums) avg)"],
                          :where   [["?person", "person/handle", "?handle"],
                                    ["?person", "person/favNums", "?nums"]],
                          :groupBy ["?handle"],
                          :opts    {:prettyPrint true}}
          ledger         (test/rand-ledger
                           test/ledger-chat
                           {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db             (fdb/db (:conn test/system) ledger {:syncTo block})
          ; groupBy on favNums excludes any entries without a favNum
          ; exclude these from base set for validation purposes
          base-res       (<?? (fdb/query-async db base-query))
          base-res*      (reduce-kv (fn [m k v]
                                      (if-let [favNums (get v "favNums")]
                                        (assoc m (get v "handle")
                                                 (merge v
                                                        {:index   k
                                                         :count   (count favNums)
                                                         :avg     (average favNums)
                                                         :avg-dec (average-decimal favNums)}))
                                        m))
                                    {} base-res)
          groupBy-res    (<?? (fdb/query-async db groupBy-query))
          groupBy-res'   (<?? (fdb/query-async db groupBy-query'))]

      ; validate string groupBy
      (is (= (-> groupBy-res keys count) (-> base-res* keys count))) ;validate # of handles - groupBy is a subset
      (is (empty?
            (utils/without-nils
              (loop [[k & r-k] (keys base-res*)
                     [v & r-v] (vals base-res*)
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
      (is (= (-> groupBy-res' keys count) (-> base-res* keys count))) ;validate # of handles - groupBy is a subset
      (is (empty?
            (utils/without-nils
              (loop [[k & r-k] (keys base-res*)
                     [v & r-v] (vals base-res*)
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
                                                     "; observed: " obs-vals))))))))))))))


(deftest select-boolean-predicates
  (let [ledger (test/rand-ledger
                 test/ledger-chat
                 {:http/schema ["chat.edn" "chat-preds.edn"]})
        {:keys [block]} (test/transact-data ledger "chat.edn")
        db     (fdb/db (:conn test/system) ledger {:syncTo block})]
    (testing "with true predicate"
      (let [query {:select ["?person"] :from "person"
                   :where  [["?person" "person/active" true]]}
            res   (<?? (fdb/query-async db query))]
        (is (= 4 (count res)))))
    (testing "with false predicate"
      (let [query {:select ["?person"] :from "person"
                   :where  [["?person" "person/active" false]]}
            res   (<?? (fdb/query-async db query))]
        (is (= 1 (count res)))))))


(deftest block-and-tx-queries
  (testing "_block and _tx query should return same result"
    (let [q-tx      {:select ["*"] :from "_tx"}
          q-block   {:select ["*"] :from "_block"}
          ledger    (test/rand-ledger
                      test/ledger-chat
                      {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db        (fdb/db (:conn test/system) ledger {:syncTo block})
          res-tx    @(fdb/query db q-tx)
          res-block @(fdb/query db q-block)
          _tx-sids  (map :_id res-tx)]
      (is (= res-tx res-block) "_tx and _block query results should be identical")
      (is (= _tx-sids
             (range -1 (-> _tx-sids last dec) -1))
          "All _tx sids are all negative, decrementing by 1, ending -1"))))


(deftest stringified-opts
  (testing "opts keys with keyword and string keys"
    (let [q1     {:select ["*"]
                  :from   "_tx"
                  :limit  1}
          q2     {:select ["*"]
                  :from   "_tx"
                  :opts   {:limit 1}}
          q3     {:select ["*"]
                  :from   "_tx"
                  :opts   {"limit" 1}}
          ledger (test/rand-ledger
                   test/ledger-chat
                   {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db     (fdb/db (:conn test/system) ledger {:syncTo block})
          res-q1 @(fdb/query db q1)
          res-q2 @(fdb/query db q2)
          res-q3 @(fdb/query db q3)]
      (is (= res-q1 res-q2 res-q3) ":limit 1, {:opts {:limit 1}} and {:opts {'limit' 1}} are identical"))))


(deftest limit+orderBy
  (testing "orderBy query with limit should order first, then limit"
    (let [q-all     {:select  ["*"]
                     :from    "_tx"
                     :orderBy ["DESC", "_block/instant"]}
          q-limit   {:select  ["*"]
                     :from    "_tx"
                     :limit   1
                     :orderBy ["DESC", "_block/instant"]}
          ledger    (test/rand-ledger
                      test/ledger-chat
                      {:http/schema ["chat.edn" "chat-preds.edn"]})
          {:keys [block]} (test/transact-data ledger "chat.edn")
          db        (fdb/db (:conn test/system) ledger {:syncTo block})
          last-tx   @(fdb/query db {:select "(min ?s)"
                                    :where  [["?s" "rdf:type" "_tx"]]})
          res-all   @(fdb/query db q-all)
          res-limit @(fdb/query db q-limit)]
      (is (= last-tx (-> res-all first :_id)) "Latest _tx sid should be same as first result for all _tx query sorted descending")
      (is (= res-limit [(-> res-all first)]) "Ordered query results with :limit 1 should be same as (first <no limit ordered results>)"))))
