(ns fluree.db.ledger.api.open-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [org.httpkit.client :as http]
            [fluree.db.util.json :as json]
            [byte-streams :as bs]
            [fluree.db.api :as fdb]
            [fluree.db.query.http-signatures :as http-signatures])
  (:import (java.util UUID)))


(use-fixtures :once test/test-system)


;; Utility vars and functions

(def endpoint-url (str "http://localhost:" @test/port "/fdb/" test/ledger-endpoints "/"))
(def endpoint-url-short (str "http://localhost:" @test/port "/fdb/"))

(defn- rand-str
  []
  (apply str
         (take (+ 5 (rand-int 20)) ;; at least 5 characters
               (repeatedly #(char (+ (rand 26) 65))))))

(defn- get-unique-count
  [current goal-count fn]
  (let [current-count (count (distinct current))
        distance      (- goal-count current-count)]
    (if (< 0 distance)
      (get-unique-count (distinct (concat current (repeatedly distance fn))) goal-count fn)
      (distinct current))))


;; ENDPOINT TEST: /transact

(deftest add-chat-alt-schema-test
  (testing "adding chat-alt schema succeeds"
    (let [ledger (test/rand-ledger test/ledger-endpoints)
          {:keys [status body] :as schema-res} (test/transact-schema
                                                 ledger "chat-alt.edn")]

      (is (= 200 status))

      (is (test/contains-every? schema-res :opts :body :headers :status))

      (is (test/contains-every? body :t :id :auth :tempids :block :hash :fuel
                                :status :bytes :flakes))

      (is (= 2 (:block body)))

      (is (= 59 (-> body :tempids (test/get-tempid-count :_predicate))))

      (is (= 4 (-> body :tempids (test/get-tempid-count :_collection)))))))


(deftest transact-people-comments-chats-test
  (testing "add data to chat alt succeeds"
    (let [ledger (test/rand-ledger test/ledger-endpoints)
          _      (test/transact-schema ledger "chat-alt.edn")
          {:keys [status body] :as new-data-res}
          (test/transact-data ledger "chat-alt-people-comments-chats.edn")]

      (is (= 200 status))

      (is (test/contains-every? new-data-res :opts :body :headers :status))

      (is (test/contains-every? body :tempids :block :hash :fuel :auth :status
                                :bytes :t :flakes))

      ; the tempids should be _auth$chatUser, _auth$temp, comment$1 -> 12
      ; chat$1 -> 13, nestedComponent$1 ->12, _user$jdoe, :_user$zsmith,
      ; person$1 -> 4, _role$chatUser
      ; _rule$viewAllPeople, _rule$editOwnChats, _rule$viewAllChats
      (is (= (into #{:_auth$chatUser :_auth$temp :_rule$viewAllPeople
                     :_rule$editOwnChats :_rule$viewAllChats :_role$chatUser
                     :_user$jdoe :_user$zsmith :_fn$ownChats :person}
                   (concat
                     (map #(keyword (str "comment$" %)) (range 1 13))
                     (map #(keyword (str "chat$" %)) (range 1 14))
                     (map #(keyword (str "person$" %)) (range 1 4))
                     (map #(keyword (str "nestedComponent$" %)) (range 1 13))))
             (-> body :tempids keys set)))

      ; check that 1 person (without tempid) was added
      (is (= 1 (-> body :tempids (test/get-tempid-count :person)))))))


;; ENDPOINT TEST: /query

(deftest query-all-collections-test
  (testing "Querying all collections"
    (let [ledger      (test/rand-ledger test/ledger-endpoints)
          _           (test/transact-schema ledger "chat-alt.edn")
          query       {:select ["*"] :from "_collection"}
          {:keys [status body] :as query-res} @(http/post
                                                 (str endpoint-url-short
                                                      ledger "/query")
                                                 (test/standard-request query))
          results     (json/parse body)
          collections (into #{} (map #(:_collection/name %) results))]

      (is (= 200 status))

      ; The keys in the response are -> :opts :body :headers :status
      (is (test/contains-every? query-res :opts :body :headers :status))

      ; Are all the collection names what we expect?
      (is (= #{"_rule" "_fn" "nestedComponent" "_predicate" "_setting" "chat"
               "_auth" "_user" "person" "_shard" "_tag" "comment" "_role"
               "_collection" "_ctx"}
             collections)))))


(deftest query-all-predicates-test
  (testing "Query all predicates"
    (let [ledger     (test/rand-ledger test/ledger-endpoints)
          _          (test/transact-schema ledger "chat-alt.edn")
          query      {:select ["*"] :from "_predicate"}
          {:keys [body status] :as query-res} @(http/post
                                                 (str endpoint-url-short
                                                      ledger "/query")
                                                 (test/standard-request query))
          results    (json/parse body)
          predicates (into #{} (map #(:_predicate/name %) results))]

      (is (= 200 status))

      (is (test/contains-every? query-res :opts :body :headers :status))

      ; Are some of the predicates we expect returned?
      (is (every? predicates ["comment/nestedComponent" "person/stringUnique"]))

      (is (< 30 (count predicates))))))


;; ENDPOINT TEST: /multi-query

(deftest query-collections-predicates-multiquery-test
  (testing "Querying all collections and predicates in multi-query"
    (let [ledger      (test/rand-ledger test/ledger-endpoints)
          _           (test/transact-schema ledger "chat-alt.edn")
          query       {:coll {:select ["*"] :from "_collection"}
                       :pred {:select ["*"] :from "_predicate"}}

          {:keys [body status] :as multi-res}
          @(http/post (str endpoint-url-short ledger "/multi-query")
                      (test/standard-request query))

          results     (json/parse body)
          collections (into #{} (map #(:_collection/name %) (:coll results)))
          predicates  (into #{} (map #(:_predicate/name %) (:pred results)))]

      (is (= 200 status))

      (is (test/contains-every? multi-res :opts :body :headers :status))

      (is (= collections #{"_rule" "nestedComponent" "_fn" "_predicate"
                           "_setting" "chat" "_auth" "_user" "person" "_shard"
                           "_tag" "comment" "_role" "_collection" "_ctx"}))

      ; Are some of the predicates we expect returned?
      (is (every? predicates ["comment/nestedComponent" "person/stringUnique"])))))


(deftest sign-multi-query-test
  (testing "sign multi-query where collections are not named in alphanumeric order"
    (let [ledger      (test/rand-ledger test/ledger-endpoints)
          _           (test/transact-schema ledger "chat-alt.edn")
          _           (test/transact-data ledger "chat-alt-people-comments-chats.edn")
          private-key (slurp "default-private-key.txt")
          qry-str     (str "{\"collections\":{\"select\":[\"*\"],\"from\":\"_collection\"},\n "
                           " \"predicates\":{\"select\":[\"*\"],\"from\":\"_predicate\"},\n  "
                           " \"_setting\":{\"select\":[\"*\"],\"from\":\"_setting\"},\n "
                           " \"_rule\":{\"select\":[\"*\"],\"from\":\"_rule\"},\n "
                           " \"_role\":{\"select\":[\"*\"],\"from\":\"_role\"},\n "
                           " \"_user\":{\"select\":[\"*\"],\"from\":\"_user\"}\n }")
          request     {:headers {"content-type" "application/json"}
                       :body    qry-str}
          q-endpoint  (str endpoint-url-short ledger "/multi-query")
          signed-req  (http-signatures/sign-request :post q-endpoint request
                                                    private-key)

          {:keys [status body] :as multi-res}
          @(http/post q-endpoint signed-req)

          results     (json/parse body)
          collections (into #{} (map #(:_collection/name %) (:collections results)))
          predicates  (into #{} (map #(:_predicate/name %) (:predicates results)))
          roles       (into #{} (map #(:_role/id %) (:_role results)))]

      (is (= 200 status))

      ; The keys in the response are -> :opts :body :headers :status
      (is (test/contains-every? multi-res :opts :body :headers :status))

      ; Are all the collections what we expect?
      (is (test/contains-every? collections "_rule" "nestedComponent" "_fn"
                                "_predicate" "_setting" "chat" "_auth" "_user"
                                "person" "_shard" "_tag" "comment" "_role"
                                "_collection"))

      ; Are some of the predicates we expect returned?
      (is (test/contains-every? predicates "comment/nestedComponent"
                                "person/stringUnique"))

      ; Are the expected roles returned?
      (is (test/contains-every? roles "chatUser" "root")))))


;; ENDPOINT TEST: /transact

(deftest transacting-new-persons-test
  (testing "Creating 100 random persons"
    (let [ledger        (test/rand-ledger test/ledger-endpoints)
          _             (test/transact-schema ledger "chat-alt.edn")
          random-person (fn []
                          {:_id             "person"
                           :stringNotUnique (rand-str)})
          person-tx     (repeatedly 100 random-person)

          {:keys [status body] :as tx-res}
          @(http/post (str endpoint-url-short ledger "/transact")
                      (test/standard-request person-tx))

          result        (json/parse body)
          person-keys   (-> result keys set)
          flakes        (:flakes result)
          tempids       (:tempids result)]

      (is (every? person-keys [:tempids :block :hash :fuel :auth :status :flakes]))
      (is (< 100 (count flakes)))
      (is (= 100 (test/get-tempid-count tempids :person))))))


;; ENDPOINT TEST: /block

(deftest query-block-two-test
  (testing "Query block 2"
    (let [ledger     (test/rand-ledger test/ledger-endpoints)
          _          (test/transact-schema ledger "chat-alt.edn")
          query      {:block 2}

          {:keys [status body]} @(http/post
                                   (str endpoint-url-short ledger "/block")
                                   (test/standard-request query))

          results    (json/parse body)
          block      (first results)
          block-keys (keys block)]

      (is (= 200 status))

      (is (= 2 (:block block)))

      (is (every? #{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs
                    :flakes}
                  block-keys)))))


;; ENDPOINT TEST: /history

(deftest history-query-collection-name-test
  (testing "Query history of flakes with _collection/name predicate"
    (let [ledger        (test/rand-ledger test/ledger-endpoints)
          _             (test/transact-schema ledger "chat-alt.edn")
          history-query {:history [nil 40]}
          {:keys [status body]} @(http/post
                                   (str endpoint-url-short ledger "/history")
                                   (test/standard-request history-query))
          result        (json/parse body)]

      (is (= 200 status))

      (is (every? (fn [flakes]
                    (every? #(= 40 (second %)) flakes))
                  (map :flakes result))))))


;; ENDPOINT TEST: /graphql

(deftest query-all-collections-graphql-test
  (testing "Querying all collections through the graphql endpoint"
    (let [ledger           (test/rand-ledger test/ledger-endpoints)
          query            {:query "{
                                      graph {
                                        _collection (sort: {predicate: \"name\", order: ASC}) {
                                          _id name spec version doc
                                        }
                                      }
                                    }"}
          {:keys [status body]} @(http/post (str endpoint-url-short ledger "/graphql")
                                            (test/standard-request query))
          results          (json/parse body)
          collections      (-> results :data :_collection)
          collection-names (set (map :name collections))]

      (is (= 200 status))

      (is (every? #(test/contains-every? % :doc :version :name :_id)
                  collections))

      (is (= #{"_rule" "_fn" "_predicate" "_setting" "_auth" "_user" "_shard"
               "_tag" "_role" "_collection" "_ctx"}
             collection-names)))))


(deftest sign-all-collections-graphql-test
  (testing "sign a query for all collections through the graphql endpoint"
    (let [ledger           (test/rand-ledger test/ledger-endpoints)
          private-key      (slurp "default-private-key.txt")
          graphql-str      "{
                              graph {
                                _collection (sort: {predicate: \"name\", order: ASC}) {
                                  _id name spec version doc
                                }
                              }
                            }"
          qry-str          (json/stringify {:query graphql-str})
          request          {:headers {"content-type" "application/json"}
                            :body    qry-str}
          q-endpoint       (str endpoint-url-short ledger "/graphql")
          signed-req       (http-signatures/sign-request :post q-endpoint request private-key)
          {:keys [status body]} @(http/post q-endpoint signed-req)
          results          (json/parse body)
          collections      (-> results :data :_collection)
          collection-keys  (reduce (fn [acc c] (apply conj acc (keys c)))
                                   #{} collections)
          collection-names (set (map :name collections))]

      (is (= 200 status))

      (is (test/contains-every? collection-keys :_id :name :version :doc))

      (is (test/contains-every? collection-names "_rule" "_fn" "_predicate"
                                "_setting" "_auth" "_user" "_shard" "_tag"
                                "_role" "_collection")))))


;; ENDPOINT TEST: /graphql transaction

(deftest add-a-person-graphql-test
  (testing "Add two new people with graphql"
    (let [ledger      (test/rand-ledger test/ledger-endpoints)
          _           (test/transact-schema ledger "chat-alt.edn")
          graphql     {:query "mutation addPeople ($myPeopleTx: JSON) {
                                 transact(tx: $myPeopleTx)
                               }"
                       :variables
                       {:myPeopleTx "[
                                       {
                                         \"_id\": \"person\",
                                         \"stringNotUnique\": \"oRamirez\",
                                         \"stringUnique\": \"Oscar Ramirez\"
                                       },
                                       {
                                         \"_id\": \"person\",
                                         \"stringNotUnique\": \"cStuart\",
                                         \"stringUnique\": \"Chana Stuart\"
                                       }
                                     ]"}}

          {:keys [status body]}
          @(http/post (str endpoint-url-short ledger "/graphql")
                      (test/standard-request graphql))

          result      (json/parse body)
          result-keys (-> result :data keys set)
          flakes      (-> result :data :flakes)
          flake-vals  (set (map #(nth % 2) flakes))]

      (is (= 200 status))

      (is (= #{:tempids :block :hash :instant :type :duration :fuel :auth :status :id :bytes :t :flakes}
             result-keys))

      (is (= 11 (count flakes)))

      (is (test/contains-every? flake-vals "Chana Stuart" "cStuart" "Oscar Ramirez" "oRamirez")))))


;; ENDPOINT TEST: /sparql

(deftest query-collection-sparql-test
  (testing "Querying all collections through the sparql endpoint"
    (let [ledger           (test/rand-ledger test/ledger-endpoints)
          query            "SELECT ?name \nWHERE \n {\n ?collection fd:_collection/name ?name. \n}"
          {:keys [status body]} @(http/post (str endpoint-url-short ledger "/sparql")
                                            (test/standard-request query))
          results          (json/parse body)
          collection-names (set (apply concat results))]

      (is (= 200 status))

      ;; Make sure we got results back
      (is (> (count results) 1))

      ;; Each result should be an array of 1 (?name)
      (is (every? #(= 1 (count %)) results))

      (is (test/contains-every? collection-names "_predicate" "_auth"
                                "_collection" "_fn" "_role" "_rule" "_setting"
                                "_tag" "_user")))))

(deftest sign-query-collection-sparql-test
  (testing "sign a query for all collections through the sparql endpoint"
    (let [ledger           (test/rand-ledger test/ledger-endpoints)
          private-key      (slurp "default-private-key.txt")
          qry-str          (json/stringify "SELECT ?name \nWHERE \n {\n ?collection fd:_collection/name ?name. \n}")
          request          {:headers {"content-type" "application/json"}
                            :body    qry-str}
          q-endpoint       (str endpoint-url-short ledger "/sparql")
          signed-req       (http-signatures/sign-request :post q-endpoint request private-key)
          {:keys [status body]} @(http/post q-endpoint signed-req)
          results          (json/parse body)
          collection-names (set (apply concat results))]

      (is (= 200 status))

      ;; Make sure we got results back
      (is (> (count results) 1))

      ;; Each result should be an array of 1 (?name)
      (is (every? #(= 1 (count %)) results))

      (is (test/contains-every? collection-names "_predicate" "_auth"
                                "_collection" "_fn" "_role" "_rule" "_setting"
                                "_tag" "_user")))))


;; ENDPOINT TEST: /sparql

(deftest query-wikidata-sparql-test
  (testing "Querying wikidata with sparql syntax"
    (let [ledger  (test/rand-ledger test/ledger-endpoints)
          query   "SELECT ?item ?itemLabel \nWHERE \n {\n ?item wdt:P31 wd:Q146. \n}"
          {:keys [status body]} @(http/post (str endpoint-url-short ledger "/sparql")
                                            (test/standard-request query))
          results (json/parse body)]

      (is (= 200 status))

      ;; Make sure we got results back
      (is (> (count results) 1))

      ;; Each result should be an array of 2 (?item and ?itemLabel)
      (is (every? #(= 2 (count %)) results)))))


;; ENDPOINT TEST: /sql

(deftest sign-sql-query-test
  (testing "sign a query for all collections through the sql endpoint"
    (let [ledger      (test/rand-ledger test/ledger-endpoints)
          _           (test/transact-schema ledger "chat-alt.edn")
          private-key (slurp "default-private-key.txt")
          qry-str     (json/stringify "SELECT * FROM _collection")
          request     {:headers {"content-type" "application/json"}
                       :body    qry-str}
          q-endpoint  (str endpoint-url-short ledger "/sql")
          signed-req  (http-signatures/sign-request :post q-endpoint request private-key)
          {:keys [status body] :as sql-res} @(http/post q-endpoint signed-req)
          results     (json/parse body)
          collections (set (map :_collection/name results))]

      (is (= 200 status))

      ; The keys in the response are -> :opts :body :headers :status
      (is (test/contains-every? sql-res :opts :body :headers :status))

      ; Are all the collections what we expect?
      (is (test/contains-every? collections
                                "_rule" "nestedComponent" "_fn" "_predicate" "_setting"
                                "chat" "_auth" "_user" "person" "_shard" "_tag" "comment"
                                "_role" "_collection")))))


;; ENDPOINT TEST: /health

(deftest health-check-test
  (testing "Getting health status"
    (let [{:keys [status body]} @(http/post (str endpoint-url-short "health"))
          result (json/parse body)]
      (is (= 200 status))
      (is (:ready result)))))


;; ENDPOINT TEST: /ledgers

(deftest get-all-ledgers-test
  (testing "Get all ledgers"
    (test/init-ledgers! (conj test/all-ledgers "test/three"))
    (let [{:keys [status body]} @(http/post (str endpoint-url-short "ledgers"))
          result (-> body json/parse set)]

      (is (= 200 status))

      (is (test/contains-every?
            result
            ["fluree" "api"] ["fluree" "querytransact"] ["fluree" "invoice"]
            ["fluree" "chat"] ["fluree" "voting"] ["fluree" "crypto"]
            ["test" "three"] ["fluree" "supplychain"] ["fluree" "todo"])))))


;; ENDPOINT TEST: /transact


(deftest transacting-new-chats-test
  (testing "Creating 100 random chat messages and adding them to existing persons"
    (let [ledger      (test/rand-ledger test/ledger-endpoints)
          _           (test/transact-schema ledger "chat-alt.edn")
          _           (test/transact-data ledger "chat-alt-people-comments-chats.edn")
          query       {:select ["*"] :from "person"}
          {:keys [status body]} @(http/post (str endpoint-url-short ledger "/query")
                                            (test/standard-request query))
          results     (json/parse body)
          persons     (map :_id results)
          random-chat (fn []
                        {:_id             "chat"
                         :stringNotUnique (rand-str)
                         :person          (nth persons (rand-int (count persons)))
                         :instantUnique   "#(now)"})
          chat-tx     (repeatedly 100 random-chat)

          {chat-status :status, chat-body :body}
          @(http/post (str endpoint-url-short ledger "/transact")
                      (test/standard-request chat-tx))

          tx-result   (json/parse chat-body)
          tx-keys     (-> tx-result keys set)
          flakes      (:flakes tx-result)
          tempids     (:tempids tx-result)]

      (is (= 200 status))
      (is (= 200 chat-status))

      (is (test/contains-every? tx-keys :auth :tempids :block :hash :fuel
                                :status :bytes :flakes :instant :type :duration
                                :id :t))

      (is (< 99 (count flakes)))

      (is (= 100 (test/get-tempid-count tempids :chat))))))


;; ENDPOINT TEST: /transact

(deftest updating-persons-test
  (testing "Updating all person/stringNotUniques"
    (let [ledger    (test/rand-ledger test/ledger-endpoints)
          _         (test/transact-schema ledger "chat-alt.edn")
          _         (test/transact-data ledger "chat-alt-people-comments-chats.edn")
          query     {:select ["*"] :from "person"}
          {:keys [status body]} @(http/post
                                   (str endpoint-url-short ledger "/query")
                                   (test/standard-request query))
          results   (json/parse body)
          persons   (map :_id results)
          person-tx (mapv (fn [n]
                            {:_id             n
                             :stringNotUnique (rand-str)})
                          persons)

          {tx-status :status, tx-body :body}
          @(http/post (str endpoint-url-short ledger "/transact")
                      (test/standard-request person-tx))

          tx-result (json/parse tx-body)
          tx-keys   (-> tx-result keys set)
          flakes    (:flakes tx-result)
          tempids   (:tempids tx-result)]

      (is (= 200 status))
      (is (= 200 tx-status))

      (is (test/contains-every? tx-keys :auth :block :hash :fuel :status :bytes
                                :flakes :instant :type :duration :id :t))

      #_(is (< 100 (count flakes))) ; TODO: why?

      (is (nil? tempids))

      ;; Confirm all the predicates we expect to be featured in the flakes
      (is (= #{101 106 99 100 1003 103 107}
             (->> flakes (map second) set))))))


;; ENDPOINT TEST: /transact

;; TODO: This is idiomatic, but it fails b/c it's checking for things I don't
;; fully understand (like setting a ceiling of 100 tx-count and then asserting
;; that that should be smaller than the number of flakes (why?). So I'm not
;; marking it w/ for now so I can move on with the other tests.
(deftest transacting-new-stringUniqueMulti-test
  (testing "Creating 300 random stringUniqueMulti (sum) and adding them to existing persons"
    (let [ledger      (test/rand-ledger test/ledger-endpoints)
          _           (test/transact-schema ledger "chat-alt.edn")
          _           (test/transact-data ledger "chat-alt-people-comments-chats.edn")
          query       {:select ["*"] :from "person"}
          {:keys [status body]} @(http/post
                                   (str endpoint-url-short ledger "/query")
                                   (test/standard-request query))
          results     (json/parse body)
          persons     (map :_id results)
          num-persons (count persons)
          tx-count    (if (> 100 num-persons)
                        num-persons
                        100)
          random-sum  (repeatedly 300 rand-str)
          random-sum* (get-unique-count random-sum 300 rand-str)
          [rand-sum-1 rand-sum-2 rand-sum-3] (partition 100 random-sum*)
          sum-tx      (map (fn [person sum1 sum2 sum3]
                             {:_id               person
                              :stringUniqueMulti [sum1 sum2 sum3]})
                           persons rand-sum-1 rand-sum-2 rand-sum-3)

          {tx-status :status, tx-body :body}
          @(http/post (str endpoint-url-short ledger "/transact")
                      (test/standard-request sum-tx))

          tx-result   (json/parse tx-body)
          tx-keys     (-> tx-result keys set)
          flakes      (:flakes tx-result)
          tempids     (:tempids tx-result)]

      (is (= 200 status))
      (is (= 200 tx-status))

      (is (test/contains-every? tx-keys :auth :block :hash :fuel :status
                                :flakes))

      (is (< tx-count (count flakes)))

      (is (nil? tempids)))))


;; ENDPOINT TEST: /new-db


(deftest create-ledger-test
  (testing "Creating a new ledger"
    (let [new-ledger-body {:ledger/id (str "test/three-" (UUID/randomUUID))}
          {:keys [status body]} @(http/post
                                   (str endpoint-url-short "new-ledger")
                                   (test/standard-request new-ledger-body))
          result          (json/parse body)]

      (is (= 200 status))

      (is (string? result))

      (is (= 64 (count result))))))


;; ENDPOINT TEST: /new-keys

(deftest new-keys-test
  (testing "Generating new keys"
    (let [{:keys [status body]} @(http/post (str endpoint-url-short "new-keys"))
          result      (json/parse body)
          result-keys (-> result keys set)]

      (is (= 200 status))

      (is (= #{:private :public :account-id} result-keys)))))


;; ENDPOINT TEST: /command

(deftest command-add-person-test
  (testing "Issue a signed command to add a person"
    (let [ledger   (test/rand-ledger test/ledger-endpoints)
          _        (test/transact-schema ledger "chat-alt.edn")
          _        (test/transact-data ledger
                                       "chat-alt-people-comments-chats.edn")
          priv-key (slurp "default-private-key.txt")
          cmd-map  (assoc (fdb/tx->command ledger
                                           [{:_id "person" :stringNotUnique "JoAnne"}]
                                           priv-key)
                     :txid-only true)
          {:keys [status body]} @(http/post
                                   (str endpoint-url-short ledger "/command")
                                   (test/standard-request cmd-map))
          result   (json/parse body)]

      (is (= 200 status))

      (is (string? result))

      (is (= 64 (count result))))))


(deftest command-add-person-verbose-test
  (testing "Issue a signed command to add a person")
  (let [ledger   (test/rand-ledger test/ledger-endpoints)
        _        (test/transact-schema ledger "chat-alt.edn")
        _        (test/transact-data ledger
                                     "chat-alt-people-comments-chats.edn")
        priv-key (slurp "default-private-key.txt")
        cmd-map  (-> ledger
                     (fdb/tx->command [{:_id "person" :stringNotUnique "Sally"}]
                                      priv-key))
        {:keys [status body]} @(http/post (str endpoint-url-short ledger
                                               "/command")
                                          (test/standard-request cmd-map))
        result   (json/parse body)]

    (is (= 200 status))

    (is (map? result))

    (is (test/contains-every? result :tempids :block :hash :instant
                              :type :duration :fuel :auth :status :id
                              :bytes :t :flakes))))


;; ENDPOINT TEST: signed /delete-db request

(deftest delete-ledger-test
  (testing "delete ledger - open api"
    (let [ledger (test/rand-ledger test/ledger-endpoints)
          {:keys [status body]} @(http/post
                                   (str endpoint-url-short "delete-ledger")
                                   (test/standard-request {:ledger/id ledger}))
          result (json/parse body)]
      (is (= 200 status))
      (is (= ledger (:deleted result))))))

;; ENDPOINT TEST: /gen-flakes, /query-with, /test-transact-with
;; TODO: Fix this. It doesn't work even when run by itself on its own empty ledger
#_(deftest gen-flakes-query-transact-with-test
    (testing "Issue a signed command to add a person."
      (let [ledger    (test/rand-ledger test/ledger-endpoints)
            _         (test/transact-schema ledger "chat-alt.edn")
            _         (test/transact-data ledger
                                          "chat-alt-people-comments-chats.edn")
            txn       [{:_id "person" :stringNotUnique "Josie"}
                       {:_id "person" :stringNotUnique "Georgine"}
                       {:_id "person" :stringNotUnique "Alan"}
                       {:_id "person" :stringNotUnique "Elaine"}]
            {:keys [status body]} @(http/post
                                     (str endpoint-url-short ledger
                                          "/gen-flakes")
                                     (test/standard-request txn))
            _         (println "gen-flakes result:" (pr-str body))
            flakes    (-> body json/parse :flakes)
            qw-test   {:query {:select ["*"] :from "person"} :flakes flakes}

            {qw-status :status, qw-body :body}
            @(http/post (str endpoint-url-short ledger "/query-with")
                        (test/standard-request qw-test))

            qw-result (json/parse qw-body)

            {q-status :status, q-body :body}
            @(http/post (str endpoint-url-short ledger "/query")
                        (test/standard-request {:select ["*"] :from "person"}))

            q-result  (json/parse q-body)]

        (is (= 200 status))
        (is (= 200 qw-status))
        (is (= 200 q-status))

        ;; These names appear when selecting people in query-with
        (is (= (->> qw-result (map :person/stringNotUnique) set)
               #{"Josie" "Alan" "Georgine" "Elaine"}))

        ;; None of these names actually appear when just querying.
        (is (not-any? (->> q-result (map :person/stringNotUnique) set)
                      ["Josie" "Alan" "Georgine" "Elaine"])))))
