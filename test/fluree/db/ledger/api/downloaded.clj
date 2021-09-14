(ns fluree.db.ledger.api.downloaded
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.util.log :as log]
            [clojure.tools.reader.edn :as edn]
            [clojure.java.io :as io]
            [org.httpkit.client :as http]
            [fluree.db.util.json :as json]
            [byte-streams :as bs]
            [fluree.db.api :as fdb]))

(use-fixtures :once test/test-system)

;; Utility vars and functions

(def endpoint-url (str "http://localhost:" @test/port "/fdb/" test/ledger-endpoints "/"))
(def endpoint-url-short (str "http://localhost:" @test/port "/fdb/"))

(defn- rand-str
  []
  (apply str
         (take (+ 5 (rand-int 20))                          ;; at least 5 characters
               (repeatedly #(char (+ (rand 26) 65))))))

(defn- get-unique-count
  [current goal-count fn]
  (let [current-count (count (distinct current))
        distance      (- goal-count current-count)]
    (if (< 0 distance)
      (get-unique-count (distinct (concat current (repeatedly distance fn))) goal-count fn)
      (distinct current))))


(defn standard-request
  ([body]
   (standard-request body {}))
  ([body opts]
   {:headers (cond-> {"content-type" "application/json"}
               (:token opts) (assoc "Authorization" (str "Bearer " (:token opts))))
    :body    (json/stringify body)}))

;; ENDPOINT TEST: /transact
(deftest add-schema*
  (testing "Add schema"
    (let [filename      "../test/fluree/db/ledger/Resources/ChatAltVersion/schema.edn"
          tx            (edn/read-string (slurp (io/resource filename)))
          schema-res    @(http/post (str endpoint-url "transact") (standard-request tx))
          response-keys (keys schema-res)
          status        (:status schema-res)
          body          (-> schema-res :body bs/to-string json/parse)
          body-keys     (keys body)]

      (is (= 200 status))

      (is (map #(#{:headers :status :opts :body} %)
               response-keys))

      (is (map #(#{:tx-subid :tx :txid :authority :auth :signature :tempids
                   :block :hash :fuel-remaining :time :fuel :status :block-bytes
                   :timestamp :flakes})
               body-keys))

      (is (= 2 (:block body)))

      (is (= 59 (-> body :tempids (test/get-tempid-count :_predicate))))

      (is (= 4 (-> body :tempids (test/get-tempid-count :_collection)))))))



;; ENDPOINT TEST: /transact

(deftest new-people-comments-chats-auth
  (testing "Add auth, roles, rules to test.two"
    (let [filename      "../test/fluree/db/ledger/Resources/ChatAltVersion/people-comments-chats-auth.edn"
          tx            (edn/read-string (slurp (io/resource filename)))
          new-data-res  @(http/post (str endpoint-url "transact") (standard-request tx))
          response-keys (keys new-data-res)
          status        (:status new-data-res)
          body          (-> new-data-res :body bs/to-string json/parse)
          bodyKeys      (keys body)]

      (is (= 200 status))

      ; The keys in the response are -> :request-time :aleph/keep-alive? :headers :status :connection-time :body
      (is (not (empty? (remove nil? (map #(#{:request-time :aleph/keep-alive? :headers :status :connection-time :body} %)
                                         response-keys)))))

      ; The keys in the body are :tempids :block :hash :time :status :block-bytes :timestamp :flakes
      (is (not (empty? (remove nil? (map #(#{:tempids :block :hash :fuel-remaining :time :fuel :status :block-bytes :timestamp :flakes} %)
                                         bodyKeys)))))

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
      (= 1 (-> body :tempids (test/get-tempid-count :person))))))


(deftest standalone-new-people*
  (add-schema*)
  (new-people-comments-chats-auth))


;; ENDPOINT TEST: /query

(deftest query-all-collections
  (testing "Querying all collections"
    (let [query               {:select ["*"] :from "_collection"}
          queryCollectionsRes @(http/post (str endpoint-url "query") (standard-request query))
          responseKeys        (keys queryCollectionsRes)
          status              (:status queryCollectionsRes)
          body                (-> queryCollectionsRes :body bs/to-string json/parse)
          collections         (into #{} (map #(:_collection/name %) body))]

      (is (= 200 status))

      ; The keys in the response are -> :request-time :aleph/keep-alive? :headers :status :connection-time :body
      (is (not (empty? (remove nil? (map #(#{:request-time :aleph/keep-alive? :headers :status :connection-time :body} %)
                                         responseKeys)))))

      ; Are all the collection names what we expect?
      (is (= collections #{"_rule" "nestedComponent" "_fn" "_predicate" "_setting" "chat" "_auth" "_user" "person" "_shard" "_tag" "comment" "_role" "_collection"})))))


(deftest query-collections*
  (add-schema*)
  (query-all-collections))



;; ENDPOINT TEST: /query

(deftest query-all-predicates
  (testing "Query all predicates."
    (let [query               {:select ["*"] :from "_predicate"}
          queryCollectionsRes @(http/post (str endpoint-url "query") (standard-request query))
          responseKeys        (keys queryCollectionsRes)
          status              (:status queryCollectionsRes)
          body                (-> queryCollectionsRes :body bs/to-string json/parse)
          predicates          (into #{} (map #(:_predicate/name %) body))]


      (is (= 200 status))

      ; The keys in the response are -> :request-time :aleph/keep-alive? :headers :status :connection-time :body
      (is (not (empty? (remove nil? (map #(#{:request-time :aleph/keep-alive? :headers :status :connection-time :body} %)
                                         responseKeys)))))

      ; Are some of the predicates we expect returned?
      (is (every? boolean (map #(predicates %) ["comment/nestedComponent" "person/stringUnique"])))


      (is (< 30 (count predicates))))))


;; ENDPOINT TEST: /multi-query

(deftest query-collections-predicates-multiquery
  (testing "Querying all collections and predicates in multi-query"
    (let [query        {:coll {:select ["*"] :from "_collection"}
                        :pred {:select ["*"] :from "_predicate"}}
          multiRes     @(http/post (str endpoint-url "multi-query") (standard-request query))
          responseKeys (keys multiRes)
          status       (:status multiRes)
          body         (-> multiRes :body bs/to-string json/parse)
          collections  (into #{} (map #(:_collection/name %) (:coll body)))
          predicates   (into #{} (map #(:_predicate/name %) (:pred body)))]

      (is (= 200 status))

      ; The keys in the response are -> :request-time :aleph/keep-alive? :headers :status :connection-time :body
      (is (not (empty? (remove nil? (map #(#{:request-time :aleph/keep-alive? :headers :status :connection-time :body} %)
                                         responseKeys)))))

      ; Are all the predicates what we expect?
      (is (= collections #{"_rule" "nestedComponent" "_fn" "_predicate" "_setting" "chat" "_auth" "_user" "person" "_shard" "_tag" "comment" "_role" "_collection"}))

      ; Are some of the predicates we expect returned?
      (is (every? boolean (map #(predicates %) ["comment/nestedComponent" "person/stringUnique"]))))))


(deftest query-collections-predicates*
  (add-schema*)
  (query-all-collections)
  (query-all-predicates)
  (query-collections-predicates-multiquery))


;; ENDPOINT TEST: /transact

(deftest transacting-new-persons
  (testing "Creating 100 random persons"
    (let [randomPerson (fn []
                         {:_id             "person"
                          :stringNotUnique (rand-str)})
          personTx     (repeatedly 100 randomPerson)
          personRes    @(http/post (str endpoint-url "transact") (standard-request personTx))
          personBody   (-> personRes :body bs/to-string json/parse)
          personKeys   (keys personBody)
          flakes       (:flakes personBody)
          tempids      (:tempids personBody)]

      (is (every? boolean (map #((set personKeys) %) [:tempids :block :hash :fuel :auth :status :flakes])))
      (is (< 100 (count flakes)))
      (is (= 100 (test/get-tempid-count tempids :person))))))

(deftest standalone-transacting-new-persons
  (add-schema*)
  (transacting-new-persons))



;; ENDPOINT TEST: /block

(deftest query-block-two
  (testing "Query block 2"
    (let [query     {:block 2}
          res       @(http/post (str endpoint-url "block") (standard-request query))
          block     (-> res :body bs/to-string json/parse first)
          blockKeys (keys block)]

      (is (= 2 (:block block)))

      (is (every? boolean (map #(#{:block :hash :instant :txns :block-bytes :cmd-types :t :sigs :flakes} %) (set blockKeys)))))))

(deftest standalone-block-two-query
  (add-schema*)
  (query-block-two))



;; ENDPOINT TEST: /history

(deftest history-query-collection-name
  (testing "Query history of flakes with _collection/name predicate"
    (let [history-query {:history [nil 40]}
          resp          @(http/post (str endpoint-url "history") (standard-request history-query))
          result        (-> resp :body bs/to-string json/parse)]

      (is (map #(= 40 (second %)) result)))))

(deftest standalone-history-query
  (add-schema*)
  (history-query-collection-name))



;; ENDPOINT TEST: /graphql


(deftest query-all-collections-graphql*
  (testing "Querying all collections through the graphql endpoint"
    (let [query               {:query "{  graph {  _collection (sort: {predicate: \"name\", order: ASC}) { _id name spec version doc}}}"}
          queryCollectionsRes @(http/post (str endpoint-url "graphql") (standard-request query))
          body                (-> queryCollectionsRes :body bs/to-string json/parse)
          collection-names    (into #{} (map #(:name %) (-> body :data :_collection)))]

      ; Are the collection keys what we expect?
      (is (map #(#{:doc :version :spec :name :_id} %) body))

      ;; Are the collections what we expect?
      (is (not (empty? (remove nil? (map #((into #{} collection-names) %) ["_rule" "_fn" "_predicate" "_setting" "_auth" "_user" "_shard" "_tag" "_role" "_collection"])))))

      (is (every? boolean (map #(collection-names %) ["_rule" "_fn" "_predicate" "_setting" "_auth" "_user" "_shard" "_tag" "_role" "_collection"]))))))


(deftest repeated-query-all-collections-graphql*
  (query-all-collections-graphql*)
  (query-all-collections-graphql*)
  (query-all-collections-graphql*)
  (query-all-collections-graphql*)
  (query-all-collections-graphql*)
  (query-all-collections-graphql*)
  (query-all-collections-graphql*)
  (query-all-collections-graphql*)
  (query-all-collections-graphql*))

;; ENDPOINT TEST: /graphql transaction


(deftest add-a-person-graphql
  (testing "Add two new people with graphql."
    (let [body                {:query "mutation addPeople ($myPeopleTx: JSON) { transact(tx: $myPeopleTx)
}" :variables {:myPeopleTx "[
    { \"_id\": \"person\", \"stringNotUnique\": \"oRamirez\", \"stringUnique\": \"Oscar Ramirez\" },
    { \"_id\": \"person\", \"stringNotUnique\": \"cStuart\", \"stringUnique\": \"Chana Stuart\" }]"}}
          queryCollectionsRes @(http/post (str endpoint-url "graphql") (standard-request body))
          res                 (-> queryCollectionsRes :body bs/to-string json/parse)
          resKeys             (-> (keys res) set)
          flakeValSet         (-> (map (fn [flake]
                                         (nth flake 2)) (-> res :data :flakes)) set)]

      (is (= (-> res :data keys set) #{:tempids :block :hash :instant :type :duration :fuel :auth :status :id :bytes :t :flakes}))

      (is (= 11 (-> res :data :flakes count)))

      (is (every? #(flakeValSet %) ["Chana Stuart" "cStuart" "Oscar Ramirez" "oRamirez"])))))


(deftest standalone-add-person-graphql*
  (add-schema*)
  (add-a-person-graphql))



;; ENDPOINT TEST: /sparql


(deftest query-collection-sparql*
  (testing "Querying all collections through the sparql endpoint"
    (let [query            "SELECT ?name \nWHERE \n {\n ?collection fd:_collection/name ?name. \n}"
          res              @(http/post (str endpoint-url "sparql") (standard-request query))
          body             (-> res :body bs/to-string json/parse)
          collection-names (into #{} (apply concat body))]

      ;; Make sure we got results back
      (is (> (count body) 1))

      ;; Each result should be an array of 1 (?name)
      (is (every? boolean (map #(= 1 (count %)) body)))

      (is (every? boolean (map #(collection-names %) ["_predicate" "_auth" "_collection" "_fn" "_role" "_rule" "_setting" "_tag" "_user"]))))))



;; ENDPOINT TEST: /sparql


(deftest query-wikidata-sparql*
  (testing "Querying wikidata with sparql syntax"
    (let [query "SELECT ?item ?itemLabel \nWHERE \n {\n ?item wdt:P31 wd:Q146. \n}"
          res   @(http/post (str endpoint-url "sparql") (standard-request query))
          body  (-> res :body bs/to-string json/parse)]

      ;; Make sure we got results back
      (is (> (count body) 1))

      ;; Each result should be an array of 2 (?item and ?itemLabel)
      (is (every? boolean (map #(= 2 (count %)) body))))))



;; ENDPOINT TEST: /health

(deftest health-check
  (testing "Getting health status"
    (let [healthRes  @(http/post (str endpoint-url-short "health"))
          healthBody (-> healthRes :body bs/to-string json/parse)]
      (is (:ready healthBody)))))


;; ENDPOINT TEST: /dbs

(deftest get-all-dbs
  (testing "Get all dbs"
    (let [dbRes  @(http/post (str endpoint-url-short "dbs"))
          dbBody (-> dbRes :body bs/to-string json/parse)]

      (is (= #{["fluree" "api"] ["fluree" "querytransact"]
               ["fluree" "chat"] ["fluree" "voting"] ["fluree" "crypto"]
               ["test" "three"] ["fluree" "supplychain"] ["fluree" "todo"]} (set dbBody))))))


;; ENDPOINT TEST: /transact


(deftest transacting-new-chats
  (testing "Creating 100 random chat messages and adding them to existing persons"
    (let [query       {:select ["*"] :from "person"}
          query-res   @(http/post (str endpoint-url "query") (standard-request query))
          body        (-> query-res :body bs/to-string json/parse)
          persons     (map #(:_id %) body)
          random-chat (fn []
                        {:_id             "chat"
                         :stringNotUnique (rand-str)
                         :person          (nth persons (rand-int (count persons)))
                         :instantUnique   "#(now)"})
          chat-tx     (repeatedly 100 random-chat)
          chat-res    @(http/post (str endpoint-url "transact") (standard-request chat-tx))
          chat-body   (-> chat-res :body bs/to-string json/parse)
          chat-keys   (keys chat-body)
          flakes      (:flakes chat-body)
          tempids     (:tempids chat-body)]

      ; Status = 200 for the response
      (is (= 200 (:status chat-body)))

      (is (every? #(#{:tx-subid :tx :txid :authority :auth :signature :tempids
                      :block :hash :fuel-remaining :time :fuel :status
                      :bytes :timestamp :flakes :instant :type :duration :id :t}
                    %)
                  (set chat-keys)))

      (is (< 99 (count flakes)))

      (is (= 100 (test/get-tempid-count tempids :chat))))))


(deftest standalone-add-new-chats*
  (add-schema*)
  (new-people-comments-chats-auth)
  (transacting-new-chats))


;; ENDPOINT TEST: /transact

(deftest updating-persons
  (testing "Updating all person/stringNotUniques"
    (let [query       {:select ["*"] :from "person"}
          query-res   @(http/post (str endpoint-url "query") (standard-request query))
          body        (-> query-res :body bs/to-string json/parse)
          persons     (map #(:_id %) body)
          person-tx   (mapv (fn [n]
                              {:_id             n
                               :stringNotUnique (rand-str)}) persons)
          person-res  @(http/post (str endpoint-url "transact") (standard-request person-tx))
          person-body (-> person-res :body bs/to-string json/parse)
          person-keys  (keys person-body)
          flakes      (:flakes person-body)
          tempids     (:tempids person-body)]

      (is (every? #(#{:tx-subid :tx :txid :authority :auth :signature :tempids
                      :block :hash :fuel-remaining :time :fuel :status :bytes
                      :timestamp :flakes :instant :type :duration :id :t}
                    %)
                  (set person-keys)))

      (is (< 100 (count flakes)))

      (is (= 0 (count tempids)))

      ;; Confirm all the predicates we expect to be featured in the flakes
      (is (= #{101 106 99 100 1003 103 107}
             (->> flakes (map second) set))))))

(comment
  (deftest standalone-repeated-updating-persons
    (add-schema*)
    (new-people-comments-chats-auth)
    (updating-persons)
    (updating-persons)
    (updating-persons)
    (updating-persons)
    (updating-persons)
    (updating-persons)
    (updating-persons)
    (updating-persons)
    (updating-persons)))


;; ENDPOINT TEST: /transact

(deftest transacting-new-stringUniqueMulti
  (testing "Creating 300 random stringUniqueMulti (sum) and adding them to existing persons"
    (let [query      {:select ["*"] :from "person"}
          queryRes   @(http/post (str endpoint-url "query") (standard-request query))
          body       (-> queryRes :body bs/to-string json/parse)
          persons    (map #(:_id %) body)
          numPersons (count persons)
          txCount    (if (> 100 numPersons)
                       numPersons
                       100)
          randomSUM  (repeatedly 300 rand-str)
          randomSUM* (get-unique-count randomSUM 300 rand-str)
          rS1        (take 100 randomSUM*)
          rS2        (take 100 (drop 100 randomSUM*))
          rS3        (take 100 (drop 200 randomSUM*))
          sumTx      (map (fn [person sum1 sum2 sum3]
                            {:_id               person
                             :stringUniqueMulti [sum1 sum2 sum3]}) persons rS1 rS2 rS3)
          sumRes     @(http/post (str endpoint-url "transact") (standard-request sumTx))
          sumBody    (-> sumRes :body bs/to-string json/parse)
          sumKeys    (keys sumBody)
          flakes     (:flakes sumBody)
          tempids    (:tempids sumBody)]

      (is (map #(#{:tx-subid :tx :txid :authority :auth :signature :tempids :block :hash :fuel-remaining :time :fuel :status :block-bytes :timestamp :flakes} %)
               (set sumKeys)))
      (is (< txCount (count flakes)))
      (is (= 0 (count tempids))))))

(deftest standalone-transacting-new-stringUniqueMulti
  (add-schema*)
  (new-people-comments-chats-auth)
  (transacting-new-persons)
  (transacting-new-stringUniqueMulti))



;; ENDPOINT TEST: /new-db


(deftest create-database-test*
  (testing "Creating a new database"
    (let [new-database-body {:db/id "test/three"}
          res               @(http/post (str endpoint-url-short "new-db") (standard-request new-database-body))
          body              (-> res :body bs/to-string json/parse)]

      (is (= 200 (:status res)))

      (is (string? body))

      (is (= 64 (count body))))))



;; ENDPOINT TEST: /new-keys


(deftest new-keys*
  (testing "Generating new keys"
    (let [res     @(http/post (str endpoint-url-short "new-keys"))
          body    (-> res :body bs/to-string json/parse)
          keyKeys (keys body)]

      (is (= 200 (:status res)))

      (is (= #{:private :public :account-id} (set keyKeys))))))


;; ENDPOINT TEST: /command


(deftest command-add-person
  (testing "Issue a signed command to add a person."
    (let [privKey (slurp "default-private-key.txt")
          cmd-map (fdb/tx->command test/ledger-endpoints [{:_id "person" :stringNotUnique "JoAnne"}]
                                   privKey)
          res     @(http/post (str endpoint-url "command") (standard-request cmd-map))
          body    (-> res :body bs/to-string json/parse)]

      (is (= 200 (:status res)))

      (is (string? body))

      (is (= 64 (count body))))))

(deftest command-add-person-verbose
  (testing "Issue a signed command to add a person.")
  (let [privKey  (slurp "default-private-key.txt")
        cmd-map  (fdb/tx->command test/ledger-endpoints
                                  [{:_id "person" :stringNotUnique "Sally"}]
                                  privKey)
        cmd-map* (assoc cmd-map :txid-only false)
        res      @(http/post (str endpoint-url "command") (standard-request cmd-map*))
        body     (-> res :body bs/to-string json/parse)]

    (is (= 200 (:status res)))

    (is (map? body))

    (is (test/contains-many? body :tempids :block :hash :instant
                             :type :duration :fuel :auth :status :id
                             :bytes :t :flakes))))

(deftest standalone-add-person-command*
  (add-schema*)
  (command-add-person)
  (command-add-person-verbose))


;; TODO - can't test this with other tests - fails. Can't have any txns processed between gen-flakes and query-with. Not sure how to make sure of that. Running the independent version succeeds.
;; ENDPOINT TEST: /gen-flakes, /query-with, /test-transact-with
(deftest test-gen-flakes-query-transact-with
  (testing "Issue a signed command to add a person."
    (let [txn          [{:_id "person" :stringNotUnique "Josie"} {:_id "person" :stringNotUnique "Georgine"}
                        {:_id "person" :stringNotUnique "Alan"} {:_id "person" :stringNotUnique "Elaine"}]
          flakes-res   @(http/post (str endpoint-url "gen-flakes") (standard-request txn))
          flakes       (-> flakes-res :body bs/to-string json/parse :flakes)
          qw-test      {:query {:select ["*"] :from "person"} :flakes flakes}
          qw-res       @(http/post (str endpoint-url "query-with") (standard-request qw-test))
          qw-body      (-> qw-res :body bs/to-string json/parse)
          person-query @(http/post (str endpoint-url "query") (standard-request {:select ["*"] :from "person"}))
          person-body  (-> person-query :body bs/to-string json/parse)]

      ;; These names appear when selecting people in query-with
      (is (every? #((-> (map :person/stringNotUnique qw-body) set) %)
                  ["Josie" "Alan" "Georgine" "Elaine"]))

      ;; None of these names actually appear when just querying.
      (is (not-any? #((-> (map :person/stringNotUnique person-body) set) %)
                    ["Josie" "Alan" "Georgine" "Elaine"])))))


(deftest standalone-test-gen-flakes-query-transact-with
  (add-schema*)
  (test-gen-flakes-query-transact-with))

(deftest api-test
  (add-schema*)
  (new-people-comments-chats-auth)
  (query-all-collections)
  (query-all-predicates)
  (query-collections-predicates-multiquery)
  (transacting-new-persons)
  (query-block-two)
  (history-query-collection-name)
  (repeated-query-all-collections-graphql*)
  (query-collection-sparql*)
  (query-wikidata-sparql*)
  (transacting-new-chats)
  (updating-persons)
  (updating-persons)
  (updating-persons)
  (updating-persons)
  (transacting-new-stringUniqueMulti)
  (create-database-test*)
  (new-keys*)
  (add-a-person-graphql)
  (command-add-person)
  (command-add-person-verbose)
  (get-all-dbs)
  (health-check))
