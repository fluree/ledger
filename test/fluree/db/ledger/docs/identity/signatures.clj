(ns fluree.db.ledger.docs.identity.signatures
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.ledger.api.downloaded :as downloaded]
            [fluree.db.api :as fdb]
            [org.httpkit.client :as http]
            [clojure.core.async :as async]
            [fluree.db.util.json :as json]
            [fluree.db.query.http-signatures :as http-signatures]
            [byte-streams :as bs]))


;; Use fdb-api-open = false, everything that goes through the endpoints needs to be signed
(def key-maps (atom {}))

(defn closed-api
  [f]
  (do (test/test-system f {:fdb-api-open false
                           :fdb-group-servers     "ABC@localhost:11002"
                           :fdb-group-this-server "ABC"
                           :fdb-storage-type      "memory"
                           :fdb-api-port          test/alt-port }) (reset! key-maps {})))


(def endpoint (str "http://localhost:" test/alt-port "/fdb/" test/ledger-chat "/"))

(defn add-keys-to-atom
  [n]
  (dotimes [k n]
    (swap! key-maps assoc k (fdb/new-private-key))))


(defn send-parse-request
  [type req opts]
  (-> @(http/post (str endpoint type) (downloaded/standard-request req opts))
      :body
      bs/to-string
      json/parse))

;; Create auth records -> root, all persons, all persons no handles
(deftest create-auth
  (let [_       (add-keys-to-atom 3)
        addAuth [{:_id   "_auth"
                  :id    (get-in @key-maps [0 :id])
                  :roles [["_role/id" "root"]]}
                 { :_id "_auth"
                  :id   (get-in @key-maps [1 :id])
                  :roles ["_role$allPersons"]}
                 { :_id "_auth"
                  :id   (get-in @key-maps [2 :id])
                  :roles ["_role$noHandles"]}
                 {:_id "_role$allPersons"
                  :id "allPersons"
                  :rules ["_rule$allPersons"]}
                 {:_id "_role$noHandles"
                  :id  "noHandles"
                  :rules ["_rule$allPersons" "_rule$noHandles"]}
                 {:_id "_rule$allPersons"
                  :id "role$allPersons"
                  :collection "person"
                  :collectionDefault true
                  :fns [["_fn/name" "true"]]
                  :ops ["all"]}
                 {:_id "_rule$noHandles"
                  :id "noHandles"
                  :collection "person"
                  :predicates ["person/handle"]
                  :fns [["_fn/name" "false"]]
                  :ops ["all"]}]
        addAuthResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat addAuth))]
    (is (= 200 (:status addAuthResp)))

    (is (= 27 (-> addAuthResp :flakes count)))))

;; Endpoint test - unsigned queries should throw an error, no signature

(deftest unsigned-queries
  (testing "An unsigned query should throw an error")
  (let [endpoint-map {"query" {:select ["*"] :from "person"}
                      "block" {:block 1}
                      "graphql" {:query "{  graph {  _collection (sort: {predicate: \"name\", order: ASC}) { _id name spec version doc}}}"}
                      "sparql" "SELECT ?name \nWHERE \n {\n ?collection fd:_collection/name ?name. \n}"
                      "history" {:history [["_collection/name" "_user"]]}
                      "multi-query" {:person {:select ["*"] :from "person"}}}
        opts  {:throw-exceptions false}
        resps (reduce-kv (fn [acc key val]
                               (->> (send-parse-request key val opts)
                                   (conj acc))) [] endpoint-map)]

    (is (every? #(= 401 (:status %)) resps))

    (is (every? #(= "Request requires authentication." (:message %)) resps))))


;; Endpoint test - /query - should return filtered results

(deftest signed-queries
  (let [myRequest {:headers {"content-type" "application/json"}
                   :body    (json/stringify {:select ["*"] :from "person"})}

        q-endpoint (str endpoint "query")

        root-auth (get-in @key-maps [0 :private])
        person-auth (get-in @key-maps [1 :private])
        noHandle-auth (get-in @key-maps [2 :private])

        signedRoot (-> (http-signatures/sign-request :post q-endpoint myRequest root-auth) (assoc :content-type :json))
        signedPerson(-> (http-signatures/sign-request :post q-endpoint myRequest person-auth) (assoc :content-type :json))
        signedNoHandle (-> (http-signatures/sign-request :post q-endpoint myRequest noHandle-auth) (assoc :content-type :json))

        rootResp (-> @(http/post q-endpoint signedRoot) :body bs/to-string json/parse)
        personResp (-> @(http/post q-endpoint signedPerson) :body bs/to-string json/parse)
        noHandleResp (-> @(http/post q-endpoint signedNoHandle) :body bs/to-string json/parse)]

      (is (= (count rootResp) (count personResp) (count noHandleResp)))

      (is (some #(:person/handle %) rootResp))

      ;; This test failed randomly. Not sure why - 12.2
      (is (some #(:person/handle %) personResp))

      (is (not-any? #(:person/handle %) noHandleResp))))


;; Endpoint test - /transact - should throw error, /command endpoint shouldn't

(deftest transact-endpoint
  (testing "The /transact endpoint should not work.")
  (let [addPerson [{ :_id "person" :handle "anna"}]
        resp (-> @(http/post (str endpoint "transact") (downloaded/standard-request addPerson)) :body bs/to-string json/parse)]

    (is (= 401 (:status resp)))))


;; TODO - error should be in _tx/error?
(defn sign-cmd-fetch-tx-id
  [ledger private-key txn opts]
  (let [cmd   (fdb/tx->command ledger txn private-key)
        tx-id  (send-parse-request "command" cmd opts)
        _ (async/<!! (async/timeout 1000))]
    (async/<!! (fdb/query-async (basic/get-db ledger) {:select ["*"] :from ["_tx/id" tx-id]}))))


;; Endpoint test - /command - should allow txns based on auth permissions
(deftest signed-commands
  (let [opts  {:throw-exceptions false}
        root-key (get-in @key-maps [0 :private])
        person-key (get-in @key-maps [1 :private])
        noHandle-key (get-in @key-maps [2 :private])
        txn     [{ :_id "chat" :message "hi!"}]
        resps   (map #(sign-cmd-fetch-tx-id test/ledger-chat % txn opts) [root-key person-key noHandle-key])
        errs    (map #(get % "_tx/error") resps)]

    (is (nil? (first errs)))

    (is (=  "400 db/write-permission Insufficient permissions for predicate: chat/message within collection: chat." (second errs) (nth errs 2)))))


(deftest Signatures
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (create-auth)
  (unsigned-queries)
  (signed-queries)
  (transact-endpoint)
  (signed-commands))


(deftest Signatures-export
  (closed-api Signatures))
