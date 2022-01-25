(ns fluree.db.ledger.docs.identity.signatures-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [org.httpkit.client :as http]
            [clojure.core.async :as async :refer [<!!]]
            [fluree.db.util.json :as json]
            [fluree.db.query.http-signatures :as http-signatures]))

;; Use fdb-api-open = false, everything that goes through the endpoints needs to be signed
(use-fixtures :once
              (partial test/test-system
                       {:fdb-api-open          false
                        :fdb-group-servers     "ABC@localhost:11002"
                        :fdb-group-this-server "ABC"
                        :fdb-storage-type      "memory"
                        :fdb-api-port          @test/port}))


(def endpoint (str "http://localhost:" @test/port "/fdb/"))


(defn send-parse-request
  [ledger type req opts]
  (-> req
      (test/standard-request opts)
      (->> (http/post (str endpoint ledger "/" type)))
      deref
      :body
      json/parse))


(defn create-auths
  "Creates 3 auths in the given ledger: root, all persons, all persons no
  handles. Returns of vector of [key-maps create-txn-result]."
  [ledger]
  (let [keys     (vec (repeatedly 3 fdb/new-private-key))
        add-auth [{:_id   "_auth"
                   :id    (get-in keys [0 :id])
                   :roles [["_role/id" "root"]]}
                  {:_id   "_auth"
                   :id    (get-in keys [1 :id])
                   :roles ["_role$allPersons"]}
                  {:_id   "_auth"
                   :id    (get-in keys [2 :id])
                   :roles ["_role$noHandles"]}
                  {:_id   "_role$allPersons"
                   :id    "allPersons"
                   :rules ["_rule$allPersons"]}
                  {:_id   "_role$noHandles"
                   :id    "noHandles"
                   :rules ["_rule$allPersons" "_rule$noHandles"]}
                  {:_id               "_rule$allPersons"
                   :id                "role$allPersons"
                   :collection        "person"
                   :collectionDefault true
                   :fns               [["_fn/name" "true"]]
                   :ops               ["all"]}
                  {:_id        "_rule$noHandles"
                   :id         "noHandles"
                   :collection "person"
                   :predicates ["person/handle"]
                   :fns        [["_fn/name" "false"]]
                   :ops        ["all"]}]]
    [keys (->> add-auth
               (fdb/transact-async (:conn test/system) ledger)
               <!!)]))


(deftest create-auth-test
  (let [ledger (test/rand-ledger test/ledger-chat)
        [_ {:keys [status flakes]}] (create-auths ledger)]
    (is (= 200 status))

    (is (= 28 (count flakes)))))


(deftest unsigned-queries-test
  (testing "An unsigned query should throw an error"
    (let [ledger       (test/rand-ledger test/ledger-chat)
          endpoint-map {"query"       {:select ["*"] :from "person"}
                        "block"       {:block 1}
                        "graphql"     {:query "{  graph {  _collection (sort: {predicate: \"name\", order: ASC}) { _id name spec version doc}}}"}
                        "sparql"      "SELECT ?name \nWHERE \n {\n ?collection fd:_collection/name ?name. \n}"
                        "history"     {:history [["_collection/name" "_user"]]}
                        "multi-query" {:person {:select ["*"] :from "person"}}}
          opts         {:throw-exceptions false}
          resps        (reduce-kv (fn [acc key val]
                                    (conj acc (send-parse-request ledger key val opts)))
                                  [] endpoint-map)]

      (is (every? #(= 401 (:status %)) resps))

      (is (every? #(= "Request requires authentication." (:message %)) resps)))))


(deftest signed-queries-test
  (testing "/query endpoint should return filtered results"
    (let [ledger           (test/rand-ledger test/ledger-chat)

          [keys] (create-auths ledger)

          _                (test/transact-schema ledger "chat.edn" :clj)
          _                (test/transact-schema ledger "chat-preds.edn" :clj)
          _                (test/transact-data ledger "chat.edn" :clj)

          req              {:headers {"content-type" "application/json"}
                            :body    (json/stringify {:select ["*"] :from "person"})}

          q-endpoint       (str endpoint ledger "/query")

          root-auth        (get-in keys [0 :private])
          person-auth      (get-in keys [1 :private])
          no-handle-auth   (get-in keys [2 :private])

          signed-root      (http-signatures/sign-request :post q-endpoint req
                                                         root-auth)
          signed-person    (http-signatures/sign-request :post q-endpoint req
                                                         person-auth)
          signed-no-handle (http-signatures/sign-request :post q-endpoint req
                                                         no-handle-auth)

          {root-body :body :as root-resp}
          (-> signed-root
              (->> (http/post q-endpoint))
              deref
              (test/safe-update :body string? json/parse))

          {person-body :body :as person-resp}
          (-> signed-person
              (->> (http/post q-endpoint))
              deref
              (test/safe-update :body string? json/parse))

          {no-handle-body :body :as no-handle-resp}
          (-> signed-no-handle
              (->> (http/post q-endpoint))
              deref
              (test/safe-update :body string? json/parse))

          responses        [root-resp person-resp no-handle-resp]
          bodies           [root-body person-body no-handle-body]]

      (is (every? #(= 200 %) (map :status responses)))

      (is (apply = (map count bodies)))

      (is (some :person/handle root-body))

      (is (some :person/handle person-body))

      (is (not-any? :person/handle no-handle-body)))))


(deftest transact-endpoint-test
  (testing "The /transact endpoint should not work."
    (let [ledger     (test/rand-ledger test/ledger-chat)
          add-person [{:_id "person" :handle "anna"}]
          {:keys [status]} (->> add-person
                                test/standard-request
                                (http/post (str endpoint ledger "/transact"))
                                deref
                                :body
                                json/parse)]

      (is (= 401 status)))))


(defn sign-cmd-fetch-tx-id
  [ledger private-key txn opts]
  (let [cmd   (fdb/tx->command ledger txn private-key)
        tx-id (send-parse-request ledger "command" cmd opts)
        _     (async/<!! (async/timeout 1000))]
    (async/<!! (fdb/query-async (basic/get-db ledger)
                                {:select ["*"] :from ["_tx/id" tx-id]}))))


(deftest signed-commands
  (testing "/command endpoint should (dis)allow txns based on auth permissions"
    (let [ledger        (test/rand-ledger test/ledger-chat)
          _             (test/transact-schema ledger "chat.edn" :clj)
          _             (test/transact-schema ledger "chat-preds.edn" :clj)

          [keys] (create-auths ledger)

          root-key      (get-in keys [0 :private])
          person-key    (get-in keys [1 :private])
          no-handle-key (get-in keys [2 :private])

          opts          {:throw-exceptions false}
          txn           [{:_id "chat" :message "hi!"}]
          resps         (mapcat #(sign-cmd-fetch-tx-id ledger % txn opts)
                                [root-key person-key no-handle-key])
          errs          (map #(get % "_tx/error") resps)]

      (is (nil? (first errs)))

      (is (= "400 db/write-permission Insufficient permissions for predicate: chat/message within collection: chat."
             (second errs) (nth errs 2))))))
