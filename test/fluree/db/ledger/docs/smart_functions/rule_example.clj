(ns fluree.db.ledger.docs.smart-functions.rule-example
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]
            [clojure.core.async :as async]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.test-helpers :as test]
            [fluree.db.query.http-signatures :as http-signatures]
            [org.httpkit.client :as http]
            [byte-streams :as bs]
            [fluree.db.util.json :as json]))

(use-fixtures :once test/test-system)

(def jdoe {:auth        "TfKYG5F5iCsii1JvGGY2Pv6bPVVbZ2ERjmJ"
           :private-key "1787cab58d5b146a049f220c975d5dce7904c63f25d6d834d6980c427b47f412"})

(def zsmith {:auth        "TfFzb1tZDkGMBqWr8xMmRVvYmNYFKH9aNpi"
             :private-key "c0588115314065f7949f87f0f6adda3a252105be89b5080c56bb889cd20d841f"})

(def endpoint (str "http://localhost:" @test/port "/fdb/" test/ledger-chat "/"))

(deftest add-permission-scheme
  (let [person-auth [{:_id  "_predicate", :name "person/auth", :doc "Reference to a database auth.",
                      :type "ref", :restrictCollection "_auth"}]
        _           (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat person-auth))
        filename    "../test/fluree/db/ledger/Resources/ChatApp/ruleExample.edn"
        txn         (-> filename io/resource slurp edn/read-string)
        resp        (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat txn))]
    (is (= 200 (:status resp)))))

(deftest permissioned-query
  (let [query        {:chat   {:select ["*" {"chat/comments" ["*"]}] :from "chat"}
                      :person {:select ["*"] :from "person"}}
        my-request   {:headers {"content-type" "application/json"}
                      :body    (json/stringify query)}
        q-endpoint   (str endpoint "multi-query")
        level-1-req  (http-signatures/sign-request :post q-endpoint my-request (:private-key jdoe))
        level-2-req  (http-signatures/sign-request :post q-endpoint my-request (:private-key zsmith))
        l1-resp @(http/post q-endpoint level-1-req)
        l2-resp @(http/post q-endpoint level-2-req)
        level-1-resp (-> l1-resp :body bs/to-string json/parse)
        level-2-resp (-> l2-resp :body bs/to-string json/parse)]

    ;; Level 1 should not be able to view chat/comments
    (is (= #{} (->> level-1-resp :chat (map :chat/comments) flatten (remove nil?) set)))

    ;; Level 2 should be able to view chat/comments + comment messages
    (is (= #{"Zsmith is responding!" "Welcome Diana!"}
           (->> (map #(get-in % [:chat/comments 0 :comment/message]) (:chat level-2-resp)) (remove nil?) set)))

    ;; Level 1 should only be able to view person/handles and _ids.
    (is (= #{:person/handle :_id}
           (->> (:person level-1-resp) (map keys) flatten set)))

    ;; Level 2 should be able to view all person predicates
    (is (= #{:person/handle :_id :person/favNums :person/age :person/favArtists
             :person/follows :person/auth :person/fullName :person/favMovies
             :person/active}
           (->> (:person level-2-resp) (map keys) flatten set)))))

(deftest permissioned-transaction
  (let [jdoeChat     [{:_id "chat$1", :message "Hey there!", :person ["person/handle" "jdoe"]}]
        addOther     (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat jdoeChat zsmith))
                         test/safe-Throwable->map
                         :cause)
        addOwn       (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat jdoeChat jdoe))
        chat$1       (get (:tempids addOwn) "chat$1")
        jdoeChatEdit [{:_id chat$1 :message "Attemping to edit"}]
        editOther    (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat jdoeChatEdit zsmith))
                         test/safe-Throwable->map
                         :cause)
        editOwn      (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat jdoeChatEdit jdoe))]

    (is (= "Insufficient permissions for predicate: chat/message within collection: chat."
           addOther))

    (is (= 200 (:status addOwn)))

    (is (= "Insufficient permissions for predicate: chat/message within collection: chat."
           editOther))

    (is (= 200 (:status editOwn)))))


(deftest rule-example-test
  (add-permission-scheme)
  (permissioned-query)
  (permissioned-transaction))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (rule-example-test))


