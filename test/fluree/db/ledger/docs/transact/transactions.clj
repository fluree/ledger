(ns fluree.db.ledger.docs.transact.transactions
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async :refer [<!!]]
            [clojure.string :as str])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :once test/test-system-deprecated)

(defn get-db
  ([ledger-name]
   (get-db ledger-name {}))
  ([ledger-name opts]
   (fdb/db (:conn test/system) ledger-name opts)))

(defn get-conn
  []
  (:conn test/system))

(defn cleanup-data
  [identifiers]
  (let [txn  (map (fn [identifier]
                    {:_id identifier :_action "delete"})
                  identifiers)
        resp (<!! (fdb/transact-async (get-conn)
                                      test/ledger-query+transact
                                      txn))]
    (if (= 200 (:status resp))
      resp
      (throw (ex-info "Cleaning up data failed"
                      {:identifiers identifiers
                       :response    resp})))))

;; Add collections

(deftest add-collections*
  (testing "Add the person collection"
    (let [collection-txn  [{:_id  "_collection"
                            :name "person"}]
          collection-resp (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact collection-txn))]

      (is (= 200 (:status collection-resp)))
      (is (= 2 (:block collection-resp)))
      (is (= 1 (count (:tempids collection-resp)))))))

;; Add predicates

(deftest add-predicates
  (testing "Add predicates for the chat app"
    (let [predicate-txn   [{:_id    "_predicate"
                            :name   "person/handle"
                            :type   "string"
                            :unique true}
                           {:_id  "_predicate"
                            :name "person/fullName"
                            :type "string"}
                           {:_id                "_predicate"
                            :name               "person/user"
                            :type               "ref"
                            :restrictCollection "_user"}
                           {:_id  "_predicate"
                            :name "person/age"
                            :type "int"}]
          collection-resp (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact predicate-txn))]

      (is (= 200 (:status collection-resp)))
      (is (= 3 (:block collection-resp)))
      (is (= 1 (count (:tempids collection-resp)))))))

(deftest transact-with-temp-ids
  (testing "Issue basic transactions with temporary ids"
    (let [predicate-txn   [{:_id "_user$lEliasz", :username "lEliasz", :auth ["_auth$temp", "_auth$other"]}
                           {:_id      "person",
                            :handle   "lEliasz",
                            :fullName "Louis Eliasz",
                            :user     "_user$lEliasz"}
                           {:_id "_auth$temp", :id "tempAuthRecord"}
                           {:_id "_auth$other", :id "otherAuthRecord"}]
          collection-resp (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact predicate-txn))]

      (is (= 200 (:status collection-resp)))
      (is (-> collection-resp :tempids (get "_user$lEliasz"))))))

(deftest nested-transaction
  (testing "Issue a transaction with nested data rather than tempID reference"
    (let [predicate-txn   [{:_id      "person",
                            :handle   "ajohnson",
                            :fullName "Andrew Johnson"
                            :user
                            {:_id "_user", :username "ajohnson"}}]


          collection-resp (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact predicate-txn))]

      (is (= 200 (:status collection-resp)))
      (is (and (-> collection-resp :tempids (get "person")) (-> collection-resp :tempids (get "_user")))))))


(deftest update-user
  (testing "should replace existing user w/ new user in same txn"
    (let [pred-txn   [{:_id "_user$wMorgan", :username "wMorgan"}
                      {:_id      "person$wmorgan"
                       :handle   "wmorgan"
                       :fullName "Wes Morgan"
                       :user     "_user$wMorgan"}]
          resp1      (<!! (fdb/transact-async (get-conn) test/ledger-query+transact
                                              pred-txn))
          _          (assert (= 200 (:status resp1)))
          update-txn [{:_id "_user$other" :username "other"}
                      {:_id  ["person/handle" "wmorgan"]
                       :user "_user$other"}]
          resp2      (<!! (fdb/transact-async (get-conn) test/ledger-query+transact
                                              update-txn))
          _          (assert (= 200 (:status resp2)))
          qry        {:select {:?user ["*"]}
                      :where  [["?person" "person/handle" "wmorgan"]
                               ["?person" "person/user" "?user"]]}
          qry-resp   (<!! (fdb/query-async (basic/get-db
                                             test/ledger-query+transact
                                             {:syncTo (:block resp2)})
                                           qry))]
      (is (= 1 (count qry-resp)))
      (is (= "other" (-> qry-resp first (get "_user/username"))))
      (cleanup-data [["person/handle" "wmorgan"]
                     ["_user/username" "wMorgan"]
                     ["_user/username" "other"]])))
  (testing "should replace existing user w/ pre-existing user in two-tuple"
    (let [pred-txn   [{:_id "_user$wMorgan", :username "wMorgan"}
                      {:_id "_user$other", :username "other"}
                      {:_id      "person$wmorgan"
                       :handle   "wmorgan"
                       :fullName "Wes Morgan"
                       :user     "_user$wMorgan"}]
          resp1      (<!! (fdb/transact-async (get-conn) test/ledger-query+transact
                                              pred-txn))
          _          (is (= 200 (:status resp1))
                         (pr-str resp1))
          update-txn [{:_id  ["person/handle" "wmorgan"]
                       :user ["_user/username" "other"]}]
          resp2      (<!! (fdb/transact-async (get-conn) test/ledger-query+transact
                                              update-txn))
          _          (is (= 200 (:status resp2))
                         (pr-str resp2))
          qry        {:select {:?user ["*"]}
                      :where  [["?person" "person/handle" "wmorgan"]
                               ["?person" "person/user" "?user"]]}
          qry-resp   (<!! (fdb/query-async (basic/get-db
                                             test/ledger-query+transact
                                             {:syncTo (:block resp2)})
                                           qry))]
      (is (= 1 (count qry-resp)))
      (is (= "other" (-> qry-resp first (get "_user/username"))))
      (cleanup-data [["person/handle" "wmorgan"]
                     ["_user/username" "wMorgan"]
                     ["_user/username" "other"]])))
  (testing "should replace existing user w/ pre-existing user id"
    (let [pred-txn      [{:_id "_user$wMorgan", :username "wMorgan"}
                         {:_id "_user$other", :username "other"}
                         {:_id      "person$wmorgan"
                          :handle   "wmorgan"
                          :fullName "Wes Morgan"
                          :user     "_user$wMorgan"}]
          resp1         (<!! (fdb/transact-async (get-conn) test/ledger-query+transact
                                                 pred-txn))
          other-user-id (get-in resp1 [:tempids "_user$other"])
          _             (is (= 200 (:status resp1))
                            (pr-str resp1))
          update-txn    [{:_id  ["person/handle" "wmorgan"]
                          :user other-user-id}]
          resp2         (<!! (fdb/transact-async (get-conn) test/ledger-query+transact
                                                 update-txn))
          _             (is (= 200 (:status resp2))
                            (pr-str resp2))
          qry           {:select {:?user ["*"]}
                         :where  [["?person" "person/handle" "wmorgan"]
                                  ["?person" "person/user" "?user"]]}
          qry-resp      (<!! (fdb/query-async (basic/get-db
                                                test/ledger-query+transact
                                                {:syncTo (:block resp2)})
                                              qry))]
      (is (= 1 (count qry-resp)))
      (is (= "other" (-> qry-resp first (get "_user/username"))))
      (cleanup-data [["person/handle" "wmorgan"]
                     ["_user/username" "wMorgan"]
                     ["_user/username" "other"]]))))


(deftest duplicate-unique-predicate
  (testing "Duplicate predicate should return an exception"
    (let [failure-txn  [{:_id "person",
                         :handle "ajohnson",
                         :fullName "Andrew A Johnson"
                         :user {:_id "_user", :username "aajohnson"}}]
          failure-resp (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact failure-txn))]
      (is (instance? ExceptionInfo failure-resp))
      (is (-> failure-resp
              ex-data
              :status
              (= 400)))
      (is (-> failure-resp
              ex-data
              :error
              (= :db/invalid-tx)))
      (is (-> failure-resp
              ex-message
              (str/starts-with? "Unique predicate person/handle with value: ajohnson matched an existing subject"))))))


(deftest upserting-data
  (testing "Issue a transaction to upsert data instead of creating a new entity"
    (let [failure-txn   [{:_id "person", :handle "ajohnson", :age 26}]
          failure-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact failure-txn))
          _a            (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact
                                                       [{:_id ["_predicate/name" "person/handle"]
                                                         :upsert true}]))
          success-txn   [{:_id "person", :handle "ajohnson", :age 26}]
          success-resp  (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact success-txn))]

      (is (and (= 200 (:status success-resp)) (not (= 200 (:status failure-resp))))))))

(deftest deleting-data
  (testing "Issue a transaction to delete parts of data and to delete an entire subject"
    (let [delete-txn        [{:_id ["person/handle" "ajohnson"] :_action "delete"}
                             {:_id ["person/handle" "lEliasz"] :fullName nil}
                             {:_id ["_user/username" "lEliasz"] :auth [["_auth/id" "tempAuthRecord"]] :_action "delete"}]
          delete-resp       (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact delete-txn))
          test-query        {:select ["*", {"person/user" ["*", {"_user/auth" ["*"]}]}] :from "person"}
          test-resp         (async/<!! (fdb/query-async
                                         (basic/get-db test/ledger-query+transact
                                                       {:syncTo (:block delete-resp)})
                                         test-query))
          remaining-person  (some #(when (get % "person/handle") %) test-resp)]

      (is (= 200 (:status delete-resp)))
      (is (not (some #(= "ajohnson" (get % "person/handle")) test-resp)))
      (is (some #(= "lEliasz" (get % "person/handle")) test-resp))
      (is (not (get remaining-person "person/fullName")))
      (is (= 1 (-> remaining-person (get "person/user") (get "_user/auth") count))))))

(deftest expired-transaction
  (testing "Issue an expired transaction, expect a validation error"
    (let [expired-txn [{:_id "_user" :username "Jonah"}]
          opts        {:expire (- (System/currentTimeMillis) 100)}
          txn-resp    (async/<!! (fdb/transact-async (get-conn) test/ledger-query+transact expired-txn opts))]
      (is (instance? ExceptionInfo txn-resp))
      (is (-> txn-resp
              ex-data
              :error
              (= :db/invalid-command)))
      (is (-> txn-resp
              ex-message
              (str/starts-with? "Transaction 'expire', when provided, must be epoch millis and be later than now."))))))


(deftest transaction-basics
  (testing "Testing suite for the Transaction section of the docs")
  (add-collections*)
  (add-predicates)
  (transact-with-temp-ids)
  (nested-transaction)
  (duplicate-unique-predicate)
  (update-user)
  (upserting-data)
  (deleting-data)
  (expired-transaction))


