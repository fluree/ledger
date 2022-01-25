(ns fluree.db.ledger.docs.smart-functions.intro
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [clojure.core.async :as async]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]))

(use-fixtures :once test/test-system-deprecated)

(deftest add-three-add-ten
  (let [add-three [{:_id "_fn", :name "addThree", :params ["n"], :code "(+ 3 n)"}]
        add-three-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat add-three))
        add-ten [{:_id "_fn", :name "addTen", :params ["n"], :code "(+ 7 (addThree n))"}]
        add-ten-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat add-ten))
        chat [{ :_id "chat" :message "#(str \"My age is: \" (addTen 27))"}]
        add-chat (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat chat))
        message-flake (->> add-chat :flakes (filter #(< 0 (first %))) first)]

   (is (= "My age is: 37" (nth message-flake 2)))))

(deftest intro-test
  (add-three-add-ten))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (intro-test))
