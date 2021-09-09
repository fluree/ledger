(ns fluree.db.ledger.general.todo-permissions
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]))


(use-fixtures :once test/test-system)

(def sys-admin {:private "602798c87164f0c1e1b2fe0f7f229d32218d828cc51ef78b462eccaa05983e4c"
                :auth "Tf3sgBQ9G6EsrG65DXdWWamWXX3AxiDaq4z"})

(def scott {:private "1f719543da120a66e970b7e031fa4d87bdb1cb300978a023279116ff33f084b5"
            :auth "TexrrPHNapSfqxpG2HRm7Pfv1vwULWgzb8P"})

(def kevin {:private "8b3372c2289d31040a1a0f55e63eebbb1ff755435d5f77b8ba991bc865a2eda7"
            :auth "TfJDfxRYkFoWQjc5fgCGm3ifsgPJckMLRNQ"})

(def jay {:private "bcee1d4916bad2078599bd426a424525ad749224f01f7a719a332c962c83352f"
          :auth "Tf54gwMW2nLvirfhkjjXDxpWfwsMW5fDQgH"})

(deftest add-schema
  (testing "Add the todo collection and its predicates")
  (let [schema-txn  [{:_id "_collection",
                      :name "todo"},
                     {:_id "_predicate",
                      :name "todo/auth",
                      :type "ref",
                      :restrictCollection "_auth"},
                     {:_id "_predicate",
                      :name "todo/id",
                      :type "string",
                      :unique true},
                     {:_id "_predicate",
                      :name "todo/doc",
                      :type "string"}]
        schema-resp  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-todo schema-txn))]

    ;; status should be 200
    (is (= 200 (:status schema-resp)))

    ;; block should be 2
    (is (= 2 (:block schema-resp)))

    ;; there should be 3 _predicate tempids
    (is (= 3 (test/get-tempid-count (:tempids schema-resp) "_predicate")))

    ;; there should be 2 tempid keys
    (is (= 2 (count (keys (:tempids schema-resp)))))))

(deftest add-smart-functions
  (testing "Add supporting smart functions")
  (let [sf-txn  [{:_id "_fn$ownTodo?",
                  :name "ownTodo?",
                  :code "(relationship? (?sid) [\"todo/auth\"] (?auth_id))"},
                 {:_id "_rule$ownTodo",
                  :id "ownTodo",
                  :doc "User can only control their own todos",
                  :fns ["_fn$ownTodo?"],
                  :ops ["all"],
                  :collection "todo",
                  :collectionDefault true},
                 {:_id "_role$ownTodo",
                  :id "ownTodo",
                  :rules ["_rule$ownTodo"]}]
        sf-resp  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-todo sf-txn))]

    ;; status should be 200
    (is (= 200 (:status sf-resp)))

    ;; block should be 3
    (is (= 3 (:block sf-resp)))

    ;; there should be 3 tempids
    (is (= 3 (count (:tempids sf-resp))))))

(deftest add-user-auth
  (testing "Add _user, _auth")
  (let [ua-txn [{:_id "_auth$sysadmin",
                 :id (:auth sys-admin),
                 :roles [[ "_role/id", "root" ]]},
                {:_id "_auth$scott",
                 :id (:auth scott),
                 :roles [[ "_role/id", "ownTodo" ]]},
                {:_id "_auth$kevin",
                 :id (:auth kevin),
                 :roles [[ "_role/id", "ownTodo"]]},
                {:_id "_auth$jay",
                 :id (:auth jay),
                 :roles [[ "_role/id", "ownTodo" ]]}]
        ua-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-todo ua-txn))]
    ;; status should be 200
    (is (= 200 (:status ua-resp)))

    ;; block should be 4
    (is (= 4 (:block ua-resp)))

    ;; there should be 4 tempids
    (is (= 4 (count (:tempids ua-resp))))))


(deftest add-todos
  (testing "Add to-dos for each user")
  (let [td-txn [{:_id "todo$Scott",
                 :id "Scott",
                 :doc "Todo item Scott",
                 :auth ["_auth/id", (:auth scott)]},
                {:_id "todo$Kevin",
                 :id "Kevin",
                 :doc "Todo item Kevin",
                 :auth ["_auth/id", (:auth kevin)]},
                {:_id "todo$Jay",
                 :id "Jay",
                 :doc "Todo item Jay",
                 :auth ["_auth/id", (:auth jay)]},
                {:_id "todo$SysAdmin",
                 :id "SysAdmin",
                 :doc "Todo item SysAdmin",
                 :auth ["_auth/id", (:auth sys-admin)]}]
        td-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-todo td-txn))]
    ;; status should be 200
    (is (= 200 (:status td-resp)))

    ;; block should be 5
    (is (= 5 (:block td-resp)))

    ;; there should be 4 tempids
    (is (= 4 (count (:tempids td-resp))))))

(deftest query-auth
  (testing "Verify auth records exist")
  (let [id-list (-> (basic/get-db test/ledger-todo)
                    (fdb/query-async {:select ["*"] :from "_auth" :opts {:meta true}})
                    async/<!!
                    (as-> res (reduce-kv
                                (fn [z _ v]
                                  (into z [(-> v (get "todo/auth") (get "_id"))]))
                                []
                                res)))]
    (is (= 5 (count id-list)))))

(deftest query-todo-root-auth
  (testing "testing root auth sees all to-dos")
  (let [id-list (-> (basic/get-db test/ledger-todo {:auth ["_auth/id" (:auth sys-admin)]})
                    (fdb/query-async {:select ["*"] :from "todo"})
                    async/<!!
                    (as-> res (reduce-kv
                                (fn [z _ v]
                                  (into z [(-> v (get "todo/auth") (get "_id"))]))
                                []
                                res)))]
    (is (= 4 (count id-list)))))

(deftest query-own-todo
  (testing "testing non-system admin users see only own to-do")
  (let [id-list (-> (fdb/db (:conn test/system) test/ledger-todo {:auth ["_auth/id" (:auth kevin)]})
                    (fdb/query-async {:select ["*"] :from "todo"})
                    async/<!!
                    (as-> res (reduce-kv
                                (fn [z _ v]
                                  (into z [(-> v (get "todo/auth") (get "_id"))]))
                                []
                                res)))]
    (is (= 1 (count id-list)))))

(deftest retract-todo-auth-failure
  (testing "testing that non-system admin cannot delete someone else's to-do")
  (let [txn [{:_id ["todo/id" "Kevin"], :_action "delete"}]
        opts {:auth (:auth scott) :private-key (:private scott) :txid-only false}
        resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-todo txn opts))]
    (is (= clojure.lang.ExceptionInfo (type resp)))
    (is (str/includes? resp "Insufficient permissions"))))

(deftest retract-todo-own
  (testing "testing that non-system admin cannot delete own to-do")
  (let [txn [{:_id ["todo/id" "Scott"], :_action "delete"}]
        opts {:auth (:auth scott) :private-key (:private scott) :txid-only false}
        resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-todo txn opts))]
    (is (= 200 (:status resp)))))

(deftest retract-todo-system-admin
  (testing "testing that system admin can delete any to-do")
  (let [txn [{:_id ["todo/id" "Jay"], :_action "delete"}]
        opts {:auth (:auth sys-admin) :private-key (:private sys-admin) :txid-only false}
        resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-todo txn opts))]
    (is (= 200 (:status resp)))))

(deftest todo-auth-tests
  (add-schema)
  (add-smart-functions)
  (add-user-auth)
  (add-todos)
  (query-todo-root-auth)
  (query-own-todo)
  (retract-todo-auth-failure)
  (query-auth) ;; verify db cache
  (retract-todo-own)
  (retract-todo-system-admin))