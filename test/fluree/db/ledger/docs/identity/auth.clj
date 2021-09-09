(ns fluree.db.ledger.docs.identity.auth
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.string :as str]))

(use-fixtures :once test/test-system)

; Add new auth record with permissions and test
(deftest viewPersonNoHandle
  (let [{:keys [private public id]} (fdb/new-private-key)
        addAuth         [{:_id   "_auth"
                          :id    id
                          :roles ["_role$standardUser"]}
                         {:_id   "_role$standardUser"
                          :id    "standardUser"
                          :rules ["_rule$viewAllPerson" "_rule$noHandle"]}
                         {:_id               "_rule$viewAllPerson"
                          :id                "viewAllPerson"
                          :collection        "person"
                          :collectionDefault true
                          :fns               [["_fn/name" "true"]]
                          :ops               ["all"]}
                         {:_id        "_rule$noHandle"
                          :id         "noHandle"
                          :collection "person"
                          :predicates ["person/handle"]
                          :fns        [["_fn/name" "false"]]
                          :ops        ["all"]}]
        addAuthResp     (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat addAuth))
        personQuery     {:select ["handle", "favNums"] :from "person" :opts {:meta true}}

        ;; TODO - ideally, we should wait to return db at a given block, for now just retrying once

        _               (async/<!! (async/timeout 1000))

        newDb           (basic/get-db test/ledger-chat {:auth ["_auth/id" id]})
        personQueryResp (async/<!! (fdb/query-async newDb personQuery))
        retry?          (not= (:block personQueryResp) (:block addAuthResp))
        personRes       (if retry?
                          (do (async/<!! (async/timeout 1000))
                              (async/<!! (fdb/query-async (basic/get-db test/ledger-chat {:auth ["_auth/id" id]})
                                                          personQuery)))
                          (:result personQueryResp))]

    (is (= 200 (:status addAuthResp)))

    ;; Auth can view favNums
    (is (some #(get % "favNums") personRes))

    ;; Auth cannot view handles
    (is (not-any? #(get % "handle") personRes))))


;; Auth should be able to sign for a transactions it normally wouldn't be able to
;; if it is the authority for an auth that DOES have the ability to issue that txn.


(deftest createAuthority
  ;; TODO - sometimes fails, does this have to do with padding in crypto?
  (let [{:keys [private public id]} (fdb/new-private-key)
        opts          {:timeout 240000}
        addAuth       [{:_id "_auth$authority"
                        :id  id}
                       {:_id       "_auth"
                        :id        "testAuth"
                        :authority ["_auth$authority"]
                        :roles     ["_role$editUser"]}
                       {:_id   "_role$editUser"
                        :id    "editUser"
                        :rules ["_rule$editPerson"]}
                       {:_id               "_rule$editPerson"
                        :id                "editPerson"
                        :collection        "person"
                        :collectionDefault true
                        :fns               [["_fn/name" "true"]]
                        :ops               ["all"]}]
        addAuthResp   (async/<!! (fdb/transact-async
                                   (basic/get-conn)
                                   test/ledger-chat
                                   addAuth
                                   opts))
        addPerson     [{:_id "person" :handle "authority"}]
        opts-with-auth (merge opts {:auth        "testAuth"
                                    :private-key private})
        addPersonResp (async/<!! (fdb/transact-async
                                   (basic/get-conn)
                                   test/ledger-chat
                                   addPerson
                                   opts-with-auth))
        testResp      (-> (async/<!! (fdb/transact-async
                                       (basic/get-conn)
                                       test/ledger-chat
                                       addPerson
                                       {:auth "testAuth"}))
                          test/safe-Throwable->map
                          :cause)]
    (is (= 200 (:status addAuthResp)))

    (is (= 200 (:status addPersonResp)))

    (is (= id (:authority addPersonResp)))

    (is (= "testAuth" (:auth addPersonResp)))

    (is (str/includes? testResp "is not an authority for auth: "))))

(deftest auth-test
  (viewPersonNoHandle)
  (createAuthority))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (auth-test))