(ns fluree.db.ledger.smart-functions-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as fdb]
            [fluree.db.util.json :as json]
            [fluree.db.query.http-signatures :as http-sig]
            [byte-streams :as bs]
            [org.httpkit.client :as http]))

(use-fixtures :once test/test-system)

(deftest custom-smart-fn-add-three-add-ten-test
  (testing "custom smart functions can compose"
    (let [ledger            (test/rand-ledger test/ledger-chat)
          _                 (test/transact-schema ledger "chat.edn")
          _                 (test/transact-schema ledger "chat-preds.edn")
          add-three-tx      [{:_id  "_fn", :name "addThree", :params ["n"]
                              :code "(+ 3 n)"}]
          add-three-tx-resp (<!! (fdb/transact-async (:conn test/system) ledger
                                                     add-three-tx))
          add-ten-tx        [{:_id  "_fn", :name "addTen", :params ["n"]
                              :code "(+ 7 (addThree n))"}]
          add-ten-tx-resp   (<!! (fdb/transact-async (:conn test/system) ledger
                                                     add-ten-tx))
          chat-tx           [{:_id "chat", :message "#(str \"My age is: \" (addTen 27))"}]
          chat-tx-resp      (<!! (fdb/transact-async (:conn test/system) ledger
                                                     chat-tx))
          message-flake     (->> chat-tx-resp :flakes (filter #(< 0 (first %))) first)]

      (is (= "My age is: 37" (nth message-flake 2))))))

(deftest custom-smart-fn-cannot-access-clojure.core-test
  (testing "trying to call clojure.core/str fns fails"
    (let [ledger            (test/rand-ledger test/ledger-chat)
          _                 (test/transact-schema ledger "chat.edn")
          _                 (test/transact-schema ledger "chat-preds.edn")
          add-three-tx      [{:_id  "_fn", :name "addThree", :params ["n"]
                              :code "(+ 3 n)"}]
          add-three-tx-resp (<!! (fdb/transact-async (:conn test/system) ledger
                                                     add-three-tx))
          add-ten-tx        [{:_id  "_fn", :name "addTen", :params ["n"]
                              :code "(+ 7 (addThree n))"}]
          add-ten-tx-resp   (<!! (fdb/transact-async (:conn test/system) ledger
                                                     add-ten-tx))
          chat-tx           [{:_id     "chat"
                              :message "#(clojure.core/str \"My age is: \" (addTen 27))"}]
          chat-tx-resp      (<!! (fdb/transact-async (:conn test/system) ledger
                                                     chat-tx))]

      (is (= "Unknown function: clojure.core/str"
             (-> chat-tx-resp test/safe-Throwable->map :cause)))))

  (testing "trying to call clojure.core/+ in custom fns fails"
    (let [ledger            (test/rand-ledger test/ledger-chat)
          _                 (test/transact-schema ledger "chat.edn")
          _                 (test/transact-schema ledger "chat-preds.edn")
          add-three-tx      [{:_id  "_fn", :name "addThree", :params ["n"]
                              :code "(+ 3 n)"}]
          add-three-tx-resp (<!! (fdb/transact-async (:conn test/system) ledger
                                                     add-three-tx))
          add-ten-tx        [{:_id  "_fn", :name "addTen", :params ["n"]
                              :code "(clojure.core/+ 7 (addThree n))"}]
          add-ten-tx-resp   (<!! (fdb/transact-async (:conn test/system) ledger
                                                     add-ten-tx))
          chat-tx           [{:_id     "chat"
                              :message "#(str \"My age is: \" (addTen 27))"}]
          chat-tx-resp      (<!! (fdb/transact-async (:conn test/system) ledger
                                                     chat-tx))]

      (is (= "Unknown function: clojure.core/+"
             (-> chat-tx-resp test/safe-Throwable->map :cause))))))

(deftest predicate-spec-non-negative-test
  (let [ledger        (test/rand-ledger test/ledger-chat)
        _             (test/transact-schema ledger "chat.edn")
        _             (test/transact-schema ledger "chat-preds.edn")
        non-neg-spec  [{:_id  "_fn$nonNeg",
                        :name "nonNegative?",
                        :doc  "Checks that a value is non-negative",
                        :code "(<= 0 (?o))"}
                       {:_id  ["_predicate/name" "person/favNums"],
                        :spec ["_fn$nonNeg"]}]
        add-spec-resp (<!! (fdb/transact-async (:conn test/system)
                                               ledger non-neg-spec))
        test-spec     [{:_id "person", :handle "aJay", :favNums [12 -4 57]}]
        test-resp     (-> (<!! (fdb/transact-async (:conn test/system)
                                                   ledger test-spec))
                          test/safe-Throwable->map :cause)]

    (is (= "Predicate spec failed for predicate: person/favNums." test-resp))))

(deftest full-name-req-test
  (testing "missing fullName is rejected"
    (let [ledger         (test/rand-ledger test/ledger-chat)
          _              (test/transact-schema ledger "chat.edn")
          _              (test/transact-schema ledger "chat-preds.edn")
          full-name-spec [{:_id     ["_collection/name" "person"]
                           :spec    ["_fn$fullNameReq"]
                           :specDoc "A person is required to have a fullName."}
                          {:_id  "_fn$fullNameReq",
                           :name "fullNameReq",
                           :code "(boolean (get (query (str \"{\\\"select\\\": [\\\"*\\\"], \\\"from\\\": \" (?sid) \"}\")) \"person/fullName\"))"}]
          add-spec-resp  (<!! (fdb/transact-async (:conn test/system) ledger
                                                  full-name-spec))
          test-spec      [{:_id "person", :handle "noFullName"}]
          test-resp      (-> (<!! (fdb/transact-async (:conn test/system) ledger
                                                      test-spec))
                             test/safe-Throwable->map :cause)]

      (is (= "Collection spec failed for: person. A person is required to have a fullName."
             test-resp))))

  (testing "fullName can't be retracted"
    (let [ledger         (test/rand-ledger test/ledger-chat)
          _              (test/transact-schema ledger "chat.edn")
          _              (test/transact-schema ledger "chat-preds.edn")
          full-name-spec [{:_id     ["_collection/name" "person"]
                           :spec    ["_fn$fullNameReq"]
                           :specDoc "A person is required to have a fullName."}
                          {:_id  "_fn$fullNameReq",
                           :name "fullNameReq",
                           :code "(boolean (get (query (str \"{\\\"select\\\": [\\\"*\\\"], \\\"from\\\": \" (?sid) \"}\")) \"person/fullName\"))"}]
          add-spec-resp  (<!! (fdb/transact-async (:conn test/system) ledger
                                                  full-name-spec))
          add-person     [{:_id "person", :handle "deleteMe", :fullName "To Be Deleted"}]
          _              (<!! (fdb/transact-async (:conn test/system) ledger add-person))
          test-spec-1    [{:_id ["person/handle" "deleteMe"] :fullName nil}]
          test-resp-1    (-> (<!! (fdb/transact-async (:conn test/system) ledger test-spec-1))
                             test/safe-Throwable->map :cause)
          test-spec-2    [{:_id ["person/handle" "deleteMe"] :_action "delete"}]
          test-resp-2    (<!! (fdb/transact-async (:conn test/system) ledger test-spec-2))]

      (is (= "Collection spec failed for: person. A person is required to have a fullName."
             test-resp-1))
      (is (map? test-resp-2))
      (is (= 200 (:status test-resp-2)))
      (is (= 8 (-> test-resp-2 :flakes count))))))

(deftest rules-test
  (let [jdoe     {:auth        "TfKYG5F5iCsii1JvGGY2Pv6bPVVbZ2ERjmJ"
                  :private-key "1787cab58d5b146a049f220c975d5dce7904c63f25d6d834d6980c427b47f412"}
        zsmith   {:auth        "TfFzb1tZDkGMBqWr8xMmRVvYmNYFKH9aNpi"
                  :private-key "c0588115314065f7949f87f0f6adda3a252105be89b5080c56bb889cd20d841f"}
        ledger   (test/rand-ledger test/ledger-chat)
        endpoint (str "http://localhost:" @test/port "/fdb/" ledger "/")]

    (testing "can add permissions"
      (let [person-auth [{:_id  "_predicate", :name "person/auth", :doc "Reference to a database auth.",
                          :type "ref", :restrictCollection "_auth"}]
            _           (test/transact-schema ledger "chat.edn")
            _           (test/transact-schema ledger "chat-preds.edn")
            _           (test/transact-data ledger "chat.edn")
            _           (<!! (fdb/transact-async (:conn test/system) ledger
                                                 person-auth))
            resp        (test/transact-data ledger "chat-rules.edn")]
        (is (= 200 (:status resp)))))

    (testing "permissioned query"
      (let [query        {:chat   {:select ["*" {"chat/comments" ["*"]}]
                                   :from   "chat"}
                          :person {:select ["*"] :from "person"}}
            my-request   {:headers {"content-type" "application/json"}
                          :body    (json/stringify query)}
            q-endpoint   (str endpoint "multi-query")
            level-1-req  (http-sig/sign-request :post q-endpoint my-request
                                                (:private-key jdoe))
            level-2-req  (http-sig/sign-request :post q-endpoint my-request
                                                (:private-key zsmith))
            l1-resp      @(http/post q-endpoint level-1-req)
            l2-resp      @(http/post q-endpoint level-2-req)
            level-1-resp (-> l1-resp :body bs/to-string json/parse)
            level-2-resp (-> l2-resp :body bs/to-string json/parse)]

        ;; Level 1 should not be able to view chat/comments
        (is (= #{} (->> level-1-resp :chat (map :chat/comments) flatten
                        (remove nil?) set)))

        ;; Level 2 should be able to view chat/comments + comment messages
        (is (= #{"Zsmith is responding!" "Welcome Diana!"}
               (->> (map #(get-in % [:chat/comments 0 :comment/message])
                         (:chat level-2-resp)) (remove nil?) set)))

        ;; Level 1 should only be able to view person/handles and _ids.
        (is (= #{:person/handle :_id}
               (->> (:person level-1-resp) (map keys) flatten set)))

        ;; Level 2 should be able to view all person predicates (but not refs they were not given permission to)
        (is (= #{:person/handle :_id :person/favNums :person/age
                 :person/follows :person/fullName :person/active}
               (->> (:person level-2-resp) (map keys) flatten set)))))

    (testing "permissioned transaction"
      (let [jdoeChat     [{:_id    "chat$1", :message "Hey there!"
                           :person ["person/handle" "jdoe"]}]
            addOther     (-> (<!! (fdb/transact-async (:conn test/system) ledger
                                                      jdoeChat zsmith))
                             test/safe-Throwable->map
                             :cause)
            addOwn       (<!! (fdb/transact-async (:conn test/system) ledger
                                                  jdoeChat jdoe))
            chat$1       (get (:tempids addOwn) "chat$1")
            jdoeChatEdit [{:_id chat$1 :message "Attempting to edit"}]
            editOther    (-> (<!! (fdb/transact-async (:conn test/system) ledger
                                                      jdoeChatEdit zsmith))
                             test/safe-Throwable->map
                             :cause)
            editOwn      (<!! (fdb/transact-async (:conn test/system) ledger
                                                  jdoeChatEdit jdoe))]

        (is (= "Insufficient permissions for predicate: chat/message within collection: chat."
               addOther))

        (is (= 200 (:status addOwn)))

        (is (= "Insufficient permissions for predicate: chat/message within collection: chat."
               editOther))

        (is (= 200 (:status editOwn)))))))

(deftest in-transactions-test
  (let [ledger        (test/rand-ledger test/ledger-chat)
        _             (test/transact-schema ledger "chat.edn")
        _             (test/transact-schema ledger "chat-preds.edn")]
    (testing "str and ?pO fns in a transaction"
      (let [_             (test/transact-data ledger "chat.edn")
            long-desc-txn [{:_id      ["person/handle" "jdoe"]
                            :fullName "#(str (?pO) \", Sr.\")"}]
            res           (<!! (fdb/transact-async (:conn test/system) ledger
                                                   long-desc-txn))
            flakes        (-> res :flakes)
            pos-flakes    (filter #(< 0 (first %)) flakes)]

        (is (= 200 (:status res)))

        (is (= 0 (-> res :tempids count)))

        (is (= 8 (-> res :flakes count)))

        (= #{"Jane Doe" "Jane Doe, Sr."}
           (-> (map #(nth % 2) pos-flakes) set))))
    (testing "max in a transaction"
      (let [count               20
            tx                  (mapv (fn [n]
                                        {:_id      "person"
                                         :fullName (str "#(max " n " " (+ 1 n)
                                                        " " (+ 2 n) ")")})
                                      (range 1 (inc count)))
            tx-result           (<!! (fdb/transact-async (:conn test/system)
                                                         ledger tx))
            full-names          (->> (filter #(= 1002 (second %))
                                             (:flakes tx-result))
                                     (map #(nth % 2)) set)
            expected-full-names (-> (map str (range 3 (+ 3 count))) set)]

        (is (= full-names expected-full-names))))))
