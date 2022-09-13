(ns fluree.db.ledger.rule-functions-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [fluree.db.api :as fdb]
            [fluree.db.test-helpers :as test]))

(use-fixtures :once (partial test/test-system
                             {:fdb-api-open false}))

(defn assert-success
  [result]
  (if (instance? Throwable result)
    (throw result)
    result))

(defn printlnn
  [& s]
  (apply println (concat s ["\n"])))

(deftest ^:wes update-role-fn-test
  (testing "Updating a role fn takes effect right away"
    (let [jdoe-keys          (test/load-keys "jdoe-auth")
          zsmith-keys        (test/load-keys "zsmith-auth")
          ledger             (test/rand-ledger "test/role-fn-update")
          _                  (assert-success (test/transact-schema ledger "chat.edn" :clj))
          _                  (assert-success (test/transact-schema ledger "chat-preds.edn" :clj))
          _                  (assert-success (test/transact-data ledger "chat.edn" :clj))
          {:keys [block]} (assert-success (test/transact-data ledger "chat-rules.edn" :clj))
          rule-query         {:select ["*"], :from "_rule"}
          fn-query           {:select ["*"], :from "_fn"}
          role-query         {:select ["*"], :from "_role"}
          user-query         {:select ["*"], :from "_user"}
          auth-query         {:select ["*"], :from "_auth"}
          db                 (fdb/db (:conn test/system) ledger {:syncTo block})
          rule-resp          (<!! (fdb/query-async db rule-query))
          fn-resp            (<!! (fdb/query-async db fn-query))
          role-resp          (<!! (fdb/query-async db role-query))
          user-resp          (<!! (fdb/query-async db user-query))
          auth-resp          (<!! (fdb/query-async db auth-query))
          own-chats-query    {:select {"?c" ["*"]}
                              :where  [["?c" "chat/person" ["person/handle" "jdoe"]]]
                              :from   "chat"}
          own-chats          (<!! (fdb/query-async db own-chats-query))
          own-chat-id        (-> own-chats first :_id)
          edit-own-chat-txn  [{:_id          own-chat-id
                               :chat/message "Now it's this other thing"}]
          {:keys [block]
           :as   edit-resp} (assert-success
                              (<!! (fdb/transact-async (:conn test/system)
                                                       ledger edit-own-chat-txn
                                                       {:private-key (:private jdoe-keys)})))
          db                 (fdb/db (:conn test/system) ledger {:syncTo block})
          own-chats-edited   (<!! (fdb/query-async db own-chats-query))
          others-chats-query {:select {"?c" ["*"]}
                              :where  [["?c" "chat/person" ["person/handle" "zsmith"]]]
                              :from   "chat"}
          others-chats       (<!! (fdb/query-async db others-chats-query))
          others-chat-id     (-> others-chats first :_id)
          edit-others-chat-txn [{:_id others-chat-id
                                 :chat/message "Shouldn't be able to do this"}]
          edit-others-resp   (<!! (fdb/transact-async (:conn test/system)
                                                      ledger edit-others-chat-txn
                                                      {:private-key (:private jdoe-keys)}))
          ;chat-edit-rule-txn [{:_id ["_rule/id" "editOwnChats"]
          ;                     :fns [{:_id "_fn"
          ;                            :name "handle starts with z"
          ;                            :code "(re-find \"^z\" (get (?s) \"person/handle\"))"}]}]
          chat-edit-rule-txn  [{:_id ["_rule/id" "editOwnChats"]
                                :fns [{:_id "_fn"
                                       :name "always fail"
                                       :code "false"}]}]
          chat-edit-rule-resp (assert-success
                                (<!! (fdb/transact-async (:conn test/system)
                                                         ledger chat-edit-rule-txn)))
          jdoe-edits-own-chat-txn [{:_id own-chat-id
                                    :chat/message "No longer works"}]
          jdoe-edits-own-chat-resp (<!! (fdb/transact-async (:conn test/system)
                                                            ledger jdoe-edits-own-chat-txn
                                                            {:private-key (:private jdoe-keys)}))]
      (printlnn "RULES:" (pr-str rule-resp))
      (printlnn "FNS:" (pr-str fn-resp))
      (printlnn "ROLES:" (pr-str role-resp))
      (printlnn "USERS:" (pr-str user-resp))
      (printlnn "AUTHS:" (pr-str auth-resp))
      (printlnn "OWN CHATS:" (pr-str own-chats))
      (printlnn "EDIT RULE RESP:" (pr-str chat-edit-rule-resp))
      (is (= "Now it's this other thing" (-> own-chats-edited first (get "chat/message"))))
      (is (instance? Throwable edit-others-resp))
      (is (= :db/write-permission (-> edit-others-resp ex-data :error)))
      (is (instance? Throwable jdoe-edits-own-chat-resp)
          (str "jdoe own chat edit was not an error: " (pr-str jdoe-edits-own-chat-resp)))
      (is (= :db/write-permission (-> jdoe-edits-own-chat-resp ex-data :error))
          (str "jdoe own chat edit was unexpected error type: "
               (pr-str jdoe-edits-own-chat-resp))))))

;; This may or may not be worth testing
#_(deftest ^:wes root-role-test
    (testing "adding a new truthy fn works"
      (let [ledger        (test/rand-ledger "test/root-role")
            rule-fn-tx    [{:_id ["_rule/id" "root"]
                            :fns [{:_id  "_fn"
                                   :name "test"
                                   :code "(boolean 1)"}]}]
            rule-fn-resp  (<!! (fdb/transact-async (:conn test/system) ledger
                                                   rule-fn-tx))
            new-user-tx   [{:_id "_user", "_user/username" "tester"}]
            new-user-resp (<!! (fdb/transact-async (:conn test/system) ledger
                                                   new-user-tx))]
        (is (= 200 (:status rule-fn-resp))
            (str "Unexpected response status from rule-fn txn: " (pr-str rule-fn-resp)))
        (is (= 200 (:status new-user-resp))
            (str "Unexpected response status from new-user txn: " (pr-str new-user-resp))))))
