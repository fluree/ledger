(ns fluree.db.ledger.docs.schema.predicates
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.string :as str]))

(use-fixtures :once test/test-system)

(deftest add-predicate-long-desc
  (testing "Add long description to collections.")
  (let [long-desc-txn     [{:_id "_predicate", :name "_predicate/longDescription", :type "string"}]
        res               (async/<!! (fdb/transact-async
                                       (basic/get-conn)
                                       test/ledger-chat
                                       long-desc-txn
                                       {:timeout 240000}))

        add-long-desc-txn [{:_id             ["_predicate/name" "person/handle"],
                            :longDescription "I have a lot to say about this predicate, so this is a longer description about the person/handle predicate"}
                           {:_id             "_predicate",
                            :name            "_user/age",
                            :type            "int",
                            :longDescription "I have a lot to say about this predicate, so this is a longer description about the _user/age predicate"}]

        add-long-desc-res (async/<!! (fdb/transact-async
                                       (basic/get-conn)
                                       test/ledger-chat
                                       add-long-desc-txn
                                       {:timeout 240000}))]

    (is (= 200 (:status res)))
    (is (= 200 (:status add-long-desc-res)))

    (is (= 1 (-> res :tempids count)))
    (is (= 1 (-> add-long-desc-res :tempids count)))

    (is (= 9 (-> res :flakes count)))
    (is (= 11 (-> add-long-desc-res :flakes count)))))


(deftest query-predicate-name-predicate
  (testing "Query the _predicate/name predicate")
  (let [query-predicates {:select ["*"]
                          :from   ["_predicate/name" "_predicate/name"]}
        db               (basic/get-db test/ledger-chat)
        res              (-> (async/<!! (fdb/query-async db query-predicates))
                             first)]

    (is (= "_predicate/name" (get res "_predicate/name")))))



(deftest predicate-upsert
  (testing "Attempt to upsert _predicate/name, then set upsert")
  (let [txn                [{:_id "_predicate", :name "_user/username", :doc "The user's username"}]
        res                (-> (async/<!! (fdb/transact-async
                                            (basic/get-conn)
                                            test/ledger-chat
                                            txn
                                            {:timeout 480000})) test/safe-Throwable->map :cause)
        set-upsert         [{:_id ["_predicate/name" "_predicate/name"], :upsert true}]
        upsertRes          (async/<!! (fdb/transact-async
                                        (basic/get-conn)
                                        test/ledger-chat
                                        set-upsert
                                        {:timeout 480000}))
        attemptToUpsertRes (async/<!! (fdb/transact-async
                                        (basic/get-conn)
                                        test/ledger-chat
                                        txn
                                        {:timeout 480000}))]

    (is (str/includes? res "Predicate _predicate/name does not allow upsert"))
    (is (str/includes? res "duplicates an existing _predicate/name (_user/username)"))

    (is (= 200 (:status upsertRes)))

    (is (= 200 (:status attemptToUpsertRes)))

    (is (= 9 (-> attemptToUpsertRes :flakes count)))))


(deftest query-all-predicates
  (testing "Query all predicates with filter")
  (let [query-predicates {:select      {:?predicate ["*"]},
                          :where       [["?predicate" "_predicate/name" "?name"]]
                          :filter      ["(re-find (re-pattern \"^_collection\") ?name)"]
                          :prettyPrint true}
        db               (basic/get-db test/ledger-chat)
        res              (async/<!! (fdb/query-async db query-predicates))]
    (is (every? (fn [r] (-> r (get "predicate")
                            (get "_predicate/name") (str/includes? "_collection"))) res))))


(deftest good-bad-predicate-names
  (testing "Ensure bad predicate names throw an exception")
  (let [good-pred-names ["person/hometown" "_user/hometown" "person/1...." "person/12a-"
                         "person/a._" "person/0aA_..----"]
        bad-pred-names  ["location_Via_anything" "person/__city" "person/_city" ":bad:uri"]
        good-tx         (mapv (fn [n] {:_id  "_predicate"
                                       :name n
                                       :type "string"}) good-pred-names)
        bad-tx          (mapv (fn [n] [{:_id  "_predicate"
                                        :name n
                                        :type "string"}]) bad-pred-names)
        good-resp       (async/<!! (fdb/transact-async
                                     (basic/get-conn)
                                     test/ledger-chat
                                     good-tx
                                     {:timeout 240000}))
        bad-resp        (async/<!! (basic/issue-consecutive-transactions
                                     test/ledger-chat
                                     bad-tx
                                     {:timeout 240000}))
        bad-resp-errs   (map #(-> % test/safe-Throwable->map :cause) bad-resp)]

    (is (= (count bad-pred-names) (count bad-resp-errs)))

    (is (every? #(str/includes? % "Invalid predicate name.") bad-resp-errs))

    (is (= 200 (:status good-resp)))))


(deftest automatic-component-deletion
  (testing "Add a chat with comments, then delete the chat, and make sure the comments are deleted too.")
  (let [create-chat-comments-txn [{:_id      "chat$1"
                                   :message  "This is a chat message"
                                   :comments ["comment$1" "comment$2"]}
                                  {:_id     "comment$1"
                                   :message "Hi, this is comment 1!"}
                                  {:_id     "comment$2"
                                   :message "Hi, this is comment 2!"}]
        add-data-resp            (async/<!! (fdb/transact-async
                                              (basic/get-conn)
                                              test/ledger-chat
                                              create-chat-comments-txn
                                              {:timeout 240000}))
        tempids                  (:tempids add-data-resp)
        chat$1                   (get tempids "chat$1")
        comment$1                (get tempids "comment$1")
        comment$2                (get tempids "comment$2")
        delete-chat              [{:_id chat$1 :_action "delete"}]
        delete-resp              (async/<!! (fdb/transact-async
                                              (basic/get-conn)
                                              test/ledger-chat
                                              delete-chat
                                              {:timeout 240000}))
        cmnt-flakes              (filter #(#{comment$1 comment$2} (first %))
                                         (:flakes delete-resp))]

    (is (= 200 (:status add-data-resp)))
    (is (= 200 (:status delete-resp)))

    ;; Are there 2 flakes corresponding to the comment _ids in delete-resp?
    (is (= 2 (count cmnt-flakes)))

    ;; Are they both retractions?
    (is (every? #(false? (nth % 4)) cmnt-flakes))))


(deftest unique-test
  (testing "Throws an error when attempting to write a person/handle that already exists (unique predicate)."
    (let [add-person      [{:_id "person" :handle "jdoe"}]
          add-person-resp (-> (async/<!! (fdb/transact-async
                                           (basic/get-conn)
                                           test/ledger-chat
                                           add-person
                                           {:timeout 240000}))
                              test/safe-Throwable->map
                              :cause)]

      (is (str/includes? add-person-resp "Predicate person/handle does not allow upsert"))
      (is (str/includes? add-person-resp "duplicates an existing person/handle (jdoe)")))))

(deftest predicates-test
  (add-predicate-long-desc)
  (query-predicate-name-predicate)
  (predicate-upsert)
  (query-all-predicates)
  (good-bad-predicate-names)
  (automatic-component-deletion)
  (unique-test))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (predicates-test))


