(ns fluree.db.ledger.docs.examples.voting
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]))

(use-fixtures :once test/test-system)

;; Voting Example

(def soft-cell-opts {:auth        "TfHzKHsTdXVhbjskqesPTi6ZqwXHghFb1yK"
                     :private-key "4b288665f5e5f9b1078d3c54f916a86433557fbc16ffcb8de827104739c84ed4"})

(def dMR-opts {:auth        "TfFoQ4yB3vFn3th7Vce36Cb45fDau255GdH"
               :private-key "46e37823bfe73ac2b5e440238cb2b65a1cb4115721f23202e543c454faab8449"})

(def rSF-opts {:auth        "TfBvBxdxcXNrDQY8aNcYmoUuA2TC1CTiWAK"
               :private-key "afa6b042a342845c3bf4ea5fd2690d8548d5169fd18d18081ac8ac9093c2e43c"})

;; Add schema

(deftest add-schema
  (testing "Add schema for the voting app")
  (let [filename    "../test/fluree/db/ledger/Resources/Voting/schema.edn"
        txn         (edn/read-string (slurp (io/resource filename)))
        schema-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting txn))]

    ;; status should be 200
    (is (= 200 (:status schema-resp)))

    ;; block should be 2
    (is (= 2 (:block schema-resp)))

    ;; there should be 2 _collection tempids
    (is (= 2 (test/get-tempid-count (:tempids schema-resp) "_collection")))

    ;; there should be 10 _predicate tempids
    (is (= 10 (test/get-tempid-count (:tempids schema-resp) "_predicate")))

    ;; there should be 2 tempids keys
    (is (= 2 (count (keys (:tempids schema-resp)))))))

;; Add sample data

(deftest add-sample-data
  (testing "Add sample data for the voting app")
  (let [filename  "../test/fluree/db/ledger/Resources/Voting/data.edn"
        txn       (edn/read-string (slurp (io/resource filename)))
        data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 3
    (is (= 3 (:block data-resp)))

    ;; there should be 11 tempids
    (is (= 11 (count (:tempids data-resp))))))

;; Add permissions

(deftest add-permissions
  (testing "Add permissions for the voting app")
  (let [filename  "../test/fluree/db/ledger/Resources/Voting/permissions.edn"
        txn       (edn/read-string (slurp (io/resource filename)))
        data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 4
    (is (= 4 (:block data-resp)))

    ;; there should be 12 tempids
    (is (= 6 (count (:tempids data-resp))))))


;; Preventing Vote Fraud - add ownAuth?

(deftest prevent-voter-fraud
  (testing "Preventing Vote Fraud - add ownAuth? smart function")
  (let [txn  [{:_id "_fn$ownAuth", :_fn/name "ownAuth?", :_fn/code "(== (?o) (?auth_id))"}
              {:_id ["_predicate/name" "vote/yesVotes"], :spec ["_fn$ownAuth"]}
              {:_id ["_predicate/name" "vote/noVotes"], :spec ["_fn$ownAuth"]}]
        resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting txn))]

    ;; status should be 200
    (is (= 200 (:status resp)))

    ;; block should be 5
    (is (= 5 (:block resp)))

    ;; there should be 1 tempids
    (is (= 1 (count (:tempids resp))))))

;; Propose a change

(deftest propose-a-change
  (testing "Soft Cell proposes a change to their username")
  (let [txn  [{:_id       "change",
               :name      "softCellNameChange",
               :doc       "It's time for a change!",
               :subject   ["_user/username" "softCell"],
               :predicate ["_predicate/name" "_user/username"],
               :object    "hardCell",
               :vote      "vote$softCell"}
              {:_id "vote$softCell", :name "softCellNameVote", :yesVotes [["_auth/id" "TfHzKHsTdXVhbjskqesPTi6ZqwXHghFb1yK"]]}]
        resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting txn soft-cell-opts))]

    ;; status should be 200
    (is (= 200 (:status resp)))

    ;; block should be 6
    (is (= 6 (:block resp)))

    ;; there should be 2 tempids
    (is (= 2 (count (:tempids resp))))))

;; Add several smart functions

(deftest create-vote-smart-function
  (testing "Add several smart functions"
    (let [opts            {:timeout 240000}
          voteWhereTxn    [{:_id  "_fn",
                            :name "voteWhere",
                            :code "(str \"[[\\\"?change\\\", \\\"change/subject\\\", \" (?sid) \"],[\\\"?change\\\", \\\"change/predicate\\\", \" (?pid) \"],[\\\"?change\\\", \\\"change/object\\\", \\\"\" (?o) \"\\\"], [\\\"?change\\\", \\\"change/vote\\\", \\\"?vote\\\"]]\")"}]
          resp            (async/<!! (fdb/transact-async
                                       (basic/get-conn)
                                       test/ledger-voting
                                       voteWhereTxn
                                       opts))
          voteTxn         [{:_id  "_fn",
                            :name "vote",
                            :code "(query (str \"{\\\"select\\\": {\\\"?vote\\\": [\\\"*\\\"] }, \\\"where\\\":\" (voteWhere) \"}\"))"}]
          resp2           (async/<!! (fdb/transact-async
                                       (basic/get-conn)
                                       test/ledger-voting
                                       voteTxn
                                       opts))
          yesNoVotes      [{:_id "_fn", :name "noVotes", :code "(get-all (nth (vote) 0) [\"vote/noVotes\" \"_id\"] )"}
                           {:_id "_fn", :name "yesVotes", :code "(get-all (nth (vote) 0) [\"vote/yesVotes\" \"_id\"] )"}]
          resp3           (async/<!! (fdb/transact-async
                                       (basic/get-conn)
                                       test/ledger-voting
                                       yesNoVotes
                                       opts))
          minVotesPercent [{:_id    "_fn",
                            :name   "minWinPercentage",
                            :params ["percentage"],
                            :code   "(> (/ (count (yesVotes)) (+ (count (yesVotes)) (count (noVotes)))) percentage)"}
                           {:_id "_fn", :name "minVotes", :params ["n"], :code "(> (+ (count (yesVotes))  (count (noVotes))) n)"}]
          resp4           (async/<!! (fdb/transact-async
                                       (basic/get-conn)
                                       test/ledger-voting
                                       minVotesPercent
                                       opts))
          addUNSpec       [{:_id "_fn$2VotesMajority", :name "2VotesMajority", :code "(and (minVotes 2) (minWinPercentage 0.5))"}
                           {:_id ["_predicate/name" "_user/username"], :spec ["_fn$2VotesMajority"]}]
          resp5           (async/<!! (fdb/transact-async
                                       (basic/get-conn)
                                       test/ledger-voting
                                       addUNSpec
                                       opts))]

      ;; status should be 200
      (is (= 200 (:status resp)))
      (is (= 200 (:status resp2)))
      (is (= 200 (:status resp3)))
      (is (= 200 (:status resp4)))
      (is (= 200 (:status resp5)))

      ;; block should be 7, 8, 9, 10, 11
      (is (= 7 (:block resp)))
      (is (= 8 (:block resp2)))
      (is (= 9 (:block resp3)))
      (is (= 10 (:block resp4)))
      (is (= 11 (:block resp5)))

      ;; is the count of tempids as expected?
      (is (= 1 (count (:tempids resp))))
      (is (= 1 (count (:tempids resp2))))
      ;; 2 _fn tempids
      (is (= 2 (test/get-tempid-count (:tempids resp3) "_fn")))
      (is (= 2 (test/get-tempid-count (:tempids resp4) "_fn")))
      (is (= 1 (count (:tempids resp5)))))))


;; Invalid transaction

(deftest invalid-change
  (let [attemptChange [{:_id ["_user/username" "softCell"], :username "hardCell"}]
        changeRespErrors    (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting attemptChange soft-cell-opts))
                          test/extract-errors
                          :meta
                          :errors)]

    (is (= 1 (count changeRespErrors)))

    (is (= (first changeRespErrors)
           {:status 400
            :error :db/predicate-spec
            :cause [87960930223082 50 "hardCell" -23 true nil]
            :message "Predicate spec failed for predicate: _user/username."}))))


;; Add Votes

(deftest add-votes
  (let [vote1              [{:_id ["vote/name" "softCellNameVote"], :yesVotes [["_auth/id" "TfFoQ4yB3vFn3th7Vce36Cb45fDau255GdH"]]}]
        vote1Resp          (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting vote1 dMR-opts))
        vote2              [{:_id ["vote/name" "softCellNameVote"], :yesVotes [["_auth/id" "TfBvBxdxcXNrDQY8aNcYmoUuA2TC1CTiWAK"]]}]
        vote2Resp          (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting vote2 rSF-opts))
        usernameChange     [{:_id ["_user/username" "softCell"], :username "hardCell"}]
        usernameChangeResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-voting usernameChange soft-cell-opts))]

    ;; status should be 200
    (is (= 200 (:status vote1Resp)))
    (is (= 200 (:status vote2Resp)))
    (is (= 200 (:status usernameChangeResp)))


    ;; is the count of tempids as expected?
    (is (= 0 (count (:tempids vote1Resp))))
    (is (= 0 (count (:tempids vote2Resp))))
    (is (= 0 (count (:tempids usernameChangeResp))))

    ;; is the count of flakes as expected?
    (is (= 7 (count (:flakes vote1Resp))))
    (is (= 7 (count (:flakes vote2Resp))))
    (is (= 8 (count (:flakes usernameChangeResp))))))

(deftest voting-test
  (add-schema)
  (add-sample-data)
  (add-permissions)
  (prevent-voter-fraud)
  (propose-a-change)
  (create-vote-smart-function)
  (invalid-change)
  (add-votes))






