(ns fluree.db.ledger.docs.examples.cryptocurrency
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]))

(use-fixtures :once test/test-system)

;; Cryptocurrency xample

(def crypto-man-opts {:auth "TfDao2xAPN1ewfoZY6BJS16NfwZ2QYJ2cF2"
                      :private-key "745f3040cbfba59ba158fc4ab295d95eb4596666c4c275380491ac658cf8b60c"})

(def crypto-woman-opts {:auth "Tf6mUADU4SDa3yFQCn6D896NSdnfgQfzTAP"
                        :private-key "65a55074e1de61e08845d4dc5b997260f5f8c20b39b8070e7799bf92a006ad19"})

;; Add schema

(deftest add-schema
  (testing "Add schema for the cryptocurrency app")
  (let [txn   [{:_id "_collection", :name "wallet"}
               {:_id "_predicate", :name "wallet/balance", :type "int"}
               {:_id "_predicate", :name "wallet/user", :type "ref", :restrictCollection "_user"}
               {:_id "_predicate", :name "wallet/name", :type "string", :unique true},
               {:_id "_predicate", :name "_auth/descId", :type "string", :unique true}]
        schema-resp  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto txn))]

    ;; status should be 200
    (is (= 200 (:status schema-resp)))

    ;; block should be 2
    (is (= 2 (:block schema-resp)))

    ;; there should be 5 tempids
    (is (= 5 (count (:tempids schema-resp))))))

;; Add sample data

(deftest add-sample-data
  (testing "Add sample data for the voting app")
  (let [txn   [{:_id "_user$cryptoMan", :username "cryptoMan"}
               {:_id "_user$cryptoWoman", :username "cryptoWoman"}
               {:_id "wallet$cryptoMan", :name "cryptoMan", :balance 200, :user "_user$cryptoMan"}
               {:_id "wallet$cryptoWoman", :name "cryptoWoman", :balance 200, :user "_user$cryptoWoman"}]
        data-resp  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 3
    (is (= 3 (:block data-resp)))

    ;; there should be 4 tempids
    (is (= 4 (count (:tempids data-resp))))))


;; Add permissions

(deftest add-permissions
  (testing "Add permissions for the voting app")
  (let [filename  "../test/fluree/db/ledger/Resources/Cryptocurrency/permissions.edn"
        txn   (edn/read-string (slurp (io/resource filename)))
        data-resp  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 4
    (is (= 4 (:block data-resp)))

    ;; there should be 12 tempids
    (is (= 7 (count (:tempids data-resp))))))


;; Add smart functions

(deftest add-smart-functions
  (let [smart-function-txn [{:_id ["_predicate/name" "wallet/balance"], :spec ["_fn$nonNegative?" "_fn$subtractOwnAddOthers?"], :specDoc "You can only add to others balances, and only subtract from your own balance. No balances may be negative"}
                            {:_id "_fn$nonNegative?", :name "nonNegative?", :code "(< -1 (?o))"}
                            {:_id "_fn$subtractOwnAddOthers?",
                             :name "subtractOwnAddOthers?",
                             :code "(if-else (ownWallet?)  (> (?pO) (?o)) (< (?pO) (?o))))",
                             :doc "You can only add to others balances, and only subtract from your own balance"}]
        resp  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto smart-function-txn))]

    ;; status should be 200
    (is (= 200 (:status resp)))

    ;; block should be 5
    (is (= 5 (:block resp)))))

;; Test Add Own, Subtract Others

(deftest test-add-own-subtract-others
  (let [;; should succeed
        crypto-man-add-others [{:_id ["wallet/name" "cryptoWoman"], :balance 205}]
        resp1  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto crypto-man-add-others crypto-man-opts))

        ;; should fail
        crypto-man-add-own [{:_id ["wallet/name" "cryptoMan"], :balance 205}]
        resp2                 (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto crypto-man-add-own crypto-man-opts))
                                  test/safe-Throwable->map
                                  :cause)

        ;; should succeed
        crypto-woman-subtract-own  [{:_id ["wallet/name" "cryptoWoman"], :balance 195}]
        resp3 (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto crypto-woman-subtract-own crypto-woman-opts))

        ;; should fail
        crypto-woman-add-own  [{:_id ["wallet/name" "cryptoWoman"], :balance "#(+ (?pO) 5)"}]
        resp4 (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto crypto-woman-add-own crypto-woman-opts))
                  test/safe-Throwable->map
                  :cause)

        ;; should fail
        crypto-woman-subtract-others  [{:_id ["wallet/name" "cryptoMan"], :balance "#(- (?pO) 5)"}]
        resp5 (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto crypto-woman-subtract-others crypto-woman-opts))
                  test/safe-Throwable->map
                  :cause)]

    ;; status should be 200for resp1 and resp3
    (is (= 200 (:status resp1)))
    (is (= 200 (:status resp3)))

    (is (= resp2  "You can only add to others balances, and only subtract from your own balance. No balances may be negative Value: 205"))

    (is (= resp4 "You can only add to others balances, and only subtract from your own balance. No balances may be negative Value: 200"))

    (is (= resp5 "You can only add to others balances, and only subtract from your own balance. No balances may be negative Value: 195"))))


;; Add smart function cryptoSpent = cryptoReceived

(deftest spent-is-received
  (let [smartFunctionTxn [{:_id ["_predicate/name" "wallet/balance"],
                           :txSpec ["_fn$evenCryptoBalance"],
                           :txSpecDoc "The values of added and retracted wallet/balance flakes need to be equal"}
                          {:_id "_fn$evenCryptoBalance",
                           :name "evenCryptoBalance?",
                           :code "(== (objT) (objF))",
                           :doc "The values of added and retracted wallet/balance flakes need to be equal"}]
        resp  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto smartFunctionTxn))]

    ;; status should be 200
    (is (= 200 (:status resp)))

    ;; count of tempids should be 1
    (is (= 1 (-> resp :tempids count)))

    ;; there should be 12 flakes
    (is (= 12 (-> resp :flakes count)))))

;; Test spent = received

(deftest test-spent-is-received
  (let [;; should fail
        uneven-txn [{:_id ["wallet/name" "cryptoMan"], :balance "#(+ (?pO) 10)"}
                    {:_id ["wallet/name" "cryptoWoman"], :balance "#(- (?pO) 5)"}]
        resp1      (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto uneven-txn crypto-woman-opts))
                       test/safe-Throwable->map
                       :cause)

        even-txn [{:_id ["wallet/name" "cryptoMan"], :balance "#(- (?pO) 10)"}
                  {:_id ["wallet/name" "cryptoWoman"], :balance "#(+ (?pO) 10)"}]

        ;; should fail - signed as cryptoWoman
        resp2      (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto even-txn crypto-woman-opts))
                       test/safe-Throwable->map
                       :cause)

        ;; should succeed - signed as cryptoMan
        resp3      (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-crypto even-txn crypto-man-opts))]

    ;; status should be 200 for resp3
    (is (= 200 (:status resp3)))

    (is (= resp1 "The predicate wallet/balance does not conform to spec. The values of added and retracted wallet/balance flakes need to be equal"))

    (is (= resp2 "You can only add to others balances, and only subtract from your own balance. No balances may be negative Value: 205"))))

(deftest cryptocurrency-test
  (add-schema)
  (add-sample-data)
  (add-permissions)
  (add-smart-functions)
  (test-add-own-subtract-others)
  (spent-is-received)
  (test-spent-is-received))
