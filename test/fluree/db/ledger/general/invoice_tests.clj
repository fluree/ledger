(ns fluree.db.ledger.general.invoice-tests
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.test-helpers :as test]))

(use-fixtures :once test/test-system-deprecated)

;; Auths
(def antonio {:auth        "TfLK8tUpjYpabx7p64c9ZRwSgNUCCvxAAWG"
              :private-key "c9cdec8fb328a823ddfbe37115ec448109d86bab594305c8066e7633e5b63ba6"})

(def freddie {:auth        "TfKqMRbSU7cFzX9UthQ7Ca4GoEZg7KJWue9"
              :private-key "509a01fe94a32466d7d3ad378297307f897a7d385a219d79725994ce06041896"})

(def scott   {:auth        "TfCFawNeET5FFHAfES61vMf9aGc1vmehjT2"
              :private-key "a603e772faec02056d4ec3318187487d62ec46647c0cba7320c7f2a79bed2615"})


;; Helper function: Query-as auth
(defn query-as
  [auth]
  (-> (basic/get-db test/ledger-invoice {:auth ["_auth/id" auth]})
      (fdb/query-async {:select ["*"] :from "invoice"})
      async/<!!))

;; Helper function: Get values for key
(defn get-values-for-key
  [m k]
  (reduce-kv
    (fn [z _ v] (into z [(-> v (get k))]))
    []
    m))

;; Add schema
(deftest add-schema
  (testing "Add schema for the invoice app"
    (let [filename    "../test/fluree/db/ledger/Resources/invoice/schema.edn"
          txn         (edn/read-string (slurp (io/resource filename)))
          schema-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-invoice txn))]

      ;; status should be 200
      (is (= 200 (:status schema-resp)))

      ;; block should be 2
      (is (= 2 (:block schema-resp)))

      ;; there should be 3 _collection tempids
      (is (= 3 (test/get-tempid-count (:tempids schema-resp) "_collection")))

      ;; there should be 11 _predicate tempids
      (is (= 11 (test/get-tempid-count (:tempids schema-resp) "_predicate")))

      ;; there should be 1 _role tempid
      (is (= 1 (test/get-tempid-count (:tempids schema-resp) "_role"))))))


;; Add sample data
(deftest add-sample-data
  (testing "Add sample data for the invoice app"
    (let [filename  "../test/fluree/db/ledger/Resources/invoice/data.edn"
          txn       (edn/read-string (slurp (io/resource filename)))
          data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-invoice txn))]

      ;; status should be 200
      (is (= 200 (:status data-resp)))

      ;; block should be 3
      (is (= 3 (:block data-resp)))

      ;; there should be 14 tempids
      (is (= 14 (count (:tempids data-resp)))))))

;; Verify each user's results:
;; 1) Freddie can see all
;; 2) Scott can only see NewCo
;; 3) Antonio can only see TechCo
(deftest verify-only-own-invoices-using-auth
  (testing "verify can see own invoices"
    (let [root-res    (-> freddie
                          :auth
                          query-as
                          (get-values-for-key "invoice/id"))
          scott-res   (-> scott
                          :auth
                          query-as
                          (get-values-for-key "invoice/id"))
          antonio-res (-> antonio
                          :auth
                          query-as
                          (get-values-for-key "invoice/id"))]

      (is (= #{"A-000" "B-000"} (set root-res)))
      (is (= #{"A-000"} (set scott-res)))
      (is (= #{"B-000"} (set antonio-res))))))

(deftest invoice-tests
  (add-schema)
  (add-sample-data)
  (verify-only-own-invoices-using-auth))