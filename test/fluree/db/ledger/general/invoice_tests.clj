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

(def scott {:auth        "TfCFawNeET5FFHAfES61vMf9aGc1vmehjT2"
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


(deftest add-context-smartfunction
  (testing "Adding a context smartfunction"
    (let [txn       [{:_id       ["_role/id", "level1User"] ;; add CTX to the level1User role
                      :_role/ctx [{:_id       "_ctx$orgs"
                                   :_ctx/name "myorg"
                                   :_ctx/key  "orgs"
                                   :_ctx/fn   {:_id      "_fn$getOrgs"
                                               :_fn/name "getOrgs"
                                               :_fn/code "(get-all (?auth_id) [\"_user/_auth\" \"org/_employees\"])"}}]}]
          tx-resp   (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-invoice txn))
          new-db    (basic/get-db test/ledger-invoice {:syncTo (:block tx-resp)
                                                       :auth   ["_auth/id" (:auth scott)]})
          newco-sid @(fdb/subid new-db ["org/name" "NewCo"])]
      (is (= (:ctx (async/<!! new-db)) {"orgs" #{newco-sid}})))))


(deftest use-context-in-smartfunction
  (testing "Use the context added above in a new SmartFunction"
    (let [txn           [{:_id         ["_role/id" "level1User"],
                          :_role/rules "_rule$viewOrgEmployees"}
                         ;; Add rule for query of _user(s) that checks org(s) they belong to and verifies same as requesting user
                         {:_id                     "_rule$viewOrgEmployees",
                          :_rule/id                "viewOrgEmployees",
                          :_rule/doc               "Allow employees to see others in same org.",
                          :_rule/collection        "_user",
                          :_rule/fns               ["_fn$userSameOrg"],
                          :_rule/ops               ["query"],
                          :_rule/collectionDefault true}
                         ;; add fn that checks user's 'orgs' context value contains the org the respective _user belongs to
                         {:_id      "_fn$userSameOrg",
                          :_fn/name "userSameOrg",
                          :_fn/code "(contains? (ctx \"orgs\") (get (?sid) \"org/_employees\"))",
                          :_fn/doc  "Returns truthy if _user is in same org(s) as requesting identity."}
                         ;; add an extra user in the same org as Scott, so we can see if Scott can see them
                         {:_id      "_user$fannie"
                          :username "fannie"}
                         {:_id       ["org/name" "NewCo"]
                          :employees ["_user$fannie"]}]
          tx-resp       (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-invoice txn))
          scott-db      (basic/get-db test/ledger-invoice {:syncTo (:block tx-resp)
                                                           :auth   ["_auth/id" (:auth scott)]})
          antonio-db    (basic/get-db test/ledger-invoice {:syncTo (:block tx-resp)
                                                           :auth   ["_auth/id" (:auth antonio)]})
          root-db       (basic/get-db test/ledger-invoice {:syncTo (:block tx-resp)})
          query         {:select ["_user/username"]
                         :from   "_user"}
          root-q-res    @(fdb/query root-db query)
          scott-q-res   @(fdb/query scott-db query)
          antonio-q-res @(fdb/query antonio-db query)]
      ;; root db sees all users:
      (is (= root-q-res
             [{"_user/username" "fannie", :_id 87960930223084}
              {"_user/username" "antonio", :_id 87960930223083}
              {"_user/username" "scott", :_id 87960930223082}
              {"_user/username" "freddie", :_id 87960930223081}]))
      ;; scott can see only _user in the same org
      (is (= scott-q-res
             [{:_id 87960930223084, "_user/username" "fannie"}
              {:_id 87960930223082, "_user/username" "scott"}]))
      ;; antonio can only see himself, no other users are in same org
      (is (= antonio-q-res
             [{"_user/username" "antonio", :_id 87960930223083}])))))


(deftest invoice-tests
  (add-schema)
  (add-sample-data)
  (verify-only-own-invoices-using-auth)
  (add-context-smartfunction)
  (use-context-in-smartfunction))