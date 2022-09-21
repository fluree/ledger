(ns fluree.db.ledger.predicate.json-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as fdb]
            [fluree.db.util.json :as json]
            [org.httpkit.client :as http]))

(use-fixtures :once test/test-system)

(deftest transact-json-test
  (let [ledger (test/rand-ledger "pred/json")
        _ (test/assert-success
            (test/transact-schema ledger "json-preds"))
        api-url (str "http://localhost:" @test/port "/fdb/" ledger "/")]
    (testing "valid JSON succeeds"
      (let [txn "[{\"_id\": \"_user\", \"_user/username\": \"tester\", \"_user/json\": {\"foo\": \"bar\"}}]"
            {:keys [status body]} @(http/post (str api-url "transact")
                                              {:headers {"Content-Type" "application/json"}
                                               :body    txn})]
        (is (= 200 status))
        (is (some #(-> % (nth 2) (= "{\"foo\":\"bar\"}")) (-> body json/parse :flakes)))))
    (testing "invalid JSON fails"
      (let [txn "[{\"_id\": \"_user\", \"_user/username\": \"tester\", \"_user/json\": {\"foo\": \"bar\"}]" ; missing closing }
            {:keys [status]} @(http/post (str api-url "transact")
                                         {:headers {"Content-Type" "application/json"}
                                          :body    txn})]
        ;; This should really return a 4xx error, but it's a 5xx error as of the date I wrote this test.
        ;; So I'm just checking that this is in the "error range" of HTTP status codes.
        (is (< 399 status))))))

(deftest query-json-test
  (let [ledger (test/rand-ledger "pred/json")
        _      (test/assert-success
                 (test/transact-schema ledger "json-preds"))
        {:keys [block]} (test/assert-success
                          (test/transact-data ledger "json-preds"))
        db     (fdb/db (:conn test/system) ledger {:syncTo block})]

    (testing "returns JSON string by default"
      (let [query {:select ["*"]
                   :from   "_user"}
            res   (<!! (fdb/query-async db query))]
        (is (every? string? (map #(get % "_user/json") res)))
        (let [parsed (map #(-> % (get "_user/json") json/parse) res)]
          (is (every? #{{:foo "bar"} {:bizz "buzz"}} parsed)))))

    (testing "basic query from collection returns parsed JSON when requested"
      (let [query {:select ["*"]
                   :from   "_user"
                   :opts   {:parseJSON true}}
            res   (<!! (fdb/query-async db query))]
        (is (= {:foo "bar"} (-> res
                                (->> (some #(when (= "aj" (get % "_user/username")) %)))
                                (get "_user/json"))))
        (is (= {:bizz "buzz"} (-> res
                                  (->> (some #(when (= "ajFriend" (get % "_user/username")) %)))
                                  (get "_user/json"))))))

    (testing "analytical query from collection rdf:type returns parsed JSON when requested"
      (let [query {:select {"?s" ["*"]}
                   :where  [["?s", "rdf:type", "_user"]]
                   :opts   {:parseJSON true}}
            res   (<!! (fdb/query-async db query))]
        (is (= {:foo "bar"} (-> res
                                (->> (some #(when (= "aj" (get % "_user/username")) %)))
                                (get "_user/json"))))
        (is (= {:bizz "buzz"} (-> res
                                  (->> (some #(when (= "ajFriend" (get % "_user/username")) %)))
                                  (get "_user/json"))))))

    (testing "basic query from two-tuple returns parsed JSON when requested"
      (let [query {:select ["*" {:friend ["*"]}]
                   :from   ["_user/username", "aj"],
                   :opts   {:parseJSON true}}
            res   (<!! (fdb/query-async db query))]
        (is (= {:foo "bar"} (-> res first (get "_user/json")))
            (str "Unexpected query result: " (pr-str res)))
        (is (= {:bizz "buzz"} (-> res first (get-in ["friend" "_user/json"])))
            (str "Unexpected query result: " (pr-str res)))))

    (testing "basic query from subject _id returns parsed JSON when requested"
      (let [query {:select ["*" {:friend ["*"]}]
                   :from 87960930223081
                   :opts {:parseJSON true}}
            res (<!! (fdb/query-async db query))]
        (is (= {:foo "bar"} (-> res first (get "_user/json")))
            (str "Unexpected query result:" (pr-str res)))
        (is (= {:bizz "buzz"} (-> res first (get-in ["friend" "_user/json"])))
            (str "Unexpected query result:" (pr-str res)))))

    (testing "analytical query from triples returns parsed JSON when requested"
      (let [query {:select {"?s" ["*"]}
                   :where [["?s" "_user/username" "?o"]]
                   :opts {:parseJSON true}}
            res (<!! (fdb/query-async db query))]
        (is (= {:foo "bar"} (-> res first (get "_user/json")))
            (str "Unexpected query result: " (pr-str res)))
        (is (= {:bizz "buzz"} (-> res second (get "_user/json")))
            (str "Unexpected query result: " (pr-str res)))))

    (testing "basic query with graph crawl returns parsed JSON when requested"
      (let [query {:select ["*" {:friend ["*"]}]
                   :from   "_user"
                   :opts {:parseJSON true}}
            res (<!! (fdb/query-async db query))]
        (is (= {:bizz "buzz"} (-> res first (get "_user/json"))))
        (is (= {:foo "bar"} (-> res second (get "_user/json"))))
        (is (= {:bizz "buzz"} (-> res second (get-in ["friend" "_user/json"]))))))))
