(ns fluree.db.ledger.docs.smart-functions.in-transactions
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]))

(use-fixtures :once test/test-system)

(deftest test-str-?pO
  (testing "Test the str and ?pO functions in a transaction"
    (let [long-desc-txn [{:_id ["person/handle" "jdoe"], :fullName "#(str (?pO) \", Sr.\")"}]
          res  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat long-desc-txn))
          flakes (-> res :flakes)
          pos-flakes (filter #(< 0 (first %)) flakes)]

      (is (= 200 (:status res)))

      (is (= 0 (-> res :tempids count)))

      (is (= 8 (-> res :flakes count)))

      (= #{"Jane Doe" "Jane Doe, Sr."}   (-> (map #(nth % 2) pos-flakes) set)))))


(deftest test-max
  (testing "Test transactions using max smart function in a transaction"
    (let [count  20
          tx        (mapv (fn [n]
                           {:_id          "person"
                            :fullName (str "#(max " n " " (+ 1 n) " " (+ 2 n) ")")})
                         (range 1 (inc count)))
          tx-result (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-chat tx))
          full-names  (->> (filter #(= 1002 (second %)) (:flakes tx-result))
                           (map #(nth % 2)) set)
          expected-full-names   (-> (map str (range 3 (+ 3 count))) set)]

      (is (= full-names expected-full-names)))))

(deftest in-transactions-test
  (test-str-?pO)
  (test-max))

(deftest tests-independent
  (basic/add-collections*)
  (basic/add-predicates)
  (basic/add-sample-data)
  (basic/graphql-txn)
  (in-transactions-test))
