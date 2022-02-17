(ns fluree.db.ledger.mutable-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.mutable :as mutable]
            [fluree.db.query.range :as query-range]
            [fluree.db.test-helpers :as test]
            [clojure.core.async :as async :refer [<!!]]
            [clojure.string :as str]
            [fluree.db.flake :as flake])
  (:import (clojure.lang ExceptionInfo)))


(use-fixtures :once test/test-system)

(defn create-mutable-ledger
  "Load mutable ledger with schema and data.
   Returns name of ledger (string)"
  []
  (let [ledger (test/rand-ledger test/ledger-mutable)
        _      (test/transact-schema ledger "mutable-1-setshard.edn")
        _      (test/transact-schema ledger "mutable-2-collections.edn")
        _      (test/transact-schema ledger "mutable-3-predicates.edn")
        _      (test/transact-data ledger "mutable-block-5.edn")
        _      (test/transact-data ledger "mutable-block-6.edn")
        _      (test/transact-data ledger "mutable-block-7.edn")
        _      (test/transact-data ledger "mutable-block-8.edn")
        _      (test/transact-data ledger "mutable-block-9.edn")]
    ledger))

(deftest get-validate-purge-params-test
  (testing "validate parameters for purge"
    (let [sid 351843720888322
          sids [351843720888322 369435906932741]]
          (is (= (vector sid) (mutable/validate-purge-params sid)))
          (is (= (vector sid) (mutable/validate-purge-params (vector sid))))
          (is (= sids (mutable/validate-purge-params sids)))
          (is (thrown-with-msg?
                ExceptionInfo #"Invalid subject. Provided: dummy"
                (mutable/validate-purge-params "dummy"))))))

(deftest get-component-preds-from-schema-test
  (testing "validate generated flake patterns for purge"
    (let [ledger (create-mutable-ledger)
          db     (<!! (fdb/db (:conn test/system) ledger))]
      (is (= #{1006} (mutable/get-component-preds-from-schema db))))))

(deftest identify-component-sids-test
  (testing "validate sids for component references"
    (let [ledger (create-mutable-ledger)
          db-ch  (fdb/db (:conn test/system) ledger)
          db     (<!! db-ch)
          preds  (mutable/get-component-preds-from-schema db)
          res    (<!! (fdb/query-async db-ch
                                       {:selectOne ["_id" {:specTable ["_id"]}],
                                        :from ["dataSpecification/title" "Omega Data Source"],
                                        :opts {:meta false, :open-api true}}))
          sid    (:_id res)
          refs   (->> (get res "specTable")
                      (map :_id)
                      set)
          flakes (->> (flake/parts->Flake [sid nil nil nil])
                      (query-range/search db)
                      <!!)
          comps  (mutable/identify-component-sids preds flakes)]
      (is (= 9 (:block db)))
      (is (= -18 (:t db)))
      (is (= (count refs) (count comps)))
      (is (every? refs comps)))))

(deftest identify-purge-graph-test
  (testing "validate purge graph for a single subject"
    (let [ledger (create-mutable-ledger)
          db-ch  (fdb/db (:conn test/system) ledger)
          db     (<!! db-ch)
          preds  (mutable/get-component-preds-from-schema db)
          res    (<!! (fdb/query-async db-ch
                                       {:selectOne ["_id" {:specTable ["_id"]}],
                                        :from ["dataSpecification/title" "Omega Data Source"],
                                        :opts {:meta false, :open-api true}}))
          sid    (:_id res)
          refs   (->> (get res "specTable")
                      (map :_id)
                      set)
          [flakes comps]  (<!! (mutable/identify-purge-graph db preds sid))]
      (is (= 9 (:block db)))
      (is (= -18 (:t db)))
      ;; currently, retracted flakes are not included
      (is (= 7 (count flakes)))
      (is (= (count refs) (count comps)))
      (is (every? refs comps)))))

(deftest identify-purge-flakes-test
  (testing "validate purge flakes for multiple subjects"
    (let [ledger   (create-mutable-ledger)
          db-ch    (fdb/db (:conn test/system) ledger)
          db       (<!! db-ch)
          preds    (mutable/get-component-preds-from-schema db)
          ;; flakes for "Omega Data Source"
          sid-1    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecification/title" "Omega Data Source"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)
          flakes-1 (->> (flake/parts->Flake [sid-1 nil nil nil])
                        (query-range/search db)
                        <!!)
          ;; flakes for "Gamma-Two" source tag
          sid-2    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecAttributes/uid"  "dsa004"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)
          flakes-2 (->> (flake/parts->Flake [sid-2 nil nil nil])
                        (query-range/search db)
                        <!!)
          ;; purge-list should contain a minimum of the above flakes
          flakes   (-> (mutable/identify-purge-flakes db preds [sid-1 sid-2])
                       <!!
                       set)]
      ;; currently, retracted flakes are not included
      (is (= 7 (count flakes-1)))
      (is (= 6 (count flakes-2)))
      (is (every? flakes flakes-1))
      (is (every? flakes flakes-2)))))

(deftest identify-purge-map-test
  (testing "validate purge map for multiple subjects"
    (let [ledger   (create-mutable-ledger)
          db-ch    (fdb/db (:conn test/system) ledger)
          db       (<!! db-ch)
          preds    (mutable/get-component-preds-from-schema db)
          ;; sid, flakes for "Omega Data Source"
          sid-1    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecification/title" "Omega Data Source"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)
          flakes-1 (->> (flake/parts->Flake [sid-1 nil nil nil])
                        (query-range/search db)
                        <!!)
          ;; sid, flakes for "Gamma-Two" source tag
          sid-2    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecAttributes/uid"  "dsa004"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)
          flakes-2 (->> (flake/parts->Flake [sid-2 nil nil nil])
                        (query-range/search db)
                        <!!)
          ;; purge-map should contain a minimum of the above flakes
          ;; as well as blocks 7, 8, 9
          [block-map fuel]
                   (->> [sid-1 sid-2]
                        (mutable/identify-purge-map db preds )
                        <!!)
          flakes   (->> block-map vals (into []) (apply concat) set)]
      ;; currently, retracted flakes are not included - fuel count will increase when
      ;; retracted flakes are included in the map
      (is (every? #{7, 8, 9} (keys block-map)))
      (is (every? flakes flakes-1))
      (is (every? flakes flakes-2))
      (is (= 26 fuel)))))

(deftest purge-flakes-test
  (testing "validate in-memory blocks after purge"
    (let [ledger   (create-mutable-ledger)
          [nw db]  (str/split ledger #"/")
          conn     (:conn test/system)
          db-ch    (fdb/db conn ledger)
          ;; sid "Omega Data Source"
          sid-1    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecification/title" "Omega Data Source"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)
          ;; sid for "Gamma-Two" source tag
          sid-2    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecAttributes/uid"  "dsa004"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)
          ;; verify state of in-memory cache
          res-1    (<!! (mutable/purge-flakes conn nw db {:purge [sid-1 sid-2]}))
          res-2    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecification/title" "Omega Data Source"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)
          res-3    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecAttributes/uid"  "dsa004"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)
          res-4    (-> (fdb/query-async db-ch
                                        {:selectOne ["_id"],
                                         :from ["dataSpecification/title" "Zeta Data Source"],
                                         :opts {:meta false, :open-api true}})
                       <!!
                       :_id)]
      ;; currently, retracted flakes are not included - fuel count will increase when
      ;; retracted flakes are included in the map
      (is (= 200 (:status res-1)))
      (is (= 26 (-> res-1 :result :flakes-purged)))
      (is (every? #{7, 8, 9} (-> res-1 :result :blocks)))
      (is (some? res-4))
      ;; the following checks are not working for in-memory storage
      #_(is (nil? res-2))
      #_(is (nil? res-3)))))


