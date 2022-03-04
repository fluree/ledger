(ns fluree.db.ledger.integration.file-storage-test
  "Most tests use in-memory ledgers, but these test a few ledger operations
  with file storage to make sure that's not broken."
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as api])
  (:import (org.apache.commons.io FileUtils)))

(use-fixtures :once (fn [t]
                      (let [data-dir (test/create-temp-dir)]
                        (test/test-system
                          {:fdb-storage-type      "file"
                           :fdb-consensus-type    "raft"
                           :fdb-storage-file-root (.getPath data-dir)}
                          t)
                        (FileUtils/deleteDirectory data-dir))))

(deftest ^:integration create-new-ledger-test
  (testing "new ledger initializes, accepts transactions, & responds to queries"
    (let [ledger (test/rand-ledger "file-storage/test")]
      (is (= 200 (:status (api/transact (:conn test/system) ledger
                                        [{:_id "_user", "_user/username" "tester"}])))))))
