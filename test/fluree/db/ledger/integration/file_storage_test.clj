(ns fluree.db.ledger.integration.file-storage-test
  "Most tests use in-memory ledgers, but these test a few ledger operations
  with file storage to make sure that's not broken."
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.api :as api]
            [clojure.java.io :as io]
            [clojure.core.async :refer [<!!]])
  (:import (org.apache.commons.io FileUtils)))

(use-fixtures :once (fn [t]
                      (let [data-dir (test/create-temp-dir)]
                        (test/test-system
                          {:fdb-storage-type        "file"
                           :fdb-consensus-type      "raft"
                           :fdb-storage-file-root   (.getPath data-dir)
                           :fdb-group-log-directory (-> data-dir
                                                        (io/file "group")
                                                        .getPath)}
                          t)
                        (Thread/sleep 100) ; don't race system shutdown to delete files
                        (FileUtils/deleteDirectory data-dir))))

(deftest ^:integration create-new-ledger-test
  (testing "new ledger initializes, accepts transactions, & responds to queries"
    (let [ledger (test/rand-ledger "file-storage/test")
          txn-result (<!! (api/transact-async
                            (:conn test/system) ledger
                            [{:_id "_user", "_user/username" "tester"}]))]
      (is (= 200 (:status txn-result)))
      (let [db (api/db (:conn test/system) ledger)
            query-result (<!! (api/query-async db {:selectOne [:_user/username]
                                                   :from "_user"}))]
        (is (= "tester" (:_user/username query-result)))))))
