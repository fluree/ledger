(ns fluree.db.ledger.transact.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [fluree.db.ledger.memorydb :as memorydb]
            [fluree.db.ledger.transact.core :as tx-core]
            [fluree.db.api :as fdb]
            [fluree.db.util.json :as json])
  (:import (java.time Instant)))

;; TODO - some specific tests that we should consider including beyond the Fluree docs/examples tests we currently run.
;;      - many of those current tests will likely cover the below, but may not cover all of them... and sometimes the
;;      - underlying issue is difficult to discern.

;; test these things:
;; tag is properly resolved (add a new _predicate - the 'type' is a tag and can be compared with its repective const/$... value
;; - if unique predicate resolves to a different existing subject, allow transaction to happen only if a different part of tx retracts existing flake
;; - tempids - add correctly, multiple identical new create just one flake
;; - incoming transaction cannot have a negative value? - if we allow, need to validate for prior tx, and doesn't overwrite hash, etc.
;; dependent transactions look like it may not work correct - revisit
;; check if a new tag is used and we (a) properly auto-generate tag if needed and (b) don't auto-generate it if the transaction has the new tag as a transaction item
;; check transaction expirations work
;; check duplicate transactions (txid) won't work
;; test two txi for same subject with single-cardinality value doesn't get added twice (either resolved via unique :true, or just using same pred-ident for _id)
;; test error and execution stops when fuel runs out
;; test nil JSON tx values with tempid subjects get handled correctly (not sure if we should throw, or drop)
;; test unique values that have tempids for (a) single/multi cardinality, (b) where tempid gets resolved to existing subject with ':unique true', (c) where that resolved subject is already used for unique value


(def private-key "c457227f6f7ee94c3b2a32fbf055b33df42578d34047c14b2c9fe64273dce957")
(def fake-conn (memorydb/fake-conn))
(def ledger "test/ledger223")
(def memorydb (async/<!! (memorydb/new-db fake-conn ledger {:master-auth-private private-key})))
(def base-tx (fdb/tx->command ledger [{:_id "_collection", :name "person"}
                                      {:_id "_collection", :name "chat"}
                                      {:_id "_collection", :name "comment"}
                                      {:_id "_collection", :name "artist"}
                                      {:_id "_collection", :name "movie"}
                                      {:_id "_predicate", :name "person/handle", :doc "The person's unique handle", :unique true, :type "string"}
                                      {:_id "_predicate", :name "person/fullName", :doc "The person's full name.", :type "string", :index true}
                                      {:_id "_predicate", :name "person/age", :doc "The person's age in years", :type "int", :index true}
                                      {:_id "_predicate", :name "person/follows", :doc "Any persons this subject follows", :type "ref", :restrictCollection "person"}
                                      {:_id "_predicate", :name "person/favNums", :doc "The person's favorite numbers", :type "int", :multi true}
                                      {:_id "_predicate", :name "person/favArtists", :doc "The person's favorite artists", :type "ref", :restrictCollection "artist", :multi true}
                                      {:_id "_predicate", :name "person/favMovies", :doc "The person's favorite movies", :type "ref", :restrictCollection "movie", :multi true}
                                      {:_id "_predicate", :name "person/user", :type "ref", :restrictCollection "_user"}
                                      {:_id "_predicate", :name "chat/message", :doc "A chat message", :type "string", :fullText true}
                                      {:_id "_predicate", :name "chat/person", :doc "A reference to the person that created the message", :type "ref", :restrictCollection "person"}
                                      {:_id "_predicate", :name "chat/instant", :doc "The instant in time when this chat happened.", :type "instant", :index true}
                                      {:_id "_predicate", :name "chat/comments", :doc "A reference to comments about this message", :type "ref", :component true, :multi true, :restrictCollection "comment"}
                                      {:_id "_predicate", :name "comment/message", :doc "A comment message.", :type "string", :fullText true}
                                      {:_id "_predicate", :name "comment/person", :doc "A reference to the person that made the comment", :type "ref", :restrictCollection "person"}
                                      {:_id "_predicate", :name "artist/name", :type "string", :unique true}
                                      {:_id "_predicate", :name "movie/title", :type "string", :fullText true, :unique true}
                                      ]
                              private-key))


;(def tx-result (async/<!! (tjson/build-transaction nil memorydb {:command base-tx} (dec (:t memorydb)) (Instant/now))))
;(def tx-result (async/<!! (tjson/build-transaction nil memorydb
;                                                   {:command (fdb/tx->command ledger [{:_id "_collection", :name "person"}] private-key)}
;                                                   (dec (:t memorydb)) (Instant/now))))

(comment

  tx-result
  temp-result

  (async/<!! (fluree.db.dbproto/-with-t memorydb temp-result))

  )
#_(deftest sql-query-parser-test
  (testing "IRI support"
    )
  (testing "Transaction children properly extracted"
    (let [nested-tx [{:_id "_collection"
                      :name "_collection/test"}
                     {:_id 12345
                      :sub [{:_id "mysub1"
                             :name "blah"}
                            {:_id "mysub2"
                             :name "blah2"}
                            {:_id "mysub3"
                             :name "blah3"
                             :nested {:_id "subsub"
                                      :name "blah4"}}]}
                     :_id ["test/me" 123]
                     :asub {:_id "hi"
                            :name "hithere"}]]
      )
    ))