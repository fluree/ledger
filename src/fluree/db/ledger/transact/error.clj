(ns fluree.db.ledger.transact.error
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.ledger.transact.tx-meta :as tx-meta]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log]
            [fluree.db.util.core :as util]
            [clojure.string :as str]
            [fluree.db.flake :as flake])
  (:import (fluree.db.flake Flake)))

(set! *warn-on-reflection* true)

;; transaction error responses
(defn decode-exception
  [e]
  (let [exd     (ex-data e)
        status  (or (:status exd) 500)
        error   (or (:error exd) :db/unexpected-error)
        message (str/join " " [status (util/keyword->str error) (ex-message e)])]
    (when (= 500 status)
      (log/error e (str "Unexpected Error, please contact support with exception details: " message)))
    {:message message
     :status  status
     :error   error}))


(defn handler
  [e tx-state]
  (go-try
    (let [{:keys [message status error]} (decode-exception e)
          {:keys [db-root auth-id authority-id t txid tx-type fuel]} tx-state
          flakes           (->> (tx-meta/tx-meta-flakes tx-state message)
                                (into (flake/sorted-set-by flake/cmp-flakes-block)))
          hash-flake       (tx-meta/generate-hash-flake flakes tx-state)
          all-flakes       (conj flakes hash-flake)
          fast-forward-db? (:tt-id db-root)
          db-after         (if fast-forward-db?
                             (<? (dbproto/-forward-time-travel db-root all-flakes))
                             (<? (dbproto/-with-t db-root all-flakes)))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db-root [:stats :size]))]
      {:error        error
       :t            t
       :hash         (.-o ^Flake hash-flake)
       :db-before    db-root
       :db-after     db-after
       :flakes       all-flakes
       :tempids      nil
       :bytes        tx-bytes
       :fuel         (+ (:spent @fuel) tx-bytes (count flakes) 1)
       :status       status
       :txid         txid
       :auth         auth-id
       :authority    authority-id
       :type         tx-type
       :remove-preds nil})))


(defn error-priority-sort
  "Comparison function for sorting errors by priority.
  This enables multiple servers to always produce the idential set of errors for a given
  transaction that they each may compute independently.
  - higher error status comes first (i.e. 403 error preceeds a 400 error)
  - equal errors alphabetically sort by error message"
  [a b]
  (let [status-cmp (compare (:status b) (:status a))]
    (if (= 0 status-cmp)
      (compare (:message a) (:message b))
      status-cmp)))


(defn spec-error
  [return-map errors {:keys [db-root] :as tx-state}]
  (go-try
    (let [sorted-errors    (sort error-priority-sort errors)
          reported-error   (first sorted-errors)            ;; we only report a single error in the ledger
          {:keys [message status error]} reported-error
          flake-err-msg    (str/join " " [status (util/keyword->str error) message])
          flakes           (->> (tx-meta/tx-meta-flakes tx-state flake-err-msg)
                                (into (flake/sorted-set-by flake/cmp-flakes-block)))
          hash-flake       (tx-meta/generate-hash-flake flakes tx-state)
          all-flakes       (conj flakes hash-flake)
          fast-forward-db? (:tt-id db-root)
          db-after         (if fast-forward-db?
                             (<? (dbproto/-forward-time-travel db-root all-flakes))
                             (<? (dbproto/-with-t db-root all-flakes)))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db-root [:stats :size]))]
      (assoc return-map :error error
                        :errors sorted-errors
                        :db-after db-after
                        :status status
                        :flakes all-flakes
                        :hash (.-o ^Flake hash-flake)
                        :tempids nil
                        :bytes tx-bytes
                        :remove-preds nil))))


(defn pre-processing-handler
  "Called when there is an exception before the transaction started, therefore
  we don't yet have a tx-state so we must make a synthetic one here.

  Exception here might be because:
  - transaction dependency not met
  - could not resolve auth/authority

  Returns an async channel."
  [e db-root tx-map]
  (let [{:keys [txid auth authority cmd sig type]} tx-map
        tx-state {:t            (dec (:t db-root))
                  :db-root      db-root
                  :db-before    db-root
                  :fuel         (atom {:spent 0})
                  :txid         txid
                  :auth-id      auth
                  :authority-id authority
                  :tx-string    cmd
                  :signature    sig
                  :tx-type      type
                  :errors       (atom nil)}]
    (handler e tx-state)))
