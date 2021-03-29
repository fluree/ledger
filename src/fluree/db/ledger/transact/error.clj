(ns fluree.db.ledger.transact.error
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.util.tx :as tx-util]
            [fluree.db.ledger.transact.tx-meta :as tx-meta]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log]
            [fluree.db.util.core :as util]
            [clojure.string :as str]
            [fluree.db.flake :as flake]
            [fluree.crypto :as crypto])
  (:import (fluree.db.flake Flake)))

;; transaction error responses
(defn decode-exception
  [e]
  (let [exd     (ex-data e)
        status  (or (:status exd) 500)
        error   (or (:error exd) :db/unexpected-error)
        message (str/join " " [status (util/keyword->str error) (.getMessage e)])]
    (when (= 500 status)
      (log/error e (str "Unexpected Error, please contact support with exception details: " message)))
    {:message message
     :status  status
     :error   error}))


(defn handler
  [e tx-state]
  (go-try
    (let [{:keys [message status error]} (decode-exception e)
          {:keys [db-before auth-id authority-id t txid tx-type fuel]} tx-state
          flakes           (->> (tx-meta/tx-meta-flakes tx-state message)
                                (into (flake/sorted-set-by flake/cmp-flakes-block)))
          hash-flake       (tx-meta/generate-hash-flake flakes tx-state)
          all-flakes       (conj flakes hash-flake)
          fast-forward-db? (:tt-id db-before)
          db-after         (if fast-forward-db?
                             (<? (dbproto/-forward-time-travel db-before all-flakes))
                             (<? (dbproto/-with-t db-before all-flakes)))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db-before [:stats :size]))]
      {:error        error
       :t            t
       :hash         (.-o ^Flake hash-flake)
       :db-before    db-before
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
  [return-map errors {:keys [db-before] :as tx-state}]
  (go-try
    (let [sorted-errors    (sort error-priority-sort errors)
          reported-error   (first sorted-errors)            ;; we only report a single error in the ledger
          {:keys [message status error]} reported-error
          flake-err-msg    (str/join " " [status (util/keyword->str error) message])
          flakes           (->> (tx-meta/tx-meta-flakes tx-state flake-err-msg)
                                (into (flake/sorted-set-by flake/cmp-flakes-block)))
          hash-flake       (tx-meta/generate-hash-flake flakes tx-state)
          all-flakes       (conj flakes hash-flake)
          fast-forward-db? (:tt-id db-before)
          db-after         (if fast-forward-db?
                             (<? (dbproto/-forward-time-travel db-before all-flakes))
                             (<? (dbproto/-with-t db-before all-flakes)))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db-before [:stats :size]))]
      (assoc return-map :error error
                        :errors sorted-errors
                        :db-after db-after
                        :status status
                        :flakes all-flakes
                        :hash (.-o ^Flake hash-flake)
                        :tempids nil
                        :bytes tx-bytes
                        :remove-preds nil))))


(defn throw-validation-exception
  "This should not happen, but somehow the cmd-date could not be parsed.
  Because of this we don't have basic information about the transaction
  but we still want to record the tx. We'll assume cmd-data is a string
  and instead of extracting a command out of it (which didn't work"
  [e db cmd-data t]
  (try
    (let [{:keys [message status error]} (decode-exception e)
          {:keys [sig cmd]} (:command cmd-data)
          txid             (crypto/sha3-256 cmd)
          tx-state         {:tx-string cmd :signature sig :t t :txid txid :fuel (atom {:spent 0})}
          flakes           (->> (tx-meta/tx-meta-flakes tx-state message)
                                (into (flake/sorted-set-by flake/cmp-flakes-block)))
          hash-flake       (tx-meta/generate-hash-flake flakes tx-state)
          all-flakes       (conj flakes hash-flake)
          fast-forward-db? (:tt-id db)
          db-after         (if fast-forward-db?
                             (<? (dbproto/-forward-time-travel db all-flakes))
                             (<? (dbproto/-with-t db all-flakes)))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db [:stats :size]))]
      {:error        error
       :t            t
       :hash         (.-o ^Flake hash-flake)
       :db-before    db
       :db-after     db-after
       :flakes       all-flakes
       :tempids      nil
       :bytes        tx-bytes
       :fuel         (+ (:spent @(:fuel tx-state)) tx-bytes (count flakes) 1)
       :status       status
       :txid         txid
       :auth         nil
       :authority    nil
       :type         nil
       :remove-preds nil})
    (catch Exception e
      (log/error e "Exiting!! Fatal exception trying to process command validation. Unable to extract anything useful"
                 {:cmd-data cmd-data
                  :t        t})
      (System/exit 1))))


(defn pre-processing-handler
  "Called when there is an exception before the transaction started, therefore
  we don't yet have a tx-state so we must make a synthetic one here.

  Exception here might be because:
  - transaction dependency not met
  - could not resolve auth/authority"
  [e db cmd-data t]
  (let [{:keys [error]} (decode-exception e)]
    (if (= error :db/command-parse-exception)               ;; error happened parsing/validating command map... need special handling as same parsing attempted below
      (throw-validation-exception e db cmd-data t)
      (let [tx-map   (tx-util/validate-command (:command cmd-data))
            {:keys [txid auth authority cmd sig type]} tx-map
            tx-state {:t            t
                      :db-before    db
                      :fuel         (atom {:spent 0})
                      :txid         txid
                      :auth-id      auth
                      :authority-id authority
                      :tx-string    cmd
                      :signature    sig
                      :tx-type      type
                      :errors       (atom nil)}]
        (handler e tx-state)))))
