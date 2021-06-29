(ns fluree.db.ledger.transact.txfunction
  (:require [fluree.db.dbfunctions.core :as dbfunctions]))

;; functions related to transaction functions

;; TODO - can probably parse function string to final 'lisp form' when generating TxFunction
(defrecord TxFunction [fn-str])

(defn tx-fn?
  "Returns true if a transaction function"
  [x]
  (instance? TxFunction x))

(defn execute
  "Returns a core async channel with response"
  [tx-fn _id pred-info {:keys [auth db-root instant fuel]}]
  (dbfunctions/execute-tx-fn db-root auth nil _id (pred-info :id) (:fn-str tx-fn) fuel instant))

