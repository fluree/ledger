(ns fluree.db.ledger.transact.txfunction
  (:require [fluree.db.dbfunctions.core :as dbfunctions]))

(set! *warn-on-reflection* true)

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
  (dbfunctions/execute-tx-fn
    {:db            db-root
     :auth          auth
     :credits       nil
     :s             _id
     :p             (pred-info :id)
     :o             (:fn-str tx-fn)
     :fuel          fuel
     :block-instant instant}))

