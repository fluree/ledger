(ns fluree.db.ledger.util
  (:require [clojure.core.async :refer [go <! <!!]]))


;; core async helpers

(defn throw-if-exception
  "Helper method that checks if x is Exception and if yes, wraps it in a new
  exception, passing though ex-data if any, and throws it. The wrapping is done
  to maintain a full stack trace when jumping between multiple contexts."
  [x]
  (if (and (not (nil? x)) (instance? Exception x))
    (throw (ex-info (or (.getMessage x) (str x))
                    (or (ex-data x) {})
                    x))
    x))

(defmacro go-try
  "Asynchronously executes the body in a go block. Returns a channel
which will receive the result of the body when completed or the
exception if an exception is thrown. You are responsible to take
this exception and deal with it! This means you need to take the
result from the channel at some point."
  [& body]
  `(go
     (try
       ~@body
       (catch Exception e#
         e#))))


(defmacro <?
  "Same as core.async <! but throws an exception if the channel returns a
throwable error."
  [ch]
  `(throw-if-exception (<! ~ch)))


(defn <??
  "Same as core.async <!! but throws an exception if the channel returns a
throwable error."
  [ch]
  (throw-if-exception (<!! ch)))
