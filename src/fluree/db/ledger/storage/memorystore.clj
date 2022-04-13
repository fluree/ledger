(ns fluree.db.ledger.storage.memorystore
  (:require [fluree.db.util.async :refer [go-try]]))

(set! *warn-on-reflection* true)

(def memory-store (atom {}))

(defn connection-storage-read
  "Default function for connection storage."
  [key]
  (go-try (get @memory-store key)))


(defn connection-storage-write
  "Default function for connection storage writing."
  [key val]
  (go-try (if (nil? val)
            (swap! memory-store dissoc key)
            (swap! memory-store assoc key val))
          true))

(defn connection-storage-delete
  "Default function for connection storage writing."
  [key]
  (go-try
    (swap! memory-store dissoc key)
    true))

(defn connection-storage-list
  [_]
  (go-try
   (->> @memory-store
        keys
        (map (fn [k] {:name k})))))


(defn close
  "Resets memory store."
  []
  (reset! memory-store {})
  true)
