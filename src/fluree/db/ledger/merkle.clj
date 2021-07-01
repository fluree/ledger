(ns fluree.db.ledger.merkle
  (:require [fluree.crypto :as crypto]))

(defn exp [x n]
  (loop [acc 1 n n]
    (if (zero? n) acc
                  (recur (* x acc) (dec n)))))

(defn find-closest-power-2
  [n]
  (loop [i 1]
    (if (>= (exp 2 i) n)
      (exp 2 i)
      (recur (inc i)))))

(defn generate-hashes
  [cmds]
  (loop [[f s & r] cmds
         acc []]
    (let [hash (crypto/sha2-256 (str f s))
          acc* (conj acc hash)]
      (if r
        (recur r acc*)
        acc*))))


