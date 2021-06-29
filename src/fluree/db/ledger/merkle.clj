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

(defn generate-merkle-root
  "hashes should already be in the correct order."
  [& hashes]
  (let [count-cmds   (count hashes)
        repeat-last  (- count-cmds (find-closest-power-2 count-cmds))
        leaves-ordrd (concat hashes (repeat repeat-last (last hashes)))]
    (loop [merkle-results (apply generate-hashes leaves-ordrd)]
      (if (> 1 (count merkle-results))
        (recur (apply generate-hashes merkle-results))
        (first merkle-results)))))


