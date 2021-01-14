(ns fluree.db.ledger.indexing.full-text
  (:require [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [clojure.string :as str]
            [fluree.db.query.analytical-full-text :as full-text]
            [clucie.core :as lucene]
            [clucie.store :as lucene-store])
  (:import fluree.db.flake.Flake))

(defn index-store
  [storage-path [network dbid]]
  (let [store-path (str/join "/" [storage-path network dbid "lucene"])]
    (lucene-store/disk-store store-path)))

(defn get-subject
  [idx-store lang subj]
  (let [analyzer (full-text/analyzer lang)
        subj-id  (str subj)]
    (-> idx-store
        (lucene/search {:_id subj-id} 1 analyzer 0 1)
        first)))

(defn group-by-subject
  "Expects `flake-chan` to be a channel that contains a stream of flakes.
  Returns a channel that will contain a stream of pairs representing the
  predicate-object mappings of the input flake stream grouped by subject.

  Each item in the output channel has the following structure:

  [`s` [[`p` `o`] ...]]

  where the first element `s` is a subject id that appears at most once in the
  output stream, and the second element is a list of pairs where each pair `p`
  and `o` is the predicate and object, respectfully, from one of the input
  flakes for the subject `s`."
  [flake-chan]
  (let [;; reduce the flakes in `flake-chan` into a map whose keys are subject
        ;; ids and values are lists of all the predicate-object pairs appearing
        ;; in the `flake-chan` for that subject.
        subj-attr-chan (async/reduce (fn [m ^Flake f]
                                       (let [subj (.-s f)
                                             pred (.-p f)
                                             obj  (.-o f)]
                                         (update m subj conj [pred obj])))
                                     {} flake-chan)

        ;; turn a stream of maps into a stream of key-value pairs that appear in
        ;; each map
        out-chan       (chan nil (mapcat seq))]

    (async/pipe subj-attr-chan out-chan)

    out-chan))
