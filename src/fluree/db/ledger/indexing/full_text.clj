(ns fluree.db.ledger.indexing.full-text
  (:require [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [fluree.db.full-text :as full-text]
            [clucie.core :as lucene]
            [clucie.store :as lucene-store])
  (:import fluree.db.flake.Flake))

(defn group-by-subject
  "Expects `flake-chan` to be a channel that contains a stream of flakes.
  Returns a channel that will contain a stream of pairs representing the
  predicate-object mappings of the input flake stream grouped by subject.

  Each item in the output channel has the following structure:

  [`s` {`p1` `o1`, `p2` `o2`, ...}]

  where the first element `s` is a subject id that appears at most once in the
  output stream, and the second element is a map where each key-value pair is
  the predicate and object from a flake appearing in the input stream for that
  subject."
  [flake-chan]
  (let [;; reduce the flakes in `flake-chan` into a map whose keys are subject
        ;; ids and values are lists of all the predicate-object pairs appearing
        ;; in the `flake-chan` for that subject.
        subj-attr-chan (async/reduce (fn [m ^Flake f]
                                       (let [subj (.-s f)
                                             pred (.-p f)
                                             obj  (.-o f)]
                                         (update m subj assoc pred obj)))
                                     {} flake-chan)

        ;; turn a stream of maps into a stream of the key-value pairs that
        ;; appear in each map
        out-chan       (chan nil (mapcat seq))]

    (async/pipe subj-attr-chan out-chan)

    out-chan))

(defn process-subjects
  "Processes each subject - predicate-map pair in `subj-chan` with the supplied
  `processor` function. The processor should modify the index with the writer
  and return updated stats tracking successes and errors"
  [processor writer init-stats subj-chan]
  (go-loop [stats init-stats]
    (if-let [[subj pred-map] (<! subj-chan)]
      (let [stats* (processor writer stats subj pred-map)]
        (recur stats*))
      stats)))

(defn index-subjects
  "Add the subjects from `subj-chan` to the index using the `writer`. Keeps track
  of successfully `:indexed` subjects and `:errors` in the stats map returned."
  [writer init-stats subj-chan]
  (process-subjects (fn [writer stats subj pred-map]
                      (try
                        (full-text/put-subject writer subj pred-map)
                        (log/trace "Indexed full text predicates for subject "
                                   subj)
                        (update stats :indexed inc)

                        (catch Exception e
                          (log/error e (str "Error indexing full text "
                                            "predicates for subject "
                                            subj))
                          (update stats :errors inc))))

                    writer init-stats subj-chan))

(defn purge-subjects
  "Remove the included predicates for each subject from `subj-chan` from the index
  using the `writer`. Keeps track of successfully `:purged` subjects and
  `:errors` in the stats map returned."
  [init-stats writer subj-chan]
  (process-subjects (fn [writer stats subj pred-map]
                      (try
                        (full-text/purge-subject)
                        (log/trace "Purged stale full text predicates for "
                                   "subject " subj)
                        (update stats :purged inc)

                        (catch Exception e
                          (log/error e (str "Error purging stale full text "
                                            "predicates for subject " subj))
                          (update stats :errors inc))))

                    writer init-stats subj-chan))

