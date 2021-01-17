(ns fluree.db.ledger.indexing.full-text
  (:require [fluree.db.full-text.store :as full-text]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.schema :as schema]
            [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clucie.core :as lucene]
            [clucie.store :as lucene-store])
  (:import fluree.db.flake.Flake
           java.time.Instant))

(defn separate-by-op
  [flakes]
  (let [grouped (group-by #(.-op ^Flake %) flakes)
        add     (get grouped true)
        rem     (get grouped false)]
    [add rem]))

(defn predicate-flakes
  "Returns a channel that will contain a stream of every flake known to `db`
  involving any of the predicates in the `preds` sequence."
  [db preds]
  (let [idx-chan (->> preds
                      (map (fn [p]
                             (query-range/index-range db :psot = [p])))
                      async/merge)
        out-chan (chan nil (mapcat seq))]
    (async/pipe idx-chan out-chan)))

(defn updated-predicates
  "Returns a pair of channels, [`add` `rem`], where `add` contains a stream of all
  the flakes known to `db` involving predicates that were newly added to the
  full text index in the `flakes` sequences, and `rem` contains a stream of all
  the flakes known to db involving the predicates retracted from the full text
  index in `flakes`"
  [db flakes]
  (->> flakes
       (filter full-text/predicate?)
       separate-by-op
       (map (partial predicate-flakes db))))

(defn current-index-predicates
  [d]
  (-> db :schema :fullText))

(defn updated-subjects
  "Returns a pair of channels, [`add` `rem`], where `add` contains a stream of the
  added flakes in the `flakes` sequence involving predicates that were already
  tracked in the full text index for `db`, and `rem` contains a stream of all
  the retracted flakes from the flakes sequence involving the predicates that
  were previously tracked in the full text index for `db`."
  [db flakes]
  (let [cur-idx-preds (current-index-predicates db)

        [cur-updates cur-removals]
        (->> flakes
             (filter (fn [^Flake f]
                       (contains? cur-idx-preds (.-p f))))
             separate-by-op)

        cur-update-ch  (async/to-chan! cur-updates)

        ;; We only want to add flakes to "remove" if that s-p is being
        ;; removed, not if it's being updated
        update-set     (->> cur-updates
                            (map (fn [^Flake f]
                                   [(.-s f) (.-p f)]))
                            (into #{}))
        cur-rem-ch     (->> cur-removals
                            (filter (fn [^Flake f]
                                      (not (contains? update-set
                                                      [(.-s f) (.-p f)]))))
                            async/to-chan!)]
    [cur-update-ch cur-rem-ch]))

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
  (let [subj-map-chan (async/reduce (fn [m ^Flake f]
                                      (let [subj (.-s f)
                                            pred (.-p f)
                                            obj  (.-o f)]
                                        (update m subj assoc pred obj)))
                                    {} flake-chan)
        out-chan      (chan nil (mapcat seq))]
    (async/pipe subj-map-chan out-chan)))

(defn process-subjects
  "Processes each subject - predicate-map pair in `subj-chan` with the supplied
  `processor` function. The processor should modify the index for each item in
  the `subj-chan` and return updated stats tracking successes and errors.
  Returns a channel that will eventually contain the updated stats."
  [processor init-stats subj-chan]
  (go-loop [stats init-stats]
    (if-let [[subj pred-map] (<! subj-chan)]
      (let [stats* (processor stats subj pred-map)]
        (recur stats*))
      stats)))

(defn index-subjects
  "Add the subjects from `subj-chan` to the index using the `writer`. Keeps track
  of successfully `:indexed` subjects and `:errors` in the stats map returned."
  [writer init-stats subj-chan]
  (process-subjects (fn [stats subj pred-map]
                      (try
                        (full-text-store/put-subject writer subj pred-map)
                        (log/trace "Indexed full text predicates for subject "
                                   subj)
                        (update stats :indexed inc)

                        (catch Exception e
                          (log/error e (str "Error indexing full text "
                                            "predicates for subject " subj))
                          (update stats :errors inc))))

                    init-stats subj-chan))

(defn index-flakes
  [writer init-stats flake-chan]
  (->> flake-chan
       group-by-subject
       (index-subjects writer init-stats)))

(defn purge-subjects
  "Remove the included predicates for each subject from `subj-chan` from the index
  using the `writer`. Keeps track of successfully `:purged` subjects and
  `:errors` in the stats map returned."
  [init-stats writer subj-chan]
  (process-subjects (fn [stats subj pred-map]
                      (try
                        (full-text-store/purge-subject)
                        (log/trace "Purged stale full text predicates for "
                                   "subject " subj)
                        (update stats :purged inc)

                        (catch Exception e
                          (log/error e (str "Error purging stale full text "
                                            "predicates for subject " subj))
                          (update stats :errors inc))))

                    init-stats subj-chan))

(defn purge-flakes
  [writer init-stats flake-chan]
  (->> flake-chan
       group-by-subject
       (purge-subjects writer init-stats)))

(defn reset-index
  [writer {:keys [network dbid] :as db}]
  (let [cur-idx-preds (current-index-predicates db)
        idx-queue     (predicate-flakes db cur-idx-preds)
        initial-stats {:indexed 0, :errors 0}]
    (full-text-store/forget writer)
    (index-flakes writer initial-stats idx-queue)))

(defn update-index
  [writer {:keys [network dbid] :as db} {:keys [flakes] :as block}]
  (go
    (let [[add-pred-ch rem-pred-ch]  (updated-predicates db flakes)
          [cur-update-ch cur-rem-ch] (updated-subjects db flakes)

          add-queue (async/merge add-pred-ch cur-update-ch)
          rem-queue (async/merge rem-pred-ch cur-rem-ch)

          initial-stats {:indexed 0, :purged 0, :errors 0}
          indexed-stats (<! (index-flakes writer initial-stats add-queue))
          final-stats   (<! (purge-flakes writer indexed-stats rem-queue))]

      final-stats)))


(defn write-block
  [{:keys [network dbid] :as db} storage-path {:keys [flakes] :as block}]
  (let [start-time  (Instant/now)
        coordinates {:network network, :dbid dbid, :block (:block block)}
        lang        (-> db :settings :language (or :default))]

    (log/info (str "Full-Text Search Index began processing new block at: "
                   start-time)
              coordinates)

    (go
      (with-open [store  (full-text-store/storage storage-path [network dbid])
                  writer (full-text-store/writer store lang)]

        (let [stats    (if (schema/get-language-change flakes)
                         (<! (reset-index writer db))
                         (<! (update-index writer db block)))

              end-time (Instant/now)
              duration (- (.toEpochMilli end-time)
                          (.toEpochMilli start-time))

              status   (-> stats
                           (merge coordinates)
                           (assoc :duration duration))]

          (full-text-store/register-block writer status)

          (log/info (str "Full-Text Search Index ended processing new block at: "
                         end-time)
                    status)

          status)))))
