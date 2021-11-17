(ns fluree.db.ledger.indexing.full-text
  (:require [fluree.db.api :as fdb]
            [fluree.db.full-text :as full-text]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.schema :as schema]
            [clojure.core.async :as async :refer [<! chan go go-loop]]
            [clojure.tools.logging :as log])
  (:import (fluree.db.flake Flake)
           (java.time Instant)
           (java.io Closeable)))

(set! *warn-on-reflection* true)

(defn separate-by-op
  [flakes]
  (let [{add true, rem false} (group-by #(.-op ^Flake %) flakes)]
    [add rem]))

(defn predicate-flakes
  "Returns a channel that will contain a stream of every flake known to `db`
  involving any of the predicates in the `preds` sequence."
  [db preds]
  (let [idx-chan (->> preds
                      (map (fn [p]
                             (query-range/index-range db :psot = [p])))
                      async/merge)
        out-chan (chan 1 cat)]
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
  [db]
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
        out-chan      (chan 1 (mapcat seq))]
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
  "Add the subjects from `subj-chan` to the index using the `wrtr`. Keeps track
  of successfully `:indexed` subjects and `:errors` in the stats map returned."
  [idx wrtr init-stats subj-chan]
  (process-subjects (fn [stats subj pred-map]
                      (try
                        (full-text/put-subject idx wrtr subj pred-map)
                        (log/trace "Indexed full text predicates for subject "
                                   subj)
                        (update stats :indexed inc)

                        (catch Exception e
                          (log/error e (str "Error indexing full text "
                                            "predicates for subject " subj))
                          (update stats :errors inc))))

                    init-stats subj-chan))

(defn index-flakes
  [idx wrtr init-stats flake-chan]
  (->> flake-chan
       group-by-subject
       (index-subjects idx wrtr init-stats)))

(defn purge-subjects
  "Remove the included predicates for each subject from `subj-chan` from the index
  using the `wrtr`. Keeps track of successfully `:purged` subjects and
  `:errors` in the stats map returned."
  [idx wrtr init-stats subj-chan]
  (process-subjects (fn [stats subj pred-map]
                      (try
                        (full-text/purge-subject idx wrtr subj pred-map)
                        (log/trace "Purged stale full text predicates for "
                                   "subject " subj)
                        (update stats :purged inc)

                        (catch Exception e
                          (log/error e (str "Error purging stale full text "
                                            "predicates for subject " subj))
                          (update stats :errors inc))))

                    init-stats subj-chan))

(defn purge-flakes
  [idx wrtr init-stats flake-chan]
  (->> flake-chan
       group-by-subject
       (purge-subjects idx wrtr init-stats)))

(defn reset-index
  [idx wrtr {:keys [network dbid] :as db}]
  (log/info "Resetting full text index for ledger " network "/" dbid)
  (let [cur-idx-preds (current-index-predicates db)
        idx-queue     (predicate-flakes db cur-idx-preds)
        initial-stats {:indexed 0, :errors 0}]
    (full-text/forget idx wrtr)
    (index-flakes idx wrtr initial-stats idx-queue)))

(defn update-index
  [idx wrtr db {:keys [flakes]}]
  (go
    (let [[add-pred-ch rem-pred-ch]  (updated-predicates db flakes)
          [cur-update-ch cur-rem-ch] (updated-subjects db flakes)

          add-queue (async/merge [add-pred-ch cur-update-ch])
          rem-queue (async/merge [rem-pred-ch cur-rem-ch])

          initial-stats {:indexed 0, :purged 0, :errors 0}
          indexed-stats (<! (index-flakes idx wrtr initial-stats add-queue))
          final-stats   (<! (purge-flakes idx wrtr indexed-stats rem-queue))]

      final-stats)))


(defn write-block
  [idx wrtr {:keys [network dbid] :as db} {:keys [flakes] :as block}]
  (let [start-time  (Instant/now)
        coordinates {:network network, :dbid dbid, :block (:block block)}]
    (log/info (str "Full-Text Search Index began processing new block at: "
                   start-time)
              coordinates)
    (go
      (let [stats    (if (schema/get-language-change flakes)
                       (<! (reset-index idx wrtr db))
                       (<! (update-index idx wrtr db block)))
            end-time (Instant/now)
            duration (- (.toEpochMilli ^Instant end-time)
                        (.toEpochMilli ^Instant start-time))
            status   (-> stats
                         (merge coordinates)
                         (assoc :duration duration))]
        (full-text/register-block idx wrtr status)
        (log/info (str "Full-Text Search Index ended processing new block at: "
                       end-time)
                  status)
        status))))


(defn write-range
  [idx wrtr db start-block end-block]
  (let [block-chan (-> db
                       (fdb/block-range start-block end-block)
                       (async/pipe (chan 1 (mapcat seq))))]
    (go-loop [results []]
      (if-let [block (<! block-chan)]
        (let [write-status (<! (write-block idx wrtr db block))]
          (recur (conj results write-status)))
        results))))


(defn sync-index
  [idx wrtr {:keys [network dbid block] :as db}]
  (let [last-indexed (-> idx
                         full-text/read-block-registry
                         :block
                         (or 0))
        first-block  (inc last-indexed)
        last-block   block]
    (if (<= first-block last-block)
      (do (log/info (str "Syncing full text index from block: " first-block
                         " to block " last-block " for ledger " network "/"
                         dbid))
          (write-range idx wrtr db first-block last-block))
      (do (log/info "Full text index up to date")
          (async/to-chan [])))))

(defn full-reset
  [idx wrtr db]
  (go
    (let [stats (<! (reset-index idx wrtr db))
          status (assoc stats :block (:block db))]
      (full-text/register-block idx wrtr status)
      status)))


(defn start-indexer
  "Manage full-text indexing processes in the background. Initializes an index
  writer and listener in a background thread and returns a map containing two
  function values. The function under the `stop-fn` key stops the background
  listener thread after disposing of any resources, and the function under the
  `process-fn` key sends messages to the background listener that runs different
  indexing jobs based on the `:action` key of the message map, and will return a
  channel that will eventually contain the result of the index operation. The
  recognized actions are `:block`, `:range`, and `:reset`."
  [conn]
  (when (-> conn :meta :file-storage-path)
    (let [write-q (chan)
          closer  (fn []
                    (async/close! write-q))
          runner  (fn [msg]
                    (let [resp-ch (chan)]
                      (async/put! write-q [msg resp-ch])
                      resp-ch))]
      (log/info "Starting Full Text Indexer")

      (go-loop []
        (if-let [[msg resp-ch] (<! write-q)]
          (let [{:keys [action db]} msg
                {:keys [network dbid]} db
                lang (-> db :settings :language (or :default))]
            (with-open [^Closeable idx (full-text/open-storage conn network dbid lang)
                        ^Closeable wrtr (full-text/writer idx)]
              (let [result  (case action
                              :block (let [{:keys [block]} msg]
                                       (<! (write-block idx wrtr db block)))
                              :forget (full-text/forget idx wrtr)
                              :range (let [{:keys [start end]} msg]
                                       (<! (write-range idx wrtr db start end)))
                              :reset (<! (full-reset idx wrtr db))
                              :sync  (<! (sync-index idx wrtr db))
                              ::unrecognized-action)]
                (if result
                  (async/put! resp-ch result (fn [val]
                                               (when val
                                                 (async/close! resp-ch))))
                  (async/close! resp-ch))))
            (recur))
          (log/info "Stopping Full Text Indexer")))

      {:close   closer
       :process runner})))
