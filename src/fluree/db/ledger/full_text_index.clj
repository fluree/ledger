(ns fluree.db.ledger.full-text-index
  (:require [clucie.core :as clucie]
            [fluree.db.query.analytical-full-text :as full-text]
            [clucie.store :as store]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log]
            [clojure.core.async :refer [timeout go-loop]]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [clojure.java.io :as io]
            [fluree.db.query.range :as query-range]
            [clojure.string :as str]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.constants :as const]
            [fluree.db.api :as fdb])
  (:import java.time.Instant))

(defn disk-store
  [path-to-dir network dbid]
  (store/disk-store (str path-to-dir network "/" dbid "/lucene")))

(defn retry-fn
  [fn-to-retry error-msg timeout-ms max-times subject]
  (go-loop [retry-times max-times
            ;; set timeout to 1 sec for first retry
            timeout* 1000]
    (if (<= retry-times 0)
      (log/warn "Item not successfully added to index. Retried max amount of times for subject: " subject)
      (do (<? (timeout timeout*))
          (let [retry? (try (do (fn-to-retry) false)
                            (catch Exception e (let [cause (:cause (Throwable->map e))]
                                                 (if (str/includes? cause error-msg)
                                                   true
                                                   (throw e)))))
                _      (log/trace (str "Retry successful for subject: " subject))]
            (if retry?
              (recur (dec max-times) timeout-ms)))))))

(defn add-flakes-to-store
  ([storage-dir network dbid flakes language]
   (add-flakes-to-store storage-dir network dbid flakes language {}))
  ([storage-dir network dbid flakes language opts]
   (go-try
    (let [store           (disk-store storage-dir network dbid)
          analyzer        (full-text/analyzer language)
          subject-flakes  (group-by #(.-s %) flakes)
          subjects        (keys subject-flakes)
          subject-count   (count subjects)
          timeout-ms      (or (:timeout opts) 500)
          max-retry-times (or (:retry opts) 10)]
      (loop [[s & r] subjects]
        (if-not s
          (do (when subjects
                (log/info (str "Add full text flakes to store complete for: " subject-count " subjects.")))
              subject-count)
          (let [subject   s
                flakes    (get subject-flakes subject)
                existing  (-> (try (clucie/search store {:_id (str s)} 1 analyzer 0 1)
                                   (catch Exception e [])) first)
                flake-map (reduce (fn [acc f]
                                    (assoc acc (keyword (str (.-p f))) (.-o f))) {} flakes)
                s-res     (if existing
                            (merge existing flake-map)
                            (let [cid (flake/sid->cid s)]
                              (assoc flake-map :_id s :collection cid)))
                add-fn    (if existing
                            (fn [] (clucie/update! store s-res (keys s-res) :_id (str s) analyzer))
                            (fn [] (clucie/add! store [s-res] (keys s-res) analyzer)))
                add-resp  (try (add-fn)
                               (catch Exception e (let [cause     (:cause (Throwable->map e))
                                                        error-msg "Lock held by this virtual machine"]
                                                    (if (str/includes? cause error-msg)
                                                      (<? (retry-fn add-fn error-msg timeout-ms max-retry-times subject)) (throw e)))))]
            (log/trace (str "Added flakes for subject: " subject " to full text index."))
            (<? (timeout timeout-ms))
            (recur r))))))))


(defn remove-flakes-from-store
  [storage-dir network dbid flakes language]
  (let [store          (disk-store storage-dir network dbid)
        analyzer       (full-text/analyzer language)
        subject-flakes (group-by #(.-s %) flakes)]
    (mapv (fn [[s flakes]]
            (let [_         (log/trace "Removing flakes from full text index: " flakes)
                  existing  (-> (try (clucie/search store {:_id (str s)} 1 analyzer 0 1)
                                     (catch Exception e [])) first)
                  flake-map (when existing (reduce (fn [acc f]
                                                     (dissoc acc (keyword (str (.-p f))))) existing flakes))]
              (when existing (clucie/update! store flake-map (keys flake-map) :_id s)))) subject-flakes)))

;; From https://gist.github.com/edw/5128978#file-delete-recursively-clj
(defn delete-files-recursively
  [f1 & [silently]]
  (let [f1* (io/file f1)]
    (when (.isDirectory f1*)
      (doseq [f2 (.listFiles f1*)]
        (delete-files-recursively f2 silently)))
    (when (.isFile f1*)
      (io/delete-file f1* silently))))

(defn track-full-text
  [dir ledger block]
  (with-open [w (clojure.java.io/writer (str dir ledger "/lucene/block.txt") :append false)]
    (.write w (str block))))

(defn check-full-text-block
  [dir ledger]
  (try (let [file (str dir ledger "/lucene/block.txt")]
         (slurp file))
       (catch Exception e nil)))

(defn reset-full-text-index
  [db path-to-dir network dbid]
  (go-try
   (let [start-time (Instant/now)]
     (log/info (str "Full-Text Search Index Reset began at: " start-time)
               {:network network
                :dbid    dbid})
     (delete-files-recursively (str path-to-dir network "/" dbid "/lucene"))
     (let [fullTextPreds (-> db :schema :fullText)
           language      (or (-> db :settings :language) :default)
           addFlakes     (loop [[pred & r] fullTextPreds
                                full []]
                           (if pred
                             (recur r (concat full (<? (query-range/index-range db :psot = [pred])))) full))
           add-result    (<? (add-flakes-to-store path-to-dir network dbid addFlakes language))
           end-time      (Instant/now)]
       (log/info (str "Full-Text Search Index Reset ended at: " start-time)
                 {:network  network
                  :dbid     dbid
                  :duration (- (.toEpochMilli end-time)
                               (.toEpochMilli start-time))})
       add-result))))

;;; TODO - handle multi predicates - prevent from going into index - handle at schema setting level.
(defn handle-block
  [db storage-dir network dbid block]
  (go-try
   (let [start-time (Instant/now)]
     (log/info (str "Full-Text Search Index Update began at: " start-time)
               {:network network
                :dbid    dbid
                :block   (:block block)})
     (if (schema-util/get-language-change (:flakes block))
       (do (log/info "Resetting full text index")
           (<? (reset-full-text-index db storage-dir network dbid))
           (track-full-text storage-dir (str network "/" dbid) (:block db)))
       (let [language            (or (-> db :settings :language) :default)
             [fullTextAdd fullTextRemove] (reduce (fn [[add remove] flake]
                                                    (if (= const/$_predicate:fullText (.-p flake))
                                                      (if (.-op flake)
                                                        [(conj add flake) remove]
                                                        [add (conj remove flake)])
                                                      [add remove]))
                                                  [[] []] (:flakes block))
             addToIndex          (-> (map #(try (<?? (query-range/index-range db :psot = [(.-s %)]))
                                                (catch Exception e []))
                                          fullTextAdd) flatten)
             removeFromIndex     (-> (map #(try (<?? (query-range/index-range db :psot = [(.-s %)]))
                                                (catch Exception e []))
                                          fullTextRemove) flatten)
             fullText            (-> db :schema :fullText)
             lucene-add-queue    (concat (filter #(and (fullText (.-p %)) (.-op %)) (:flakes block)) addToIndex)
             lucene-remove-queue (concat (filter (fn [flake]
                                                   (and (fullText (.-p flake))
                                                        (not (.-op flake))
                                                        ;; We only want to add flakes to "remove" if that
                                                        ;; s-p is being removed, not if
                                                        ;; it's being updated
                                                        (not (some #(and (= (.-s flake) (.-s %))
                                                                         (= (.-p flake) (.-p %))) lucene-add-queue))))
                                                 (:flakes block)) removeFromIndex)]

         (do (<? (add-flakes-to-store storage-dir network dbid lucene-add-queue language))
             (remove-flakes-from-store storage-dir network dbid lucene-remove-queue language)
             (track-full-text storage-dir (str network "/" dbid) (:block block)))))
     (let [end-time (Instant/now)]
       (log/info (str "Full-Text Search Index Update ended at: " end-time)
                 {:network  network
                  :dbid     dbid
                  :block    (:block block)
                  :duration (- (.toEpochMilli end-time)
                               (.toEpochMilli start-time))})))))

(defn sync-full-text-index
  [db storage-dir network dbid start-block end-block]
  (go-try (loop [[block & r] (range start-block (inc end-block))]
            (if block
              (let [block-data (-> (<? (fdb/block-range db block block)) first)]
                (do (<? (handle-block db storage-dir network dbid block-data))
                    (recur r)))
              true))))


(comment
  (def db (<?? (fluree.db.api/db (:conn user/system) "fluree/test")))


  (<?? (fdb/block-range db 2 2))
  (reset-full-text-index db "data/ledger/" "shi" "test2")

  (def store (disk-store "data/ledger/" "shi" "test2"))

  (clucie/search store {:_id (str 351843720888321)} 1 (standard-analyzer) 0 1))
