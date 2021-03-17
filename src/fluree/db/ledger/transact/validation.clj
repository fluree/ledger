(ns fluree.db.ledger.transact.validation
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.util.async :as async-util]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [clojure.core.async :as async]
            [fluree.db.util.core :as util]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.ledger.transact.schema :as tx-schema]
            [fluree.db.ledger.transact.tx-meta :as tx-meta]
            [fluree.db.util.log :as log]
            [fluree.db.permissions-validate :as perm-validate]
            [fluree.db.flake :as flake]
            [fluree.db.api :as fdb])
  (:import (fluree.db.flake Flake)))

;;; functions to validate transactions

(defn queue-validation-fn
  "Queues a validation function to be run once we have a db-after completed.
  There are a few basic types of validations functions, some of which must
  also queue flakes into tx-state that will be inputs to the function
  - predicate - no flake queueing necessary as run once per flake
  - predicate-tx - must queue all the predicate flakes of this type which may be
                   across multiple transaction items. flake-or-flakes will be a single flake.
                   As this is executed once per predicate, the function might already be
                   queued, and if so 'f' will be nil
  - collection - must queue all the collection flakes per sid, as they may be across
                 multiple transactions (but usually will not be). flake-or-flakes will be a sequence"
  [fn-type {:keys [validate-fn] :as tx-state} f flakes pid-or-sid]
  (case fn-type
    :predicate (swap! validate-fn update :queue conj f)
    :predicate-tx (swap! validate-fn (fn [validate-data]
                                       (cond-> validate-data
                                               f (update :queue conj f)
                                               true (update-in [:tx-spec pid-or-sid] into flakes))))
    :collection (swap! validate-fn (fn [validate-data]
                                     (let [existing-flakes (get-in validate-data [:c-spec pid-or-sid])]
                                       (cond-> validate-data
                                               true (assoc-in [:c-spec pid-or-sid] (into existing-flakes flakes))
                                               ;; if flakes exist, we already queued a fn for this subject, don't want multiple
                                               (empty? existing-flakes) (update :queue conj f)))))))


;; TODO - this can be done per schema change, not per transaction, to make even more efficient.
(defn build-function
  "Builds a function based on a function subject-id (or a list of function subject ids) and
  delivers the executable function to the provided promise.

  Promise is used here to cache this work so it is only done once per transaction.

  fn-type should be:
  - predSpec - for predicate Spec
  - "
  [fn-subjects db promise fn-type]
  (async/go
    (try
      (let [fn-str    (if (sequential? fn-subjects)
                        (->> fn-subjects                    ;; combine multiple functions with an (and ...) wrapper
                             (map #(query-range/index-range db :spot = [% const/$_fn:code]))
                             async/merge
                             (async/into [])
                             (map #(if (util/exception? %) (throw %) (.-o ^Flake %)))
                             dbfunctions/combine-fns)
                        (-> (<? (query-range/index-range db :spot = [fn-subjects const/$_fn:code]))
                            ^Flake first
                            (.-o)))
            fn-parsed (<? (dbfunctions/parse-fn db fn-str fn-type nil))]
        (deliver promise fn-parsed))
      (catch Exception e (deliver promise e)))))


(defn resolve-function
  "Returns promise for function that will get used. Tries to get one from cache
  and if not already processed then will create a new promise channel.
  Resistant to race conditions which are unlikely, but possible."
  [fn-subjects db validate-fn-atom fn-type]
  (or (get-in @validate-fn-atom [:cache fn-subjects])
      (let [p (promise)]
        (swap! validate-fn-atom update :cache
               (fn [fn-cache]
                 (if (get fn-cache fn-subjects)
                   fn-cache                                 ;; something put a promise in here while we were checking, just return
                   (do (build-function fn-subjects db p fn-type)
                       (assoc fn-cache fn-subjects p)))))
        ;; return whatever promise was in the cache - either one we just created or existing one if a race condition existed
        (get-in @validate-fn-atom [:cache fn-subjects]))))


(defn- async-response-wrapper
  "Wraps an async response with a formatted response."
  [async-response function]
  (async/go
    (let [res (async/<! async-response)]
      (function res))))


(defn update-tx-spent-fuel
  "Executing functions should consume fuel. Adds fuel to the master fuel atom."
  [fuel spent]
  (swap! fuel (fn [fuel] (-> fuel
                             (update :credits - spent)
                             (update :spent + spent))))
  (when (neg? (get @fuel :credits))
    (throw (ex-info "Transaction unable to complete, all allocated fuel has been exhausted."
                    {:status 400
                     :error  :db/insufficient-fuel}))))

;; predicate specs

(defn- pred-spec-response
  "Returns a true for a valid spec response, or an exception (but does not throw) for an invalid one.
  If response is an exception, wraps exception message."
  [specDoc predicate-name ^Flake flake response]
  (cond
    ;; some error in processing happened, don't allow transaction but communicate internal error
    (util/exception? response)
    (ex-info (str "Internal execution error for predicate spec: " (.getMessage ^Exception response) ". "
                  (if specDoc
                    (str specDoc " Value: " (.-o flake))
                    (str "Object " (.-o flake) " does not conform to the spec for predicate: " predicate-name)))
             {:status 400
              :error  :db/invalid-tx
              :cause  response})

    ;; any truthy value, spec succeeded - allow transaction
    response
    true

    ;; non truthy value, spec failed - do not allow transaction
    :else
    (ex-info (str (if specDoc
                    (str specDoc " Value: " (.-o flake))
                    (str "Object " (.-o flake) " does not conform to the spec for predicate: " predicate-name)))
             {:status 400
              :error  :db/invalid-tx})))


(defn run-predicate-spec
  [fn-promise ^Flake flake predicate-name specDoc {:keys [fuel t auth db-after] :as tx-state}]
  (let [sid (.-s flake)
        pid (.-p flake)
        o   (.-o flake)]
    (try
      (let [fuel-atom (atom {:stack   []
                             :credits (:credits @fuel)
                             :spent   0})
            f         @fn-promise
            res       (f {:db      @db-after
                          :sid     sid
                          :pid     pid
                          :o       o
                          :flakes  [flake]
                          :auth_id auth
                          :state   fuel-atom
                          :t       t})]
        ;; update main tx fuel count with the fuel spent to execute this tx function
        (update-tx-spent-fuel fuel (:spent @fuel-atom))

        (if (async-util/channel? res)
          (async-response-wrapper res (partial pred-spec-response specDoc predicate-name flake))
          (pred-spec-response specDoc predicate-name flake res)))
      (catch Exception e (pred-spec-response specDoc predicate-name flake e)))))


(defn queue-pred-spec
  "Flakes param flows through, queues spec for flake"
  [flakes flake pred-info {:keys [validate-fn db-root] :as tx-state}]
  (let [spec           (pred-info :spec)
        specDoc        (pred-info :specDoc)
        predicate-name (pred-info :name)
        fn-promise     (resolve-function spec db-root validate-fn "predSpec")
        pred-spec-fn   (partial run-predicate-spec fn-promise flake predicate-name specDoc)]
    (queue-validation-fn :predicate tx-state pred-spec-fn nil nil)
    flakes))


;; predicate tx-spec

(defn- pred-tx-spec-response
  [pred-name tx-spec-doc response]
  (util/exception? response)
  (ex-info (str "Internal execution error for predicate txSpec: " (.getMessage ^Exception response) ". "
                "The predicate " pred-name " does not conform to the txSpec. " tx-spec-doc)
           {:status 400
            :error  :db/invalid-tx
            :cause  response})

  response
  true

  :else
  (ex-info (str "The predicate " pred-name " does not conform to the txSpec. " tx-spec-doc)
           {:status 400
            :error  :db/invalid-tx}))


(defn run-predicate-tx-spec
  "This function is designed to be called with a (partial pid pred-name txSpecDoc) and
  returns a function whose only argument is tx-state, which can be used to get the final
  list of predicate flakes affected by this predicate."
  [pid pred-tx-fn pred-name tx-spec-doc {:keys [db-root auth instant fuel validate-fn t] :as tx-state}]
  (try
    (let [pid-flakes (get-in @validate-fn [:tx-spec pid])
          fuel-atom  (atom {:stack   []
                            :credits (:credits @fuel)
                            :spent   0})
          f          @pred-tx-fn
          res        (f {:db      db-root
                         :pid     pid
                         :instant instant
                         :flakes  pid-flakes
                         :auth_id auth
                         :state   fuel-atom
                         :t       t})]
      ;; update main tx fuel count with the fuel spent to execute this tx function
      (update-tx-spent-fuel fuel (:spent @fuel-atom))

      (if (async-util/channel? res)
        (async-response-wrapper res (partial pred-tx-spec-response pred-name tx-spec-doc))
        (pred-tx-spec-response pred-name tx-spec-doc res)))
    (catch Exception e (pred-tx-spec-response pred-name tx-spec-doc e))))


(defn- build-predicate-tx-spec-fn
  "When a predicate-tx-spec function hasn't already been queued for a particular predicate,
  do so and place the function into the validating function queue for processing."
  [pred-info db]
  (let [pred-tx-fn  (promise)
        pred-name   (pred-info :name)
        tx-spec-doc (pred-info :txSpecDoc)
        pid         (pred-info :id)
        queue-fn    (partial run-predicate-tx-spec pid pred-tx-fn pred-name tx-spec-doc)]

    ;; kick off building function, will put realized function into pred-tx-fn promise
    (build-function (pred-info :txSpec) db pred-tx-fn "predSpec")
    ;; return function
    queue-fn))


(defn queue-predicate-tx-spec
  "Passes 'flakes' through function untouched, but queues predicate spec for
  execution once db-after is resolved.

  Predicates that have a txSpec defined need to run once for all flakes with the
  same predicate as inputs.

  Queuing a flake here adds it to a map by predicate. We also kick off resolving
  the txSpec function in the background if not already done, so it can be ready
  when the transaction is completed to run the validation.
  For each predicate that requires a txSpec function to be run, we store
  a two-tuple of the function (as a promise) and a list of flakes for that predicate
  that must be validated."
  [flakes predicate-flakes pred-info {:keys [validate-fn db-root] :as tx-state}]
  (let [pid        (pred-info :id)
        tx-spec-fn (when (empty? (get-in @validate-fn [:tx-spec pid]))
                     ;; first time called (no existing flakes for this tx-spec), generate and queue fn also
                     (build-predicate-tx-spec-fn pred-info db-root))]
    (queue-validation-fn :predicate-tx tx-state tx-spec-fn predicate-flakes pid)
    flakes))


;; collection specs

(defn- collection-spec-response
  [flakes c-spec-doc response]
  (util/exception? response)
  (ex-info (str "Internal execution error for collection spec: " (.getMessage ^Exception response) ". "
                "Transaction does not adhere to the collection spec: " c-spec-doc)
           {:status 400
            :error  :db/invalid-tx
            :flakes flakes
            :cause  response})

  response
  true

  :else
  (ex-info (str "Transaction does not adhere to the collection spec: " c-spec-doc)
           {:status 400
            :error  :db/invalid-tx
            :flakes flakes}))


(defn run-collection-spec
  "Runs a collection spec. Will only execute collection spec if there are still flakes for
  the subject that exist."
  [sid c-spec-fn c-spec-doc {:keys [db-after instant validate-fn auth t fuel] :as tx-state}]
  (async/go
    (try
      (let [subject-flakes (get-in @validate-fn [:c-spec sid])
            has-adds?      (some (fn [^Flake flake] (when (true? (.-op flake)) true)) subject-flakes) ;; stop at first `true` .-op
            deleted?       (or (not has-adds?)
                               (empty? (<? (query-range/index-range @db-after :spot = [sid]))))]
        (if deleted?
          true
          (let [fuel-atom (atom {:stack   []
                                 :credits (:credits @fuel)
                                 :spent   0})
                f         @c-spec-fn
                res       (f {:db      @db-after
                              :instant instant
                              :sid     sid
                              :flakes  subject-flakes
                              :auth_id auth
                              :t       t
                              :state   fuel-atom})]

            ;; update main tx fuel count with the fuel spent to execute this tx function
            (update-tx-spent-fuel fuel (:spent @fuel-atom))

            (if (async-util/channel? res)
              (collection-spec-response subject-flakes c-spec-doc (<? res))
              (collection-spec-response subject-flakes c-spec-doc res)))))
      (catch Exception e (collection-spec-response (get-in @validate-fn [:c-spec sid]) c-spec-doc e)))))


(defn queue-collection-spec
  [collection c-spec-fn-ids {:keys [validate-fn db-root] :as tx-state} subject-flakes]
  (let [sid        (.-s ^Flake (first subject-flakes))
        c-spec-fn  (resolve-function c-spec-fn-ids db-root validate-fn "collectionSpec")
        c-spec-doc (or (dbproto/-c-prop db-root :specDoc collection) collection) ;; use collection name as default specDoc
        execute-fn (partial run-collection-spec sid c-spec-fn c-spec-doc)]
    (queue-validation-fn :collection tx-state execute-fn subject-flakes sid)))

(defn queue-predicate-collection-spec
  [tx-state subject-flakes]
  (let [sid        (.-s ^Flake (first subject-flakes))
        execute-fn (partial tx-schema/validate-schema-predicate sid)]
    (queue-validation-fn :collection tx-state execute-fn subject-flakes sid)))

(defn queue-tx-meta-collection-spec
  [tx-state subject-flakes]
  (let [sid        (.-s ^Flake (first subject-flakes))
        execute-fn (partial tx-meta/valid-tx-meta? sid)]
    (queue-validation-fn :collection tx-state execute-fn subject-flakes sid)))

(defn check-collection-specs
  "If a collection spec is needed, register it for processing the subject's flakes."
  [collection {:keys [db-root] :as tx-state} subject-flakes]
  (let [c-spec-fn-ids    (dbproto/-c-prop db-root :spec collection)
        pred-collection? (= "_predicate" collection)
        tx-collection?   (= "_tx" collection)]
    (when c-spec-fn-ids
      (queue-collection-spec collection c-spec-fn-ids tx-state subject-flakes))
    (when pred-collection?
      (queue-predicate-collection-spec tx-state subject-flakes))
    (when tx-collection?
      (queue-tx-meta-collection-spec tx-state subject-flakes)))
  subject-flakes)

;; Permissions

(defn permissions
  "Validates transaction based on the state of the new database."
  [db-before candidate-db flakes]
  (go-try
    (let [tx-permissions (:permissions db-before)
          no-filter?     (true? (:root? tx-permissions))]
      (if no-filter?
        ;; everything allowed, just return
        true
        ;; go through each statement and check
        (loop [[^Flake flake & r] flakes]
          (when (> (.-s flake) const/$maxSystemPredicates)
            (when-not (if (.-op flake)
                        (<? (perm-validate/allow-flake? candidate-db flake tx-permissions))
                        (<? (perm-validate/allow-flake? db-before flake tx-permissions)))
              (throw (ex-info (format "Insufficient permissions for predicate: %s within collection: %s."
                                      (dbproto/-p-prop db-before :name (.-p flake))
                                      (dbproto/-c-prop db-before :name (flake/sid->cid (.-s flake))))
                              {:status 400
                               :error  :db/write-permission}))))
          (if r
            (recur r)
            true))))))

;; dependencies

(defn tx-deps-check
  "A transaction can optionally include a list of dependent transactions.
  Returns true if dependency check is successful, throws exception if there
  is an error."
  [db tx-map]
  (let [deps (:deps tx-map)]
    (go-try
      (if (empty? deps)
        true
        (let [res (->> (reduce-kv (fn [query-acc key dep]
                                    (-> query-acc
                                        (update :selectOne conj (str "?error" key))
                                        (update :where conj [(str "?tx" key) "_tx/id" dep])
                                        (update :optional conj [(str "?tx" key) "_tx/error" (str "?error" key)])))
                                  {:selectOne [] :where [] :optional []} deps)
                       (fdb/query-async (go-try db))
                       <?)]
          (if (and (not (empty? res)) (every? nil? res))
            true
            (throw (ex-info (str "One or more of the dependencies for this transaction failed: " deps)
                            {:status 403 :error :db/invalid-auth}))))))))


;; Runtime

(defn flush-fn-queue
  "Flushes the validation function queue onto queue-ch."
  [queue-ch queue]
  (async/go-loop [[queue-fn & r] queue]
    (cond
      ;; not more functions to process, close queue-ch which will trigger result-ch to close once finished processing
      (nil? queue-fn)
      (async/close! queue-ch)

      ;; >! will return true if channel isn't closed and put successful, recur
      (true? (async/>! queue-ch queue-fn))
      (recur r)

      ;; put was not successful, queue-ch got closed due to an error result
      :else
      nil)))


(defn run
  "Runs validation functions in parallel according to parallelism. Will return
  'true' if all functions pass (or if there were no functions to process)

  validate-fn is an atom that contains:
  - queue
  - cache
  - tx-spec
  - c-spec"
  [all-flakes {:keys [validate-fn] :as tx-state} parallelism]
  (go-try
    (let [{:keys [queue]} @validate-fn]
      (if (empty? queue)                                    ;; if nothing in queue, return true for success.
        true
        (let [tx-state* (assoc tx-state :flakes all-flakes)

              queue-ch  (async/chan parallelism)
              result-ch (async/chan parallelism)
              af        (fn [f res-chan]
                          (async/go
                            (let [fn-result (async/<! (f tx-state*))]
                              (async/put! res-chan fn-result)
                              (async/close! res-chan))))]

          ;; kicks off process to push queue onto queue-ch
          (flush-fn-queue queue-ch queue)

          ;; start executing functions, pushing results to result-ch. result-ch will close once queue-ch closes
          (async/pipeline-async parallelism result-ch af queue-ch)

          ;; read results. If an exception occurs, close the queue-ch to stop execution
          (loop []
            (let [next-res (async/<! result-ch)]
              (cond
                ;; no more functions, complete - queue-ch closed as queue was exhausted
                (nil? next-res)
                true

                ;; exception, close channels and return exception
                (util/exception? next-res)
                (do (async/close! queue-ch)
                    (async/close! result-ch)
                    next-res)

                ;; anything else, all good - keep going
                :else (recur)))))))))
