(ns fluree.db.ledger.transact.validation
  (:require [fluree.db.util.async :refer [<? <?? go-try channel?] :as async-util]
            [fluree.db.dbfunctions.core :as dbfunctions]
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
            [fluree.db.api :as fdb]
            [fluree.db.ledger.transact.tempid :as tempid])
  (:import (fluree.db.flake Flake)))

(set! *warn-on-reflection* true)

;;; functions to validate transactions

(defn queue-validation-fn
  "Queues a validation function to be run once we have a db-after completed.
  There are a few basic types of validations functions, some of which must
  also queue flakes into tx-state that will be inputs to the function
  - unique - no flake queuing necessary - just run at end of transaction
  - predicate - no flake queueing necessary as run once per flake
  - predicate-tx - must queue all the predicate flakes of this type which may be
                   across multiple transaction items. flake-or-flakes will be a single flake.
                   As this is executed once per predicate, the function might already be
                   queued, and if so 'f' will be nil
  - collection - must queue all the collection flakes per sid, as they may be across
                 multiple transactions (but usually will not be). flake-or-flakes will be a sequence"
  [fn-type {:keys [validate-fn]} f flakes pid-or-sid]
  (case fn-type
    :unique (swap! validate-fn update :queue conj f)
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
;; TODO - calls to this now block to solve resource contention issues - can remove the promise as no longer do in background
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
      (let [fn-str    (->> fn-subjects                      ;; combine multiple functions with an (and ...) wrapper
                           (map #(query-range/index-range db :spot = [% const/$_fn:code]))
                           async/merge
                           (async/into [])
                           (<?)
                           (map #(if (util/exception? %) (throw %) (.-o ^Flake (first %))))
                           dbfunctions/combine-fns)
            fn-parsed (<? (dbfunctions/parse-and-wrap-fn db fn-str fn-type))]
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
                   (do (<?? (build-function fn-subjects db p fn-type))
                       (assoc fn-cache fn-subjects p)))))
        ;; return whatever promise was in the cache - either one we just created or existing one if a race condition existed
        (get-in @validate-fn-atom [:cache fn-subjects]))))


(defn- async-response-wrapper
  "Wraps a core.async response coming off a port with a formatted response."
  [port response-fn]
  (async/pipe port (async/chan 1 (map response-fn))))


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

;; unique: true special handling
;; unique values, in specific situations, require validation after the transaction is finalized to make sure
;; (a) if there is an existing unique match (pred+object), it can be OK so long as the existing match
;;     is deleted somewhere within the transaction
;; (b) any tempids used for unique:true might have resolved to existing subjects (via ':upsert true') which
;;     needs to happen after any such resolution happens

(defn queue-check-unique-match-retracted
  "if there is an existing unique match (pred+object), it can be OK so long as the existing match
  is deleted in the transaction, which we can validate once the transaction is complete by looking
  for the specific retraction flake we expect to see - else throw."
  [existing-flake _id pred-info object tx-state]
  (let [f (fn [{:keys [flakes t]}]
            (let [check-flake (flake/flip-flake existing-flake t)
                  retracted?  (contains? flakes check-flake)]
              (if retracted?
                true
                (throw (ex-info (str "Unique predicate " (pred-info :name) " with value: "
                                     object " matched an existing subject.")
                                {:status   400
                                 :error    :db/invalid-tx
                                 :cause    [_id (pred-info :name) object]
                                 :conflict [(.-s ^Flake existing-flake)]})))))]
    (queue-validation-fn :unique tx-state f nil nil)))


(defn queue-check-unique-tempid-still-unique
  "if there is an existing unique match (pred+object), it can be OK so long as the existing match
  is deleted in the transaction, which we can validate once the transaction is complete by looking
  for the specific retraction flake we expect to see - else throw.
  - tempid    - the tempid object value that is supposed to be unique
  - _id       - the _id of the transaction item that caused this validation to be run, might be a tempid
  - pred-info - pred-info function allows getting different information about this predicate
  - tx-state  - transaction state, which is also passed in as the only argument in the final validation fn"
  [tempid _id pred-info tx-state]
  (let [f (fn [{:keys [tempids db-after]}]
            (go-try
              (let [tempid-sid (get @tempids tempid)
                    _id*       (if (tempid/TempId? _id)
                                 (get @tempids _id)
                                 _id)
                    matches    (<? (query-range/index-range @db-after :post = [(pred-info :id) tempid-sid]))
                    matches-n  (count matches)]
                ;; should be a single match, whose .-s is the final _id of the transacted flake
                (if (not= 1 matches-n)
                  (throw (ex-info (str "Unique predicate " (pred-info :name) " with a tempid value: "
                                       (:user-string tempid) " resolved to subject: " tempid-sid
                                       ", which is not unique.")
                                  {:status 400 :error :db/invalid-tx :tempid tempid}))
                  ;; one match as expected... extra check here as match .-s should always equal the _id*,
                  ;; else something strange happened
                  (or (= _id* (.-s ^Flake (first matches)))
                      (throw (ex-info (str "Unique predicate resolved to mis-matched subject.")
                                      {:status   500
                                       :error    :db/unexpected-error
                                       :cause    [(:user-string tempid) (pred-info :name)]
                                       :conflict (vec (first matches))})))))))]
    (queue-validation-fn :unique tx-state f nil nil)))


;; predicate specs

(defn- pred-spec-response
  "Returns a true for a valid spec response, or an exception (but does not throw) for an invalid one.
  If response is an exception, wraps exception message."
  [specDoc predicate-name ^Flake flake response]
  (cond
    ;; some error in processing happened, don't allow transaction but communicate internal error
    (util/exception? response)
    (ex-info (str "Internal execution error for predicate spec: " (ex-message response) ". "
                  "Predicate spec failed for predicate: " predicate-name "." (when specDoc (str " " specDoc)))
             {:status     400
              :error      :db/predicate-spec
              :cause      (vec flake)
              :specDoc    specDoc
              :ex-message (ex-message response)
              :ex-data    (ex-data response)})

    ;; any truthy value, spec succeeded - allow transaction
    response
    true

    ;; non truthy value, spec failed - do not allow transaction
    :else
    (ex-info (str "Predicate spec failed for predicate: " predicate-name "." (when specDoc (str " " specDoc)))
             {:status 400
              :error  :db/predicate-spec
              :cause  (vec flake)})))


(defn run-predicate-spec
  [fn-promise ^Flake flake predicate-name specDoc {:keys [fuel t auth db-after]}]
  (let [sid (.-s flake)
        pid (.-p flake)
        o   (.-o flake)]
    (try
      (let [fuel-atom (atom {:stack   []
                             :credits (:credits @fuel)
                             :spent   0})
            f         @fn-promise
            _         (log/debug "predicate spec fn:" f)
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
  (let [fn-sids        (pred-info :spec)
        specDoc        (pred-info :specDoc)
        predicate-name (pred-info :name)
        fn-promise     (resolve-function fn-sids db-root validate-fn "predSpec")
        pred-spec-fn   (-> run-predicate-spec
                           (partial fn-promise flake predicate-name specDoc)
                           (with-meta {:type      :predicate-spec
                                       :predicate predicate-name
                                       :target    flake
                                       :fn-sid    fn-sids
                                       :doc       specDoc}))]
    (queue-validation-fn :predicate tx-state pred-spec-fn nil nil)
    flakes))


;; predicate tx-spec

(defn- pred-tx-spec-response
  [pred-name flakes tx-spec-doc response]
  (cond
    (util/exception? response)
    (ex-info (str "Internal execution error for predicate txSpec: " (.getMessage ^Exception response) ". "
                  "Predicate txSpec failed for: " pred-name "." (when tx-spec-doc (str " " tx-spec-doc)))
             {:status     400
              :error      :db/predicate-tx-spec
              :cause      flakes
              :ex-message (ex-message response)
              :ex-data    (ex-data response)})

    response
    true

    :else
    (ex-info (str "Predicate txSpec failed for: " pred-name "." (when tx-spec-doc (str " " tx-spec-doc)))
             {:status 400
              :error  :db/predicate-tx-spec
              :cause  flakes})))


(defn run-predicate-tx-spec
  "This function is designed to be called with a (partial pid pred-name txSpecDoc) and
  returns a function whose only argument is tx-state, which can be used to get the final
  list of predicate flakes affected by this predicate."
  [pid pred-tx-fn pred-name tx-spec-doc {:keys [db-root auth instant fuel validate-fn t]}]
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
        (async-response-wrapper res (partial pred-tx-spec-response pred-name pid-flakes tx-spec-doc))
        (pred-tx-spec-response pred-name pid-flakes tx-spec-doc res)))
    (catch Exception e (pred-tx-spec-response pred-name (get-in @validate-fn [:tx-spec pid]) tx-spec-doc e))))


(defn- build-predicate-tx-spec-fn
  "When a predicate-tx-spec function hasn't already been queued for a particular predicate,
  do so and place the function into the validating function queue for processing."
  [pred-info db]
  (let [pred-tx-fn  (promise)
        pred-name   (pred-info :name)
        tx-spec-doc (pred-info :txSpecDoc)
        pid         (pred-info :id)
        fn-sids     (pred-info :txSpec)
        queue-fn    (-> run-predicate-tx-spec
                        (partial pid pred-tx-fn pred-name tx-spec-doc)
                        (with-meta {:type    :predicate-tx-spec
                                    :target  pred-name
                                    :fn-sids fn-sids}))]

    ;; kick off building function, will put realized function into pred-tx-fn promise
    (<?? (build-function fn-sids db pred-tx-fn "predSpec"))
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
  [flakes collection c-spec-doc response]
  (cond
    (util/exception? response)
    (ex-info (str "Internal execution error for collection spec: " (.getMessage ^Exception response) ". "
                  "Collection spec failed for: " collection "." (when c-spec-doc (str " " c-spec-doc)))
             {:status 400
              :error  :db/collection-spec
              :flakes flakes
              :cause  response})

    response
    true

    :else
    (ex-info (str "Collection spec failed for: " collection "." (when c-spec-doc (str " " c-spec-doc)))
             {:status 400
              :error  :db/collection-spec
              :flakes flakes})))


(defn run-collection-spec
  "Runs a collection spec. Will only execute collection spec if there are still flakes for
  the subject that exist."
  [collection sid c-spec-fn c-spec-doc {:keys [db-after db-before instant validate-fn auth t fuel]}]
  (async/go
    (try
      (let [subject-flakes (get-in @validate-fn [:c-spec sid])
            has-adds?      (some flake/op subject-flakes) ;; stop at first `true` .-op
            has-retracts?  (some (complement flake/op) subject-flakes)
            deleted?       (or (false? has-adds?) ; has-adds? is nil when not found
                               (empty? (<? (query-range/index-range @db-after :spot = [sid]))))]
        (let [fuel-atom (atom {:stack   []
                               :credits (:credits @fuel)
                               :spent   0})
              f         @c-spec-fn
              res       (f {:db        @db-after
                            :db-before db-before
                            :delete    has-retracts?
                            :instant   instant
                            :sid       sid
                            :flakes    subject-flakes
                            :auth_id   auth
                            :t         t
                            :state     fuel-atom})
              res*      (if (channel? res) (async/<! res) res)]

          ;; update main tx fuel count with the fuel spent to execute this tx function
          (update-tx-spent-fuel fuel (:spent @fuel-atom))
          (collection-spec-response subject-flakes collection c-spec-doc res*)))
      (catch Exception e (collection-spec-response (get-in @validate-fn [:c-spec sid]) collection c-spec-doc e)))))


(defn queue-collection-spec
  [collection c-spec-fn-ids {:keys [validate-fn db-root] :as tx-state} subject-flakes]

  (let [sid        (flake/s (first subject-flakes))
        c-spec-fn  (resolve-function c-spec-fn-ids db-root validate-fn "collectionSpec")
        c-spec-doc (or (dbproto/-c-prop db-root :specDoc collection) collection) ;; use collection name as default specDoc
        execute-fn (-> run-collection-spec
                       (partial collection sid c-spec-fn c-spec-doc)
                       (with-meta {:type   :collection-spec
                                   :target sid
                                   :fn-sid c-spec-fn-ids
                                   :doc    c-spec-doc}))]
    (queue-validation-fn :collection tx-state execute-fn subject-flakes sid)))

(defn queue-predicate-collection-spec
  [tx-state subject-flakes]
  (let [sid        (flake/s (first subject-flakes))
        execute-fn (-> tx-schema/validate-schema-predicate
                       (partial sid)
                       (with-meta {:type   :collection-spec
                                   :target sid
                                   :fn     :validate-schema-predicate}))]
    (queue-validation-fn :collection tx-state execute-fn subject-flakes sid)))

(defn queue-tx-meta-collection-spec
  [tx-state subject-flakes]
  (let [sid        (flake/s (first subject-flakes))
        execute-fn (-> tx-meta/valid-tx-meta?
                       (partial sid)
                       (with-meta {:type   :collection-spec
                                   :target sid
                                   :fn     :validate-tx-meta}))]
    (queue-validation-fn :collection tx-state execute-fn subject-flakes sid)))

(defn check-collection-specs
  "If a collection spec is needed, register it for processing the subject's flakes."
  [collection {:keys [db-root] :as tx-state} subject-flakes]
  (let [c-spec-fn-ids (dbproto/-c-prop db-root :spec collection)]
    (when c-spec-fn-ids
      (queue-collection-spec collection c-spec-fn-ids tx-state subject-flakes))
    ;; schema changes and user-specified _tx require internal custom validations
    (cond
      (= "_predicate" collection)
      (queue-predicate-collection-spec tx-state subject-flakes)

      (= "_tx" collection)
      (queue-tx-meta-collection-spec tx-state subject-flakes)

      (= "_collection" collection)
      (tx-schema/validate-collection-name subject-flakes)

      :else nil)
    subject-flakes))

;; Permissions

(defn permissions
  "Validates transaction based on the state of the new database.

  Exceptions here should throw: caught by go-try."
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
                               :error  :db/tx-permission}))))
          (if r
            (recur r)
            true))))))

(defn run-permissions-checks
  [all-flakes {:keys [db-before db-after]} parallelism]
  (go-try
    (let [db-after       @db-after
          queue-ch       (async/chan parallelism)
          result-ch      (async/chan parallelism)
          tx-permissions (:permissions db-before)
          af             (fn [^Flake flake res-chan]
                           (async/go
                             (try
                               (let [fn-res (if (.-op flake)
                                              (async/<! (perm-validate/allow-flake? db-after flake tx-permissions))
                                              (async/<! (perm-validate/allow-flake? db-before flake tx-permissions)))
                                     res    (or fn-res      ;; any truthy value means valid
                                                (ex-info (format "Insufficient permissions for predicate: %s within collection: %s."
                                                                 (dbproto/-p-prop db-before :name (.-p flake))
                                                                 (dbproto/-c-prop db-before :name (flake/sid->cid (.-s flake))))
                                                         {:status 400
                                                          :error  :db/write-permission
                                                          :cause  (vec flake)}))]
                                 (async/put! res-chan res)
                                 (async/close! res-chan))
                               (catch Exception e (async/put! res-chan e)
                                                  (async/close! res-chan)))))]

      (->> all-flakes
           (filter (fn [^Flake flake] (> (.-s flake) const/$maxSystemPredicates))) ;; skip all system predicates
           (async/onto-chan! queue-ch))

      (async/pipeline-async parallelism result-ch af queue-ch)

      (loop [errors []]
        (let [next-res (async/<! result-ch)]
          (cond
            ;; no more functions, complete - queue-ch closed as queue was exhausted
            (nil? next-res)
            (->> errors
                 (map #(let [ex (ex-data %)]
                         (when (= 500 (:status ex))
                           (log/error % "Unexpected validation error in transaction! Flakes:" all-flakes))
                         (assoc ex :message (ex-message %))))
                 (not-empty))

            (util/exception? next-res)
            (recur (conj errors next-res))

            ;; anything else, all good - keep going
            :else (recur errors)))))))


;; dependencies

(defn tx-deps-check
  "A transaction can optionally include a list of dependent transactions.
  Returns true if dependency check is successful, throws exception if there
  is an error.

  Exceptions here should throw: catch by go-try."
  [db {:keys [deps] :as tx-map}]
  (go-try
    (let [res (->> deps
                   (reduce-kv (fn [query-acc key dep]
                                (-> query-acc
                                    (update :selectOne conj (str "?error" key))
                                    (update :where conj [(str "?tx" key) "_tx/id" dep])
                                    (update :optional conj [(str "?tx" key) "_tx/error" (str "?error" key)])))
                              {:selectOne [] :where [] :optional []})
                   (fdb/query-async (go-try db))
                   <?)]
      (if (and (seq res) (every? nil? res))
        true
        (throw (ex-info (str "One or more of the dependencies for this transaction failed: " deps)
                        {:status 400 :error :db/invalid-dependency}))))))


;; Runtime

(defn run-queued-specs
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
      (when (not-empty queue)                               ;; if nothing in queue, return
        (let [tx-state* (assoc tx-state :flakes all-flakes)
              queue-ch  (async/chan parallelism)
              result-ch (async/chan parallelism)
              af        (fn [f res-chan]
                          (async/go
                            (let [fn-result (try (f tx-state*)
                                                 (catch Exception e e))]
                              (async/>! res-chan
                                  (if (channel? fn-result)
                                    (async/<! fn-result)
                                    fn-result))
                              (async/close! res-chan))))]

          ;; kicks off process to push queue onto queue-ch
          (async/onto-chan! queue-ch queue)

          ;; start executing functions, pushing results to result-ch. result-ch will close once queue-ch closes
          (async/pipeline-async parallelism result-ch af queue-ch)

          ;; read results, for now we collection all errors
          (loop [errors []]
            (let [next-res (async/<! result-ch)]
              (cond
                ;; no more functions, complete - queue-ch closed as queue was exhausted
                (nil? next-res)
                (->> errors
                     (map #(let [ex (ex-data %)]
                             (when (= 500 (:status ex))
                               (log/error % "Unexpected validation error in transaction! Flakes:" all-flakes))
                             (assoc ex :message (ex-message %))))
                     (not-empty))

                (util/exception? next-res)
                (recur (conj errors next-res))

                ;; anything else, all good - keep going
                :else (recur errors)))))))))
