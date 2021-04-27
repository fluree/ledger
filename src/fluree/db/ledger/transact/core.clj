(ns fluree.db.ledger.transact.core
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [clojure.core.async :as async]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.json :as json]
            [fluree.db.spec :as fspec]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.util.tx :as tx-util]
            [fluree.db.ledger.transact.retract :as tx-retract]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.ledger.transact.tags :as tags]
            [fluree.db.ledger.transact.txfunction :as txfunction]
            [fluree.db.ledger.transact.auth :as tx-auth]
            [fluree.db.ledger.transact.tx-meta :as tx-meta]
            [fluree.db.ledger.transact.validation :as tx-validate]
            [fluree.db.ledger.transact.error :as tx-error]
            [fluree.db.ledger.transact.schema :as tx-schema]
            [clojure.string :as str])
  (:import (fluree.db.flake Flake)))


(def ^:const parallelism
  "Processes this many transaction items in parallel."
  8)

(defn register-validate-fn
  [f {:keys [validate-fn] :as tx-state}]
  (swap! validate-fn update :queue conj f))

(defn- txi?
  "Returns true if a transaction item - must be a map and have _id as one of the keys"
  [x]
  (and (map? x) (contains? x "_id")))

(defn- txi-list?
  "Returns true if a sequential? list of txis"
  [x]
  (and (sequential? x)
       (every? txi? x)))

(defn- resolve-nested-txi
  "Takes a predicate-value from a transaction item (what will be the Flakes' .-o value),
  and if that value contains children/nested transaction item(s), returns a two-tuple of
  [tempid(s) nested-txi(s)-with-updated-tempids], else returns nil if no nested txi.

   Tempids need to be validated and generated here, because if they are not unique tempids
   we must make them unique so they point to the correct subjects once flattened."
  [predicate-value tx-state]
  (cond (txi-list? predicate-value)
        (let [txis (map #(assoc % "_id" (tempid/new (get % "_id") tx-state)) predicate-value)]
          [(map #(get % "_id") txis) txis])

        (txi? predicate-value)
        (let [tempid (tempid/new (get predicate-value "_id") tx-state)]
          [tempid (assoc predicate-value "_id" tempid)])))


(defn resolve-ident-strict
  "Resolves ident (from cache if exists). Will throw exception if ident cannot be resolved."
  [ident {:keys [db-root idents] :as tx-state}]
  (go-try
    (if-let [cached (get @idents ident)]
      cached
      (let [resolved (<? (dbproto/-subid db-root ident false))]
        (if (nil? resolved)
          (throw (ex-info (str "Invalid identity, does not exist: " (pr-str ident))
                          {:status 400 :error :db/invalid-tx}))
          (do
            (swap! idents assoc ident resolved)
            resolved))))))


(defn- resolve-collection-name
  "Resolves collection name from _id"
  [_id {:keys [db-root] :as tx-state}]
  (cond (tempid/TempId? _id)
        (:collection _id)

        (neg-int? _id)
        "_tx"

        (int? _id)
        (->> (flake/sid->cid _id)
             (dbproto/-c-prop db-root :name))))


(defn predicate-details
  "Returns function for predicate to retrieve any predicate details"
  [predicate collection db]
  (let [full-name (if (str/includes? predicate "/")
                    predicate
                    (str collection "/" predicate))]
    (if-let [pred-id (get-in db [:schema :pred full-name :id])]
      (fn [property] (dbproto/-p-prop db property pred-id))
      (throw (throw (ex-info (str "Predicate does not exist: " predicate)
                             {:status 400 :error :db/invalid-tx}))))))


(defn conform-object-value
  "Attempts to coerce any value to internal form."
  [object type]
  ;; note type :ref and :tag are not sent to this function, so
  (case type
    :string (if (string? object)
              object
              (fspec/type-check object type))
    :json (try (json/stringify object)
               (catch Exception _
                 (throw (ex-info (str "Unable to serialize JSON from value: " (pr-str object))
                                 {:status 400
                                  :error  :db/invalid-tx}))))

    :ref object                                             ;; will have already been conformed
    :tag object                                             ;; will have already been conformed
    ;; else
    ;; TODO - type-check creates an atom to hold errors, we really just need to throw exception if error exists
    (fspec/type-check object type)))


(defn register-unique!
  "Registers unique value in the tx-state and return true if successful.
  Will be unsuccessful if the ident already exists in the unique value cache
  and return false.

  Ident is a two-tuple of [pred-id object/value]"
  [ident {:keys [uniques] :as tx-state}]
  ;; uniques is a set/#{} wrapped in an atom
  (let [uniques* (swap! uniques
                        (fn [uniques-set]
                          (if (contains? uniques-set ident)
                            (conj uniques-set ::duplicate-detected) ;; check for this special keyword in result
                            (conj uniques-set ident))))]
    (if (contains? uniques* ::duplicate-detected)
      false                                                 ;; already registered, should throw downstream
      true)))


(defn resolve-unique
  "If predicate is unique, need to determine if any matching values already exist both
  in existing ledger, but also within the transaction.

  If they exist in the existing ledger, but upsert? is true, we can resolve the
  tempid to subject id (but only if a different upsert? predicate didn't already
  resolve it to a different subject!)

  If error is not thrown, returns the provided object argument."
  [object _id pred-info {:keys [db-before] :as tx-state}]
  (go-try
    (let [pred-id        (pred-info :id)
          existing-flake (when-not (tempid/TempId? object)
                           (-> (query-range/index-range db-before :post = [pred-id object])
                               <?
                               first))]
      (when (false? (register-unique! [pred-id object] tx-state))
        (throw (ex-info (str "Unique predicate " (pred-info :name) " was used more than once "
                             "in the transaction with the value of: " object ".")
                        {:status 400 :error :db/invalid-tx})))
      (if (tempid/TempId? object)
        (tx-validate/queue-check-unique-tempid-still-unique object _id pred-info tx-state)
        (cond
          ;; no matching existing flake, move on
          (nil? existing-flake) nil

          ;; lookup subject matches subject, will end up ignoring insert downstream unless :retractDuplicates is true
          (= (.-s ^Flake existing-flake) _id) _id

          ;; found existing subject and tempid, so set tempid value (or throw if already set to different subject)
          (and (tempid/TempId? _id) (pred-info :upsert))
          (do
            (tempid/set _id (.-s ^Flake existing-flake) tx-state) ;; will throw if tempid was already set to a different subject
            (.-s ^Flake existing-flake))

          ;; tempid, but not upsert - throw
          (tempid/TempId? _id)
          (throw (ex-info (str "Unique predicate " (pred-info :name) " with value: "
                               object " matched an existing subject: " (.-s ^Flake existing-flake) ".")
                          {:status 400 :error :db/invalid-tx :tempid _id}))

          ;; not a tempid, but subjects don't match
          ;; this can be OK assuming a different txi is retracting the existing flake
          ;; register a validating fn for post-processing to check this and throw if not the case
          (not= (.-s ^Flake existing-flake) _id)
          (do
            (tx-validate/queue-check-unique-match-retracted existing-flake _id pred-info object tx-state)
            _id))))))


(defn resolve-object-item
  "Resolves object into its final state so can be used for consistent comparisons with existing data."
  [tx-state {:keys [pred-info id o] :as smt}]
  (go-try
    (let [type (pred-info :type)
          o*   (if (txfunction/tx-fn? o)                    ;; should only happen for multi-cardinality objects
                 (<? (txfunction/execute o id pred-info tx-state))
                 o)
          o**  (cond
                 (nil? o*) nil

                 (= :ref type) (cond
                                 (tempid/TempId? o*) o*     ;; tempid, don't need to resolve yet
                                 (string? o*) (tempid/use o* tx-state)
                                 (int? o*) (<? (resolve-ident-strict o* tx-state))
                                 (util/pred-ident? o*) (<? (resolve-ident-strict o* tx-state)))

                 (= :tag type) (<? (tags/resolve o* pred-info tx-state))

                 :else (conform-object-value o* type))]
      (when (and (pred-info :unique) (not (nil? o**)))
        (<? (resolve-unique o** id pred-info tx-state)))
      (cond-> (assoc smt :o o**)
              (nil? o**) (assoc :action :retract)
              (tempid/TempId? o**) (assoc :o-tempid? true)))))


(defn add-singleton-flake
  "Adds new-flake assuming not a duplicate. A retract-flake (if not nil)
  is a matching flake in the existing db (i.e. a single-cardinality
  flake must retract an existing single-cardinality value if it already
  exists).

  Performs some logic to determine if the new flake should get added at
  all (i.e. if retract-flake is identical to the new flake)."
  [^Flake new-flake ^Flake retract-flake pred-info]
  (cond
    (nil? retract-flake)
    [new-flake]

    (= (.-o new-flake) (.-o retract-flake))                 ;; flakes are identical - if :retractDuplicates then include
    (if (pred-info :retractDuplicates)
      [new-flake retract-flake]
      [])

    :else
    [new-flake retract-flake]))


(defn resolve-action
  "Returns one of 3 possible action types based one _action and if the
  k-v pairs of the JSON transaction are empty:
  - :add - adding transaction items (which can be over-ridden by a nil value of a key pair)
  - :retract - retracting transaction items (all k-v pairs will attempt deletes)
  - :retract-subject - retract all values for a given subject."
  [_action empty-kv? _id _id-type]
  (if (= :delete (keyword _action))
    (do
      (when (= :tempid _id-type)
        (throw (ex-info (str "Deletions with a tempid are not allowed: " _id)
                        {:status 400 :error :db/invalid-transaction})))
      (if empty-kv?
        :retract-subject
        :retract))
    :add))


(defn id-type
  "Returns id-type as either:
  - tempid
  - pred-ident (i.e. [_user/username 'janedoe']
  - sid (long integer)

  Throws if none of thse valid types."
  [_id]
  (cond
    (tempid/TempId? _id) :tempid
    (util/pred-ident? _id) :pred-ident
    (int? _id) :sid
    :else (throw (ex-info (str "Invalid _id: " _id)
                          {:status 400 :error :db/invalid-transaction}))))


(defn generate-statements
  [{:keys [db-before] :as tx-state} txi]
  (let [_id            (get txi "_id")
        _action        (get txi "_action")
        _meta          (get txi "_meta")
        _p-o-pairs     (dissoc txi "_id" "_action" "_meta")
        _id-type       (id-type _id)
        _id*           (if (= :pred-ident _id-type)
                         (<?? (resolve-ident-strict _id tx-state))
                         _id)
        action         (resolve-action _action (empty? _p-o-pairs) _id _id-type)
        collection     (resolve-collection-name _id* tx-state)
        base-statement {:iri        nil
                        :id         _id*
                        :tempid?    (= :tempid _id-type)
                        :action     action
                        :collection collection
                        :o-tempid?  nil}]
    (if (= :retract-subject action)
      [base-statement]                                      ;; no k-v pairs to iterate over
      (reduce-kv (fn [acc pred obj]
                   (let [pred-info (predicate-details pred collection db-before)
                         smt       (assoc base-statement :pred-info pred-info
                                                         :p (pred-info :id)
                                                         :o obj)]
                     ;; for multi-cardinality, create a statement for each object
                     (if (pred-info :multi)
                       (reduce #(conj %1 (assoc smt :o %2)) acc (if (sequential? obj) (into #{} obj) [obj]))
                       (conj acc smt))))
                 [] _p-o-pairs))))


(defn- extract-children*
  "Takes a single transaction item (txi) and returns a two-tuple of
  [updated-txi nested-txi-list] if nested (children) transactions are found.
  If none found, will return [txi nil] where txi will be unaltered."
  [txi tx-state]
  (let [txi+tempid (if (util/temp-ident? (get txi "_id"))
                     (assoc txi "_id" (tempid/new (get txi "_id") tx-state))
                     txi)]
    (reduce-kv
      (fn [acc k v]
        (cond
          (string? v)
          (if (dbfunctions/tx-fn? v)
            (let [[txi+tempid* found-txis] acc]
              [(assoc txi+tempid* k (txfunction/->TxFunction v)) found-txis])
            acc)

          (or (txi-list? v) (txi? v))
          (let [[nested-ids nested-txis] (resolve-nested-txi v tx-state)
                [txi+tempid* found-txis] acc
                found-txis* (if (sequential? nested-txis)
                              (concat found-txis nested-txis)
                              (conj found-txis nested-txis))]
            [(assoc txi+tempid* k nested-ids) found-txis*])

          :else acc))
      [txi+tempid nil] txi+tempid)))


(defn extract-children
  "From original txi, returns list of all txis included nested ones. If no nested txis are found,
  will just return original txi in a list. When nested txis are found, original txi will be flattened
  by updating the nested txis with their respective tempids. Will recursively check all nested txis for
  children."
  [txi tx-state]
  (let [[updated-txi found-txis] (extract-children* txi tx-state)]
    (if found-txis
      ;; recur on children (nested transactions) for possibly additional children
      (let [found-nested (mapcat #(extract-children % tx-state) found-txis)]
        (conj found-nested updated-txi))
      [updated-txi])))


(defn ->tx-state
  [db t block-instant {:keys [auth auth-sid authority authority-sid tx-permissions txid cmd sig nonce type] :as tx-map}]
  (let [tx        (case (:type tx-map)                      ;; command type is either :tx or :new-db
                    :tx (:tx tx-map)
                    :new-db (tx-util/create-new-db-tx tx-map))
        format    (cond
                    (get-in tx [0 "_id"]) :json
                    (get-in tx [0 "@id"]) :json-ld)
        db-before (cond-> db
                          tx-permissions (assoc :permissions tx-permissions))]
    {:db-before        db-before
     :db-root          db
     :db-after         (atom nil)                           ;; store updated db here
     :flakes           nil                                  ;; holds final list of flakes for tx once complete
     :permissions      tx-permissions
     :auth-id          auth                                 ;; auth id string in _auth/id
     :auth             auth-sid                             ;; auth subject-id integer
     :authority-id     authority                            ;; authority id string as per _auth/id (or nil if no authority)
     :authority        authority-sid                        ;; authority subject-id integer (or nil if no authority)
     :t                t
     :instant          block-instant
     :txid             txid
     :tx-type          type
     :format           format
     :tx               tx
     :tx-string        cmd
     :signature        sig
     :nonce            nonce
     :fuel             (atom {:stack   []
                              :credits 1000000
                              :spent   0})
     ;; hold map of all tempids to their permanent ids. After initial processing will use this to fill
     ;; all tempids with the permanent ids.
     :tempids          (atom {})
     ;; hold same tempids as :tempids above, but stores them in insertion order to ensure when
     ;; assigning permanent ids, it will be done in a predicable order
     :tempids-ordered  (atom [])
     ;; idents (two-tuples of unique predicate + value) may be used multiple times in same tx
     ;; we keep the ones we've already resolved here as a cache
     :idents           (atom {})
     ;; if a tempid resolves to existing subject via :upsert predicate, set it here. tempids don't need
     ;; to check for existing duplicate values, but if a tempid resolves via upsert, we need to check it
     :upserts          (atom nil)                           ;; cache of resolved identities
     ;; Unique predicate + value used in transaction kept here, to ensure the same unique is not used
     ;; multiple times within the transaction
     :uniques          (atom #{})
     ;; If a predicate schema change removes an index (either by turning off index:true or unique:true)
     ;; then we capture the subject ids here and pass back in the transaction result for indexing
     :remove-from-post (atom nil)
     ;; we may generate new tags as part of the transaction. Holds those new tags, but also a cache
     ;; of tag lookups to speed transaction by avoiding full lookups of the same tag multiple times in same tx
     :tags             (atom nil)
     ;; Some predicates may require extra validation after initial processing, we register functions
     ;; here for that purpose, 'cache' holds cached functions that are ready to execute
     :validate-fn      (atom {:queue   (list) :cache {}
                              ;; need to track respective flakes for predicates (for tx-spec) and subject changes (collection-specs)
                              :tx-spec nil :c-spec nil})}))


(defn pipeline-aggregator
  "
  accumulator - a collection into which the final results will be placed into
                i.e. a vector, or sorted set.
  aggregate-type is either
  - :conj   - aggregates into accumulator with (conj accumulator x)
  - :concat - aggregates into accumulator with (into accumulator x)"
  [af aggregate-type accumulator statements]
  (async/go
    (let [queue-ch     (async/to-chan! statements)
          result-ch    (async/chan parallelism)
          aggregate-fn (cond
                         (= :conj aggregate-type) conj
                         (= :concat aggregate-type) into
                         :else (throw (ex-info (str "Unexpected Error: Invalid aggregator type: " aggregate-type
                                                    " in pipeline-aggregator.")
                                               {:status 500 :error :db/unexpected-error})))]

      (async/pipeline-async parallelism result-ch af queue-ch)

      (loop [acc accumulator]
        (let [next-res (async/<! result-ch)]
          (cond
            ;; no more functions, complete - queue-ch closed as queue was exhausted
            (nil? next-res)
            acc

            ;; exception, close channels and return exception
            (util/exception? next-res)
            (do (async/close! queue-ch)
                (async/close! result-ch)
                next-res)

            ;; anything else, all good - keep going
            :else (recur (aggregate-fn acc next-res))))))))


(defn finalize-flakes
  [{:keys [tempids upserts t] :as tx-state}
   {:keys [id action pred-info p o o-tempid? tempid? collection] :as statement}]
  (go-try
    (cond
      (= :retract-subject action)
      (<? (tx-retract/subject id tx-state))

      (= :retract action)
      (<? (tx-retract/flake id p o tx-state))

      ;; (= :add action) below
      :else
      (let [s             (if tempid? (get @tempids id) id)
            o*            (if o-tempid? (get @tempids o) o) ;; object may be a tempid, if so resolve to permanent id
            new-flake     (flake/->Flake s p o* t true nil)
            ;; retractions do not need to be checked for tempids (except when tempid resolved via an :upsert true)
            retract-flake (when (or (not tempid?) (contains? @upserts s))
                            (first (<? (tx-retract/flake s p (when (pred-info :multi) o*) tx-state)))) ;; for multi-cardinality, only retract exact matches
            flakes        (add-singleton-flake new-flake retract-flake pred-info)]

        (when (not-empty flakes)
          (tx-validate/check-collection-specs collection tx-state flakes)
          (when (pred-info :spec)
            (tx-validate/queue-pred-spec new-flake pred-info tx-state))
          (when (pred-info :txSpec)
            (tx-validate/queue-predicate-tx-spec flakes pred-info tx-state)))

        flakes))))


(defn resolve-statement-af
  [tx-state]
  (fn [statement res-ch]
    (async/go
      (if (= :retract-subject (:action statement))          ;; skip retract-subject actions, no object to resolve
        (async/put! res-ch statement)
        (->> (resolve-object-item tx-state statement)
             async/<!
             (async/put! res-ch)))
      (async/close! res-ch))))


(defn finalize-flakes-af
  [tx-state]
  (fn [statement res-ch]
    (async/go
      (->> (finalize-flakes tx-state statement)
           async/<!
           (async/put! res-ch))
      (async/close! res-ch))))


(defn do-transact
  [tx-state tx]
  (go-try
    (let [flakes-set (flake/sorted-set-by flake/cmp-flakes-block)]
      (->> tx
           (mapcat #(extract-children % tx-state))
           (mapcat (partial generate-statements tx-state))
           (pipeline-aggregator (resolve-statement-af tx-state) :conj []) ;; conforms all object (.-o) values, resolves :uniques/:uperts to final _id
           <?
           (tempid/assign-subject-ids tx-state)
           (pipeline-aggregator (finalize-flakes-af tx-state) :concat flakes-set)
           <?))))


(defn update-db-after
  "Updates db-after into tx-state"
  [db-after tx-state]
  (reset! (:db-after tx-state) db-after)
  db-after)


(defn build-transaction
  [tx-state]
  (go-try
    (let [{:keys [db-before auth-id authority-id txid tx t tx-type fuel permissions]} tx-state
          tx-flakes        (<? (do-transact tx-state tx))
          tx-meta-flakes   (tx-meta/tx-meta-flakes tx-state nil)
          tempids-map      (tempid/result-map tx-state)
          all-flakes       (cond-> (into tx-flakes tx-meta-flakes)
                                   (not-empty tempids-map) (conj (tempid/flake tempids-map t))
                                   @(:tags tx-state) (into (tags/create-flakes tx-state)))

          ;; kick off hash process in the background, it can take a while
          hash-flake       (future (tx-meta/generate-hash-flake all-flakes tx-state))
          fast-forward-db? (:tt-id db-before)
          ;; final db that can be used for any final testing/spec validation
          db-after         (-> (if fast-forward-db?
                                 (<? (dbproto/-forward-time-travel db-before all-flakes))
                                 (<? (dbproto/-with-t db-before all-flakes)))
                               dbproto/-rootdb
                               tx-util/make-candidate-db
                               (tx-meta/add-tx-hash-flake @hash-flake)
                               (update-db-after tx-state))
          tx-bytes         (- (get-in db-after [:stats :size]) (get-in db-before [:stats :size]))

          ;; kick off permissions, returns async channel so allow to process in the background
          ;; spec error reporting (next line) will take precedence over permission errors

          ;; runs all 'spec' validations
          spec-errors      (<? (tx-validate/run-queued-specs all-flakes tx-state parallelism))
          perm-errors      (when (and (nil? spec-errors)    ;; only run permissions errors if no spec errors
                                      (not (true? (:root? permissions))))
                             (<? (tx-validate/run-permissions-checks all-flakes tx-state parallelism)))]

      (cond-> {:txid         txid
               :t            t
               :auth         auth-id
               :authority    authority-id
               :db-before    db-before
               :db-after     db-after                       ;; will get replaced if there is an error
               :status       200                            ;; will get replaced if there is an error
               :errors       nil                            ;; will get replaced if there is an error
               :flakes       (conj all-flakes @hash-flake)  ;; will get replaced if there is an error
               :hash         (.-o ^Flake @hash-flake)       ;; will get replaced if there is an error
               :tempids      tempids-map                    ;; will get replaced if there is an error
               :bytes        tx-bytes                       ;; will get replaced if there is an error
               :remove-preds (tx-schema/remove-from-post-result tx-state) ;; will get replaced if there is an error
               :fuel         nil                            ;; additional fuel will be consumed while processing validations, fill in at end
               :type         tx-type}

              ;; replace response with error response if errors detected
              spec-errors
              (-> (tx-error/spec-error spec-errors tx-state) <?)

              ;; only care about permission errors if no spec errors exist
              perm-errors
              (-> (tx-error/spec-error perm-errors tx-state) <?)

              ;; add fuel at end
              true
              (#(assoc % :fuel (+ (:spent @fuel) tx-bytes (count all-flakes) 1)))))))


(defn transact
  [db cmd-data t block-instant]
  (async/go
    (try
      (let [tx-map   (try (tx-util/validate-command (:command cmd-data))
                          (catch Exception e
                            (log/error e "Unexpected error parsing command: " (pr-str cmd-data))
                            (throw (ex-info "Unexpected error parsing command."
                                            {:status   500
                                             :error    :db/command-parse-exception
                                             :cmd-data cmd-data}
                                            e))))
            _        (when (not-empty (:deps tx-map))       ;; transaction has dependencies listed, verify they are satisfied
                       (<? (tx-validate/tx-deps-check db tx-map)))
            tx-map*  (<? (tx-auth/add-auth-ids-permissions db tx-map))
            tx-state (->tx-state db t block-instant tx-map*)
            result   (async/<! (build-transaction tx-state))]
        (if (util/exception? result)
          (<? (tx-error/handler result tx-state))
          result))
      (catch Exception e
        (async/<! (tx-error/pre-processing-handler e db cmd-data t))))))
