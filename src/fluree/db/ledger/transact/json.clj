(ns fluree.db.ledger.transact.json
  (:require [fluree.db.ledger.transact.subject :refer :all]
            [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [clojure.string :as str]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.json :as json]
            [fluree.db.spec :as fspec]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.session :as session]
            [fluree.db.ledger.bootstrap :as bootstrap])
  (:import (fluree.db.flake Flake)))

;; TODO - add ^:const
(def parallelism 10)

(defrecord SubjectJSON [json subject tempid? action flakes]
  ITxSubject
  (-flakes [_])
  (-iri [_] :none)
  (-flatten [_] nil))


(defn new-subject
  [subject tx-state db]
  (map->SubjectJSON {:subject  subject
                     :tx-state tx-state
                     :db       db
                     :cache    (atom nil)}))

(defrecord TempId [user-string collection unique])

(defn TempId?
  [x]
  (instance? TempId x))

(defn to-tempid*
  [tempid]
  (let [[collection id] (str/split tempid #"[^\._a-zA-Z0-9]" 2)
        key (if id
              (keyword collection id)
              (keyword collection (str (util/random-uuid))))]
    (->TempId tempid collection key)))

(defn to-tempid
  "Generates a new tempid record from tempid string. Registers tempid in :tempids atom within
  tx-state to track all tempids in tx, and also their final resolution value.

  defrecord equality will consider tempids with identical values the same, even if constructed separately.
  We therefore construct a tempid regardless if it has already been created, but are careful not to
  update any existing subject id that might have already been mapped to the tempid."
  [tempid {:keys [tempids] :as tx-state}]
  (let [tempid* (to-tempid* tempid)]
    (swap! tempids update tempid* identity)                 ;; don't touch any set value, otherwise nil
    tempid*))

(defn use-tempid
  "Returns a tempid that will be used for a Flake object value, but only returns it if
  it already exists. If it does not exist, it means it is a tempid used as a value, but it was never used
  as a subject."
  [tempid {:keys [tempids] :as tx-state}]
  (let [tempid* (to-tempid* tempid)]
    (if (contains? @tempids tempid*)
      tempid*
      (throw (ex-info (str "Tempid " tempid " used as a value, but there is no corresponding subject in the transaction")
                      {:status 400
                       :error  :db/invalid-tx})))))

(defn to-tag-tempid
  "Generates a _tag tempid"
  [tag {:keys [tempids] :as tx-state}]
  (let [tempid (->TempId tag "_tag" (keyword tag))]
    (swap! tempids update tempid identity)
    tempid))

(defn set-tempid
  "Sets a tempid value into the cache. If the tempid was already set by another :upsert
  predicate that matched a different subject, throws an error. Returns set subject-id on success."
  [tempid subject-id {:keys [tempids] :as tx-state}]
  (swap! tempids update tempid
         (fn [existing-sid]
           (cond
             (nil? existing-sid) subject-id                 ;; hasn't been set yet, success.
             (= existing-sid subject-id) subject-id         ;; resolved, but to the same id - ok
             :else (throw (ex-info (str "Temp-id " (:user-string tempid)
                                        " matches two (or more) subjects based on predicate upserts: "
                                        existing-sid " and " subject-id ".")
                                   {:status 400
                                    :error  :db/invalid-tx})))))
  subject-id)


;; TODO - can probably parse function string to final 'lisp form' when generating TxFunction
(defrecord TxFunction [fn-str])

(defn tx-fn?
  "Returns true if a transaction function"
  [x]
  (instance? TxFunction x))


(defn- txi?
  "Returns true if a transaction item - must be a map and have _id as one of the keys"
  [x]
  (and (map? x) (contains? x :_id)))

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
        (let [txis (map #(assoc % :_id (to-tempid (:_id %) tx-state)) predicate-value)]
          [(map :_id txis) txis])

        (txi? predicate-value)
        (let [tempid (to-tempid (:_id predicate-value) tx-state)]
          [tempid (assoc predicate-value :_id tempid)])

        :else nil))


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
  [{:keys [_id] :as txi} {:keys [db] :as tx-state}]
  (assoc txi :_collection
             (cond (TempId? _id)
                   (:collection _id)

                   (neg-int? _id)
                   "_tx"

                   (int? _id)
                   (->> (flake/sid->cid _id)
                        (dbproto/-c-prop db :name)))))

(defn- resolve-collection-id
  "Resolves collection id from collection name"
  [db _collection]
  (dbproto/-c-prop db :id _collection))

(defn resolve-action
  "Returns top level action as keyword for subject based on user-supplied action (if available),
  if the subject was empty (no predicate-object pairs provided), or if the subject is using a tempid"
  [action empty-subject? tempid?]
  (let [raise (fn [action]
                (ex-info (str "Invalid _action: " (pr-str action)) {}))]
    (cond
      (string? action) (or (-> (keyword action)
                               #{:delete :insert :update})
                           (throw (raise action)))
      (nil? action) (cond tempid? :insert
                          empty-subject? :delete
                          :else :update)
      :else (throw (raise action)))))


(defn register-flakes
  "Registers a transaction's flakes that are ready for final permission/smartfunction validation"
  [{:keys [flakes] :as tx-state} registration-flakes]
  (swap! flakes into registration-flakes))

(defn register-flake
  "Registers a transaction's flake that are ready for final permission/smartfunction validation"
  [{:keys [flakes] :as tx-state} registration-flake]
  (swap! flakes conj registration-flake))

(defn register-temp-flake
  "Registers a transaction's flake that are ready for final permission/smartfunction validation"
  [{:keys [temp-flakes] :as tx-state} registration-flake]
  (swap! temp-flakes conj registration-flake))


(defn retract-subject
  "Returns retraction flakes for an entire subject. Also returns retraction
  flakes for any refs to that subject."
  [subject-id {:keys [db t] :as tx-state}]
  (go-try
    (let [flakes (query-range/index-range db :spot = [subject-id])
          refs   (query-range/index-range db :opst = [subject-id])]
      (->> (<? flakes)
           (concat (<? refs))
           (map #(flake/flip-flake % t))
           (into [])))))


(defn retract-flake
  "Retracts one or more flakes given a subject, predicate, and optionally an object value."
  [subject-id predicate-id object {:keys [db t] :as tx-state}]
  (go-try
    (let [flakes (query-range/index-range db :spot = [subject-id predicate-id object])]
      (->> (<? flakes)
           (map #(flake/flip-flake % t))))))


;; TODO - below, instead of async/into,could use a transducer to return a single clean channel that concats, and not need to use go-try here
(defn retract-multi
  "Like retract flake, but takes a list of objects that must be retracted"
  [subject-id predicate-id objects tx-state]
  (go-try
    (->> objects
         (map #(retract-flake subject-id predicate-id % tx-state))
         async/merge
         (async/into [])
         <?
         (apply concat))))


(defn predicate-details
  "Returns function for predicate to retrieve any predicate details"
  [predicate collection db]
  (let [full-name (if-let [a-ns (namespace predicate)]
                    (str a-ns "/" (name predicate))
                    (str collection "/" (name predicate)))]
    (if-let [pred-id (get-in db [:schema :pred full-name :id])]
      (fn [property] (dbproto/-p-prop db property pred-id))
      (throw (throw (ex-info (str "Predicate does not exist: " predicate)
                             {:status 400 :error :db/invalid-tx}))))))

(defn conform-object-value
  "Attempts to coerce any value to internal form."
  [object type]
  (log/info "conform-object-value:" type object)
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
    (fspec/type-check object type)                          ;; TODO - type-check creates an atom to hold errors, we really just need to throw exception if error exists
    ))

(defn execute-tx-fn
  "Returns a core async channel with response"
  [tx-fn _id pred-info {:keys [auth db instant fuel] :as tx-state}]
  (dbfunctions/execute-tx-fn db auth nil _id (pred-info :id) (:fn-str tx-fn) fuel instant))


(defn new-tag
  [tag-name tx-state]
  (to-tempid tag-name tx-state))


(defn generate-new-tag
  [tag {:keys [tags db] :as tx-state}]
  (to-tempid tag tx-state))

(defn resolve-tag
  "Returns the subject id of the tag if it exists, or a tempid for a new tag."
  [tag pred-info {:keys [tags db] :as tx-state}]
  (go-try
    (let [pred-name (or (pred-info :name)
                        (throw (ex-info (str "Trying to resolve predicate name for tag resolution but name is unknown for pred: " (pred-info :id))
                                        {:status 400
                                         :error  :db/invalid-tx
                                         :tags   tag})))
          tag-name  (if (.contains tag "/") tag (str pred-name ":" tag))
          resolved  (-> (get @tags tag) (or (when-let [tag-sid (<? (dbproto/-tag-id db tag-name))]
                                              (swap! tags assoc tag-name tag-sid)
                                              tag-sid)))]

      (cond
        ;; don't generate new tags for :restrictTag
        (and (pred-info :restrictTag)
             (or (nil? resolved) (TempId? resolved)))
        (throw (ex-info (str tag " is not a valid tag. The restrictTag property for: " pred-name " is true. Therefore, a tag with the id " pred-name ":" tag " must already exist.")
                        {:status 400
                         :error  :db/invalid-tx
                         :tags   tag}))

        (nil? resolved) (to-tag-tempid tag-name tx-state)

        :else resolved))))

(defn register-unique
  "Registers unique value in the tx-state and return true if successful.
  Will be unsuccessful if the ident already exists in the unique value cache
  and return false.

  Ident is a two-tuple of [pred-id object/value]"
  [ident {:keys [uniques] :as tx-state}]
  (if (contains? @uniques ident)
    false                                                   ;; already registered, should throw downstream
    (do (swap! uniques conj ident)
        true)))

(defn resolve-unique
  "If predicate is unique, need to determine if any matching values already exist both
  in existing ledger, but also within the transaction.

  If they exist in the existing ledger, but upsert? is true, we can resolve the
  tempid to subject id (but only if a different upsert? predicate didn't already
  resolve it to a different subject!)

  If error is not thrown, returns the provided object argument."
  [object-ch _id pred-info {:keys [db] :as tx-state}]
  (go-try
    (log/warn "IN UNIQE!")
    (let [object       (<? object-ch)
          pred-id      (pred-info :id)
          ;; create a two-tuple of [object-value async-chan-with-found-subjects]
          ;; to iterate over. Kicks off queries (if multi) in parallel.
          obj+subj-chs (->> (if (pred-info :multi) object [object])
                            (map #(vector % (query-range/index-range db :post = [pred-id %]))))]
      (loop [[[obj subject-ch] & r] obj+subj-chs]
        (if-not obj
          ;; finished, return original object - no errors
          object
          ;; check out next ident
          (let [subject-id (some-> (<? subject-ch)
                                   ^Flake (first)
                                   (.-s))]
            (when (false? (register-unique [pred-id obj] tx-state))
              (throw (ex-info (str "Unique predicate " (pred-info :name) " was used more than once "
                                   "in the transaction with the value of: " object ".")
                              {:status 400 :error :db/invalid-tx})))
            ;; either nil subject-id, or doesn't match _id ... need to see if upsertable
            (when (not= subject-id _id)
              (if (and (pred-info :upsert) (TempId? _id))
                (set-tempid _id subject-id tx-state)        ;; will throw if tempid was already set to a different subject
                ;; not upsertable, throw/exit
                (throw (ex-info (str "Unique predicate " (pred-info :name) " with value: "
                                     object " matched an existing subject: " subject-id ".")
                                {:status 400 :error :db/invalid-tx}))))
            (recur r)))))))

(defn resolve-object-item
  "Resolves object into its final state so can be used for consistent comparisons with existing data."
  [object _id pred-info tx-state]
  (go-try
    (let [type    (pred-info :type)
          object* (if (tx-fn? object)                       ;; should only happen for multi-cardinality objects
                    (<? (execute-tx-fn object _id pred-info tx-state))
                    object)]
      (cond
        (nil? object*) (throw (ex-info (str "Multi-cardinality values cannot be null/nil: " (pred-info :name))
                                       {:status 400 :error :db/invalid-tx}))

        (= :ref type) (cond
                        (TempId? object*) object*
                        (string? object*) (use-tempid object* tx-state)
                        (util/pred-ident? object*) (<? (resolve-ident-strict object* tx-state)))

        (= :tag type) (<? (resolve-tag object* pred-info tx-state))

        :else (conform-object-value object* type)))))

(defn resolve-object
  "Resolves object into its final state so can be used for consistent comparisons with existing data."
  [object _id pred-info tx-state]
  (let [multi? (pred-info :multi)]
    (cond-> object
            (nil? object) (async/go ::delete)
            (tx-fn? object) (execute-tx-fn _id pred-info tx-state)
            (not multi?) (resolve-object-item _id pred-info tx-state)
            multi? (->> (mapv #(resolve-object-item % _id pred-info tx-state))
                        async/merge
                        (async/into [])))))

(defn generate-statements
  [{:keys [_id _action _collection] :as txi} {:keys [db t] :as tx-state}]
  (go-try
    (let [_p-o-pairs (dissoc txi :_id :_action :_meta :_collection)
          tempid?    (TempId? _id)
          action     (resolve-action _action (empty? _p-o-pairs) tempid?)]
      (log/info "action:" action)
      (if (and (= :delete action) (empty? _p-o-pairs))
        [(<? (retract-subject _id tx-state)) nil]
        (doseq [[pred obj] _p-o-pairs]
          (let [pred-info (predicate-details pred _collection db)
                pid       (pred-info :id)
                obj*      (<? (resolve-object obj _id pred-info tx-state))]
            (log/info "obj*:" pred (pr-str obj*))
            (cond
              ;; delete should have no tempids, so can register the final flakes in tx-state
              (= :delete action) (register-flakes tx-state
                                                  (if (pred-info :multi)
                                                    (<? (retract-multi _id pid obj* tx-state))
                                                    (<? (retract-flake _id pid obj* tx-state))))
              (= ::delete obj*) (register-flakes tx-state
                                                 (<? (retract-flake _id pid nil tx-state)))

              ;; multi could have a tempid as one of the values, need to look at each independently
              (pred-info :multi) (doseq [o obj*]
                                   (if (or tempid? (TempId? o))
                                     (register-temp-flake tx-state (flake/->Flake _id pid obj t true nil))
                                     (register-flake tx-state (flake/->Flake _id pid obj t true nil))))

              :else (let [new-flake (flake/->Flake _id pid obj t true nil)]
                      (if (or tempid? (TempId? obj*))
                        (register-temp-flake tx-state new-flake)
                        (do                                 ;; need to add flake and retract existing flake
                          (register-flake tx-state new-flake)
                          (->> (<? (retract-flake _id pid nil tx-state))
                               (register-flakes tx-state))))))))))))


(defn assign-tempids
  [txi tx-state]
  (if (util/temp-ident? (:_id txi))
    (assoc txi :_id (to-tempid (:_id txi) tx-state))
    txi))

(defn resolve-subject
  [{:keys [_id] :as txi} tx-state]
  (go-try
    (if (util/pred-ident? _id)
      (assoc txi :_id
                 (<? (resolve-ident-strict _id tx-state)))
      txi)))


(defn- extract-children*
  "Takes a single transaction item (txi) and returns a two-tuple of
  [updated-txi nested-txi-list] if nested (children) transactions are found.
  If none found, will return [txi nil] where txi will be unaltered."
  [txi tx-state]
  (let [txi+tempid (if (util/temp-ident? (:_id txi))
                     (assoc txi :_id (to-tempid (:_id txi) tx-state))
                     txi)]
    (reduce-kv
      (fn [acc k v]
        (cond
          (string? v)
          (if (dbfunctions/tx-fn? v)
            (let [[txi+tempid* found-txis] acc]
              [(assoc txi+tempid* k (->TxFunction v)) found-txis])
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
  [db-before t auth block-instant]
  {:db          db-before
   :db-root     (dbproto/-rootdb db-before)
   :auth        auth
   :t           t
   :instant     block-instant
   :tempids     (atom {})
   :fuel        (atom {:stack   []
                       :credits 1000000
                       :spent   0})
   :tags        (atom {:tag->sid {}
                       :new      []})
   :idents      (atom {})                                   ;; cache of resolved identities
   :uniques     (atom #{})                                  ;; holds all unique predicate values found in tx to validate they are not duplicated
   :flakes      (atom (flake/sorted-set-by flake/cmp-flakes-spot-novelty)) ;; final flakes
   :temp-flakes (atom #{})
   })


(defn tempids->permanent-ids
  [{:keys [db]}]


  )

(defn process-txi
  [txi tx-state]
  (go-try
    (-> txi
        (assign-tempids tx-state)
        (#(do (println "tempids: " %) %))
        (resolve-subject tx-state)
        (<?)
        (#(do (println "resolve-subject: " %) %))
        (resolve-collection-name tx-state)
        (#(do (println "resolve-collection-name: " %) %))
        (generate-statements tx-state)
        (<?)
        (tempids->permanent-ids)

        )))

(defn do-transact
  [db-before tx-map tx-permissions auth_id t block-instant]
  (go-try
    (let [{:keys [tx-meta rest-tx sig txid authority auth]} tx-map
          tx-state (->tx-state db-before t auth block-instant) ;; holds state data for entire transaction
          ]
      (->> rest-tx
           (mapcat #(extract-children % tx-state))
           ;; TODO - place into async pipeline
           (map #(process-txi % tx-state))
           )

      ))

  )



(comment

  (def mydb (bootstrap/testing-db (:conn user/system) "test/db1"))
  (def mydb2 (dbproto/-with (async/<!! mydb)
                            (-> (async/<!! mydb) :block inc)
                            [(flake/->Flake 2000)]
                            ))



  (-> mydb
      async/<!!)


  (def conn (:conn user/system))
  conn
  (def db (<?? (fluree.db.api/db conn "prefix/a")))



  (def tx-state (->tx-state db 42 123456 (java.time.Instant/now)))


  (-> (process-txi {:_id   ["movie/title" "Gran Torino"]
                    :title "Gran Torino2"}
                   tx-state)
      async/<!!)

  (-> (process-txi {:_id   "movie"
                    :title "BP Test"}
                   tx-state)
      async/<!!)

  (-> tx-state)



  (-> (do-transact db {:rest-tx sample-tx} nil nil 42 (java.time.Instant/now))
      async/<!!)



  (require 'clojure.core.async :refer :all)

  (def ca (async/chan 1))
  (def cb (async/chan 1))

  (pipeline
    4                                                       ; thread count, i prefer egyptian cotton
    cb                                                      ; to
    (filter even?)                                          ; transducer
    ca                                                      ; from
    )

  (doseq [i (range 10)]
    (async/go (clojure.core.async/>! ca i)))
  (async/go-loop []
    (println (async/<! cb))
    (recur))

  (async/close! ca)
  (async/close! cb)

  (inc 1)

  )

(comment

  (sequence (dedupe) [:a :b :b :c :c :c :d])

  (defn temp-async [v]
    (async/go
      (async/timeout 1000)
      (ex-info "MY ERROR" {})))

  (-> (go-try
        (cond-> 42
                true? (-> temp-async <?)))
      (async/<!!)
      (#(if (instance? Throwable %) :error %)))

  )


