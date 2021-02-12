(ns fluree.db.ledger.transact
  (:require [clojure.tools.logging :as log]
            [fluree.db.flake :as flake]
            [fluree.db.spec :as fspec]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.permissions-validate :as perm-validate]
            [fluree.db.permissions :as permissions]
            [fluree.db.util.json :as json]
            [fluree.db.util.core :as util]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.query.schema :as schema]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.query.range :as query-range]
            [fluree.db.dbproto :as dbproto]
            [fluree.crypto :as crypto]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.auth :as auth]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.session :as session]
            [fluree.db.util.tx :as tx-util]
            [fluree.db.query.fql :as fql]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try merge-into? channel?]]
            [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.merkle :as merkle])
  (:import (fluree.db.flake Flake)
           (java.time Instant)
           (java.util UUID)
           (java.net URI)))

(def collection-name-regex #"^[a-zA-Z0-9_][a-zA-Z0-9\.\-_]{0,254}$")
(def predicate-name-regex #"^[a-zA-Z0-9_][a-zA-Z0-9\.\-_]{0,254}/[a-zA-Z0-9][a-zA-Z0-9\.\-_]{0,254}$")

(def tag-id-regex #"^[^/]{1,255}/[^/]{1,255}$")

(defn url?
  "Returns true a java.net.URI is a URL."
  [^URI uri]
  (try
    (.toURL uri)
    true
    (catch Exception _ false)))

(defn- lowercase-URL
  "For a URL string, makes scheme and host parts lowercase"
  [url-string]
  (let [[_ scheme+host rest] (re-find #"^([^:]+://[^/]+)(.+)$" url-string)]
    (str (str/lower-case scheme+host) rest)))

(defn normalize-uri
  "Given a valid string URI, returns a normalized string.
  If not a valid URI, returns exception."
  [s]
  (let [uri        (-> (URI. s) .normalize)
        normalized (.toString uri)]
    (if (url? uri)
      (lowercase-URL normalized)                            ;; if a URL, we lowercase the scheme and host part
      normalized)))


(defn action->op
  [action]
  (case action
    :insert true
    :upsert true
    :update true
    :delete false
    ;; else
    (throw (ex-info (str "Invalid _action supplied: " (pr-str action) " supplied in transaction.")
                    {:status 400
                     :error  :db/invalid-tx}))))

(defn resolve-collection-id+name
  [db _id]
  (let [partition   (cond (int? _id)
                    (flake/sid->cid _id)

                    (vector? _id)
                    (let [predicate-name (first _id)
                          cname          (re-find #"^[a-zA-Z0-9_][a-zA-Z0-9\.\-_]{0,254}" predicate-name)]
                      (dbproto/-c-prop db :partition cname))

                    :else
                    (let [cname (re-find #"^[a-zA-Z0-9_][a-zA-Z0-9\.\-_]{0,254}" _id)]
                      (dbproto/-c-prop db :partition cname)))
        _     (when-not partition (throw (ex-info (str "Invalid _id, no corresponding collection exists: " (pr-str _id))
                                            {:status 400 :error :db/invalid-collection})))
        cname (dbproto/-c-prop db :name partition)]
    [partition cname]))

(defn- predicate-name-illegal-chars
  "Checks and throws if any illegal characters exist in the predicate name,
  returns original value if no error."
  [predicate-name]
  (if (or (str/includes? predicate-name "_Via_")
          (str/includes? predicate-name "__")
          (str/includes? predicate-name "/_"))
    (throw (ex-info (str "Predicate name defined with illegal characters: " predicate-name)
                    {:status 400 :error :db/invalid-predicate}))
    predicate-name))

(defn- validate-internal-predicate
  "Mostly here to throw an error, will return nil if all checks out."
  [collection-name pred-name value]
  (cond
    (= "_tag" collection-name)
    (do
      (when (and (= "_tag/id" pred-name) (not (re-matches tag-id-regex value)))
        (throw (ex-info (str "Invalid tag id. Must include a namespace with a single /, i.e: 'tagnamespace/tagname'. Provided: " value)
                        {:status 400
                         :error  :db/invalid-tag})))
      value)

    (= "_predicate" collection-name)
    (try
      (-> value
          normalize-uri
          predicate-name-illegal-chars)
      (catch Exception _
        (throw (ex-info (str "Invalid predicate name. Must be a valid URI and cannot contain '__', '/_', or '_Via_'. Provided: " value)
                        {:status 400 :error :db/invalid-predicate}))))

    (= "_collection" collection-name)
    (do
      (when (and (= "_collection/name" pred-name) (not (re-matches collection-name-regex value)))
        (throw (ex-info (str "Invalid collection name, must start with a-z, A-Z, or 0-9 and can also include .-_. Provided: " value)
                        {:status 400
                         :error  :db/invalid-collection})))
      value)

    :else value))

(defn- conform-sys-predicate
  "Validates, and then returns possibly modified values from the key-value map for system collections
  and predicates."
  [collection-name kv-map]
  (let [tag-id-key    #{:_tag/id :id const/$_tag:id}
        pred-name-key #{:_predicate/name :name const/$_predicate:name}
        coll-name-key #{:_collection/name :name const/$_collection:name}]

    ;; a map is provided, find relevant predicate if applicable.
    (cond
      (= "_tag" collection-name)
      (if-let [id-key (some tag-id-key (keys kv-map))]
        (->> (get kv-map id-key)
             (validate-internal-predicate "_tag" "_tag/id")
             (assoc kv-map id-key))
        kv-map)

      (= "_predicate" collection-name)
      (if-let [name-key (some pred-name-key (keys kv-map))]
        (->> (get kv-map name-key)
             (validate-internal-predicate "_predicate" "_predicate/name")
             (assoc kv-map name-key))
        kv-map)

      (= "_collection" collection-name)
      (if-let [name-key (some coll-name-key (keys kv-map))]
        (->> (get kv-map name-key)
             (validate-internal-predicate "_collection" "_collection/name")
             (assoc kv-map name-key))
        kv-map)

      :else kv-map)))

(defn resolve-ident
  "Acts as a local cache for ident resolution.
  Returns subject id for ident, else nil if doesn't exist."
  [db ident-cache ident]
  (go-try
    (if (contains? @ident-cache ident)
      (get @ident-cache ident)
      (let [subid (<? (dbproto/-subid db ident false))]
        (swap! ident-cache assoc ident subid)
        subid))))


(defn resolve-ident-strict
  "Like resolve-ident, but will throw if an existing subject does not exist."
  [db ident-cache ident]
  (go-try
    (let [subid (<? (resolve-ident db ident-cache ident))]
      (when-not subid
        (throw (ex-info (str "Invalid identity, does not exist: " (pr-str ident))
                        {:status 400 :error :db/invalid-tx}))) subid)))

(defn resolve-id
  [db txi _id temp-ident? ident-cache]
  (go-try (cond
            temp-ident?
            _id

            (neg-int? _id)                                  ;; this would be a block subject, so let pass
            _id

            (pos-int? _id)
            (do
              (when-not (<? (dbproto/-subid db _id))
                (throw (ex-info (str "Subject does not exist: " _id " for tx item: " (pr-str txi))
                                {:status 400 :errors :db/invalid-subject})))
              _id)

            (util/pred-ident? _id)
            (<? (resolve-ident-strict db ident-cache _id))  ;; throw if ident doesn't exist

            :else
            (throw (ex-info (str "Invalid _id: " (pr-str txi))
                            {:status 400 :error :db/invalid-tx})))))


(defn resolve-predicate-values
  [db txi ecount tempids ident-cache auth_id fuel block-instant]
  (go-try
    (let [{:keys [_id _action _meta]} txi
          temp-ident? (util/temp-ident? _id)
          _id'        (<? (resolve-id db txi _id temp-ident? ident-cache))
          [collection-id collection-name] (resolve-collection-id+name db _id')
          sys-coll?   (#{"_tag" "_predicate" "_collection"} collection-name) ;; extra manipulation / validation needed
          p-o-pairs   (cond->> (dissoc txi :_id :_action :_meta)
                               sys-coll? (conform-sys-predicate collection-name))]

      (when (and (= collection-name "_fn") (contains? p-o-pairs :code))
        (<? (permissions/parse-fn db (:code p-o-pairs) (:params p-o-pairs))))


      (if (= :delete _action)
        ;; for :delete we still need to resolve predicates into ids
        (let [txi' (loop [[[p* o] & r] p-o-pairs
                          acc {:_id     _id'
                               :_action _action}]
                     (if-not p*
                       acc
                       (let [p-str (if-let [a-ns (namespace p*)]
                                     (str a-ns "/" (name p*))
                                     (str collection-name "/" (name p*)))
                             ;; get predicate-id from p-str
                             pid   (or (dbproto/-p-prop db :id p-str)
                                       (throw (ex-info (str "Predicate does not exist: " p-str)
                                                       {:status 400 :error :db/invalid-tx})))]
                         (recur r (assoc acc pid o)))))]
          [txi' (assoc tempids _id _id') ecount nil])
        ;; everything but :delete
        (loop [[[p* o] & r] p-o-pairs
               subid-resolved (when (int? _id') _id')       ;; holds the subid this tx item has already resolved to
               uniques        nil                           ;; any unique pred-val pairs found in this txi, i.e. [1234 "some value"]
               txi            {:_id     _id'
                               :_action _action
                               :_meta   _meta}]
          (let [p-str           (if-let [a-ns (namespace p*)]
                                  (str a-ns "/" (name p*))
                                  (str collection-name "/" (name p*)))
                ;; get predicate-id from p-str
                pid             (or (dbproto/-p-prop db :id p-str)
                                    (throw (ex-info (str "Predicate does not exist: " p-str)
                                                    {:status 400 :error :db/invalid-tx})))
                unique?         (dbproto/-p-prop db :unique pid)
                tx-fn?          (dbfunctions/tx-fn? o)
                o*              (if tx-fn?
                                  (if (dbproto/-p-prop db :multi pid)
                                    (throw (ex-info (str "Database functions are not supported on multi-value predicates. Processing " p-str " in " txi ".")
                                                    {:status 400
                                                     :error  :db/invalid-db-fn}))
                                    (<? (dbfunctions/execute-tx-fn db auth_id nil _id' pid o fuel block-instant))) o)

                ;; if this predicate is unique, see if it resolves to an existing subject and check that it didn't already resolve to a different subject
                subid-resolved' (if-not unique?
                                  subid-resolved            ;; retain any existing resolved subid
                                  (let [ref?    (dbproto/-p-prop db :ref? pid)
                                        subid   (if (sequential? o*)
                                                  ;; vector... need to try each
                                                  (loop [[o' & r] o*
                                                         res-subid nil]
                                                    (if-not o'
                                                      res-subid
                                                      (let [res (if (and ref? (util/temp-ident? o'))
                                                                  res-subid
                                                                  (let [subid (<? (resolve-ident db ident-cache [p-str o']))]
                                                                    (if (and subid res-subid (not= subid res-subid))
                                                                      (throw (ex-info (format "The transaction item _id %s resolves to two different existing entities. It already resolved to subject: %s, however the unique predicate identity [%s %s] resolves to %s."
                                                                                              _id res-subid p-str o' @subid)
                                                                                      {:status 400 :error :db/invalid-tx}))
                                                                      (or subid res-subid))))]
                                                        (recur r res))))
                                                  (when-not (and ref? (util/temp-ident? o*))
                                                    (<? (resolve-ident db ident-cache [p-str o*]))))
                                        upsert? (dbproto/-p-prop db :upsert pid)]
                                    (cond
                                      ;; if we already resolved to a existing subid, but this one is different.
                                      (and subid-resolved subid
                                           (not= subid-resolved subid))
                                      (throw (ex-info (format "The transaction item _id %s resolves to two different existing entities. It already resolved to subject: %s, however the unique predicate identity [%s %s] resolves to %s."
                                                              _id subid-resolved p-str o* subid)
                                                      {:status 400 :error :db/invalid-tx}))

                                      (and subid (not upsert?))
                                      (throw (ex-info (format "Predicate %s does not allow upsert, and [%s %s] duplicates an existing %s (%s)."
                                                              p-str _id subid p-str o*)
                                                      {:status 400 :error :db/invalid-tx}))

                                      :else (or subid subid-resolved))))
                uniques'        (if unique?                 ;; add to unique pred - val pairs if unique
                                  (conj uniques [pid o*])
                                  uniques)
                txi'            (assoc txi pid o*)]
            (if (not-empty r)
              (recur r subid-resolved' uniques' txi')
              (let [[_id'' tempids' ecount'] (cond
                                               (and temp-ident? subid-resolved')
                                               [subid-resolved' (assoc tempids _id subid-resolved') ecount]

                                               temp-ident?
                                               (let [this-sid       (if-let [last-sid (get ecount collection-id)]
                                                                      (inc last-sid) ;; TODO - check if at max for collection, and if so throw
                                                                      (flake/->sid collection-id 0)) ;; first time collection is used
                                                     updated-ecount (assoc ecount collection-id this-sid)]
                                                 [this-sid (assoc tempids _id this-sid) updated-ecount])

                                               :else
                                               [_id' tempids ecount])]
                [(assoc txi' :_id _id'') tempids' ecount' uniques']))))))))

(defn resolve-idents
  "Resolves any idents and resolves the predicates to their respective identities.

  Returns the updated transaction and resolved identities, starting the tempids process."
  [db ident-cache txn-raw auth_id fuel block-instant]
  (go-try
    (loop [[txi & r] txn-raw
           txn           []
           ecount        (:ecount db)
           unique-pred-v #{}                                ;; holds any unique predicates and value pairs, i.e. [1234 "some value"]
           tempids       {}]
      (let [[txi* tempids* ecount* uniques]
            (<? (resolve-predicate-values db txi ecount tempids ident-cache auth_id fuel block-instant))
            txn*           (conj txn txi*)
            unique-pred-v* (into unique-pred-v uniques)]
        ;; check that any unique pred/value pairs haven't already been used
        (when-let [duplicate-unique (some unique-pred-v uniques)]
          (throw (ex-info (format "Multiple transaction items utilize the same unique predicate id: %s with value: %s. The second transactional item is: %s."
                                  (first duplicate-unique) (second duplicate-unique) (pr-str txi))
                          {:status 400 :error :db/invalid-tx})))
        (if (not-empty r)
          (recur r txn* ecount* unique-pred-v* tempids*)
          [txn* ecount* tempids*])))))


(defn resolve-tag
  "Returns the subject id of the tag if it exists."
  [db tag-cache tag]
  (go-try
    (if (contains? @tag-cache tag)
      (get @tag-cache tag)
      (let [tag-sid (<? (dbproto/-tag-id db tag))]
        (swap! tag-cache assoc tag tag-sid)
        tag-sid))))


(defn create-tag
  [db ecount tag-cache t tag]
  (let [tag*     (validate-internal-predicate "_tag" "_tag/id" tag)
        tag-cid  (dbproto/-c-prop db :partition "_tag")
        next-sid (if-let [last-tag-id (get ecount tag-cid)]
                   (inc last-tag-id)
                   (flake/->sid tag-cid 1))
        ecount'  (assoc ecount tag-cid next-sid)
        tag-smt  [true next-sid const/$_tag:id tag* t nil]]
    [ecount' tag-smt]))


(defn next-tag-tempid
  "Finds an unused tempid for a new tag that we must create."
  ([tempids] (next-tag-tempid tempids 1))
  ([tempids n]
   (let [tempid (str "_tag$" n)]
     (if (get tempids tempid)
       (recur tempids (inc n))
       tempid))))


(defn resolve-tags
  "If a statement is using a tag, either resolves it to an existing tag or, if _predicate/restrictTag isn't true,
  creates a new tag that will be inserted."
  [db tempids scount tag-cache statement multi?]
  (go-try
    (let [[op s p o t _meta] statement
          ;; a should be numeric ID - need to determine predicate name
          pred-name (or (dbproto/-p-prop db :name p)
                        (throw (ex-info (str "Trying to resolve predicate name for tag resolution but name is unknown for pred: " p)
                                        {:status 500
                                         :error  :db/unexpected-error
                                         :tags   o})))]
      (loop [[tag* & r] (if multi? o [o])
             scount  scount
             smts    []
             tempids tempids]
        (if (nil? tag*)
          [scount smts tempids]
          (let [tag-name (if (.contains tag* "/")
                           tag*
                           (str pred-name ":" tag*))
                tag-s    (<? (resolve-tag db tag-cache tag-name))]
            (if tag-s
              ;; tag exists
              (recur r scount (conj smts [op s p tag-s t _meta]) tempids)
              ;; tag doesn't exist
              (if (dbproto/-p-prop db :restrictTag pred-name)
                (throw (ex-info (str tag* " is not a valid tag. The restrictTag property for: " pred-name " is true. Therefore, a tag with the id " pred-name ":" tag* " must already exist.")
                                {:status 500
                                 :error  :db/unexpected-error
                                 :tags   o}))
                (let [[ecount-acc' new-tag] (create-tag db scount tag-cache t tag-name)
                      tag-s      (second new-tag)
                      tag-tempid (next-tag-tempid tempids)
                      tempids'   (assoc tempids tag-tempid tag-s)]
                  (swap! tag-cache assoc tag-name tag-tempid)
                  (recur r ecount-acc' (-> smts
                                           (conj new-tag)
                                           (conj [op s p tag-tempid t _meta])) tempids'))))))))))


(defn subject-retraction-flakes
  "Gets all flakes required to retract for an subject retraction.
  In the case the subject is ref'd by other entities, it removes all refs."
  [db sid]
  (go-try
    (let [flakes (query-range/index-range db :spot = [sid])
          refs   (query-range/index-range db :opst = [sid])]
      (into (<? flakes) (<? refs)))))


(defn txn->statements
  [db tempids ecount t txn]
  (go-try
    (let [tag-cache (atom {})]
      (loop [[txi & r] txn
             ecount-acc ecount
             statements #{}
             tempids    tempids]
        (if-not txi
          [ecount-acc statements tempids]
          (let [{:keys [_id _action _meta]} txi
                op        (action->op _action)
                p-o-pairs (dissoc txi :_id :_action :_meta)]
            (cond
              ;; delete an entire subject
              (and (= :delete _action) (empty? p-o-pairs))
              (recur r ecount-acc (->> (<? (subject-retraction-flakes db _id))
                                       (map (fn [f] [false (.-s f) (.-p f) (.-o f) t (.-m f)]))
                                       (into statements)) tempids)

              ;; delete specific predicates
              (every? nil? (vals p-o-pairs))
              (let [new-statements (<? (async/go-loop [[[p-o-pair _] & r] p-o-pairs
                                                       new-statements #{}]
                                         (if (not p-o-pair)
                                           new-statements
                                           (let [flakes          (<? (query-range/index-range db :spot = [_id p-o-pair]))
                                                 new-statements' (into new-statements
                                                                       (map (fn [f] [false (.-s f) (.-p f) (.-o f) t (.-m f)]) flakes))]
                                             (recur r new-statements')))))]
                (recur r ecount-acc (->> new-statements (into statements)) tempids))

              :else
              (let [[ecount' statements' tempids']
                    (loop [[[a v] & r*] p-o-pairs
                           tempids*    tempids
                           ecount*     ecount-acc
                           statements* statements]
                      (let [multi?      (dbproto/-p-prop db :multi a)
                            _           (when (and multi? (not (nil? v)) (not (sequential? v)))
                                          (throw (ex-info (format "Predicate, %s, is multi-cardinality and the value must be a vector/array: %s" (dbproto/-p-prop db :name a) (str v))
                                                          {:status 400 :error :db/invalid-tx})))
                            tag?        (= :tag (dbproto/-p-prop db :type a))

                            [ecount' new-statements tempids']
                            (if tag?
                              (<? (resolve-tags db tempids* ecount* tag-cache
                                                [op _id a v t nil]
                                                multi?))
                              [ecount*
                               (if (and multi? (not (nil? v)))
                                 (reduce
                                   (fn [acc' v']
                                     (conj acc' [op _id a v' t nil]))
                                   []
                                   v)
                                 [[(if (nil? v) false op) _id a v t nil]])
                               tempids*])
                            ;; use 'into' here to prevent lazy seq stack overflow for large txs if 'concat' was used instead
                            statements' (into statements* new-statements)]
                        (if (empty? r*)
                          [ecount' statements' tempids']
                          (recur r* tempids' ecount' statements'))))]
                (recur r ecount' statements' tempids')))))))))

(defn conform-value
  "Attempts to force value into  defined type, and conforms to any defined specs
  if present"
  [db s p o]
  (let [type (dbproto/-p-prop db :type p)]
    (cond
      (= :tag type)
      (if (int? o)
        o
        (throw (ex-info (str "Invalid tag, should be resolved to an subject but instead is: " (pr-str o))
                        {:status 400
                         :error  :db/invalid-tx})))

      (= :ref type)
      (let [restrict-collection (dbproto/-p-prop db :restrictCollection p)
            v-collection        (flake/sid->cid o)]
        (if (and restrict-collection (not= restrict-collection (flake/sid->cid o)))
          (throw (ex-info (str "Invalid reference for predicate: " (dbproto/-p-prop db :name p)
                               ". It is restricted to collection: " (dbproto/-c-prop db :name restrict-collection)
                               " but the value provided refers to collection: " (dbproto/-c-prop db :name v-collection))
                          {:status 400
                           :error  :db/invalid-ref}))
          o))

      ;; for JSON, we first check the spec, then serialize it
      (= :json type)
      (cond-> o
              true (#(try (json/stringify %)
                          (catch Exception e
                            (throw (ex-info (str "Unable to serialize JSON from value: " (pr-str o))
                                            {:status 400
                                             :error  :db/invalid-tx}))))))

      ;; Just type-check here, _predicate/spec check happens after flakes created
      :else
      (fspec/type-check o type))))


(defn process-obj
  [db tempids smt ref?]
  (go-try
    (let [[op s p o] smt
          o' (if ref?
               (cond
                 (util/temp-ident? o)
                 (let [ident (get tempids o)]
                   (if (nil? ident)
                     (throw (ex-info (str "Reference value '" o "' does not exist for: " [s p o])
                                     {:status 400
                                      :error  :db/invalid-ref}))
                     ident))

                 (and (false? op) (nil? o))
                 o

                 (util/subj-ident? o)
                 (or (<? (dbproto/-subid db o))
                     (throw (ex-info (str "Reference object '" o "' is not a valid tempid or identity: " [s p o])
                                     {:status 400
                                      :error  :db/invalid-ref})))

                 :else
                 (throw (ex-info (str "Reference object '" o "' is not a valid tempid or identity: " [s p o])
                                 {:status 400
                                  :error  :db/invalid-ref})))
               o)
          ;; If adding value, conform, not if deleting.
          o* (if op (conform-value db s p o') o')]
      o*)))


(defn retract-subject-flakes
  "Returns a list of flakes that need to be retracted because they are 'component's.

  Will recursively find further sub-components to retract. Keeps track of components
  already seen to stop following in case of infinite recursion.

  Note this does not 'flip' the flakes to the actual retraction flakes, just returns
  all the flakes that should be retracted."
  ([db subjects] (retract-subject-flakes db subjects #{}))
  ([db subjects seen]
   (go-try
     (loop [[sid & r] subjects
            seen   seen
            flakes []]
       (if-not sid
         flakes
         (let [new-flakes     (when-not (seen sid)
                                (<? (query-range/index-range db :spot = [sid])))
               seen*          (conj seen sid)
               new-predicates (->> new-flakes
                                   (map #(.-p %))
                                   (into #{}))
               sub-comp-preds (loop [[p & r] new-predicates
                                     acc #{}]
                                (if-not p
                                  acc
                                  (if (dbproto/-p-prop db :component p)
                                    (recur r (conj acc p))
                                    (recur r acc))))
               sub-flakes     (when (not-empty sub-comp-preds)
                                (<? (retract-subject-flakes db
                                                            (keep #(when (sub-comp-preds (.-p %))
                                                                     (.-o %))
                                                                  new-flakes)
                                                            seen*)))]
           (recur r seen (-> flakes
                             (into new-flakes)
                             (into sub-flakes)))))))))


(defn statements-to-flakes
  [db tempids statements]
  (go-try
    (loop [[smt & r] statements
           flakes (flake/sorted-set-by flake/cmp-flakes-spot-novelty)]
      (let [[op s p _ tx m] smt
            multi?   (dbproto/-p-prop db :multi p)
            ref?     (dbproto/-p-prop db :ref? p)
            o        (<? (process-obj db tempids smt ref?))
            existing (<? (query-range/index-range db :spot = [s p o]))
            flakes*  (case op
                       true (if (not-empty existing)
                              flakes                        ;; flake already exists, so ignore
                              (let [add (flake/new-flake s p o tx true m)
                                    ret (when-not multi?    ;; need to retract existing value
                                          (some-> (<? (query-range/index-range db :spot = [s p]))
                                                  (first)
                                                  (flake/flip-flake tx)))]
                                (cond-> (conj flakes add)
                                        ret (conj ret))))

                       false (let [retractions (<? (query-range/index-range db :spot = [s p o]))
                                   component?  (dbproto/-p-prop db :component p)
                                   components  (when component?
                                                 (->> retractions
                                                      (map #(.-o %)) ;; get entities
                                                      ((fn [n] (retract-subject-flakes db n #{})))
                                                      (<?)))]
                               ;; 'flip' all of our retraction flakes
                               (->> (concat retractions components)
                                    (map #(flake/flip-flake % tx))
                                    (into flakes))))]
        (if r
          (recur r flakes*)
          flakes*)))))


(defn tx+block-meta-txi
  "Will create tx/block transaction in, or merge existing tx/block transaction.

  Validates that no _id numeric values are in the block range (from 0 to Integer/MAX_VALUE)."
  [tx-meta block-sid prev-hash tx-instant]
  (merge tx-meta (util/without-nils
                   {:_id             block-sid
                    :_action         :insert
                    :_block/instant  tx-instant
                    :_block/prevHash prev-hash})))


(defn validate-permissions
  "Validates transaction based on the state of the new database."
  [db-before candidate-db flakes tx-permissions]
  (go-try
    (let [no-filter? (true? (:root? tx-permissions))]
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


;; TODO - if no predicate spec exists, could skip all of this - look to add to db's :schema
(defn valid-predicate-spec-flake?
  "Takes a db and a flake, checks whether the flake adheres to any _predicate/spec"
  [flake db auth_id]
  (go-try
    (let [pid      (.-p flake)
          pred-map (->> db :schema :pred ((fn [coll]
                                            (get coll pid))))
          spec     (:spec pred-map)
          specDoc  (:specDoc pred-map)]
      (if spec
        (let [spec-vec (if (vector? spec) spec [spec])
              query    (reduce-kv (fn [acc idx spec]
                                    (let [code-var (str "?code" idx)]
                                      (-> (update acc :selectOne conj code-var)
                                          (update :where conj [spec "_fn/code" code-var]))))
                                  {:selectOne [] :where []} spec-vec)
              fn-code  (<? (dbproto/-query db query))
              fn-code' (dbfunctions/combine-fns fn-code)
              sid      (.-s flake)
              o        (.-o flake)
              ctx      {:db      db
                        :sid     sid
                        :pid     pid
                        :o       o
                        :flakes  [flake]
                        :auth_id auth_id
                        :state   (atom {:stack   []
                                        :credits 10000000
                                        :spent   0})}
              f-meta   (<? (dbfunctions/parse-fn db fn-code' "predSpec" nil))
              res      (f-meta ctx)
              res*     (if (channel? res) (<? res) res)]
          (if res*
            true
            (throw (ex-info (str (if specDoc (str specDoc " Value: " o) (str "Object " o " does not conform to the spec for predicate: " (:name pred-map))))
                            {:status 400
                             :error  :db/invalid-tx})))) true))))

(defn validate-predicate-spec
  [db flakes auth_id block-instant]
  (go-try
    (let [true-flakes      (clojure.set/select #(.-op %) flakes)
          ;; First, we check _predicate/spec -> this runs for every single flake that is being added and has a spec.
          predSpecValid?   (loop [[flake & r] true-flakes]
                             (let [res (<? (valid-predicate-spec-flake? flake db auth_id))]
                               (if (and res r)
                                 (recur r) res)))
          ;; If _predicate/spec checks out, we test _predicate/txSpec -> This runs once with all of the flakes, both true and false,
          ;; within a transaction. This is mainly useful for checking that the sum of values for add flakes = the sum of values for
          ;; remove flakes.
          allTxPreds       (->> (map #(.-p %) flakes) (set))
          predTxSpecValid? (loop [[pred & r] allTxPreds]
                             (let [pred-map  (->> db :schema :pred ((fn [coll]
                                                                      (get coll pred))))
                                   txSpec    (:txSpec pred-map)
                                   txSpecDoc (:txSpecDoc pred-map)
                                   pred-name (:name pred-map)
                                   res       (if txSpec
                                               (let [spec-vec (if (vector? txSpec) txSpec [txSpec])
                                                     query    (reduce-kv (fn [acc idx spec]
                                                                           (let [code-var (str "?code" idx)]
                                                                             (-> (update acc :selectOne conj code-var)
                                                                                 (update :where conj [spec "_fn/code" code-var]))))
                                                                         {:selectOne [] :where []} spec-vec)
                                                     fn-code  (<? (dbproto/-query db query))
                                                     fn-code' (dbfunctions/combine-fns fn-code)
                                                     ctx      {:db      db
                                                               :pid     pred
                                                               :instant block-instant
                                                               :flakes  (filterv #(= (.-p %) pred) flakes)
                                                               :auth_id auth_id
                                                               :state   (atom {:stack   []
                                                                               :credits 10000000
                                                                               :spent   0})}
                                                     f-meta   (<? (dbfunctions/parse-fn db fn-code' "predSpec" nil))]
                                                 (f-meta ctx)) true)
                                   res*      (if (channel? res) (<? res) res)
                                   res''     (if res* res* (throw (ex-info (str "The predicate " pred-name " does not conform to spec. " txSpecDoc)
                                                                           {:status 400
                                                                            :error  :db/invalid-tx})))]
                               (if (and res'' r)
                                 (recur r) res'')))] true)))

;; TODO - this should use 'some' form to fail after fist spec fails (it currently does everything, even if a failure occurs)
;; TODO - spec for collections should be cached with the db's :schema, pointing to sids of SmartFunctions - and can use a caching fn then to get respective SmartFunctions by id
(defn validate-collection-spec
  [db-after flakes auth_id block-instant]
  (go-try
    (let [all-sids       (->> (map #(.-s %) flakes)         ;; Get all unique subjects in flakes
                              (set))
          ;; Get collection-sids for all subject collections
          collection-sid (mapv #(->> (flake/sid->cid %) (dbproto/-c-prop db-after :sid)) all-sids)

          ;; Get spec and specDoc for all entities
          specs-res      (loop [[sid & r] collection-sid
                                acc []]
                           (if-not sid
                             acc
                             (let [res      (<? (dbproto/-query db-after
                                                                {:selectOne [{"_collection/spec" ["_fn/code" "_fn/params"]} "_collection/specDoc"]
                                                                 :from      sid}))
                                   specs    (get res "_collection/spec")
                                   spec     (map #(get % "_fn/code") specs)
                                   params   (remove nil? (map #(get % "_fn/params") specs))
                                   _        (if (empty? params)
                                              nil
                                              (throw (ex-info (str "You can only use functions with additional parameters in transactions functions. ")
                                                              {:status 400
                                                               :error  :db/invalid-tx})))
                                   spec-str (dbfunctions/combine-fns spec)
                                   spec-doc (get res "_collection/specDoc")]
                               (recur r (conj acc (vector spec-str spec-doc))))))
          ;; Create a vector for all [subject spec]
          spec-vec       (map (fn [e [spec spec-doc]]
                                (vector e spec spec-doc))
                              all-sids specs-res)
          ;; Remove any entities from spec-vec that don't have specs
          spec-vec*      (remove (fn [n]
                                   (nil? (second n))) spec-vec)

          ;; Search for each subject in the candidate-db. If null, then the entire subject was deleted
          entities*      (loop [[[sid spec spec-doc] & r] spec-vec*
                                acc []]
                           (if-not sid
                             acc
                             (let [res (<? (dbproto/-query db-after {:selectOne ["*"] :from sid}))]
                               (recur r (conj acc res)))))
          spec-vec**     (remove nil? (map (fn [ent spec-vec]
                                             (if ent
                                               spec-vec
                                               nil))
                                           entities* spec-vec*))
          ctx            (remove nil? (map (fn [subject]
                                             (if subject
                                               {:db      db-after
                                                :instant block-instant
                                                :sid     (get subject "_id")
                                                :flakes  flakes
                                                :auth_id auth_id
                                                :state   (atom {:stack   []
                                                                :credits 10000000
                                                                :spent   0})})) entities*))
          f-meta         (loop [[[sid spec spec-doc params] & r] spec-vec**
                                acc []]
                           (if-not sid
                             acc
                             (recur r (conj acc (<? (dbfunctions/parse-fn db-after spec "collectionSpec" nil))))))]
      (loop
        [[f & r] f-meta
         [ctx & ctx-r] ctx
         [spec & spec-r] spec-vec**]
        (if f
          (let [res  (f ctx)
                res* (if (channel? res) (<? res) res)]
            (cond (not res*)
                  (throw (ex-info (str "Transaction does not adhere to the collection spec: " (nth spec 2))
                                  {:status 400
                                   :error  :db/invalid-tx}))

                  r
                  (recur r ctx-r spec-r)

                  :else true)) true)))))


(defn- throw-if-interrupted
  [dbid tx]
  ;; check for thread interruption before proceeding
  (when (Thread/interrupted)
    (throw (ex-info (str "Thread interrupted in transaction for db: " dbid)
                    {:status 400
                     :error  :db/thread-interrupted
                     :txn    (pr-str tx)}))))


(defn- make-candidate-db
  "Assigns a tempid to all index roots, which ensures caching for this candidate db
  is independent from any 'official' db with the same block."
  [db]
  (let [tempid  (UUID/randomUUID)
        indexes [:spot :psot :post :opst]]
    (reduce
      (fn [db idx]
        (let [index (assoc (get db idx) :tempid tempid)]
          (assoc db idx index)))
      db indexes)))

(defn adding-data?
  [new-flakes]
  (some #(if (< 0 (.-s %))
           (.-op %) false)
        new-flakes))


(defn- do-transact!
  [session db-before tx-map tx-permissions auth_id t block-instant]
  (go-try
    (let [{:keys [tx-meta rest-tx sig txid authority auth]} tx-map
          fast-forward-db?    (:tt-id db-before)
          root-db             (dbproto/-rootdb db-before)
          ident-cache         (atom {})                     ;; caches idents we look up along the way to speed up tx
          txn-with-tx-meta    (conj rest-tx tx-meta)
          credits             1000000
          fuel                (atom {:stack   []
                                     :credits credits
                                     :spent   0})
          [txn* ecount tempids] (<? (resolve-idents root-db ident-cache txn-with-tx-meta auth_id fuel block-instant))
          _                   (throw-if-interrupted (:dbid db-before) txn*)
          [ecount' statements tempids'] (<? (txn->statements root-db tempids ecount t txn*))
          _                   (throw-if-interrupted (:dbid db-before) txn*)
          sorted-statements   (sort #(Boolean/compare (first %1) (first %2)) statements)
          new-flakes          (<? (statements-to-flakes root-db tempids' sorted-statements))
          tempids-flake       (when-not (empty? tempids')
                                (flake/->Flake t const/$_tx:tempids (json/stringify tempids') t true nil))
          new-flakes*         (if tempids-flake (conj new-flakes tempids-flake) new-flakes)
          _                   (swap! fuel update-in [:spent] #(+ (count new-flakes*) %))
          new-sorted*         (apply flake/sorted-set-by flake/cmp-flakes-block new-flakes*)
          tx-hash             (tx-util/gen-tx-hash new-sorted* true)
          hash-flake          (flake/->Flake t const/$_tx:hash tx-hash t true nil)
          all-flakes          (conj new-sorted* hash-flake)
          _                   (swap! fuel update-in [:spent] #(+ (count all-flakes) %))
          db-after            (if fast-forward-db?
                                (<? (dbproto/-forward-time-travel db-before all-flakes))
                                (<? (dbproto/-with-t db-before all-flakes)))
          bytes               (- (get-in db-after [:stats :size]) (get-in db-before [:stats :size]))

          ;; final db that can be used for any final testing/spec validation
          candidate-db        (-> db-after (dbproto/-rootdb) (make-candidate-db))

          ; check if flakes affect schema
          [candidate-db'
           remove-preds] (if (and (schema-util/pred-change? all-flakes) (adding-data? all-flakes))
                           [(<? (schema/validate-schema-change candidate-db tempids' all-flakes))
                            (schema-util/remove-from-post-preds new-flakes)]
                           [candidate-db []])
          ; Validate collection and predicate specs in parallel
          coll-spec-valid?-ch (validate-collection-spec candidate-db' all-flakes auth_id block-instant)
          pred-spec-valid?-ch (validate-predicate-spec candidate-db' all-flakes auth_id block-instant)
          valid-perm?-ch      (validate-permissions db-before candidate-db' all-flakes tx-permissions)
          ;; resolve parallel specs - throws if an error
          coll-spec-valid?    (<? coll-spec-valid?-ch)
          pred-spec-valid?    (<? pred-spec-valid?-ch)
          valid-perm?         (<? valid-perm?-ch)]

      (throw-if-interrupted (:dbid db-before) txn*)

      {:t            t
       :hash         tx-hash
       :db-after     candidate-db'
       :flakes       all-flakes
       :tempids      tempids'
       :bytes        bytes
       :fuel         (+ (:spent @fuel) bytes)
       :status       200
       :txid         txid
       :auth         auth
       :authority    authority
       :type         (keyword (:type tx-map))
       :remove-preds remove-preds})))


(defn handle-error-resp
  ([error] (handle-error-resp error {}))
  ([error meta]
   (let [err-data   (ex-data error)
         message    (.getMessage error)
         error-resp (merge {:status  (or (:status err-data) 500)
                            :message message
                            :error   :db/unexpected-error}
                           (select-keys err-data [:error :cause]))]
     (if (>= (:status error-resp) 500)
       (log/error error "Transaction Exception:" (merge meta (assoc err-data :message message)))
       (log/info "Transaction Exception:" (merge meta (assoc err-data :message message))))
     error-resp)))


(defn next-tempid
  "Generates a system tempid (where the user supplied only the collection name for a tempid).
  Ensures it does not conflict with any user-provided tempid."
  [tempids id-used? collection]
  (swap! tempids update collection #(if % (inc %) 1))
  (let [next-id (get @tempids collection)
        tempid  (str collection "$" next-id)]
    (if (id-used? tempid)
      (recur tempids id-used? collection)
      tempid)))


(defn pre-validate-p-o
  [txn _ids next-tempid raise]
  (loop [[txi & r] txn
         id-actions #{}
         txn*       []]
    (when-not (map? txi)
      (raise (str "Every transaction item must be a map / object. Found: " (pr-str txi))))
    (let [{:keys [_id _action _meta]} txi
          temp-ident? (util/temp-ident? _id)
          p-o-pairs   (dissoc txi :_id :_meta :_action)
          empty-pred? (empty? p-o-pairs)
          subj-ident? (util/subj-ident? _id)
          _           (when-not (or temp-ident? subj-ident?)
                        (raise (str "Every transaction item must have a valid _id. Provided: " (pr-str txi))))

          _id*        (if (and temp-ident? (re-matches #"^[\.-_a-zA-Z0-9]+$" _id)) ;; need to assign a system tempid
                        (next-tempid _id)
                        _id)
          _action*    (cond
                        (string? _action) (keyword _action)
                        (nil? _action) (cond temp-ident?
                                             :insert

                                             (every? nil? (vals p-o-pairs))
                                             :delete

                                             :else
                                             :update)
                        :else (raise (str "Invalid _action: " (pr-str _action))))
          _meta*      (cond
                        (nil? _meta) nil
                        (map? _meta) _meta
                        (string? _meta) (try (json/parse _meta)
                                             (catch Exception e
                                               (raise (str "String for _meta is not valid JSON. Provided: " _meta))))
                        :else (raise (str "Invalid meta (_meta): " (pr-str _meta))))
          id-action   [_id* _action*]


          txn**       (conj txn* (assoc txi :_id _id* :_action _action* :_meta _meta*))]
      (when (or (and (number? _id*) (neg? _id*))
                (and (vector? _id*) (re-matches #"^_block/.*" (first _id*))))
        (raise (str "Invalid _id value, you cannot update previous block metadata. Provided: " (pr-str txi))))
      (when (and empty-pred? (not= :delete _action*))
        (raise (str "The provided transaction item has no predicates in it: " (pr-str txi))))
      (when (and (= :delete _action*) temp-ident?)
        (raise (str "A delete action doesn't make sense for a temp-ident. Provided: " (pr-str txi))))
      (when (and temp-ident? (some #(nil? (val %)) p-o-pairs))
        (raise (str "Null values are not permitted for transaction items with temp-ids. Provided: " (pr-str txi))))
      (when-not (or temp-ident? (util/subj-ident? _id*))
        (raise (str "Invalid _id: " (pr-str _id*) ". An _id must be a number, a two-tuple entity, or a tempid.")))
      (when (contains? id-actions id-action)
        (raise (str "Duplicate transaction items with same _id and _action are not allowed. Conflict found for: " (pr-str txi))))
      (if r
        (recur r (conj id-actions id-action) txn**)
        txn**))))


(defn ids-check
  "Get a list of all tempids used in any part of the transaction, including any nested transaction items.
  Temids we care about are strings that have a non-standard character appended to them along with a unique
  identifier, i.e. mycollection$123. tempids that only include the collection name cannot be referenced by
  other transaction items."
  [acc txi]
  (let [txi-id (:_id txi)
        acc*   (if (or (nil? txi-id)                        ;; a tempid that may be referenced in some part of tx is of form "collection$123"
                       (vector? txi-id)
                       (number? txi-id)
                       (re-matches #"^[\.-_a-zA-Z0-9]+$" txi-id))
                 acc
                 (conj acc txi-id))
        vals'  (vals txi)]
    (reduce
      (fn [acc n]
        (cond
          (vector? n)
          (let [vec (remove (fn [n]
                              (not (and (map? n) (get n :_id)))) n)] ;; check if nested transaction items in vector
            (if (empty? vec)
              acc
              (reduce ids-check acc vec)))                  ;; found nested transaction item(s), check those as well

          (map? n)                                          ;; a map is assumed to be a nested transaction item
          (ids-check acc n)

          :else
          acc))
      acc* vals')))


(defn- pre-validate-transaction
  "Pre-validate anything we can without having to know the exact state of the DB.
  This happens before placing tx into the queue, and it is possible a tx already in the queue
  may transact data that affects the validity of this transaction.

  Returns a map response error message if an error exists, else returns nil."
  [txn]
  (let [raise (fn [s] (throw (ex-info s {:status 400 :error :db/invalid-tx})))]
    (when (not (sequential? txn))
      (raise "Transaction must be sequential (array/vector). Did you intend to submit a query?"))
    (let [_ids        (reduce ids-check #{} txn)            ;; we need to check for _ids that have been used for nested txns, but not assign them here
          tempids     (atom {})
          ;; call next-tempid with the collection-name to get an unused temp-id if needed
          next-tempid (partial next-tempid tempids _ids)]
      (pre-validate-p-o txn _ids next-tempid raise))))

(defn json-type?
  [db pred-name collection-name]
  (let [pred-with-coll (if (namespace (symbol pred-name)) (util/keyword->str pred-name)
                                                          (str (util/keyword->str collection-name) "/" (util/keyword->str pred-name)))]
    (= :json (dbproto/-p-prop db :type pred-with-coll))))

(defn keys-to-flatten?
  [db txi]
  (reduce
    (fn [acc [k v]]
      (cond
        (map? v)                                            ;; If map, we need to check if json-type
        (let [[_ collection] (resolve-collection-id+name db (:_id txi))
              json-type? (json-type? db k collection)]
          (if json-type?
            acc
            (conj acc k)))

        (vector? v)
        (if (some map? v)                                   ;; If any of the results are a map, we need to check if json-type
          (let [[_ collection] (resolve-collection-id+name db (:_id txi))
                json-type? (json-type? db k collection)]
            (if json-type?
              acc
              (conj acc k)))
          acc)

        :else
        acc)) [] txi))

;
;(defn flatten-txi
;  "Edits txi to remove any nested transactions and replace them with tempids.
;  Adds the new txis to r, so that they can be checked for nested components."
;  [r db txi tempids _ids key]
;  (let [txi-v (get txi key)]
;    (cond (map? txi-v)
;          (let [_id   (or (:_id txi-v)
;                          (throw (ex-info (str "Every transaction item must have a valid _id. Provided:" txi-v)
;                                          {:status 400 :error :db/invalid-db})))
;                _id   (if (= _id (re-find #"^[a-zA-Z0-9_][a-zA-Z0-9\.\-_]{0,254}" _id))
;                        (next-tempid tempids _ids (second (resolve-collection-id+name db _id)))
;                        _id)
;                txi   (assoc txi key _id)
;                txi-v (assoc txi-v :_id _id)]
;            [(conj r txi-v) txi])
;
;          (vector? txi-v)
;          (let [[r txi-v] (reduce (fn [[r txi-v] txi-v-item]
;                                    (if (map? txi-v-item)
;                                      (let [_id        (or (:_id txi-v-item)
;                                                           (throw (ex-info (str "Every transaction item must have a valid _id. Provided:" txi-v-item)
;                                                                           {:status 400 :error :db/invalid-db})))
;                                            _id        (if (and (string? _id) (= _id (re-find #"^[a-zA-Z0-9_][a-zA-Z0-9\.\-_]{0,254}" _id)))
;                                                         (next-tempid tempids _ids (second (resolve-collection-id+name db _id)))
;                                                         _id)
;                                            ;; txi-v-item has to be added to r
;                                            ;; we add the tempid to the txi-v vector
;                                            txi-v-item (assoc txi-v-item :_id _id)]
;                                        [(conj r txi-v-item) (conj txi-v _id)])
;
;                                      ;; If the item in the vector is not a map, we can just push it into txi-v,
;                                      ;; and there are no changes to r
;                                      [r (conj txi-v txi-v-item)])) [r []] txi-v)]
;            [r (assoc txi key txi-v)])
;
;
;          :else
;          (throw (ex-info "Unexpected error." {:status 500 :error :db/unexpected-error})))))

(declare flatten-transaction)

;; TODO - reconfigure this awful function
(defn flatten-txi
  [txi db collection raise tempids _ids]
  (go-try (loop [original-txi {}
                 new-txis     []
                 txi          txi]
            (if (empty? txi)
              [original-txi new-txis]

              (let [key (first (keys txi))
                    val (first (vals txi))]
                (cond (and (vector? val) (every? map? val))
                      (if (json-type? db key collection)
                        (recur (assoc original-txi key val) new-txis (dissoc txi key))
                        (let [original-txi (assoc original-txi key [])
                              [original-txi new-txis'] (<? (async/go-loop [original-txi original-txi
                                                                           new-txis new-txis
                                                                           [nested-val & r] val]
                                                             (if nested-val
                                                               (let [_id          (or (:_id nested-val)
                                                                                      (raise (str "Every transaction item must have a valid _id. Provided:" val)))
                                                                     _id          (if-let [coll (re-matches #"^[\.-_a-zA-Z0-9]+$" _id)]
                                                                                    (next-tempid tempids _ids coll) _id)
                                                                     original-txi (update original-txi key conj _id)
                                                                     new-val      (assoc nested-val :_id _id)
                                                                     new-txis'    (-> (concat new-txis (<? (flatten-transaction db tempids [new-val]))) set)]
                                                                 (recur original-txi new-txis' r))
                                                               [original-txi new-txis])))]
                          (recur original-txi (-> (concat new-txis new-txis') set) (dissoc txi key))))

                      (map? val)
                      (if (json-type? db key collection)
                        (recur (assoc original-txi key val) new-txis (dissoc txi key))
                        (let [_id          (or (:_id val)
                                               (raise (str "Every transaction item must have a valid _id. Provided:" val)))
                              _id          (if-let [coll (re-matches #"^[\.-_a-zA-Z0-9]+$" _id)]
                                             (next-tempid tempids _ids coll) _id)
                              original-txi (assoc original-txi key _id)
                              new-val      (assoc val :_id _id)
                              new-txis'    (<? (flatten-transaction db tempids [new-val]))]
                          (recur original-txi (-> (concat new-txis new-txis') set) (dissoc txi key))))

                      :else
                      (recur (assoc original-txi key val) new-txis (dissoc txi key))))))))


(defn flatten-transaction
  "After tempids have been assigned to any top-level txns, assign tempids to nested txns, and flatten txn"
  [db tempids txn]
  (go-try (let [_ids  (loop [[txi & r] txn
                             all-ids #{}]
                        (if-not txi
                          all-ids
                          (recur r (ids-check all-ids txi)))) ; ; returns all _ids in set at all levels
                raise (fn [s] (throw (ex-info s {:status 400 :error :db/invalid-tx})))]
            (loop [acc []
                   [txi & r] txn]
              (if txi
                (let [_id         (:_id txi)
                      temp-ident? (util/temp-ident? _id)
                      _id'        (<? (resolve-id db txi _id temp-ident? (atom {})))
                      [_ collection] (resolve-collection-id+name db _id')
                      [original-txi new-txis] (<? (flatten-txi txi db collection raise tempids _ids))
                      all-txis    (conj new-txis original-txi)]
                  (recur (apply conj acc all-txis) r))
                acc)))))


(defn add-tx-meta
  [tx-map t]
  "Returns tx-map that updates the :tx to include initial tx-meta portion of the transaction,
  plus *adds* tw additional keys:
  :tx-meta - transaction metadata
  :tx-rest - the rest of the transaction without the tx-meta"
  (let [{:keys [tx txid auth authority nonce cmd sig]} tx-map
        _        (when-not (sequential? tx)
                   (throw (ex-info (str "Invalid transaction, it must be a vector/array/sequence. "
                                        (when (map? tx) "Looks like you sent a map/object!"))
                                   {:status 400 :error :db/invalid-tx})))
        ;; separates tx-meta from the rest of the transaction
        {:keys [tx-meta rest-tx]} (tx-util/get-tx-meta-from-tx tx)
        tx-meta* (cond-> (assoc tx-meta :_id t
                                        :_action :insert
                                        :_tx/id txid
                                        :_tx/auth ["_auth/id" auth]
                                        :_tx/tx cmd
                                        :_tx/sig sig)

                         nonce
                         (assoc :_tx/nonce nonce)

                         authority
                         (assoc :_tx/authority ["_auth/id" authority]))]
    (assoc tx-map :tx (conj rest-tx tx-meta*) :rest-tx rest-tx :tx-meta tx-meta*)))


(def tx-meta-constants {:_tx/id        const/$_tx:id
                        :_tx/auth      const/$_tx:auth
                        :_tx/tx        const/$_tx:tx
                        :_tx/sig       const/$_tx:sig
                        :_tx/nonce     const/$_tx:nonce
                        :_tx/authority const/$_tx:authority
                        :_tx/error     const/$_tx:error})


(defn generate-tx-error-flakes
  "If an error occurs, returns a set of flakes for the 't' that represents error."
  [db t tx-map command error-str]
  (go-try
    (let [db             (dbproto/-rootdb db)
          {:keys [auth authority nonce txid]} tx-map
          tx-meta        (util/without-nils
                           {:_tx/id        txid
                            :_tx/auth      (<? (dbproto/-subid db ["_auth/id" auth] false))
                            :_tx/authority (when authority (<? (dbproto/-subid db ["_auth/id" authority] false)))
                            :_tx/tx        (:cmd command)
                            :_tx/sig       (:sig command)
                            :_tx/nonce     nonce
                            :_tx/error     error-str})
          flakes         (reduce-kv
                           (fn [acc k v]
                             (if-let [p (get tx-meta-constants k)]
                               (conj acc (flake/->Flake t p v t true nil))
                               acc))
                           [] tx-meta)
          flakes*        (-> (flake/sorted-set-by flake/cmp-flakes-block flakes) first)
          encoded-flakes (->> flakes*
                              (mapv #(vector (.-s %) (.-p %) (.-o %) (.-t %) (.-op %) (.-m %)))
                              (json/stringify))
          ;; apply hash flake(s)
          hash           (crypto/sha3-256 encoded-flakes)
          hash-flake     (flake/->Flake t const/$_tx:hash hash t true nil)]
      {:t      t
       :hash   hash
       :flakes (->> (conj flakes* hash-flake)
                    (into []))})))

(defn create-new-db-tx
  [tx-map]
  (let [{:keys [db alias auth doc fork forkBlock]} tx-map
        db-name (if (sequential? db)
                  (str (first db) "/" (second db))
                  (str/replace db "/$" "/"))
        tx      (util/without-nils
                  {:_id       "db$newdb"
                   :_action   :insert
                   :id        db-name
                   :alias     (or alias db-name)
                   :root      auth
                   :doc       doc
                   :fork      fork
                   :forkBlock forkBlock})]
    [tx]))

(defn valid-authority?
  [db auth authority]
  (go-try
    (if (empty? (<? (dbproto/-search db [auth "_auth/authority" authority])))
      (throw (ex-info (str authority " is not an authority for auth: " auth)
                      {:status 403 :error :db/invalid-auth})) true)))

(defn deps-succeeded?
  [db deps]
  (go-try (if (or (not deps) (empty? deps))
            true
            (let [res (->> (reduce-kv (fn [query-acc key dep]
                                        (-> query-acc
                                            (update :selectOne conj (str "?error" key))
                                            (update :where conj [(str "?tx" key) "_tx/id" dep])
                                            (update :optional conj [(str "?tx" key) "_tx/error" (str "?error" key)])))
                                      {:selectOne [] :where [] :optional []} deps)
                           (fdb/query-async (go-try db))
                           <?)]
              (and (not (empty? res)) (every? nil? res))))))

(defn build-transaction
  [session db cmd-data next-t block-instant]
  (go-try
    (let [tx-map          (tx-util/validate-command (:command cmd-data))
          deps            (-> tx-map :cmd json/parse :deps)
          _               (when-not (<? (deps-succeeded? db deps))
                            (throw (ex-info (str "One or more of the dependencies for this transaction failed: " deps)
                                            {:status 403 :error :db/invalid-auth})))
          ; kick off resolving auth id and authority in parallel
          {:keys [auth authority]} tx-map
          auth_id-ch      (dbproto/-subid db ["_auth/id" auth] true)
          authority_id-ch (when authority
                            (let [authority-id (if (string? authority) ["_auth/id" authority] authority)]
                              (dbproto/-subid db authority-id true)))
          cmd-type        (keyword (:type tx-map))
          tx**            (case cmd-type
                            :tx (->> (:tx tx-map) (flatten-transaction db (atom {})) <? pre-validate-transaction)
                            :new-db (create-new-db-tx tx-map))
          tx-map*         (-> (assoc tx-map :tx tx**)
                              (add-tx-meta next-t))
          auth_id         (async/<! auth_id-ch)
          _               (when (util/exception? auth_id)
                            (throw (ex-info (str "Auth id for transaction does not exist in the database: " auth)
                                            {:status 403 :error :db/invalid-auth})))
          ;; validate authority is valid or throw
          _               (when authority
                            (let [authority_id (async/<! authority_id-ch)
                                  _            (when (util/exception? authority_id)
                                                 (throw (ex-info (str "Authority " authority " does not exist.")
                                                                 {:status 403 :error :db/invalid-auth})))]
                              (<? (valid-authority? db auth_id authority_id))))
          roles           (<? (auth/roles db auth_id))
          tx-permissions  (-> (<? (permissions/permission-map db roles :transact))
                              (assoc :auth auth_id))]
      (<? (do-transact! session db tx-map* tx-permissions auth_id next-t block-instant)))
    (catch Exception e
      (let [error            (ex-data e)
            fast-forward-db? (:tt-id db)
            status           (or (:status error) 500)
            error-str        (str status " "
                                  (if (:error error)
                                    (str (util/keyword->str (:error error)))
                                    "db/unexpected-error")
                                  " "
                                  (.getMessage e))
            tx-map           (try (tx-util/validate-command (:command cmd-data)) (catch Exception e nil))
            {:keys [auth authority]} tx-map
            resp-map         (<? (generate-tx-error-flakes db next-t tx-map (:command cmd-data) error-str))
            db-after         (if fast-forward-db?
                               (<? (dbproto/-forward-time-travel db (:flakes resp-map)))
                               (<? (dbproto/-with-t db (:flakes resp-map))))
            bytes            (- (get-in db-after [:stats :size]) (get-in db [:stats :size]))]
        (when (= 500 status)
          (log/error e "Unexpected error processing transaction. Please report issues to Fluree."))
        (assoc resp-map
          :status status
          :error error-str
          :bytes bytes
          :db-after db-after
          ;; TODO - report out fuel in error within ex-info for use here
          :fuel (or (:fuel (ex-data e)) 0)
          :auth auth
          :authority authority
          :type (keyword (:type tx-map)))))))


(defn build-block
  "Builds a new block with supplied transaction(s)."
  [session transactions]
  (go-try
    (let [private-key   (:tx-private-key (:conn session))
          db-before-ch  (session/current-db session)
          db-before     (<? db-before-ch)
          _             (when (nil? db-before)
                          ;; TODO - think about this error, if it is possible, and what to do with any pending transactions
                          (log/warn "Unable to find a current db. Db transaction processor closing for db: %s/%s." (:network session) (:dbid session))
                          (session/close session)
                          (throw (ex-info (format "Unable to find a current db for: %s/%s." (:network session) (:dbid session))
                                          {:status 400 :error :db/invalid-transaction})))
          block         (inc (:block db-before))
          block-instant (Instant/now)
          prev-hash     (<? (fql/query db-before {:selectOne "?hash"
                                                  :where     [["?t" "_block/number" (:block db-before)]
                                                              ["?t" "_block/hash" "?hash"]]}))
          _             (when-not prev-hash
                          (throw (ex-info (str "Unable to retrieve previous block hash. Unexpected error.")
                                          {:status 500
                                           :error  :db/unexpected-error})))
          before-t      (:t db-before)]
      ;; perform each transaction in order
      (loop [[cmd-data & r] transactions
             next-t           (dec before-t)
             db               db-before
             block-bytes      0
             block-fuel       0
             block-flakes     (flake/sorted-set-by flake/cmp-flakes-block)
             cmd-types        #{}
             txns             {}
             remove-preds-acc #{}]
        (let [tx-result     (<? (build-transaction session db cmd-data next-t block-instant))
              {:keys [db-after bytes fuel flakes tempids auth authority status error hash
                      remove-preds]} tx-result
              block-bytes*  (+ block-bytes bytes)
              block-fuel*   (+ block-fuel fuel)
              block-flakes* (into block-flakes flakes)
              cmd-type      (:type tx-result)
              cmd-types*    (conj cmd-types cmd-type)
              txns*         (assoc txns (:id cmd-data) (util/without-nils
                                                         {:t         next-t ;; subject id
                                                          :status    status
                                                          :error     error
                                                          :tempids   tempids
                                                          :bytes     bytes
                                                          :id        (:id cmd-data)
                                                          :fuel      fuel
                                                          :auth      auth
                                                          :hash      hash
                                                          :authority authority
                                                          :type      cmd-type}))
              remove-preds* (into remove-preds-acc remove-preds)]
          (if r
            (recur r (dec next-t) db-after block-bytes* block-fuel* block-flakes* cmd-types* txns*
                   remove-preds*)
            (let [block-t             (dec next-t)
                  prevHash-flake      (flake/->Flake block-t const/$_block:prevHash prev-hash block-t true nil)
                  instant-flake       (flake/->Flake block-t const/$_block:instant (.toEpochMilli ^Instant block-instant) block-t true nil)
                  number-flake        (flake/->Flake block-t const/$_block:number block block-t true nil)
                  tx-flakes           (mapv #(flake/->Flake block-t const/$_block:transactions % block-t true nil) (range block-t before-t))
                  block-flakes        (conj tx-flakes prevHash-flake instant-flake number-flake)
                  block-tx-hash       (tx-util/gen-tx-hash block-flakes)
                  block-tx-hash-flake (flake/->Flake block-t const/$_tx:hash block-tx-hash block-t true nil)
                  ;; We order each txn command according to the t
                  txn-hashes          (->> (vals txns*)
                                           (sort-by #(* -1 (:t %)))
                                           (map :hash))
                  hash                (tx-util/generate-merkle-root (conj txn-hashes block-tx-hash))
                  sigs                [(crypto/sign-message hash private-key)]
                  hash-flake          (flake/->Flake block-t const/$_block:hash hash block-t true nil)
                  sigs-ref-flakes     (loop [[sig & sigs] sigs
                                             acc []]
                                        (if-not sig
                                          acc
                                          (let [auth-sid (<? (dbproto/-subid db-before ["_auth/id" (crypto/account-id-from-message hash sig)]))
                                                acc*     (if auth-sid
                                                           (-> acc
                                                               (conj (flake/->Flake block-t const/$_block:ledgers auth-sid block-t true nil))
                                                               (conj (flake/->Flake block-t const/$_block:sigs sig block-t true nil)))
                                                           acc)]
                                            (recur sigs acc*))))
                  new-flakes*         (-> (into block-flakes sigs-ref-flakes)
                                          (conj hash-flake)
                                          (conj block-tx-hash-flake))
                  all-flakes          (into block-flakes* new-flakes*)
                  latest-db           (<? (session/current-db session))
                  ;; if db was indexing and is now complete, add all flakes to newly indexed db... else just add new block flakes to latest db.
                  db-after*           (cond
                                        (not= (:block latest-db) (:block db-before))
                                        (throw (ex-info "While performing transactions, latest db became newer. Cancelling."
                                                        {:status 500 :error :db/unexpected-error}))

                                        ;; nothing has changed, just add block flakes to latest db
                                        (= (get-in db-before [:stats :indexed]) (get-in latest-db [:stats :indexed]))
                                        (<? (dbproto/-with db-after block (sort flake/cmp-flakes-spot-novelty new-flakes*)))

                                        ;; database has been re-indexed while we were transacting. Use latest indexed
                                        ;; version and reapply all flakes from this block
                                        :else
                                        (do
                                          (log/info "---> While transacting, database has been reindexed. Reapplying all block flakes to latest."
                                                    {:original-index (get-in db-before [:stats :indexed]) :latest-index (get-in latest-db [:stats :indexed])})
                                          (<? (dbproto/-with latest-db block all-flakes))))
                  block-result        {:db-before   db-before
                                       :db-after    db-after*
                                       :cmd-types   cmd-types*
                                       :block       block
                                       :t           block-t
                                       :hash        hash
                                       :sigs        sigs
                                       :instant     (.toEpochMilli ^Instant block-instant)
                                       :flakes      (into [] all-flakes)
                                       :block-bytes (- (get-in db-after* [:stats :size]) (get-in db-before [:stats :size]))
                                       :txns        txns*}]

              ;; update db status for tx group
              (let [new-block-resp (<? (txproto/propose-new-block-async
                                         (-> session :conn :group) (:network session)
                                         (:dbid session) (dissoc block-result :db-before :db-after)))]
                (if (true? new-block-resp)
                  (do
                    ;; update cached db
                    ;(let [new-db-ch (async/promise-chan)]
                    ;  (async/put! new-db-ch (:db-after block-result))
                    ;  (session/cas-db! session db-before-ch new-db-ch))
                    ;; reindex if needed
                    ;; to do -add opts
                    (<? (indexing/index* session {:remove-preds remove-preds*}))
                    block-result)
                  (do
                    (log/warn "Proposed block was not accepted by the network because: "
                              (pr-str new-block-resp)
                              "Proposed block: "
                              (dissoc block-result :db-before :db-after))
                    false))))))))))
