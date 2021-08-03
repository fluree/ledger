(ns fluree.db.ledger.vocabs.schema.org
  (:require [fluree.db.ledger.vocabs.core :as vocabs]
            [fluree.db.util.log :as log]
            [clojure.string :as str]))

;; namespace to convert schema.org JSON-LD into a pre-parsed context files that can
;; be used by Fluree to auto-generate classes and properties.

;; map of scalar types used in schema.org
(def ^:const scalars {"Text"     :string
                      "URL"      :uri
                      "Boolean"  :boolean
                      "Date"     :date
                      "Time"     :time
                      "DateTime" :dateTime
                      "Number"   :double
                      "Integer"  :int})

;; Will tag predicates as containing Enum vals if it is a subclass of this type, or it's parent is a subclass
(def ^:const enum-class "https://schema.org/Enumeration")

(defn scalar?
  "Only works for a single range-includes item (map), not a vector of them."
  [range-includes]
  (contains? scalars (:id range-includes)))


(defn all-scalars?
  "Only works with vector (or sequential) range-includes values."
  [range-includes]
  (every? scalar? range-includes))


(defn pick-scalar
  "When multiple scalars are defined for a single property,
  we choose the most flexible type, order of which is defined with
  'priority' var below."
  [range-includes]
  (let [types    (into #{} (keep #(get scalars (:id %)) range-includes))
        priority [:string :uri :dateTime :date :time :double :int :boolean]]
    (some types priority)))


(defn scalar-type
  "Returns with the data type (as keyword, i.e. :text, :integer) or nil if not a scalar.

  Most scalars (i.e. Text, Boolean) will be the only thing listed under schema:rangeIncludes
  and it will therefore be a map.

  There are some properties that have multiple scalars as possible values. When a class is also
  a possible value we will default to the class (type :ref) and not consider the property a scalar
  at all.

  In the case of multiple scalars, i.e.:
  {:iri 'https://schema.org/petsAllowed'
   'https://schema.org/rangeIncludes' [{:id 'Text',
                                        :iri 'https://schema.org/Text'}
                                       {:id 'Boolean',
                                        :iri 'https://schema.org/Booleanv}]
   ...}

   We will pick the most flexible of the scalars as the defined type. In the case
   above a boolean can be represented as text, but text cannot be represented as a
   boolean so we go with text as the type."
  [item]
  (let [range-includes (get item "https://schema.org/rangeIncludes")]
    (if (map? range-includes)
      (get scalars (:id range-includes))
      (when (all-scalars? range-includes)
        (let [type (pick-scalar range-includes)]
          (log/info (str "Multiple scalar value options exist for property: " (:iri item)
                         " including: " (mapv :id range-includes) ". Using type: " type "."))
          type)))))


(defn is-class?
  [item]
  (some #(= "http://www.w3.org/2000/01/rdf-schema#Class" %) (:type item)))


(defn is-property?
  [item]
  (some #(= "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property" %) (:type item)))


(defn is-data-type?
  "Note that data-type should be checked before is-class?, as the data types
  i.e. https://schema.org/Text, have two types defined - DataType and Class:
  {:iri 'https://schema.org/Text'
   :type ['http://www.w3.org/2000/01/rdf-schema#Class' 'https://schema.org/DataType']
   ...}"
  [item]
  (some #(= "https://schema.org/DataType" %) (:type item)))


(defn is-superseded?
  "schema.org has properties that have been superseded by other properties
  and don't have sufficent info for data type with in them. We drop them from the schema."
  [item]
  (contains? item "https://schema.org/supersededBy"))


(defn item-type
  "Returns one of the possible 4 item types in schema.org we care about.
  - :data-type - i.e. Text, DateTime, etc. We want to ignore (remove) these as we use our internal data types.
  - :class - A class, i.e. Person
  - :property - A property, i.e. 'name'
  - :member - An instance/member of a class. In schema.org these only consist of Enum values, i.e. Male/Female"
  [item]
  (cond
    (is-data-type? item) :data-type
    (is-class? item) :class
    (is-property? item) :property
    :else :member))


(defn restrict-collections
  [item ctx]
  (let [range-includes (-> item
                           (get "https://schema.org/rangeIncludes")
                           vocabs/sequential)
        {:keys [class scalar other]} (group-by #(cond
                                                  (scalar? %) :scalar
                                                  (is-class? (get ctx (:id %))) :class
                                                  :else :other)
                                               range-includes)
        thing?         (some #(= "Thing" %) class)]
    (when other
      (throw (ex-info (str "Detected unknown types in https://schema.org/rangeIncludes for item: " item)
                      {:status 400
                       :error  :db/invalid-context})))
    (when (empty? class)
      (throw (ex-info (str "No classes specified in https://schema.org/rangeIncludes for item: " item)
                      {:status 400
                       :error  :db/invalid-context})))
    (if thing?
      nil
      class)))


(defn enrich-range-includes
  "Only does this for properties/predicates, not classes"
  [item base-ctx]
  (let [range-includes       (get item "https://schema.org/rangeIncludes")
        _                    (when-not range-includes
                               (throw (ex-info (str "No https://schema.org/rangeIncludes for: " (:id item))
                                               item)))
        scalar-type          (scalar-type item)
        type                 (or scalar-type :ref)
        restrict-collections (when (= :ref type)
                               (restrict-collections item base-ctx))
        item*                (-> item
                                 (assoc :fluree/type type)
                                 (dissoc "https://schema.org/rangeIncludes"))]
    (cond-> item*
            restrict-collections (assoc :fluree/restrictCollection restrict-collections))))


(defn- shorten-iri
  "If an IRI starts with https://schema.org/, removes so only left with
  unique ID. This works because no properties in schema.org have deeper
  nested namespaces, if that ever changes this logic will need to be changed."
  [iri]
  (if (str/starts-with? iri "https://schema.org/")
    (subs iri (count "https://schema.org/"))
    iri))


(defn is-enum?
  [item ctx]
  (let [parents (:rdfs/subClassOf item)]
    (if (some #(= enum-class (:iri %)) parents)             ;; if iri of :rdfs/subClassOf is equal to enum-class it is an enum
      true
      ;; if not an enum, but has parents, it is possible its parents are enums which makes this an enum.
      (some->> (not-empty parents)
               (keep #(get ctx (:id %)))
               (some #(is-enum? % ctx))))))


(defn enrich-member
  "Enriches member/instance items within the schema. Currently only adds :enum? true/false.

  If an instance/member of a class that is of enum, flags as an enum.

  i.e. https://schema.org/Female is a member of class https://schema.org/GenderType
  which is a subclass of https://schema.org/Enumeration. Therefore the class GenderType
  is intended to hold enum values, and therefore members/instances of GenderType (i.e. Female)
  is an enum value. Note that while GenderType is directly a subclass of Enumeration, there
  could be additional parents inbetween. This will crawl the class hierarchy to the top
  to ensure it does not find any parent of Enumeration type."
  [item base-ctx]
  (let [enum? (->> (:type item)
                   (map shorten-iri)
                   (map #(get base-ctx %))
                   (some #(is-enum? % base-ctx)))]
    (log/warn "Enriching member: " (:id item) "Types: " (->> (:type item)
                                                             (map shorten-iri)

                                                             ))
    (if enum?
      (assoc item :enum? true)
      item)))


(defn schema-is-part-of
  "Adds extra properties as needed"
  [item]
  (if-let [is-part-of (get item "https://schema.org/isPartOf")]
    (-> item
        (assoc-in [:extra "https://schema.org/isPartOf"] (:iri is-part-of))
        (dissoc "https://schema.org/isPartOf"))
    item))


(defn schema-source
  "Adds extra properties as needed"
  [item]
  (if-let [source (get item "https://schema.org/source")]
    (let [sources (->> source
                       vocabs/sequential
                       (mapv :iri))]
      (-> item
          (assoc-in [:extra "https://schema.org/source"] sources)
          (dissoc "https://schema.org/source")))
    item))


(defn enrich-classes
  "Finds parent types so they can be generated"
  [item ctx]
  (let [parent-type (->> (:type item)
                         (filter #(not= "http://www.w3.org/2000/01/rdf-schema#Class" %))
                         (mapv shorten-iri))
        enum?       (is-enum? item ctx)]
    (when (not-empty parent-type)
      (log/warn "Parent type: " parent-type item))

    (cond-> (assoc item :class? true)
            enum? (assoc :enum? true))))


(defn enrich
  [base-context]
  (reduce-kv
    (fn [acc k item]
      (let [type (item-type item)]
        (if (or (= :data-type type)                         ;; we drop all data type definitions (Text, Boolean, etc.) as we have internal typing and they shouldn't be directly used in transactions for data.
                (is-superseded? item))                      ;; superseded by properties skipped
          acc
          (assoc acc k (cond-> item
                               (= :class type) (enrich-classes base-context)
                               (= :property type) (enrich-range-includes base-context)
                               (= :member type) (enrich-member base-context)
                               true schema-is-part-of
                               true schema-source
                               )))))
    {} base-context))


(defn process
  "Reads original json-ld vocabulary, goes through our basic x-form
  process to optimize it for @context reads, and proceeds to 'enrich'
  it with functions in this namespace specific to schema.org vocab,
  lastly writes the output so it can be easily picked up by a transactor
  when processing a transaction."
  []
  (let [ontology    "https://schema.org"
        output-file "resources/contexts/schema.org.edn"]
    (-> (vocabs/load-ontology ontology)
        (vocabs/onotology->context ontology)
        enrich
        (vocabs/save-context output-file))))


(comment

  (process)

  (-> (slurp "resources/contexts/schema.org.edn")
      read-string
      (get "isBasedOn")
      )

  )