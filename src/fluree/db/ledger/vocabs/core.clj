(ns fluree.db.ledger.vocabs.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [fluree.db.util.json :as json]
            [fluree.db.util.iri :as iri-util]
            [clojure.pprint :as pprint]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]))


(defn sequential
  "Takes input x, and if not already sequential, makes it so."
  [x]
  (when x
    (if (sequential? x)
      x
      [x])))


(defn remove-base-iri
  [full-iri base-iri]
  (if (and base-iri (str/starts-with? full-iri base-iri))
    (subs full-iri (count base-iri))
    full-iri))

(defn parse-language-items
  "Parses a language item into a map containing they
  keyword of the language as the key and the value the string."
  [language-items default-lang]
  (let [language-items* (if (string? language-items)
                          [{"@language" default-lang
                            "@value" language-items}]
                          (sequential language-items))]
    (reduce
      #(let [lang (get %2 "@language")
             val (get %2 "@value")]
         (when-not (and lang val)
           (throw (ex-info (str "Parsing language item but does not contain both @language and @value: " language-items)
                           {:status 400 :error :db/invalid-context})))
         (assoc %1 (keyword lang) val))
     {} language-items*)))


;; TODO - see if same or can combine with normalize-context above
(defn expand-item
  "Expands a single item inside of a json-ld @graph"
  [item context base-iri]
  (try*
    (let [item-context (if-let [ctx-i (get item "@context")]
                         (iri-util/expanded-context ctx-i context)
                         context)
          [iri id] (when-let [id (get item "@id")]
                     (let [iri (iri-util/expand id item-context)]
                       [iri (remove-base-iri iri base-iri)]))
          type         (some->> (get item "@type")
                                sequential
                                (mapv #(iri-util/expand % item-context)))
          base         (cond-> {}
                               id (assoc :id id
                                         :iri iri)
                               type (assoc :type type))]
      (reduce-kv (fn [acc k v]
                   (if (#{"@id" "@type" "@context"} k)      ;; ignore these, they were included with 'base'
                     acc
                     (let [k* (iri-util/expand k item-context)
                           k** (cond
                                 (= "http://www.w3.org/2000/01/rdf-schema#subClassOf" k*)
                                 :rdfs/subClassOf

                                 (= "http://www.w3.org/2000/01/rdf-schema#comment" k*)
                                 :rdfs/comment

                                 (= "http://www.w3.org/2000/01/rdf-schema#label" k*)
                                 :rdfs/label

                                 :else
                                 k*)
                           v* (cond
                                (= :rdfs/label k**) (parse-language-items v "en")
                                (= :rdfs/subClassOf k**) (mapv #(expand-item % item-context base-iri) (sequential v))
                                (= "@list" k) (mapv #(expand-item % item-context base-iri) v)
                                (= \@ (first k)) v
                                (map? v) (expand-item v item-context base-iri)
                                (sequential? v) (mapv #(expand-item % item-context base-iri) v)
                                :else (iri-util/expand v item-context))]
                       (assoc acc k** v*))))
                 base item))
    (catch* e
            (log/error e (str "Error parsing json-ld item: " item))
            (throw e))))


(defn expand-graph
  "Expands a JSON-LD document. Either can be a map with a @context and @graph key, or
   can be a vector of elements (just the @graph part).

   If a base-iri is included, the map will include not the full iri, but the iri excluding the
   base."
  ([json-ld] (expand-graph json-ld nil))
  ([json-ld base-iri]
   (let [map?      (map? json-ld)
         context   (when map?
                     (get json-ld "@context"))
         graph     (if map?
                     (get json-ld "@graph")
                     json-ld)
         context*  (if context
                     (iri-util/expanded-context context)
                     {})
         base-iri* (str base-iri "/")]                      ;; add trailing slash to base-iri for matching/removing
     (reduce (fn [acc item]
               (let [item* (expand-item item context* base-iri*)]
                 (assoc acc (:id item*) item*)))
             {} graph))))


(defn load-ontology
  ([iri] (load-ontology iri "ontologies/"))
  ([iri ontology-dir]
   (let [path     (second (str/split iri #"://"))           ;; remove i.e. http://, or https://
         ontology (some-> (str ontology-dir path ".json")
                          io/resource
                          slurp)]
     (json/parse ontology false))))


(defn onotology->context
  [ontology base-iri]
  (expand-graph ontology base-iri))


(defn save-context
  [context filename]
  (->> (pprint/pprint context)
       with-out-str
       (spit filename)))



