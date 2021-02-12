(ns prefix_support
  (:require [fluree.db.api :as fdb]
            [clojure.core.async :as async]))


(def conn (:conn user/system))

(def ledger "prefix/f")

(def db (fdb/db conn ledger))

(defn current-schema []
  (-> conn
      (fdb/db ledger)
      (async/<!!)
      :schema))



(def sample-schema-shacl
  [{"@context"    {"sh"  "http://www.w3.org/ns/shacl#"
                   "xsd" "http://www.w3.org/2001/XMLSchema#"}
    "@id"         ":PersonShape"
    "@type"       "sh:NodeShape"
    "sh:class"    {"@id" ":Person"}
    "sh:property" [{"@id" ":HasGivenName"}
                   {"@id" ":HasName"}
                   {"@id" ":HasGender"}]}
   {"@id"         ":HasGivenName"
    "a"           "sh:PropertyShape"
    "sh:path"     {"@id" "http://schema.org/givenName"}
    "sh:datatype" {"@id" "xsd:string"}
    "sh:minCount" 1
    "sh:maxCount" 1}
   {"@id"         ":HasName"
    "a"           "sh:PropertyShape"
    "sh:path"     {"@id" "http://schema.org/name"}
    "sh:datatype" {"@id" "xsd:string"}
    "sh:minCount" 1
    "sh:maxCount" 1}
   {"@id"         ":HasGender"
    "a"           "sh:PropertyShape"
    "sh:path"     {"@id" "http://schema.org/gender"}
    "sh:datatype" {"@id" "xsd:string"}
    "sh:pattern"  "male|female"
    "sh:minCount" 1}
   ])
;
;:UserShape a sh:NodeShape                                   ;
;sh:targetClass :User                                        ;
;sh:nodeKind sh:IRI                                          ;
;sh:property :HasEmail                                       ;
;sh:property :HasGender                                      ;
;sh:property :MaybeBirthDate                                 ;
;sh:property :KnowsUsers .
;
;:HasEmail sh:path schema:name                               ;
;sh:minCount 1                                               ;
;sh:maxCount 1                                               ;
;sh:datatype xsd:string .
;
;:HasGender sh:path schema:gender                            ;
;sh:minCount 1                                               ;
;sh:maxCount 1                                               ;
;sh:or (
;       [sh:in (schema:Male schema:Female)]
;       [sh:datatype xsd:string]
;       ) .
;
;:MaybeBirthDate sh:path schema:birthDate                    ;
;sh:maxCount 1                                               ;
;sh:datatype xsd:date .
;
;:KnowsUsers sh:path schema:knows                            ;
;sh:class :User .


(def sample-data
  [{"@id"         "ex:person#Brian"
    "rdf:type"    "Student"
    "person:name" "Brian"}])

(def sample-q {:select ["?name"]
               :where  [["?person" "person:name" "?name"]]})


(comment

  @(fdb/transact conn ledger sample-schema-shacl)



  @(fdb/query db {:select [:*] :from "_prefix"})
  @(fdb/query db {:select [:*] :from "_predicate" :limit 3})
  ;; Create a prefixed predicate

  @(->> [{:_id    "_predicate"
          :name   "fluree:test"
          :type   "string"
          :unique true}]
        (fdb/transact conn ledger))

  ;; schema cache
  (-> (current-schema)
      :pred
      keys)

  )