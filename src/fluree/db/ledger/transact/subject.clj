(ns fluree.db.ledger.transact.subject)


(defprotocol ITxSubject
  (-iri [this] "Returns the IRI of the subject")
  (-iri-full [this] "Expands IRI's prefix if prefix defined")
  (-id [this] "Returns the internal Fluree integer ID of the subject")
  (-children [this] "Returns any embedded children subjects as new Subjects")
  (-flakes [this] "Returns list of flakes")
  (-retractions [this] "Returns retraction flakes")
  (-flatten [this] "Finds any nested transactions and returns new Subjects"))


