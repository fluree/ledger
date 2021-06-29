(ns fluree.db.ledger.export
  (:require [clojure.java.io :as io]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async]
            [fluree.db.api :as fdb]
            [fluree.db.query.range :as query-range]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.flake :as flake]
            [fluree.db.dbproto :as dbproto]
            [clojure.string :as str]
            [clojure.data.xml :as xml]
            [fluree.db.util.core :as util]))

(defn sid->cname
  [db sid]
  (->> (flake/sid->cid sid)
       (dbproto/-c-prop db :name)))

(defn format-subject
  [s]
  (str/replace s "-" "_tx"))

(defn format-predicate
  [db p]
  (-> (dbproto/-p-prop db :name p)
      (str/replace "/" "-")
      (#(str "fdb:" %))))

(defn format-object-ttl
  [pred-type o]
  (condp = pred-type
    :tag (str "fdb:" o)
    :ref (str "fdb:" (format-subject o))
    :string (str "\"" o "\"")
    pred-type o))

(defn format-object-xml
  [pred-type p o]
  (let [tag (keyword p)]
    (condp = pred-type
      :tag (xml/element tag {:rdf:resource (str "fdb:" (format-subject o))})
      :ref (xml/element tag {:rdf:resource (str "fdb:" (format-subject o))})
      pred-type (xml/element tag {} o))))

(defn fdb-prefix
  [network dbid block]
  (str "http://www.flur.ee/fdb/" network "/" dbid "/" block "#"))

(defn get-prefixes-ttl
  [network dbid block]
  (str "@prefix fdb: <" (fdb-prefix network dbid block) ">."
       "\n@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.\n"))

(defn wrap-xml-fn
  [tag content]
  (fn [& args]
    (apply xml/element tag content args)))

(defn get-prefixes-xml-fn
  [network dbid block]
  (wrap-xml-fn :rdf:RDF
               {:xmlns:rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                :xmlns:fdb (fdb-prefix network dbid block)}))

;; I/O

(defn write-to-file
  [file-path text]
  (with-open [w (io/writer file-path :append true)]
    (.write w text)))


(defn write-xml-to-file
  [xml-data file-path]
  (with-open [out-file (java.io.FileWriter. file-path)]
    (xml/emit xml-data out-file)))

(defn rdf-type->xml
  [fdb type]
  (xml/element :rdf:type {:rdf:resource (str fdb type)}))

(defn subject->xml
  [db flakes fdb-pfx]
  (let [s               (-> flakes first .-s)
        s'              (format-subject s)
        wrap-subject-fn (wrap-xml-fn :rdf:Description {:rdf:about (str fdb-pfx s')})
        rdf-type        (if (neg-int? s)
                          [(rdf-type->xml fdb-pfx "_tx")
                           (rdf-type->xml fdb-pfx "_block")]
                          [(rdf-type->xml fdb-pfx (sid->cname db s))])
        pred-objs       (loop [[flake & r] flakes
                               acc rdf-type]
                          (if (not flake)
                            acc
                            (let [p         (.-p flake)
                                  p'        (format-predicate db p)
                                  pred-type (dbproto/-p-prop db :type p)
                                  xml-elem  (format-object-xml pred-type p' (.-o flake))]
                              (recur r (conj acc xml-elem)))))]
    (wrap-subject-fn pred-objs)))

;; TODO - Size limit? Can/should we do multiple xml files?
(defn xml->export
  [db network dbid block file-path spot]
  (let [fdb-pfx          (fdb-prefix network dbid block)
        subject-xmls     (loop [[flake & r] spot
                                subject        nil
                                subject-flakes []
                                acc            []]
                           (if (not flake)
                             acc
                             (let [s (.-s flake)]
                               (cond (and (not flake) (empty? subject-flakes))
                                     acc

                                     (not flake)
                                     (let [subject-xml (subject->xml db subject-flakes fdb-pfx)]
                                       (conj acc subject-xml))

                                     (= subject s)
                                     (recur r s (conj subject-flakes flake) acc)

                                     (empty? subject-flakes)
                                     (recur r s (conj subject-flakes flake) acc)

                                     :else
                                     (let [subject-xml (subject->xml db (conj subject-flakes flake) fdb-pfx)]
                                       (recur r s [] (conj acc subject-xml)))))))
        wrap-prefixes-fn (get-prefixes-xml-fn network dbid block)]
    (-> (wrap-prefixes-fn subject-xmls)
        (write-xml-to-file file-path))
    file-path))

;; TTL

(defn ttl->export
  [db network dbid block file-path spot]
  (let [prefixes (get-prefixes-ttl network dbid block)
        _        (write-to-file file-path prefixes)]
    (loop [[flake & r] spot
           current-subject   nil
           current-predicate nil
           acc               ""
           count             0]
      (if (not flake)
        (do (write-to-file file-path (str acc " ."))
            file-path)
        (let [[s p o] [(.-s flake) (.-p flake) (.-o flake)]
              pred-type      (dbproto/-p-prop db :type p)
              s'             (format-subject s)
              p'             (format-predicate db p)
              o'             (format-object-ttl pred-type o)
              same-subject   (= current-subject s')
              same-predicate (= current-predicate p')
              acc'           (cond (and same-subject same-predicate)
                                   (str acc " ,\n\t\t\t\t\t\t\t\t\t" o')

                                   same-subject
                                   (str acc " ;\n\t\t\t\t\t\t" p' "\t\t\t" o')

                                   (neg-int? s)
                                   (str acc (when-not (nil? current-subject) " .")
                                        "\nfdb:" s' "\t\t\trdf:type\t\t\tfdb:_tx .\n"
                                        "\nfdb:" s' "\t\t\trdf:type\t\t\tfdb:_block .\n"
                                        "\nfdb:" s' "\t\t\t" p' "\t\t\t" o')

                                   :else
                                   (let [cname (sid->cname db s)]
                                     (str acc (when-not (nil? current-subject) " .")
                                          "\nfdb:" s' "\t\t\trdf:type\t\t\tfdb:" cname
                                          " ;\n\t\t\t\t\t\t" p' "\t\t\t" o')))
              count'         (inc count)]
          ;; TODO - determine good frequency at which to write to file
          (if (> count 100)
            (do (write-to-file file-path acc')
                (recur r s' p' "" count'))
            (recur r s' p' acc' count')))))))

(defn db->export
  ([db]
   (db->export db :ttl nil))
  ([db type]
   (db->export db type nil))
  ([db type block]
   (go-try (let [db-at-block (if block (<? (time-travel/as-of-block db block)) db)
                 spot        (<? (query-range/index-range db-at-block :spot))
                 {:keys [network dbid block]} db-at-block
                 type        (or type :ttl)
                 file-path   (str "data/exports/" network "/" dbid "/network-dbid-" block "." (util/keyword->str type))
                 _           (io/make-parents file-path)
                 _           (when (.exists (io/as-file file-path))
                               (io/delete-file file-path))]
             (condp = (keyword type)
               :xml (xml->export db network dbid block file-path spot)

               :ttl (ttl->export db network dbid block file-path spot))))))


(comment

  (def db (async/<!! (fdb/db (:conn user/system) "test/one")))
  (async/<!! (db->export db :xml))

  (:conn user/system)
  (:group (:conn user/system))

  )
