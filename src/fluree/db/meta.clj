(ns fluree.db.meta
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io])
  (:gen-class))


(defn el-content
  "Get the contents of XML element named el-name (keyword) from
  clojure.data.xml-parsed xml"
  [xml el-name]
  (when-let [el (->> xml (filter #(= (:tag %) el-name)) first)]
    (:content el)))


(defn find-pom-xml []
  (let [root-file (io/file "pom.xml")]
    (if (.exists root-file)
      root-file
      ;; If it's not at the project root, assume we're running from a JAR and
      ;; look for it in there.
      (io/resource "META-INF/maven/com.fluree/ledger/pom.xml"))))


(defn version []
  (let [pom-xml (find-pom-xml)
        pom     (-> pom-xml slurp (xml/parse-str :namespace-aware false))
        _       (assert (= :project (:tag pom))
                        (str "pom.xml appears malformed; expected top-level project element; got "
                             (:tag pom) " instead"))
        project (:content pom)]
    (-> project (el-content :version) first)))


(defn -main [cmd & _]
  (case cmd
    "version" (println (version))))
