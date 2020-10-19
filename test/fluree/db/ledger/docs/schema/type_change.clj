(ns fluree.db.ledger.docs.schema.type-change
  (:require [clojure.test :refer :all]))
;
;
;(deftest int-type-predicate-change-test*
;  (testing "Test allowed type conversions")
;
;  ;; a nil response to predicate-change-error means no problems.
;  ;; else it returns a map of the error, with a :status of 400
;
;    ;; valid: int -> long
;    (is (empty? (clojure.core.async/<!! (predicate-change-error {0 {:type {:old :int :new :long}}} {}))))
;
;    ;; valid: int -> instant
;    (is (empty? (clojure.core.async/<!! (predicate-change-error {0 {:type {:old :int :new :instant}}} {}))) )
;
;    ;; valid: int -> bigint
;    (is (empty? (clojure.core.async/<!! (predicate-change-error  {0 {:type {:old :int :new :bigint}}} {}))))
;
;    ;; valid: int -> double
;    (is (empty? (clojure.core.async/<!! (predicate-change-error  {0 {:type {:old :int :new :double}}} {}))))
;
;    ;; valid: int -> bigdec
;    (is (empty? (clojure.core.async/<!! (predicate-change-error  {0 {:type {:old :int :new :bigdec}}} {}))))
;
;    ;; invalid: int -> string
;    (is (nil? (clojure.core.async/<!! (predicate-change-error {0 {:type {:old :int :new :string}}} {})))))
;


;;(deftest long-type-predicate-change-test*
;;  (testing "Test allowed type conversions from long")
;;  ;; invalid: long -> int
;;  (is (= 400 (:status (predicate-change-error {0 {:type {:old :long :new :int}}} {}))))
;;
;;  ;; valid: long -> bigint
;;  (is (nil? (predicate-change-error {0 {:type {:old :long :new :bigint}}} {})))
;;
;;  ;; valid: long -> double
;;  (is (nil? (predicate-change-error {0 {:type {:old :long :new :double}}} {})))
;;
;;  ;; valid: long -> bigdec
;;  (is (nil? (predicate-change-error {0 {:type {:old :long :new :bigdec}}} {})))
;;
;;  ;; invalid: long -> string
;;  (is (= 400 (:status (predicate-change-error {0 {:type {:old :long :new :string}}} {}))))
;;
;;  ;; valid: long -> instant
;;  (is (nil? (predicate-change-error {0 {:type {:old :long :new :instant}}} {}))))
;;
;;(deftest type-predicate-change-test*
;;  (testing "Test other allowed type conversions")
;;  ;; invalid: bigint -> anything
;;  (is (= 400 (:status (predicate-change-error {0 {:type {:old :bigint :new :anything}}} {}))))
;;
;;  ;; invalid: string -> anything
;;  (is (= 400 (:status (predicate-change-error {0 {:type {:old :string :new :anything}}} {}))))
;;
;;  ;; invalid: bigdec -> anything
;;  (is (= 400 (:status (predicate-change-error {0 {:type {:old :bigdec :new :anything}}} {}))))
;;
;;  ;; valid: double -> bigdec
;;  (is (nil? (predicate-change-error {0 {:type {:old :double :new :bigdec}}} {})))
;;
;;  ;; invalid: double -> randomLetters
;;  (is (= 400 (:status (predicate-change-error {0 {:type {:old :double :new :afdhdfgdf}}} {}))))
;;
;;  ;; valid: instant -> long
;;  (is (nil? (predicate-change-error {0 {:type {:old :instant :new :long}}} {})))
;;
;;  ;; valid: instant -> bigint
;;  (is (nil? (predicate-change-error {0 {:type {:old :instant :new :bigint}}} {})))
;;
;;  ;; invalid: instant -> string
;;  (is (= 400 (:status (predicate-change-error {0 {:type {:old :instant :new :string}}} {})))))
;;
;;
;;(deftest multi-predicate-change-test*
;;  (testing "Test adding and changing :multi predicate")
;;  ;; Valid: multi -> false in new _predicate
;;  (is (nil? (predicate-change-error {0 {:new? true :multi {:new false :old nil}}} {})))
;;
;;  ;; Valid: multi -> true in new _predicate
;;  (is (nil? (predicate-change-error {0 {:new? true :multi {:new true :old nil}}} {})))
;;
;;  ;; Valid: multi -> true in existing _predicate, from multi = false
;;  (is (nil? (predicate-change-error {0 {:new? false :multi {:new true :old false}}} {})))
;;
;;  ;; Valid: multi -> true in existing _predicate, from multi = nil
;;  (is (nil? (predicate-change-error {0 {:new? false :multi {:new true :old nil}}} {})))
;;
;;  ;; invalid: multi -> false in existing _predicate
;;  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Cannot convert an _predicate from multi-cardinality to single-cardinality."
;;                        (predicate-change-error {0 {:multi {:new false :old true}}} {} true))))
;;
;;(deftest unique-predicate-change-test*
;;  (testing "Test adding and changing :unique predicate")
;;
;;  ;; invalid: unique -> true in existing _predicate
;;  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Cannot convert an _predicate from non-unique to unique."
;;                        (predicate-change-error {0 {:new? false, :unique {:new true}}} {} true)))
;;
;;  ;; Valid unique -> true in new predicate
;;  (is (nil? (predicate-change-error {0 {:new? true :unique {:new true :old nil}}} {})))
;;
;;  ;; Valid unique -> false in new predicate
;;  (is (nil? (predicate-change-error {0 {:new? true :unique {:new false :old nil}}} {})))
;;
;;  ;; Valid unique -> false in existing predicate
;;  (is (nil? (predicate-change-error {0 {:new? true :unique {:new false :old true}}} {}))))
;;
;;(deftest component-predicate-change-test*
;;  (testing "Test adding and changing :unique predicate")
;;
;;  ;;; invalid: component -> true in existing _ predicate, from false
;;  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Cannot convert an _predicate from a component to a non-component."
;;                        (predicate-change-error {0 {:new? false :component {:old false :new true}}} {} true)))
;;
;;  ;;; invalid: component -> true in existing _ predicate, from nil
;;  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Cannot convert an _predicate from a component to a non-component."
;;                        (predicate-change-error {0 {:new? false :component {:old nil :new true}}} {} true)))
;;
;;  ;;; invalid: component -> true, when type != ref in new _ predicate
;;  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"A component _predicate must be of type \"ref.\""
;;                        (predicate-change-error {0 {:new? true :type {:new "int"} :component {:old nil :new true}}} {} true)))
;;
;;  ;; Valid component -> true, when type = ref in new _ predicate
;;  (is (nil? (predicate-change-error {0 {:new? true :type {:new :ref} :component {:old nil :new true}}} {} true))))
;;

