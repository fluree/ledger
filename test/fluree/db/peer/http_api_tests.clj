(ns fluree.db.peer.http-api-tests
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [fluree.db.peer.http-api :as http-api]
            [fluree.db.test-helpers :as test]
            [fluree.db.util.json :as json]
            [byte-streams :as bs])
  (:import (java.io ByteArrayInputStream)
           (org.httpkit BytesInputStream)
           (clojure.lang ExceptionInfo)))

(use-fixtures :once test/test-system-deprecated)

;; helper functions
(defn object->stream
 [obj]
  (let [ba (if (bytes? obj)
             obj
             (json/stringify-UTF8 obj))]
    (BytesInputStream. ba (count ba))))


(deftest http-api-tests
  (testing "generate password auth record for"
    (testing "user with root permissions"
      (let [rqst-base {:params {:action  :generate
                                :network "fluree"
                                :db      "invoice"}}
            resp      (->> {:password     "fluree"
                            :user         "freddie"
                            :create-user? false}
                           object->stream
                           (assoc rqst-base :body)
                           (http-api/password-handler test/system))
            token     (some-> resp
                              :body
                              bs/to-string
                              json/parse)]
        (is (= 200 (:status resp)))
        (is (string? token))))
    (testing "user with level1User permissions (1)"
      (let [rqst-base {:params {:action  :generate
                                :network "fluree"
                                :db      "invoice"}}
            resp      (->> {:password     "fluree"
                            :user         "scott"
                            :create-user? false}
                           object->stream
                           (assoc rqst-base :body)
                           (http-api/password-handler test/system))
            token     (some-> resp
                              :body
                              bs/to-string
                              json/parse)]
        (is (= 200 (:status resp)))
        (is (string? token))))
    (testing "user with level1User permissions (2)"
      (let [rqst-base {:params {:action  :generate
                                :network "fluree"
                                :db      "invoice"}}
            resp      (->> {:password     "fluree"
                            :user         "antonio"
                            :create-user? false}
                           object->stream
                           (assoc rqst-base :body)
                           (http-api/password-handler test/system))
            token     (some-> resp
                              :body
                              bs/to-string
                              json/parse)]
        (is (= 200 (:status resp)))
        (is (string? token)))))
  (testing "login via password auth using"
    (testing "invalid password"
      (let [rqst {:params {:action  :login
                           :network "fluree"
                           :db      "invoice"}
                  :body   (object->stream {:password "invalid" :user "freddie"})}]
        (is (thrown-with-msg? ExceptionInfo #"Invalid password" (http-api/password-handler test/system rqst)))))
    (testing "valid password"
      (let [rqst-base {:params {:action  :login
                                :network "fluree"
                                :db      "invoice"}}
            resp      (->> {:password     "fluree"
                            :user         "freddie"}
                           object->stream
                           (assoc rqst-base :body)
                           (http-api/password-handler test/system))
            token     (some-> resp
                              :body
                              bs/to-string
                              json/parse)]
        (is (= 200 (:status resp)))
        (is (string? token)))))
  (testing "renew password auth token"
    (testing "without valid header"
      (let [renew-rqst  {:params {:action  :renew
                                  :network "fluree"
                                  :db      "invoice"}
                         :body (object->stream {:expire 600000})}]
        (is (thrown-with-msg?
              ExceptionInfo #"A valid JWT token must be supplied in the header for a token renewal."
              (http-api/password-handler test/system renew-rqst)))))
    (testing "with valid header"
      (let [login-rqst  {:params {:action  :login
                                  :network "fluree"
                                  :db      "invoice"}}
            login-resp  (->> {:password     "fluree"
                              :user         "scott"}
                             object->stream
                             (assoc login-rqst :body)
                             (http-api/password-handler test/system))
            token       (some-> login-resp
                                :body
                                bs/to-string
                                json/parse)
            renew-rqst  {:params {:action  :renew
                                  :network "fluree"
                                  :db      "invoice"}
                         :headers {"authorization" (str "Bearer " token)}}
            renew-resp  (->> {:jwt token
                              :expire 600000}
                             object->stream
                             (assoc renew-rqst :body)
                             (http-api/password-handler test/system))
            token-2     (some-> renew-resp
                                :body
                                bs/to-string
                                json/parse)]
        (is (= 200 (:status login-resp) (:status renew-resp)))
        (is (string? token))
        (is (string? token-2))
        (is (not= token token-2))))))

