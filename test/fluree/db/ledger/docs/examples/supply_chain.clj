(ns fluree.db.ledger.docs.examples.supply-chain
  (:require [clojure.test :refer :all]
            [fluree.db.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.tools.reader.edn :as edn]
            [clojure.string :as str]))

(use-fixtures :once test/test-system)

;; Supply Chain Example

(def cafe-opts {:auth        "Tf2j3SoemdjeTfi8t1CxjaYNmUZpWT3A8RD"
                :private-key "8a9077ab011fb152b5a043abc24c535810b5dd1d87ecd6ace7cb454dd046670b"})

(def farm-opts {:auth        "Tf2hxnc1FzAXtmk8ptwQ5V68zJjfd4tLwXL"
                :private-key "5ce7259de6397b6dbdd727fc62e9a920f41fe3017dbd76cba3f8d0c1f0275113"})

(def shipper-opts {:auth        "TfBq3t6AZ6ibCs3uxVAkW6CtPaWy7isrcRG"
                   :private-key "9ba0454eab8057f4e69a27e8ea9ab6344c73f4f1a7a829a9b521869073e92cb7"})

(def roastery-opts {:auth        "TfA6vquJMH65oQttpuURWvGnMdPPdAA69PF"
                    :private-key "36abfcd2da19781550d6c9296ada95e11ef0ebfe9acdf3723e59098dc41fe8a5"})

;; Add schema

(deftest add-schema
  (testing "Add schema for the supply chain app")
  (let [filename    "../test/fluree/db/ledger/Resources/SupplyChain/schema.edn"
        txn         (edn/read-string (slurp (io/resource filename)))
        schema-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn))]

    ;; status should be 200
    (is (= 200 (:status schema-resp)))

    ;; block should be 3
    (is (= 2 (:block schema-resp)))

    ;; there should be 5 _collection tempids
    (is (= 5 (test/get-tempid-count (:tempids schema-resp) "_collection")))

    ;; there should be _predicate tempids
    (is (= 42 (test/get-tempid-count (:tempids schema-resp) "_predicate")))

    ;; there should be a tempid key _fn$s
    (is (true? (contains? (:tempids schema-resp) "_fn$s")))

    ;; there should be 3 tempid keys total
    (is (= 3 (count (keys (:tempids schema-resp)))))))

;; Add sample data

(deftest add-sample-data
  (testing "Add sample data for the supply chain app")
  (let [filename  "../test/fluree/db/ledger/Resources/SupplyChain/data.edn"
        txn       (edn/read-string (slurp (io/resource filename)))
        data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 3
    (is (= 3 (:block data-resp)))

    ;; there should be 4 tempids for collection 'organization'
    (is (= 4 (test/get-tempid-count (:tempids data-resp) "organization")))

    ;; there should be 6 tempid keys
    (is (= 6 (count (keys (:tempids data-resp)))))))


;; Add Block 4 Smart Functions
(deftest add-block-4-smart-functions
  (testing "Add sample data for the supply chain app")
  (let [filename  "../test/fluree/db/ledger/Resources/SupplyChain/smartFunctionBlock4.edn"
        txn       (edn/read-string (slurp (io/resource filename)))
        data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 4
    (is (= 4 (:block data-resp)))

    ;; there should be 13 tempids
    (is (= 13 (count (:tempids data-resp))))))


;; Add Block 5 Smart Functions
(deftest add-block-5-smart-functions
  (testing "Add sample data for the supply chain app")
  (let [filename  "../test/fluree/db/ledger/Resources/SupplyChain/smartFunctionBlock5.edn"
        txn       (edn/read-string (slurp (io/resource filename)))
        data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 5
    (is (= 5 (:block data-resp)))

    ;; there should be 8 tempids
    (is (= 8 (count (:tempids data-resp))))))

;; Add Block 6 Smart Functions
(deftest add-block-6-smart-functions
  (testing "Add sample data for the supply chain app")
  (let [filename  "../test/fluree/db/ledger/Resources/SupplyChain/smartFunctionBlock6.edn"
        txn       (edn/read-string (slurp (io/resource filename)))
        data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 6
    (is (= 6 (:block data-resp)))

    ;; there should be 9 tempids
    (is (= 9 (count (:tempids data-resp))))))


;; Add Block 7 Smart Functions
(deftest add-block-7-smart-functions
  (testing "Add sample data for the supply chain app")
  (let [filename  "../test/fluree/db/ledger/Resources/SupplyChain/smartFunctionBlock7.edn"
        txn       (edn/read-string (slurp (io/resource filename)))
        data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 7
    (is (= 7 (:block data-resp)))

    ;; there should be 7 tempids
    (is (= 7 (count (:tempids data-resp))))))


;; Add Block 8 Smart Functions
(deftest add-block-8-smart-functions
  (testing "Add sample data for the supply chain app")
  (let [filename  "../test/fluree/db/ledger/Resources/SupplyChain/smartFunctionBlock8.edn"
        txn       (edn/read-string (slurp (io/resource filename)))
        data-resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn))]

    ;; status should be 200
    (is (= 200 (:status data-resp)))

    ;; block should be 8
    (is (= 8 (:block data-resp)))

    ;; there should be 0 tempids
    (is (= 0 (count (:tempids data-resp))))))


;; Invalid transaction - Block 9
(deftest invalid-txns-9
  (let [createPO                 {:_id       "purchaseOrder",
                                  :id        "124",
                                  :name      "myPurchaseOrder2",
                                  :issuer    ["organization/name" "The Roastery"],
                                  :issueDate "#(now)",
                                  :product   {:_id           "product",
                                              :id            "a4t57",
                                              :name          "Tuesday Coffee",
                                              :description   "Our regular monday shipment of roasted coffee",
                                              :category      "coffee",
                                              :strain        "Colombian Arabica",
                                              :quantity      100,
                                              :unitOfMeasure "lb"}}
        createPOResp             (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [createPO] roastery-opts))
                                     test/extract-errors)
        createPORespErrors       (->> createPOResp :meta :errors (map :message) (into #{}))
        createPO2                (assoc createPO :issuer ["organization/name" "Coffee on the Block"])
        createPOResp2            (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [createPO2] farm-opts))
                                     test/extract-errors)
        createPOResp2Errors      (->> createPOResp2 :meta :errors (map :message) (into #{}))
        createPO3                (dissoc createPO2 :name)
        createPOResp3            (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [createPO3] cafe-opts))
                                     test/extract-errors)
        createPOResp3Errors      (->> createPOResp3 :meta :errors (map :message) (into #{}))
        createShipment           [{:_id                     "shipment$1",
                                   :name                    "123BeanShip",
                                   :sentBy                  ["organization/name" "McDonald's Farm"],
                                   :intendedRecipient       ["organization/name" "The Roastery"],
                                   :intendedReceiptLocation "Miami, FL",
                                   :sentLocation            "McDonLand",
                                   :itemDescription         "Got the beans harvested!",
                                   :id                      "growShip123",
                                   :sentDate                "#(now)"}]
        createShipmentResp       (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain createShipment farm-opts))
                                     test/extract-errors)
        createShipmentRespErrors (->> createShipmentResp :meta :errors (map :message) (into #{}))]

    ;; should be 2 errors
    (is (= 2 (count createPORespErrors)))
    (is (contains? createPORespErrors "Predicate spec failed for predicate: purchaseOrder/name. Only a cafe can add or edit purchaseOrder/name."))
    (is (contains? createPORespErrors "Predicate spec failed for predicate: purchaseOrder/id. Only a cafe can create a purchaseOrder/id, and it's unchangeable."))

    ;; should be 3 errors
    (is (= 3 (count createPOResp2Errors)))
    (is (contains? createPOResp2Errors "Predicate spec failed for predicate: purchaseOrder/name. Only a cafe can add or edit purchaseOrder/name."))
    (is (contains? createPOResp2Errors "Predicate spec failed for predicate: purchaseOrder/id. Only a cafe can create a purchaseOrder/id, and it's unchangeable."))
    (is (contains? createPOResp2Errors "Predicate spec failed for predicate: purchaseOrder/issuer. Only organization can add self to purchaseOrder/issuer."))

    (is (contains? createPOResp3Errors "Collection spec failed for: purchaseOrder. Required predicates: id, product, issuer, issueDate, name."))

    (is (= 2 (count createShipmentRespErrors)))
    (is (contains? createShipmentRespErrors "Collection spec failed for: shipment. Required shipment predicates: id, name, sentBy, sentDate, sentLocation, itemDescription, intendedRecipient, intendedRecipientLocation. Can't create a shipment, unless it's connected to a purchaseOrder. Can't add shipper, or GPSLocation, unless you have sentSignature, shipper, and GPSLocation."))))


;; Valid transaction - Block 9
(deftest valid-txn-9
  (let [txn                [{:_id       "purchaseOrder",
                             :id        "123",
                             :name      "myPurchaseOrder",
                             :issuer    ["organization/name" "Coffee on the Block"],
                             :issueDate "#(now)",
                             :product   {:_id           "product",
                                         :id            "a4t56",
                                         :name          "Monday Coffee",
                                         :description   "Our regular monday shipment of roasted coffee",
                                         :category      "coffee",
                                         :strain        "Colombian Arabica",
                                         :quantity      100,
                                         :unitOfMeasure "lb"}}]
        createShipmentResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn cafe-opts))]

    (is (= 200 (:status createShipmentResp)))

    ;; should be 3 tempid keys, 'purchaseOrder', 'product' and a few new '_tag' get generated.
    (is (= 3 (count (keys (:tempids createShipmentResp)))))

    (is (= 22 (-> createShipmentResp :flakes count)))))

;; Invalid transaction - Block 10
(deftest invalid-txns-10
  (let [addGrowerToPO      [{:_id ["purchaseOrder/id" "123"], :grower ["organization/name" "McDonald's Farm"]}]
        addGrowerToPOResp  (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain addGrowerToPO farm-opts))
                               test/safe-Throwable->map
                               :cause)
        createShipment     [{:_id                     "shipment$1",
                             :name                    "123BeanShip",
                             :sentBy                  ["organization/name" "McDonald's Farm"],
                             :intendedRecipient       ["organization/name" "The Roastery"],
                             :intendedReceiptLocation "Miami, FL",
                             :sentLocation            "McDonLand",
                             :itemDescription         "Got the beans harvested!",
                             :id                      "growShip123",
                             :sentDate                "#(now)"}
                            {:_id ["purchaseOrder/id" "123"], :shipments ["shipment$1"]}]
        createShipmentResp (-> (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain createShipment cafe-opts))
                               test/safe-Throwable->map
                               :cause)]


    (is (= addGrowerToPOResp "Predicate spec failed for predicate: purchaseOrder/grower. Only the grower, themselves, can add or edit purchaseOrder/grower."))

    (is (= createShipmentResp "Collection spec failed for: shipment. Required shipment predicates: id, name, sentBy, sentDate, sentLocation, itemDescription, intendedRecipient, intendedRecipientLocation. Can't create a shipment, unless it's connected to a purchaseOrder. Can't add shipper, or GPSLocation, unless you have sentSignature, shipper, and GPSLocation."))))

;; Valid transaction - Block 10
(deftest valid-txn-10
  (let [txn          [{:_id ["purchaseOrder/id" "123"], :approved [["organization/name" "Coffee on the Block"]]}]
        approvedResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn cafe-opts))]

    (is (= 200 (-> approvedResp :status)))

    (is (= 0 (-> approvedResp :tempids count)))

    (is (= 7 (-> approvedResp :flakes count)))))

;; Valid transaction - Block 11
(deftest valid-txn-11
  (let [txn           [{:_id ["purchaseOrder/id" "123"], :grower ["organization/name" "McDonald's Farm"]}]
        addGrowerResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn farm-opts))]

    (is (= 200 (-> addGrowerResp :status)))

    (is (= 0 (-> addGrowerResp :tempids count)))

    (is (= 7 (-> addGrowerResp :flakes count)))))

;; Valid transaction - Block 12
(deftest valid-txn-12
  (let [txn                [{:_id ["purchaseOrder/id" "123"], :harvestDate "#(now)", :approved [["organization/name" "McDonald's Farm"]]}]
        addHarvestDateResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn farm-opts))]

    (is (= 200 (-> addHarvestDateResp :status)))

    (is (= 0 (-> addHarvestDateResp :tempids count)))

    (is (= 8 (-> addHarvestDateResp :flakes count)))))

;; Valid transaction - Block 13
(deftest valid-txn-13
  (let [txn                [{:_id ["purchaseOrder/id" "123"], :shipments ["shipment$1"]}
                            {:_id                     "shipment$1",
                             :sentSignature           ["organization/name" "McDonald's Farm"],
                             :name                    "123BeanShip",
                             :sentBy                  ["organization/name" "McDonald's Farm"],
                             :intendedRecipient       ["organization/name" "The Roastery"],
                             :intendedReceiptLocation "Miami, FL",
                             :sentLocation            "McDonLand",
                             :itemDescription         "Got the beans harvested!",
                             :id                      "growShip123",
                             :sentDate                "#(now)"}]
        createShipmentResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn farm-opts))]

    (is (= 200 (-> createShipmentResp :status)))

    (is (= 1 (-> createShipmentResp :tempids count)))

    (is (= 17 (-> createShipmentResp :flakes count)))))


;; Valid transaction - Block 14 - 17
(deftest valid-txn-14-to-17
  (let [shipment  {:_id ["shipment/id" "growShip123"], :shipper ["organization/name" "Ship Shape"], :GPSLocation "9.0817276,-79.5932235"}
        ship1Resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [shipment] shipper-opts))
        shipment2 (assoc shipment :GPSLocation "18.1187026,-78.3974237")
        ship2Resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [shipment2] shipper-opts))
        shipment3 (assoc shipment :GPSLocation "24.3780994,-80.4615216")
        ship3Resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [shipment3] shipper-opts))
        shipment4 (assoc shipment :GPSLocation "25.7825453,-80.2994987")
        ship4Resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [shipment4] shipper-opts))]

    (is (= 200 (-> ship1Resp :status)))
    (is (= 200 (-> ship2Resp :status)))
    (is (= 200 (-> ship3Resp :status)))
    (is (= 200 (-> ship4Resp :status)))

    (is (= 0 (-> ship1Resp :tempids count)))
    (is (= 0 (-> ship2Resp :tempids count)))
    (is (= 0 (-> ship3Resp :tempids count)))
    (is (= 0 (-> ship4Resp :tempids count)))

    (is (= 8 (-> ship1Resp :flakes count)))
    (is (= 8 (-> ship2Resp :flakes count)))
    (is (= 8 (-> ship3Resp :flakes count)))
    (is (= 8 (-> ship4Resp :flakes count)))))

;; Valid transaction - Block 18 - 20
(deftest valid-txn-18-to-20
  (let [receiveShipment     [{:_id ["shipment/id" "growShip123"], :receivedSignature ["organization/name" "The Roastery"]}]
        receiveShipmentResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain receiveShipment roastery-opts))
        addRoastInfo        [{:_id       ["purchaseOrder/id" "123"],
                              :roaster   ["organization/name" "The Roastery"],
                              :roastDate "#(now)",
                              :approved  [["organization/name" "The Roastery"]]}]
        addRoastInfoResp    (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain addRoastInfo roastery-opts))
        createShipment      [{:_id       ["purchaseOrder/id" "123"],
                              :shipments [{:_id                     "shipment",
                                           :sentSignature           ["organization/name" "The Roastery"],
                                           :name                    "123RoastShip",
                                           :sentBy                  ["organization/name" "The Roastery"],
                                           :intendedRecipient       ["organization/name" "Coffee on the Block"],
                                           :intendedReceiptLocation "Portland, OR",
                                           :sentLocation            "Miami, FL",
                                           :itemDescription         "Got the beans roasted!",
                                           :id                      "roasterShip123",
                                           :sentDate                "#(now)"}]}]
        createShipmentResp  (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain createShipment roastery-opts))]

    (is (= 200 (-> receiveShipmentResp :status)))
    (is (= 200 (-> addRoastInfoResp :status)))
    (is (= 200 (-> createShipmentResp :status)))

    (is (= 0 (-> receiveShipmentResp :tempids count)))
    (is (= 0 (-> addRoastInfoResp :tempids count)))
    (is (= 1 (-> createShipmentResp :tempids count)))

    (is (= 7 (-> receiveShipmentResp :flakes count)))
    (is (= 9 (-> addRoastInfoResp :flakes count)))
    (is (= 17 (-> createShipmentResp :flakes count)))))


;; Valid transaction - Block 21 - 24
(deftest valid-txn-21-to-24
  (let [shipment  {:_id         ["shipment/id" "roasterShip123"],
                   :shipper     ["organization/name" "Ship Shape"],
                   :GPSLocation "32.8205865,-96.8716267"}
        ship1Resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [shipment] shipper-opts))
        shipment2 (assoc shipment :GPSLocation "38.9764554,-107.7937101")
        ship2Resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [shipment2] shipper-opts))
        shipment3 (assoc shipment :GPSLocation "38.4162652,-121.5129772")
        ship3Resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [shipment3] shipper-opts))
        shipment4 (assoc shipment :GPSLocation "45.5426225,-122.7944694")
        ship4Resp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain [shipment4] shipper-opts))]

    (is (= 200 (-> ship1Resp :status)))
    (is (= 200 (-> ship2Resp :status)))
    (is (= 200 (-> ship3Resp :status)))
    (is (= 200 (-> ship4Resp :status)))

    (is (= 0 (-> ship1Resp :tempids count)))
    (is (= 0 (-> ship2Resp :tempids count)))
    (is (= 0 (-> ship3Resp :tempids count)))
    (is (= 0 (-> ship4Resp :tempids count)))

    (is (= 8 (-> ship1Resp :flakes count)))
    (is (= 8 (-> ship2Resp :flakes count)))
    (is (= 8 (-> ship3Resp :flakes count)))
    (is (= 8 (-> ship4Resp :flakes count)))))

(deftest block-25-close-PO
  (let [txn       [{:_id ["shipment/id" "roasterShip123"], :receivedSignature ["organization/name" "Coffee on the Block"]}
                   {:_id ["purchaseOrder/id" "123"], :closed ["organization/name" "Coffee on the Block"]}]
        closeResp (async/<!! (fdb/transact-async (basic/get-conn) test/ledger-supplychain txn cafe-opts))]

    (is (= 200 (-> closeResp :status)))

    (is (= 0 (-> closeResp :tempids count)))

    (is (= 8 (-> closeResp :flakes count)))))

(deftest supply-chain-test
  (add-schema)
  (add-sample-data)
  (add-block-4-smart-functions)
  (add-block-5-smart-functions)
  (add-block-6-smart-functions)
  (add-block-7-smart-functions)
  (add-block-8-smart-functions)
  (invalid-txns-9)
  (valid-txn-9)
  (invalid-txns-10)
  (valid-txn-10)
  (valid-txn-11)
  (valid-txn-12)
  (valid-txn-13)
  (valid-txn-14-to-17)
  (valid-txn-18-to-20)
  (valid-txn-21-to-24)
  (block-25-close-PO))
