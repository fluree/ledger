(ns planeDemo
  (:require [fluree.db.util.core :as util]
            [clojure.string :as str]
            [fluree.db.util.async :refer [<? <?? go-try channel?]]
            [fluree.db.util.log :as log]
            [fluree.db.api :as fdb]
            [fluree.db.util.json :as json]))

;; Use as starting point for randomly-generated data, so that once we generate a database, txns are
;; added in real-time

(def month-start 1564848328000)

(def schema-txn [{:_id "_collection", :name "airline"}
                 {:_id "_collection", :name "plane"}
                 {:_id "_collection", :name "person"}
                 {:_id "_collection", :name "repair"}
                 {:_id "_collection", :name "section"}
                 {:_id "_collection", :name "part"}
                 {:_id "_collection", :name "trip"}
                 {:_id "_collection", :name "location"}
                 {:_id "_collection", :name "issue"}
                 {:_id "_collection", :name "manufacturer"}
                 {:_id "_collection", :name "airport"}

                 {:_id "_predicate", :name "repair/name", :type "string", :unique true}
                 {:_id "_predicate", :name "repair/description", :type "string"}
                 {:_id "_predicate", :name "repair/startDate", :type "instant"}
                 {:_id "_predicate", :name "repair/endDate", :type "instant"}
                 {:_id "_predicate", :name "repair/plane", :type "ref", :restrictCollection "plane"}
                 {:_id "_predicate", :name "repair/part", :type "ref", :restrictCollection "part"}
                 {:_id "_predicate", :name "repair/leadPerson", :type "ref", :restrictCollection "person"}
                 {:_id "_predicate", :name "repair/location", :type "ref", :restrictCollection "airport"}

                 {:_id "_predicate", :name "person/roles", :type "string", :multi true}
                 {:_id "_predicate", :name "person/id", :type "int", :unique true}
                 {:_id "_predicate", :name "person/homeLocation", :type "ref", :restrictCollection "airport"}

                 {:_id "_predicate", :name "airline/name", :type "string", :unique true}
                 {:_id "_predicate", :name "airline/planes", :type "ref", :restrictCollection "plane", :multi true}

                 {:_id "_predicate", :name "plane/id", :type "int", :unique true}
                 {:_id "_predicate", :name "plane/name", :type "string", :unique true}
                 {:_id "_predicate", :name "plane/type", :type "tag"}
                 {:_id "_predicate", :name "plane/manufacturer", :type "ref" :restrictCollection "manufacturer"}
                 {:_id "_predicate", :name "plane/manufactureDate", :type "instant"}
                 {:_id "_predicate", :name "plane/trips", :type "ref", :multi true, :restrictCollection "trip"}
                 {:_id "_predicate", :name "plane/sections", :type "ref", :multi true, :restrictCollection "section"}

                 {:_id "_predicate", :name "section/id", :type "string", :unique true}
                 {:_id "_predicate", :name "section/type", :type "tag"}
                 {:_id "_predicate", :name "section/parts", :type "ref", :multi true, :restrictCollection "part"}

                 {:_id "_predicate", :name "part/serial-number", :type "string", :unique true}
                 {:_id "_predicate", :name "part/type", :type "tag"}
                 {:_id "_predicate", :name "part/installDate", :type "instant"}
                 {:_id "_predicate", :name "part/original", :type "boolean"}
                 {:_id "_predicate", :name "part/removeDate", :type "instant"}
                 {:_id "_predicate", :name "part/manufacturer", :type "ref", :restrictCollection "manufacturer"}

                 {:_id "_predicate", :name "manufacturer/name", :type "string", :unique true}
                 {:_id "_predicate", :name "manufacturer/auth", :type "ref", :restrictCollection "_auth" :multi true}

                 {:_id "_predicate", :name "trip/id", :type "string", :unique true}
                 {:_id "_predicate", :name "trip/startLocation", :type "ref" :restrictCollection "airport"}
                 {:_id "_predicate", :name "trip/endLocation", :type "ref" :restrictCollection "airport"}
                 {:_id "_predicate", :name "trip/startDate", :type "instant" :index true}
                 {:_id "_predicate", :name "trip/endDate", :type "instant" :index true}
                 {:_id "_predicate", :name "trip/crew", :type "ref" :restrictCollection "person" :multi true}

                 {:_id "_predicate", :name "airport/name", :type "string", :unique true}
                 {:_id "_predicate", :name "airport/abbreviation", :type "string", :unique true}
                 {:_id "_predicate", :name "airport/location", :type "geojson"}
                 {:_id    "_fn"
                  :name   "lastFlightDate"
                  :params ["id"]
                  :code   "(query (str \"{\\\"selectOne\\\": \\\"(max ?endDate)\\\", \\\"where\\\": [[\\\"?plane\\\", \\\"plane/id\\\", \" id \"], [\\\"?plane\\\", \\\"plane/trips\\\", \\\"?trip\\\"], [\\\"?trip\\\", \\\"trip/endDate\\\", \\\"?endDate\\\"]]}\"))"}
                 {:_id  "_fn"
                  :name "randomAirport"
                  :code "(query \"{\\\"selectOne\\\": \\\"(rand ?airport)\\\",\\\"where\\\": [[\\\"?airport\\\", \\\"airport/name\\\", \\\"?name\\\"]]}\")"}
                 {:_id  "_fn"
                  :name "numAirports"
                  :code "(query \"{\\\"selectOne\\\": \\\"(count ?airport)\\\",\\\"where\\\": [[\\\"?airport\\\", \\\"airport/name\\\", \\\"?name\\\"]]}\\n\")"}
                 {:_id    "_fn"
                  :name   "mechanics"
                  :params ["location" "num"],
                  :code   "(query (str \"{ \\\"select\\\": \\\"?person\\\", \\\"where\\\": [[\\\"?person\\\", \\\"person/roles\\\", \\\"mechanic\\\"], [\\\"?person\\\", \\\"person/homeLocation\\\", \" location \"]], \\\"limit\\\": \" num \"})\") )"}
                 {:_id    "_fn"
                  :name   "headMechanic"
                  :params ["location"],
                  :code   "(query (str \"{ \\\"selectOne\\\": \\\"?person\\\", \\\"where\\\": [[\\\"?person\\\", \\\"person/roles\\\", \\\"headMechanic\\\"], [\\\"?person\\\", \\\"person/homeLocation\\\", \" location \"]]})\") )"}
                 {:_id    "_fn"
                  :name   "randPart"
                  :params ["plane-id"]
                  :code   "(query (str \"{\\n  \\\"selectOne\\\": \\\"(sample 1 ?part)\\\",\\n  \\\"where\\\": [\\n      [\\\"?plane\\\", \\\"plane/id\\\", \" plane-id \"],\\n      [\\\"?plane\\\", \\\"plane/sections\\\", \\\"?section\\\"],\\n      [\\\"?section\\\", \\\"section/parts\\\", \\\"?part\\\"]]\\n}\"))" }])


(def init-txn [{:_id  "manufacturer"
                :name "Boeing"
                :auth [{:_id "_auth$1"
                        :id  "TeyfkjUCrNNomeTrGFSjZ7YWwWefThAmMBg"}]}
               {:_id  "manufacturer"
                :name "Airbus"
                :auth [{:_id "_auth$2"
                        :id  "Tf1njkUnWvz9AybRocBj1kZQc7tqxiPRaUh"}]}
               {:_id  "airline"
                :name "flureeAirline"}
               {:_id    "_fn"
                :name   "randomAirportSeeded"
                :params ["seed"]
                :code   "(nth (query (str \"{\\\"select\\\": \\\"?airport\\\",\\\"where\\\": [[\\\"?airport\\\", \\\"airport/name\\\", \\\"?name\\\"]],\\\"offset\\\":\" (rand seed (dec (numAirports))) \", \\\"limit\\\": 1}\")) 0)"}
               {:_id    "_fn"
                :name   "planeLocation"
                :params ["id"]
                :code   "(query (str \"{\\\"selectOne\\\": \\\"?endLocation\\\", \\\"where\\\": [[\\\"?plane\\\", \\\"plane/id\\\", \" id \"], [\\\"?plane\\\", \\\"plane/trips\\\", \\\"?trip\\\"], [\\\"?trip\\\", \\\"trip/endDate\\\", \" (lastFlightDate id)\"], [\\\"?trip\\\", \\\"trip/endLocation\\\", \\\"?endLocation\\\"]]}\"))"}])

(def plane-type ["Airbus A333-300" "Airbus A340-300" "Airbus A340-500" "Airbus A350-900"
                 "Boeing 777-200" "Airbus A340-600" "Boeing 777-300" "Boeing 747-400"
                 "Boeing 747-8" "Airbus A380-800" "Boeing 737-600"])

(def plane-sections ["A" "B" "C" "D" "E"])

(defn manufacturer-from-type
  [type]
  (if (str/includes? type "Boeing") "Boeing" "Airbus"))

; part types are just [section]-[part #]-[version #]
(defn generate-plane-parts
  [section manufacturer manufacture-date numParts]
  (loop [n   numParts
         acc []]
    (if (> n 0)
      (recur (dec n) (conj acc {:_id           "part"
                                :serial-number (str (util/random-uuid))
                                :type          (str section "-" n "-" (rand-int 5))
                                :installDate   manufacture-date
                                :original      true
                                :manufacturer  ["manufacturer/name" manufacturer]})) acc)))

(defn generate-people
  "Takes a map with roles as keys and count as vals. i.e. {:pilot 2 :flightAttendant 3}"
  ([role-map]
   (generate-people role-map {}))
  ([role-map opts]
   (let [role-vec (->> (reduce-kv (fn [acc role num]
                                    (concat acc (repeat num (util/keyword->str role))))
                                  [] role-map) (apply vector))]
     (loop [n       1
            [role & r] role-vec
            tempids []
            txis    []]
       (if role
         (let [new-tempid (str "person$" role "-" n "-" (rand-int Integer/MAX_VALUE))
               tempids    (conj tempids new-tempid)
               txi        {:_id   new-tempid
                           :id    (str "#(+ (inc (max-pred-val \"person/id\")) " (+ (dec n) (or (:start-n opts) 0)) ")")
                           :roles [role]}
               txi'       (if (:homeLocation opts)
                            (assoc txi :homeLocation (:homeLocation opts))
                            txi)]
           (recur (inc n) r tempids (conj txis txi'))) [tempids txis])))))



(def role-vec
  (-> []
      (concat (repeat 2 "headMechanic"))
      (concat (repeat 10 "mechanic"))
      (#(apply vector %))))

(defn add-random-person
  []
  [{:_id   "person"
    :roles [(rand-nth role-vec)]
    :id    "#(inc (max-pred-val \"person/id\"))"}])


(defn add-plane
  []
  (let [plane-type       (rand-nth plane-type)
        manufacture-date month-start
        ;(str "#(- (now) " (rand-int 10000) ")")
        manufacturer     (manufacturer-from-type plane-type)
        plane-name       (str (util/random-uuid))
        [people-tempids people-txis] (generate-people {:pilot 3 :flightAttendant 12})
        txn              [{:_id    ["airline/name" "flureeAirline"]
                           :planes ["plane$1"]}
                          {:_id             "plane$1"
                           :id              "#(inc (max-pred-val \"plane/id\"))"
                           :name            plane-name
                           :type            plane-type
                           :manufacturer    ["manufacturer/name" manufacturer]
                           :manufactureDate manufacture-date
                           :sections        (map (fn [section]
                                                   {:_id   "section"
                                                    :id    (str (util/random-uuid))
                                                    :type  section
                                                    :parts (generate-plane-parts section manufacturer
                                                                                 manufacture-date 5)})
                                                 plane-sections)
                           :trips           ["trip$1"]}
                          {:_id           "trip$1"
                           :startLocation (str "#(randomAirportSeeded " (rand-int 1000) ")")
                           :endLocation   (str "#(randomAirportSeeded " (rand-int 1000) ")")
                           :startDate     month-start
                           :endDate       (+ month-start 14400000) ;; default first trip is 4 hours
                           :crew          people-tempids}]]
    (concat txn people-txis)))


(def airport-vec (-> (slurp "dev/airport.json") json/parse :features))

(keys airport-vec)

(defn airport-txn
  [airport-vec]
  (loop [txn          []
         [airport & rest] airport-vec
         people-count 0]
    (if (not airport)
      txn
      (if-let [abbrev (and airport (get-in airport [:properties :abbrev]))]
        (let [name             (get-in airport [:properties :name])
              airport-temp     (str "airport$1" name)
              airport          {:_id          airport-temp
                                :name         name
                                :location     (json/stringify (:geometry airport))
                                :abbreviation abbrev}
              numHeadMechanics (inc (rand-int 5))
              numMechanics     (inc (rand-int 10))
              [_ mechanics] (generate-people {:headMechanic numHeadMechanics :mechanic numMechanics}
                                             {:homeLocation airport-temp
                                              :start-n      people-count})
              txn'             (->> (concat txn [airport] mechanics) (into []))
              people-count'    (+ people-count numHeadMechanics numMechanics)]
          (recur txn' rest people-count'))
        (recur txn rest people-count)))))


(def airport-1 (airport-txn (subvec airport-vec 1 200)))
(def airport-2 (airport-txn (subvec airport-vec 201 400)))
(def airport-3 (airport-txn (subvec airport-vec 401 600)))
(def airport-4 (airport-txn (subvec airport-vec 601 887)))


(defn add-trips
  [plane-id numTrips approxTripLength opts]
  (loop [n             0
         plane-txi     {:_id   ["plane/id" plane-id]
                        :trips []}
         trip-txis     []
         startLocation (str "#(planeLocation " plane-id ")")
         endLocation   (str "#(randomAirportSeeded " (rand-int 100) ")")
         startDate     (if (and (:delay opts) (zero? n))
                         (str "#(+ (lastFlightDate " plane-id ") " (:delay opts) ")")
                         (str "#(lastFlightDate " plane-id ")"))
         endDate       (str "#(+ (lastFlightDate " plane-id ") " (+ (rand-int 100) approxTripLength) ")")]
    (if (> n numTrips)
      (conj trip-txis plane-txi)
      (let [new-tempid  (str "trip$" n)
            plane-txi*  (update plane-txi :trips conj new-tempid)
            trip-txis*  (conj trip-txis {:_id           new-tempid
                                         :startLocation startLocation
                                         :endLocation   endLocation
                                         :startDate     startDate
                                         :endDate       endDate})
            nextEndDate (str "#(+ (lastFlightDate " plane-id ") " (* (+ (rand-int 100) approxTripLength) (inc n)) ")")]
        (recur (inc n) plane-txi* trip-txis* endLocation startLocation endDate nextEndDate)))))


(def repair-vec
  ["It was broken. We fixed it." "It was reeeeaally broken, but we fixed it." "We used our handy-skills to make it work." "Nothing a little baking soda couldn't fix."])

(defn create-repair-txi
  [plane-id startDate endDate]
  {:_id         "repair"
   :name        (str (util/random-uuid))
   :description (nth repair-vec (rand-int (count repair-vec)))
   :location    (str "#(planeLocation " plane-id ")")
   :leadPerson  (str "#(headMechanic (planeLocation " plane-id "))")
   :part        (str "#(randPart " plane-id ")")
   :startDate   startDate
   :endDate     endDate
   :plane       ["plane/id" plane-id]})


(defn add-trips-and-repairs
  "Randomly generate trips and repairs. Generates between numTrips and (numTrips + maxIdentTrips - 1) trips.
  Provide repairProb as a float <= 1."
  [conn db plane-id numTrips maxIdentTrips repairProb]
  (go-try (loop [n numTrips]
            (if (> 0 n)
              true
              (let [repair?   (>= repairProb (rand))
                    startDate (str "#(+ (lastFlightDate " plane-id ") " (rand-int 1000) ")")
                    randTime  (+ 1000 (rand-int 57600000))  ;; 5.76 + 10^7 = 16 hrs - used as either repair or trip time
                    endDate   (str "#(+ (lastFlightDate " plane-id ") " randTime ")")
                    trips     (rand-int maxIdentTrips)
                    txn       (if repair?
                                (let [repair-txi (create-repair-txi plane-id startDate endDate)
                                      trip-txis  (add-trips plane-id trips (rand 57600000) {:delay randTime})]
                                  (conj trip-txis repair-txi))
                                (add-trips plane-id trips randTime {:delay (rand-int 1000)}))
                    res       (<? (fdb/transact-async conn db txn))]
                (recur (- n trips)))))))

(defn add-planes
  [conn db numPlanes]
  (go-try
    (loop [n numPlanes]
      (if (> n 0)
        (let [res (<? (fdb/transact-async conn db (add-plane)))]
          (recur (dec n)))))))


(defn populate-db
  [conn db numPlanes]
  (go-try
    (do
      (<? (fdb/transact-async conn db schema-txn))
      (<? (fdb/transact-async conn db init-txn))

      (<? (fdb/transact-async conn db airport-1))

      (<? (fdb/transact-async conn db airport-2))
      (<? (fdb/transact-async conn db airport-3))
      (<? (fdb/transact-async conn db airport-4))

      (<? (add-planes conn db numPlanes)))))


(comment


  {:manufacturer "Boeing"
   :private      "ed994d20bf4f425fd4a24c859cbcc4434c0e72c298dde837497d6be47f990f02",
   :public       "0241269b5f8ae75355ee420cae5f1b7c8be5828d6a92cc35884936bd6091b4c532",
   :id           "TeyfkjUCrNNomeTrGFSjZ7YWwWefThAmMBg"}

  {:manufacturer "Airbus"
   :private      "bd835fedb350523554d21fea7b8b057ba8b221d7ada0cd196596c27991af438a",
   :public       "02b278061f5208f5e39c803b86c0c5eb660738ce819f4e464d1d4ddef0d805729b",
   :id           "Tf1njkUnWvz9AybRocBj1kZQc7tqxiPRaUh"}


  (<? (fdb/transact-async conn db init-txn))

  (def conn (:conn user/system))
  (def db "plane/demo")

  (populate-db conn db 101)

  (map #(add-trips-and-repairs conn db % 2 2 0.5) [99])


  (def planeDb (fdb/db conn "plane/demo"))

  (clojure.core.async/go-loop [n 1
                               acc []]
    (if (< n 102)
      (let [maxDateQ {:selectOne "(max ?endDate)"
                      :where     [["?plane", "plane/id", n],
                                  ["?plane", "plane/trips", "?trips"],
                                  ["?trips", "trip/endDate", "?endDate"]]}
            endDate  (<? (fdb/query-async planeDb maxDateQ))]
        (if (< endDate 1567567321387)
          (recur (inc n) (conj acc n))
          (recur (inc n) acc)))
      (log/info "NEED TO UPDATE" acc))))



