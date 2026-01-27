(ns ledger-toolkit.block-processor.optimized-state
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [ledger-toolkit.block-processor.metadata :as metadata]))

(defn initial-optimized-state []
  {:committed-facts #{}
   :current-batch-assertions #{}
   :current-batch-retractions #{}
   :schema-facts #{}
   :processed-blocks 0
   :last-t -1
   :current-batch-flakes []
   :dropped-flakes-count 0})

(defn schema-fact? [[s p o]]
  (let [collection-id (flake/sid->cid s)]
    (or (= collection-id fluree.db.constants/$_collection)
        (= collection-id fluree.db.constants/$_predicate)
        (= collection-id fluree.db.constants/$_tag)
        (= collection-id fluree.db.constants/$_fn)
        (= collection-id fluree.db.constants/$_user)
        (= collection-id fluree.db.constants/$_auth)
        (= collection-id fluree.db.constants/$_role)
        (= collection-id fluree.db.constants/$_rule)
        (= collection-id fluree.db.constants/$_setting))))

(defn process-flake-optimized [state flake]
  (let [{:keys [s p o op]} flake
        tuple [s p o]
        {:keys [committed-facts current-batch-assertions current-batch-retractions schema-facts]} state]
    (cond
      op
      (let [new-batch-assertions (conj current-batch-assertions tuple)
            new-batch-retractions (disj current-batch-retractions tuple)
            new-schema (if (schema-fact? tuple) (conj schema-facts tuple) schema-facts)]
        (assoc state
               :current-batch-assertions new-batch-assertions
               :current-batch-retractions new-batch-retractions
               :schema-facts new-schema
               :current-batch-flakes (conj (:current-batch-flakes state) flake)))
      (not op)
      (cond
        (current-batch-assertions tuple)
        (assoc state
               :current-batch-assertions (disj current-batch-assertions tuple)
               :current-batch-flakes (conj (:current-batch-flakes state) flake)
               :dropped-flakes-count (+ 2 (:dropped-flakes-count state)))
        (committed-facts tuple)
        (let [new-batch-retractions (conj current-batch-retractions tuple)
              new-schema (if (schema-fact? tuple) (disj schema-facts tuple) schema-facts)]
          (assoc state
                 :current-batch-retractions new-batch-retractions
                 :schema-facts new-schema
                 :current-batch-flakes (conj (:current-batch-flakes state) flake)))
        :else
        (assoc state :current-batch-flakes (conj (:current-batch-flakes state) flake))))))

(defn get-optimized-facts [state]
  (let [{:keys [current-batch-assertions current-batch-retractions]} state
        assertion-flakes (map (fn [[s p o]] {:s s :p p :o o :op true}) current-batch-assertions)
        retraction-flakes (map (fn [[s p o]] {:s s :p p :o o :op false}) current-batch-retractions)]
    {:assertion-facts current-batch-assertions
     :retraction-facts current-batch-retractions
     :total-facts (+ (count current-batch-assertions) (count current-batch-retractions))
     :assertion-flakes assertion-flakes
     :retraction-flakes retraction-flakes
     :all-output-flakes (concat assertion-flakes retraction-flakes)}))

(defn commit-batch-to-state [state]
  (let [{:keys [committed-facts current-batch-assertions current-batch-retractions]} state
        new-committed-facts (-> committed-facts
                                (into current-batch-assertions)
                                (into (map #(vector (first %) (second %) (nth % 2))
                                           (remove committed-facts current-batch-retractions))))]
    (assoc state
           :committed-facts new-committed-facts
           :current-batch-assertions #{}
           :current-batch-retractions #{}
           :current-batch-flakes []
           :dropped-flakes-count (:dropped-flakes-count state))))

(defn process-block-flakes-optimized [state flakes]
  (let [user-data-flakes (metadata/filter-user-data flakes)]
    (reduce process-flake-optimized state user-data-flakes)))

(defn should-output-batch? [state max-flakes-per-block]
  (let [{:keys [total-facts]} (get-optimized-facts state)]
    (>= total-facts max-flakes-per-block)))

(defn get-batch-stats [state]
  (let [facts (get-optimized-facts state)
        {:keys [current-batch-assertions current-batch-retractions dropped-flakes-count]} state]
    {:assertions (count current-batch-assertions)
     :retractions (count current-batch-retractions)
     :total-facts (:total-facts facts)
     :net-facts (- (count current-batch-assertions) (count current-batch-retractions))
     :dropped-flakes dropped-flakes-count}))

