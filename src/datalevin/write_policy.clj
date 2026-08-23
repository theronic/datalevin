;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.write-policy
  "Generic persisted guards for application-owned Datalog attributes."
  (:require
   [clojure.set :as set]
   [datalevin.constants :as c]
   [datalevin.datom :as d]))

(def persisted-policy-key :datalevin/write-policy)
(def runtime-token-key :datalevin.write-policy/admission-token)
(def runtime-write-unusable-key :datalevin.write-policy/write-unusable)

(def capabilities
  {:version 1
   :commit-generation-materialization true
   :max-tx-continuity true
   :post-expansion-enforcement true
   :persisted-policy true
   :per-open-admission-token true
   :shared-store-stale-generation-recovery true})

(def ^:dynamic *admin-write-token*
  nil)

(def protected-datalog-dbis
  #{c/eav c/ave c/giants c/schema c/meta c/kv-info})

(defn- policy-error
  [message type data]
  (throw
   (ex-info message
            (merge {:type type
                    :datalevin/error type}
                   data))))

(defn- keyword-set!
  [field value]
  (when-not (and (set? value) (every? keyword? value))
    (policy-error
     "A write-policy attribute collection must be a set of keywords."
     :datalevin/invalid-write-policy
     {:field field :value value}))
  (into (sorted-set) value))

(defn- normalize-selector
  [selector]
  (cond
    (= :same-entity selector)
    selector

    (and (vector? selector)
         (= 2 (count selector))
         (= :tuple-position (first selector))
         (nat-int? (second selector)))
    selector

    (and (vector? selector)
         (= 2 (count selector))
         (= :constant (first selector))
         (nat-int? (second selector)))
    selector

    :else
    (policy-error
     "A write-policy stamp selector is invalid."
     :datalevin/invalid-write-policy
     {:field :stamp-entity :value selector})))

(defn- normalize-rule
  [{:keys [when-attribute stamp-attribute stamp-entity] :as rule}]
  (when-not (and (map? rule)
                 (keyword? when-attribute)
                 (keyword? stamp-attribute))
    (policy-error
     "A write-policy stamp rule requires keyword attributes."
     :datalevin/invalid-write-policy
     {:field :stamp-rules :value rule}))
  {:when-attribute when-attribute
   :stamp-attribute stamp-attribute
   :stamp-entity (normalize-selector stamp-entity)})

(defn normalize
  [{:keys [guarded-attributes frozen-attributes
           commit-generation-attributes stamp-rules]
    :as policy}]
  (when-not (map? policy)
    (policy-error
     "A Datalevin write policy must be a map."
     :datalevin/invalid-write-policy
     {:value policy}))
  (let [guarded (keyword-set! :guarded-attributes guarded-attributes)
        frozen (keyword-set! :frozen-attributes frozen-attributes)
        commit-generations
        (keyword-set! :commit-generation-attributes
                      commit-generation-attributes)
        rules (->> (or stamp-rules [])
                   (mapv normalize-rule)
                   (sort-by (juxt :when-attribute
                                  :stamp-attribute
                                  (comp pr-str :stamp-entity)))
                   vec)
        rule-stamps (into #{} (map :stamp-attribute) rules)]
    (when-not (set/subset? commit-generations guarded)
      (policy-error
       "Commit-generation attributes must also be guarded."
       :datalevin/invalid-write-policy
       {:field :commit-generation-attributes
        :not-guarded (set/difference commit-generations guarded)}))
    (when-not (set/subset? rule-stamps commit-generations)
      (policy-error
       "Every stamp-rule target must be a commit-generation attribute."
       :datalevin/invalid-write-policy
       {:field :stamp-rules
        :not-commit-generations
        (set/difference rule-stamps commit-generations)}))
    {:version 1
     :guarded-attributes guarded
     :frozen-attributes frozen
     :commit-generation-attributes commit-generations
     :stamp-rules rules
     :guarded-write-hint (:guarded-write-hint policy)}))

(defn persisted-policy
  [info]
  (get @info persisted-policy-key))

(defn runtime-token
  [info]
  (get @info runtime-token-key))

(defn write-unusable-state
  [info]
  (get @info runtime-write-unusable-key))

(defn admitted?
  [info tx-meta]
  (= (runtime-token info) (:datalevin/write-token tx-meta)))

(defn with-admin-token
  [token f]
  (binding [*admin-write-token* token]
    (f)))

(defn assert-admin-admitted!
  ([info operation]
   (assert-admin-admitted! info operation nil))
  ([info operation dbi-name]
   (let [policy (persisted-policy info)
         protected? (and policy
                         (seq (:frozen-attributes policy))
                         (or (nil? dbi-name)
                             (contains? protected-datalog-dbis dbi-name)))]
     (when (and protected?
                (not= (runtime-token info) *admin-write-token*))
       (policy-error
        "A frozen Datalevin write policy rejected an administrative mutation."
        :datalevin/frozen-attribute-write
        {:operation operation :dbi-name dbi-name})))
   true))

(defn assert-schema-admitted!
  [info touched-attributes]
  (let [policy (persisted-policy info)
        protected
        (when policy
          (set/intersection (:frozen-attributes policy)
                            (set touched-attributes)))]
    (when (and (seq protected)
               (not= (runtime-token info) *admin-write-token*))
      (policy-error
       "A frozen Datalevin write policy rejected a schema mutation."
       :datalevin/frozen-attribute-write
       {:operation :update-schema
        :attributes protected})))
  true)

(defn- datom-key
  [datom]
  [(d/datom-e datom) (d/datom-a datom) (d/datom-v datom)])

(defn- selected-stamp-entity
  [datom selector]
  (cond
    (= :same-entity selector)
    (d/datom-e datom)

    (and (vector? selector) (= :tuple-position (first selector)))
    (let [value (d/datom-v datom)
          position (second selector)]
      (when-not (and (vector? value) (< position (count value)))
        (policy-error
         "A guarded datom does not satisfy its tuple stamp selector."
         :datalevin/invalid-stamp-target
         {:attribute (d/datom-a datom)
          :value value
          :selector selector}))
      (nth value position))

    (and (vector? selector) (= :constant (first selector)))
    (second selector)

    :else
    (policy-error
     "A persisted write policy contains an invalid selector."
     :datalevin/invalid-write-policy
     {:selector selector})))

(defn enforce-datoms!
  [info datoms committing-generation tx-meta]
  (when-let [policy (persisted-policy info)]
    (let [guarded (:guarded-attributes policy)
          guarded-datoms (filterv #(contains? guarded (d/datom-a %)) datoms)]
      (when (and (seq guarded-datoms) (not (admitted? info tx-meta)))
        (policy-error
         (or (:guarded-write-hint policy)
             "A guarded Datalevin attribute requires an admitted writer.")
         :datalevin/guarded-attribute-write
         {:attributes (into (sorted-set) (map d/datom-a) guarded-datoms)}))
      (let [added (filterv d/datom-added datoms)
            added-keys (into #{} (map datom-key) added)]
        (doseq [datom added
                :when (contains? (:commit-generation-attributes policy)
                                 (d/datom-a datom))]
          (when-not (= committing-generation (d/datom-v datom))
            (policy-error
             "A commit-generation datom carries a stale generation."
             :datalevin/stale-generation
             {:entity (d/datom-e datom)
              :attribute (d/datom-a datom)
              :expected committing-generation
              :actual (d/datom-v datom)})))
        (doseq [datom datoms
                rule (:stamp-rules policy)
                :when (= (:when-attribute rule) (d/datom-a datom))]
          (let [stamp-entity
                (selected-stamp-entity datom (:stamp-entity rule))
                required [stamp-entity
                          (:stamp-attribute rule)
                          committing-generation]]
            (when-not (contains? added-keys required)
              (policy-error
               "A guarded Datalevin mutation is missing its commit-generation stamp."
               :datalevin/missing-stamp
               {:attribute (d/datom-a datom)
                :entity (d/datom-e datom)
                :required-stamp required})))))))
  true)
