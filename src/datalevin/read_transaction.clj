;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;;
(ns ^:no-doc datalevin.read-transaction
  "Internal binding and accounting for explicit public read snapshots."
  (:require [clojure.string :as string]))

(def ^:dynamic *context* nil)

(def ^:dynamic *native-read-fault-injector*
  "Internal deterministic certification hook. A bound function is invoked
  immediately before an explicit snapshot obtains its bound native reader."
  nil)

(def ^:private native-read-origin-key
  ::native-read-origin)

(defonce ^:private active-readers (atom {}))

(defn- now-nanos [] (System/nanoTime))

(defn new-context
  [lmdb rtx]
  (let [id (str (java.util.UUID/randomUUID))
        context {:id id
                 :lmdb lmdb
                 :rtx rtx
                 :owner-thread (Thread/currentThread)
                 :acquired-at-nanos (now-nanos)
                 :close-state (atom :open)}]
    (swap! active-readers assoc id context)
    context))

(defn closed?
  [context]
  (= :closed @(:close-state context)))

(defn require-owner!
  [context phase]
  (when-not (identical? (:owner-thread context) (Thread/currentThread))
    (throw
     (ex-info
      "Read snapshot operation escaped its acquiring thread."
      {:type :datalevin/read-snapshot-thread-violation
       :operation phase})))
  nil)

(defn require-open!
  [context phase]
  (require-owner! context phase)
  (case @(:close-state context)
    :open nil
    :closing
    (throw
     (ex-info
      "Read snapshot close is already in progress."
      {:type :datalevin/read-snapshot-close-in-progress
       :operation phase}))
    :closed
    (throw
     (ex-info
      "Read snapshot is closed."
      {:type :datalevin/read-snapshot-closed
       :operation phase})))
  context)

(defn require-not-interrupted!
  [phase]
  (when (.isInterrupted (Thread/currentThread))
    (throw
     (ex-info
      "Explicit read snapshot operation was interrupted."
      {:type :datalevin/read-snapshot-interrupted
       :phase phase})))
  nil)

(defn begin-close!
  "Claims close ownership while retaining active-reader accounting."
  [context]
  (require-owner! context :close)
  (loop []
    (case @(:close-state context)
      :closed false
      :closing
      (throw
       (ex-info
        "Read snapshot close is already in progress."
        {:type :datalevin/read-snapshot-close-in-progress
         :operation :close}))
      :open
      (if (compare-and-set! (:close-state context) :open :closing)
        true
        (recur)))))

(defn finish-close!
  "Publishes closure only after native state is closed."
  [context]
  (when-not (compare-and-set! (:close-state context) :closing :closed)
    (throw
     (ex-info
      "Read snapshot close state was corrupted."
      {:type :datalevin/read-snapshot-close-state
       :state @(:close-state context)})))
  (swap! active-readers dissoc (:id context))
  true)

(defn abort-close!
  "Restores retryability if native close reports failure."
  [context]
  (compare-and-set! (:close-state context) :closing :open))

(defn with-context
  [context f]
  (require-open! context :access)
  (require-not-interrupted! :access)
  (when-not (fn? f)
    (throw
     (ex-info "Read snapshot body must be a function."
              {:type :datalevin/invalid-read-snapshot-body})))
  (if *context*
    (if (identical? *context* context)
      (f)
      (throw
       (ex-info
        "A different read snapshot is already bound on this thread."
        {:type :datalevin/nested-read-snapshot-conflict})))
    (binding [*context* context]
      (f))))

(defn- invoke-native-read-fault-injector!
  [phase]
  (when *native-read-fault-injector*
    (try
      (*native-read-fault-injector* phase)
      (catch Throwable error
        (throw
         (ex-info
          "The deterministic native-read fault injector failed."
          {native-read-origin-key true}
          error))))))

(defn require-bound-context!
  "Requires `context` to be the explicit reader currently bound to this
  thread. Snapshot-specific storage wrappers call this before delegating to
  LMDB, so an escaped snapshot DB cannot fall back to an ambient reader."
  [context phase]
  (require-open! context phase)
  (when-not (identical? *context* context)
    (throw
     (ex-info
      "Read snapshot storage was accessed outside its explicit scope."
      {:type :datalevin/read-snapshot-context-required
       :operation phase})))
  (when (and (= :native-read phase) *native-read-fault-injector*)
    (invoke-native-read-fault-injector! phase))
  context)

(defn- native-frame?
  [^StackTraceElement frame]
  (let [class-name (.getClassName frame)]
    (or (string/starts-with? class-name "org.lmdbjava.")
        (string/starts-with? class-name "datalevin.binding.cpp$"))))

(defn- native-read-origin?
  [throwable]
  (loop [error throwable]
    (when error
      (let [class-name (.getName (class error))]
        (or (true? (get (ex-data error) native-read-origin-key))
            (string/starts-with? class-name "org.lmdbjava.")
            (some native-frame? (.getStackTrace ^Throwable error))
            (recur (.getCause ^Throwable error)))))))

(defn native-failure-reason
  "Returns a stable public reason for a recognized LMDB/filesystem read
  failure, or nil when the throwable is not known to be native storage I/O."
  [throwable]
  (loop [error throwable
         messages []]
    (if error
      (let [data (ex-data error)
            message (some-> (ex-message error) str)
            messages (cond-> messages message (conj message))
            declared (or (:reason data) (:type data))]
        (cond
          (contains? #{:reader-slots-exhausted :map-full :disk-full
                       :map-resized :environment-closed
                       :native-read-failure}
                     declared)
          declared

          (= :lmdb/closed declared)
          :environment-closed

          :else
          (recur (.getCause ^Throwable error) messages)))
      (let [message (string/lower-case (string/join " " messages))]
        (cond
          (re-find #"mdb_readers_full" message) :reader-slots-exhausted
          (re-find #"mdb_map_full|map[ -]full" message) :map-full
          (re-find #"enospc|no space left|disk[ -]full" message) :disk-full
          (re-find #"mdb_map_resized|map[ -]resized" message) :map-resized
          (re-find #"lmdb env is closed|environment[ -]closed" message)
          :environment-closed
          (re-find #"mdb_(bad_txn|page_notfound|corrupted|panic)" message)
          :native-read-failure
          :else nil)))))

(defn classify-native-read-error
  [throwable]
  (when (native-read-origin? throwable)
    (when-let [reason (native-failure-reason throwable)]
      (ex-info
       "An explicit Datalevin snapshot native read failed."
       {:type :datalevin/read-snapshot-native-read-failed
        :operation :read
        :reason reason}
       throwable))))

(defn bound-rtx
  "Returns the explicitly bound reader for `lmdb`, or nil outside a public
  read-snapshot scope. A mismatched environment fails before native access."
  [lmdb]
  (when-let [context *context*]
    (require-open! context :native-read)
    (when-not (identical? lmdb (:lmdb context))
      (throw
       (ex-info
        "Read snapshot was used with another LMDB environment."
        {:type :datalevin/read-snapshot-environment-mismatch})))
    (when *native-read-fault-injector*
      (invoke-native-read-fault-injector! :native-read))
    (:rtx context)))

(defn bound-rtx?
  [lmdb rtx]
  (and *context*
       (identical? lmdb (:lmdb *context*))
       (identical? rtx (:rtx *context*))))

(defn cache-bypass-active?
  []
  (boolean *context*))

(defn active-reader-info
  []
  (let [now (now-nanos)
        readers (vals @active-readers)]
    {:active (count readers)
     :oldest-age-ms
     (when-let [oldest (seq (map :acquired-at-nanos readers))]
       (/ (double (- now (apply min oldest))) 1000000.0))}))

(defn active-reader-count
  "Returns the number of open explicit readers for one native environment."
  [lmdb]
  (count
   (filter #(identical? lmdb (:lmdb %))
           (vals @active-readers))))
