(ns datalevin.eacl-snapshot-contract-test
  "Regression fixtures for the EACL immutable-snapshot integration boundary."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.read-transaction :as read-tx]
   [datalevin.storage :as storage]
   [datalevin.util :as u]))

(defn- names
  [db]
  (set
   (d/q '[:find [?name ...]
          :where
          [_ :eacl.contract/name ?name]]
        db)))

(defn- snapshot-names
  [snapshot]
  (d/with-read-snapshot snapshot names))

(defn- error-data
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo error
      (ex-data error))))

(deftest ordinary-db-handle-is-live-not-an-immutable-snapshot-test
  (let [dir
        (u/tmp-dir
         (str "eacl-ordinary-db-is-live-" (random-uuid)))
        conn
        (d/get-conn
         dir
         {:eacl.contract/name
          {:db/valueType :db.type/string
           :db/unique :db.unique/identity}})]
    (try
      (d/transact! conn [{:eacl.contract/name "before"}])
      (let [cached-db (d/db conn)
            cached-max-tx (:max-tx cached-db)]
        (is (= #{"before"} (names cached-db)))

        (d/transact! conn [{:eacl.contract/name "after"}])

        (testing "one cached DB object observes a later commit"
          (is (= #{"before" "after"} (names cached-db))))
        (testing "the same object's captured revision does not reveal that change"
          (is (= cached-max-tx (:max-tx cached-db)))
          (is (< cached-max-tx (:max-tx (d/db conn)))))
        (testing "therefore native identity plus captured max-tx is unsound"
          (is (identical? cached-db cached-db))
          (is (= [cached-db cached-max-tx]
                 [cached-db (:max-tx cached-db)]))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest explicit-read-snapshot-is-stable-closeable-and-cache-isolated-test
  (let [dir (u/tmp-dir (str "eacl-explicit-snapshot-" (random-uuid)))
        conn
        (d/get-conn
         dir
         {:eacl.contract/name
          {:db/valueType :db.type/string
           :db/unique :db.unique/identity}})]
    (try
      (d/transact! conn [{:eacl.contract/name "before"}])
      (let [before-info (d/active-read-snapshot-info)
            snapshot (d/open-read-snapshot conn)
            metadata (d/read-snapshot-info snapshot)
            revision-info (d/read-snapshot-revision-info snapshot)]
        (try
          (is (= 1 (- (:active (d/active-read-snapshot-info))
                      (:active before-info))))
          (is (true? (d/read-snapshot-cache-bypass? snapshot)))
          (is (= #{"before"} (snapshot-names snapshot)))
          (is (integer? (:max-tx metadata)))
          (is (integer? (:max-eid metadata)))
          (is (= #{:max-tx :max-eid} (set (keys revision-info))))
          (is (= (select-keys metadata [:max-tx :max-eid]) revision-info))
          (is (identical? revision-info
                          (d/read-snapshot-revision-info snapshot)))
          (is (= :db.type/string
                 (get-in metadata
                         [:schema :eacl.contract/name :db/valueType])))

          (d/transact! conn [{:eacl.contract/name "after"}])

          (testing "a live query can populate the global cache at the new head"
            (is (= #{"before" "after"} (names (d/db conn)))))
          (testing "the explicit reader bypasses that cache and retains old data"
            (is (= #{"before"} (snapshot-names snapshot)))
            (is (= metadata (d/read-snapshot-info snapshot)))
            (is (= revision-info
                   (d/read-snapshot-revision-info snapshot))))
          (finally
            (is (true? (d/close-read-snapshot! snapshot)))))
        (is (false? (d/close-read-snapshot! snapshot)))
        (is (= (:active before-info)
               (:active (d/active-read-snapshot-info))))
        (is (= :datalevin/read-snapshot-closed
               (:type (error-data #(snapshot-names snapshot)))))
        (is (= :datalevin/read-snapshot-closed
               (:type
                (error-data #(d/read-snapshot-revision-info snapshot))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest revision-only-acquisition-defers-physical-schema-materialization-test
  (let [dir (u/tmp-dir (str "eacl-revision-only-snapshot-" (random-uuid)))
        conn
        (d/get-conn
         dir
         {:eacl.contract/name
          {:db/valueType :db.type/string
           :db/unique :db.unique/identity}})
        phases (atom [])]
    (try
      (d/transact! conn [{:eacl.contract/name "before"}])
      (binding [storage/*read-snapshot-fault-injector*
                #(swap! phases conj %)]
        (with-open [snapshot (d/open-read-snapshot conn)]
          (is (= [:before-native-acquisition :after-native-acquisition]
                 @phases))
          (is (= #{:max-tx :max-eid}
                 (set (keys (d/read-snapshot-revision-info snapshot)))))
          (is (= [:before-native-acquisition :after-native-acquisition]
                 @phases)
              "revision identity does not materialize physical schema")
          (is (= :db.type/string
                 (get-in (d/read-snapshot-info snapshot)
                         [:schema :eacl.contract/name :db/valueType])))
          (is (= 1 (count (filter #{:before-physical-materialization}
                                  @phases))))
          (is (= :db.type/string
                 (get-in (d/read-snapshot-info snapshot)
                         [:schema :eacl.contract/name :db/valueType])))
          (is (= 1 (count (filter #{:before-physical-materialization}
                                  @phases))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest explicit-read-snapshot-has-coherent-old-schema-across-rename-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-schema-" (random-uuid)))
        conn
        (d/get-conn
         dir
         {:eacl.contract/old-name
          {:db/valueType :db.type/string
           :db/unique :db.unique/identity}})]
    (try
      (d/transact! conn [{:eacl.contract/old-name "before"}])
      (with-open [snapshot (d/open-read-snapshot conn)]
        (d/update-schema
         conn nil nil
         {:eacl.contract/old-name :eacl.contract/new-name})
        (let [metadata (d/read-snapshot-info snapshot)
              old-values
              (d/with-read-snapshot
               snapshot
               #(set
                 (map :v
                      (d/datoms % :ave :eacl.contract/old-name))))]
          (is (contains? (:schema metadata) :eacl.contract/old-name))
          (is (not (contains? (:schema metadata)
                              :eacl.contract/new-name)))
          (is (= #{"before"} old-values))))
      (is (contains? (d/schema conn) :eacl.contract/new-name))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest explicit-read-snapshot-enforces-owner-and-nesting-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-owner-" (random-uuid)))
        conn (d/get-conn dir)
        first-snapshot (d/open-read-snapshot conn)
        second-snapshot (d/open-read-snapshot conn)]
    (try
      (is (= :datalevin/read-snapshot-thread-violation
             (:type
              @(future
                 (error-data
                  #(d/with-read-snapshot first-snapshot identity))))))
      (is (= :datalevin/read-snapshot-thread-violation
             (:type
              @(future
                 (error-data
                  #(d/read-snapshot-revision-info first-snapshot))))))
      (is (= :datalevin/read-snapshot-thread-violation
             (:type
              @(future
                 (error-data
                  #(d/close-read-snapshot! first-snapshot))))))
      (is (= :nested-ok
             (d/with-read-snapshot
              first-snapshot
              (fn [_]
                (d/with-read-snapshot
                 first-snapshot (constantly :nested-ok))))))
      (is (= :datalevin/nested-read-snapshot-conflict
             (:type
              (error-data
               #(d/with-read-snapshot
                 first-snapshot
                 (fn [_]
                   (d/with-read-snapshot
                    second-snapshot identity)))))))
      (finally
        (d/close-read-snapshot! second-snapshot)
        (d/close-read-snapshot! first-snapshot)
        (d/close conn)
        (u/delete-files dir)))))

(deftest escaped-snapshot-db-and-lazy-read-fail-closed-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-scope-" (random-uuid)))
        conn
        (d/get-conn
         dir
         {:eacl.contract/name
          {:db/valueType :db.type/string
           :db/unique :db.unique/identity}})
        escaped-db (atom nil)]
    (try
      (d/transact! conn [{:eacl.contract/name "before"}])
      (with-open [snapshot (d/open-read-snapshot conn)]
        (let [returned-datoms
              (d/with-read-snapshot
               snapshot
               (fn [snapshot-db]
                 (reset! escaped-db snapshot-db)
                 (d/datoms snapshot-db :ave
                           :eacl.contract/name)))]
          (testing "bounded datoms are fully realized before scope return"
            (is (= #{"before"} (set (map :v returned-datoms)))))
          (is (= :datalevin/read-snapshot-context-required
                 (:type
                  (error-data
                   #(d/datoms @escaped-db :ave
                              :eacl.contract/name)))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest virtual-thread-acquisition-is-rejected-before-reader-accounting-test
  (let [virtual-builder
        (try
          (clojure.lang.Reflector/invokeStaticMethod
           "java.lang.Thread" "ofVirtual" (object-array 0))
          (catch Throwable _ nil))]
    (when virtual-builder
      (let [dir (u/tmp-dir (str "eacl-snapshot-virtual-" (random-uuid)))
            conn (d/get-conn dir)
            before (d/active-read-snapshot-info)
            result (promise)
            runnable
            (reify Runnable
              (run [_]
                (deliver result
                         (error-data #(d/open-read-snapshot conn)))))
            thread
            (clojure.lang.Reflector/invokeInstanceMethod
             virtual-builder "start" (object-array [runnable]))]
        (try
          (.join ^Thread thread)
          (is (= :datalevin/unsupported-read-snapshot-runtime
                 (:type @result)))
          (is (= before (d/active-read-snapshot-info)))
          (finally
            (d/close conn)
            (u/delete-files dir)))))))

(deftest interruption-is-rejected-before-acquisition-or-snapshot-access-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-interruption-" (random-uuid)))
        conn (d/get-conn dir)
        before (d/active-read-snapshot-info)]
    (try
      (testing "an already-interrupted owner cannot acquire native state"
        (.interrupt (Thread/currentThread))
        (try
          (is (= :datalevin/read-snapshot-interrupted
                 (:type (error-data #(d/open-read-snapshot conn)))))
          (is (= before (d/active-read-snapshot-info)))
          (finally
            (Thread/interrupted))))
      (testing "interruption after acquisition fails before snapshot access"
        (let [snapshot (d/open-read-snapshot conn)]
          (try
            (.interrupt (Thread/currentThread))
            (try
              (is (= :datalevin/read-snapshot-interrupted
                     (:type
                      (error-data
                       #(d/with-read-snapshot snapshot identity)))))
              (finally
                (Thread/interrupted)))
            (finally
              (d/close-read-snapshot! snapshot))))
        (is (= before (d/active-read-snapshot-info))))
      (finally
        (Thread/interrupted)
        (d/close conn)
        (u/delete-files dir)))))

(deftest intentionally-unclosed-reader-is-observable-and-blocks-close-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-leak-detection-" (random-uuid)))
        conn (d/get-conn dir)
        before (d/active-read-snapshot-info)
        snapshot (d/open-read-snapshot conn)]
    (try
      (let [active (d/active-read-snapshot-info)]
        (is (= (inc (:active before)) (:active active)))
        (is (number? (:oldest-age-ms active)))
        (is (not (neg? (:oldest-age-ms active))))
        (is (= :datalevin/active-read-snapshots
               (:type (error-data #(d/close conn))))))
      (finally
        (d/close-read-snapshot! snapshot)
        (d/close conn)
        (u/delete-files dir)))
    (is (= before (d/active-read-snapshot-info)))))

(deftest capability-probe-is-read-only-and-describes-native-profile-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-capabilities-" (random-uuid)))
        conn (d/get-conn dir)
        before (d/active-read-snapshot-info)]
    (try
      (is (= {:supported? true
              :backend :embedded
              :ownership :closeable
              :thread-affinity :acquiring-platform-thread
              :cache-mode :bypass
              :wal? false
              :ha-mode nil}
             (dissoc (d/read-snapshot-capabilities conn) :env-flags)))
      (is (set? (:env-flags (d/read-snapshot-capabilities conn))))
      (is (= before (d/active-read-snapshot-info)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest connection-close-rejects-active-snapshot-and-remains-retryable-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-close-order-" (random-uuid)))
        conn (d/get-conn dir)
        snapshot (d/open-read-snapshot conn)]
    (try
      (is (= :datalevin/active-read-snapshots
             (:type (error-data #(d/close conn)))))
      (is (false? (d/closed? conn)))
      (is (integer?
           (d/with-read-snapshot snapshot #(-> % :max-tx))))
      (is (true? (d/close-read-snapshot! snapshot)))
      (d/close conn)
      (is (true? (d/closed? conn)))
      (finally
        (d/close-read-snapshot! snapshot)
        (d/close conn)
        (u/delete-files dir)))))

(deftest native-release-failure-retains-accounting-and-is-retryable-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-release-failure-" (random-uuid)))
        conn (d/get-conn dir)
        before (d/active-read-snapshot-info)
        snapshot (d/open-read-snapshot conn)]
    (try
      (let [close-during-release (atom nil)
            error
            (binding [storage/*read-snapshot-fault-injector*
                      (fn [phase]
                        (when (= :before-native-release phase)
                          (reset! close-during-release
                                  @(future (error-data #(d/close conn))))
                          (throw
                           (ex-info "injected"
                                    {:type :injected/release}))))]
              (error-data #(d/close-read-snapshot! snapshot)))]
        (is (= :datalevin/read-snapshot-release-failed (:type error)))
        (is (= :datalevin/active-read-snapshots
               (:type @close-during-release)))
        (is (= (inc (:active before))
               (:active (d/active-read-snapshot-info))))
        (is (integer?
             (d/with-read-snapshot snapshot #(-> % :max-tx))))
        (is (true? (d/close-read-snapshot! snapshot)))
        (is (= before (d/active-read-snapshot-info))))
      (finally
        (d/close-read-snapshot! snapshot)
        (d/close conn)
        (u/delete-files dir)))))

(deftest explicit-snapshot-survives-map-growth-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-map-growth-" (random-uuid)))
        conn
        (d/get-conn
         dir
         {:eacl.contract/name
          {:db/valueType :db.type/string
           :db/unique :db.unique/identity}
          :eacl.contract/payload
          {:db/valueType :db.type/string}}
         {:kv-opts {:mapsize 1}})]
    (try
      (d/transact! conn [{:eacl.contract/name "before"
                          :eacl.contract/payload "small"}])
      (with-open [snapshot (d/open-read-snapshot conn)]
        (let [payload (apply str (repeat 32768 "x"))
              additions
              (mapv (fn [i]
                      {:eacl.contract/name (str "after-" i)
                       :eacl.contract/payload (str payload i)})
                    (range 96))]
          (d/transact! conn additions)
          (is (= #{"before"} (snapshot-names snapshot)))
          (is (= 97 (count (names (d/db conn)))))
          (is (= #{"before"} (snapshot-names snapshot)))
          (is (true? (d/read-snapshot-cache-bypass? snapshot)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest acquisition-and-body-failures-do-not-leak-readers-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-failures-" (random-uuid)))
        conn (d/get-conn dir)
        before (d/active-read-snapshot-info)]
    (try
      (testing "native acquisition failure cannot enter accounting"
        (is (= :injected/acquisition
               (:type
                (binding [storage/*read-snapshot-fault-injector*
                          (fn [phase]
                            (when (= :before-native-acquisition phase)
                              (throw
                               (ex-info "injected"
                                        {:type :injected/acquisition}))))]
                  (error-data #(d/open-read-snapshot conn))))))
        (is (= before (d/active-read-snapshot-info))))
      (testing "metadata failure closes the already-acquired native reader"
        (is (= :injected/metadata
               (:type
                (binding [storage/*read-snapshot-fault-injector*
                          (fn [phase]
                            (when (= :after-native-acquisition phase)
                              (throw
                               (ex-info "injected"
                                        {:type :injected/metadata}))))]
                  (error-data #(d/open-read-snapshot conn))))))
        (is (= before (d/active-read-snapshot-info))))
      (testing "lexical ownership closes after a throwing body"
        (is (= :injected/body
               (:type
                (error-data
                 #(with-open [snapshot (d/open-read-snapshot conn)]
                    (d/with-read-snapshot
                     snapshot
                     (fn [_]
                       (throw
                        (ex-info "injected"
                                 {:type :injected/body})))))))))
        (is (= before (d/active-read-snapshot-info))))
      (testing "a subsequent reader still works"
        (with-open [snapshot (d/open-read-snapshot conn)]
          (is (integer? (:max-tx (d/read-snapshot-info snapshot))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest reader-slot-exhaustion-is-classified-and-does-not-leak-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-reader-slots-" (random-uuid)))
        ;; Datalevin's ordinary platform-thread reader consumes one LMDB slot
        ;; during connection/bootstrap reads, leaving one explicit slot.
        conn (d/get-conn dir nil {:kv-opts {:max-readers 2}})
        before (d/active-read-snapshot-info)
        first-snapshot (d/open-read-snapshot conn)]
    (try
      (let [error (error-data #(d/open-read-snapshot conn))]
        (is (= :datalevin/read-snapshot-acquisition-failed (:type error)))
        (is (= :reader-slots-exhausted (:reason error)))
        (is (not (contains? error :dir)))
        (is (= (inc (:active before))
               (:active (d/active-read-snapshot-info)))))
      (finally
        (d/close-read-snapshot! first-snapshot)
        (d/close conn)
        (u/delete-files dir)))
    (is (= before (d/active-read-snapshot-info)))))

(deftest native-read-storage-failures-are-classified-and-retryable-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-native-read-failure-"
                            (random-uuid)))
        conn
        (d/get-conn
         dir
         {:eacl.contract/name
          {:db/valueType :db.type/string
           :db/unique :db.unique/identity}})
        before (d/active-read-snapshot-info)]
    (try
      (d/transact! conn [{:eacl.contract/name "before"}])
      (with-open [snapshot (d/open-read-snapshot conn)]
        (doseq [[native-message reason]
                [["MDB_MAP_FULL: injected native read" :map-full]
                 ["ENOSPC: no space left on device" :disk-full]
                 ["MDB_PAGE_NOTFOUND: injected native read"
                  :native-read-failure]]]
          (let [error
                (binding [read-tx/*native-read-fault-injector*
                          (fn [_]
                            (throw (RuntimeException. native-message)))]
                  (error-data #(snapshot-names snapshot)))]
            (is (= :datalevin/read-snapshot-native-read-failed
                   (:type error)))
            (is (= reason (:reason error)))
            (is (= :read (:operation error)))
            (is (= (inc (:active before))
                   (:active (d/active-read-snapshot-info))))
            (is (= #{"before"} (snapshot-names snapshot))))))
      (is (= before (d/active-read-snapshot-info)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest application-errors-that-mimic-native-failures-are-not-reclassified-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-application-errors-"
                            (random-uuid)))
        conn (d/get-conn dir)
        before (d/active-read-snapshot-info)]
    (try
      (with-open [snapshot (d/open-read-snapshot conn)]
        (doseq [application-error
                [(RuntimeException. "MDB_MAP_FULL from application code")
                 (ex-info "application disk full"
                          {:type :map-full :reason :disk-full})]]
          (let [observed
                (try
                  (d/with-read-snapshot
                   snapshot
                   (fn [_]
                     (throw application-error)))
                  nil
                  (catch Throwable error
                    error))]
            (is (identical? application-error observed))
            (is (not= :datalevin/read-snapshot-native-read-failed
                      (:type (ex-data observed))))
            (is (= (inc (:active before))
                   (:active (d/active-read-snapshot-info)))))))
      (is (= before (d/active-read-snapshot-info)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest environment-closed-before-acquisition-is-classified-test
  (let [dir (u/tmp-dir (str "eacl-snapshot-closed-environment-"
                            (random-uuid)))
        conn (d/get-conn dir)
        db (d/db conn)
        before (d/active-read-snapshot-info)]
    (try
      (d/close conn)
      (let [error (error-data #(d/open-read-snapshot db))]
        (is (= :datalevin/read-snapshot-acquisition-failed (:type error)))
        (is (= :environment-closed (:reason error)))
        (is (= :acquire (:operation error)))
        (is (= before (d/active-read-snapshot-info))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
