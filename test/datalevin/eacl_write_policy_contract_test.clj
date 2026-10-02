(ns datalevin.eacl-write-policy-contract-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.conn :as conn]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.util :as u])
  (:import
   [java.io File]
   [java.lang ProcessBuilder]))

(def base-schema
  {:guarded/value {:db/valueType :db.type/string}
   :guarded/gen {:db/valueType :db.type/long}
   :definition/value {:db/valueType :db.type/string}
   :tuple/value {:db/valueType :db.type/tuple
                 :db/tupleTypes [:db.type/keyword :db.type/ref]}
   :app/value {:db/valueType :db.type/string}
   :ref/value {:db/valueType :db.type/ref}})

(defn- throwable-data
  [f]
  (try
    (f)
    nil
    (catch Throwable error
      (loop [cause error]
        (when cause
          (let [data (ex-data cause)]
            (if (:type data)
              data
              (recur (ex-cause cause)))))))))

(defn- temp-store
  []
  (u/tmp-dir (str "eacl-write-policy-" (random-uuid))))

(defn- close-delete!
  [conn dir]
  (when conn (d/close conn))
  (u/delete-files dir))

(defn- same-entity-policy
  []
  {:guarded-attributes #{:guarded/value :guarded/gen}
   :frozen-attributes #{:guarded/value :guarded/gen}
   :commit-generation-attributes #{:guarded/gen}
   :stamp-rules [{:when-attribute :guarded/value
                  :stamp-attribute :guarded/gen
                  :stamp-entity :same-entity}]})

(def grown-schema
  "`base-schema` re-declared unchanged beside one new attribute."
  (assoc base-schema :extension/value {:db/valueType :db.type/string}))

(def redefining-schema
  "`grown-schema` with the definition of one frozen attribute changed."
  (assoc-in grown-schema
            [:guarded/value :db/cardinality] :db.cardinality/many))

(defn- rejected-open
  "The typed error data of a rejected `create-conn`. An admitted connection
  is closed and yields nil."
  [dir schema]
  (throwable-data #(d/close (d/create-conn dir schema))))

(defn- stored-schema
  "The schema of a closed store, read without submitting a schema map."
  [dir]
  (let [conn (d/create-conn dir)]
    (try
      (d/schema conn)
      (finally
        (d/close conn)))))

(defn- shared-store-refs
  [dir]
  (get-in @@#'conn/shared-local-stores
          [(.getCanonicalPath (File. ^String dir)) :refs]))

(deftest policy-capabilities-persist-and-token-rotates-on-reopen
  (let [dir (temp-store)
        first-conn (d/create-conn dir base-schema)]
    (try
      (let [{:keys [policy write-token capabilities]}
            (d/install-write-policy! first-conn (same-entity-policy))]
        (is (= capabilities (d/write-policy-capabilities)))
        (is (= policy (d/write-policy first-conn)))
        (d/close first-conn)
        (let [second-conn (d/create-conn dir)]
          (try
            (is (= policy (d/write-policy second-conn)))
            (let [second-install
                  (d/install-write-policy! second-conn (same-entity-policy))]
              (is (uuid? (:write-token second-install)))
              (is (not= write-token (:write-token second-install))))
            (finally
              (d/close second-conn)))))
      (finally
        (close-delete! first-conn dir)))))

(deftest replacing-a-persisted-policy-requires-current-open-admission
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [{:keys [write-token]}
            (d/install-write-policy! conn (same-entity-policy))
            replacement
            (assoc (same-entity-policy)
                   :guarded-write-hint "replacement policy")]
        (is (= :datalevin/guarded-write-policy-install
               (:type
                (throwable-data
                 #(d/install-write-policy! conn replacement)))))
        (is (= "replacement policy"
               (get-in
                (d/install-write-policy!
                 conn replacement
                 {:datalevin/write-token write-token})
                [:policy :guarded-write-hint]))))
      (finally
        (close-delete! conn dir)))))

(deftest scalar-current-tx-is-the-commit-generation-without-eid-allocation
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [eid (:e (first (:tx-data
                            (d/transact! conn [{:app/value "anchor"}]))))
            max-eid-before (:max-eid (d/db conn))
            token (:write-token
                   (d/install-write-policy! conn (same-entity-policy)))
            report
            (d/transact!
             conn
             [[:db/add eid :guarded/value "committed"]
              [:db/add eid :guarded/gen :db/current-tx]]
             {:datalevin/write-token token})
            generation (:v (first (d/datoms (:db-after report)
                                             :eav eid :guarded/gen)))]
        (is (= generation (:max-tx (:db-after report))))
        (is (= max-eid-before (:max-eid (:db-after report))))
        (with-open [snapshot (d/open-read-snapshot conn)]
          (is (= generation
                 (:max-tx (d/read-snapshot-revision-info snapshot))))))
      (finally
        (close-delete! conn dir)))))

(deftest protected-datoms-require-admission-complete-stamps-and-fresh-values
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [eid (:e (first (:tx-data
                            (d/transact! conn [{:app/value "anchor"}]))))
            token (:write-token
                   (d/install-write-policy! conn (same-entity-policy)))
            admitted {:datalevin/write-token token}]
        (is (= :datalevin/guarded-attribute-write
               (:type
                (throwable-data
                 #(d/transact!
                   conn
                   [[:db/add eid :guarded/value "unadmitted"]
                    [:db/add eid :guarded/gen :db/current-tx]])))))
        (is (= :datalevin/missing-stamp
               (:type
                (throwable-data
                 #(d/transact! conn
                               [[:db/add eid :guarded/value "missing"]]
                               admitted)))))
        (is (= :datalevin/stale-generation
               (:type
                (throwable-data
                 #(d/transact!
                   conn
                   [[:db/add eid :guarded/value "stale"]
                    [:db/add eid :guarded/gen 1]]
                   admitted)))))
        (is (some? (d/transact! conn [{:app/value "ordinary"}])))
        (is (some?
             (d/transact!
              conn
              [[:db/add eid :guarded/value "valid"]
               [:db/add eid :guarded/gen :db/current-tx]]
              admitted))))
      (finally
        (close-delete! conn dir)))))

(deftest every-public-transaction-entry-converges-on-policy-enforcement
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [eid (:e (first (:tx-data
                            (d/transact! conn [{:app/value "anchor"}]))))
            token (:write-token
                   (d/install-write-policy! conn (same-entity-policy)))
            tx-meta {:datalevin/write-token token}
            tx-data (fn [value]
                      [[:db/add eid :guarded/value value]
                       [:db/add eid :guarded/gen :db/current-tx]])
            assert-current!
            (fn [report]
              (is (= (:max-tx (:db-after report))
                     (:v (first (d/datoms (:db-after report)
                                          :eav eid :guarded/gen))))))]
        (testing "direct synchronous entry"
          (assert-current! (d/transact! conn (tx-data "direct") tx-meta)))
        (testing "asynchronous entry"
          (assert-current! @(d/transact-async conn (tx-data "async") tx-meta)))
        (testing "blocking asynchronous entry"
          (assert-current! @(d/transact conn (tx-data "blocking") tx-meta)))
        (testing "explicit transaction entry"
          (let [report
                (d/with-transaction [tx-conn conn]
                  (d/transact! tx-conn (tx-data "explicit") tx-meta))]
            (assert-current! (assoc report :db-after (d/db conn)))))
        (testing "blind map fast path"
          (let [report
                (d/transact!
                 conn
                 [{:guarded/value "blind"
                   :guarded/gen :db/current-tx}]
                 tx-meta)]
            (is (some #(= :guarded/gen (:a %)) (:tx-data report)))))
        (testing "speculative with is exempt because it never commits"
          (is (seq
               (:tx-data
                (conn/with (d/db conn) (tx-data "simulated") nil true))))))
      (finally
        (close-delete! conn dir)))))

(deftest stale-shared-connection-can-refresh-and-replan
  (let [dir (temp-store)
        first-conn (d/create-conn dir base-schema)
        second-conn (d/create-conn dir base-schema)]
    (try
      (let [first-eid
            (:e (first (:tx-data
                        (d/transact! first-conn [{:app/value "first"}]))))
            second-eid
            (:e (first (:tx-data
                        (d/transact! first-conn [{:app/value "second"}]))))
            token
            (:write-token
             (d/install-write-policy! first-conn (same-entity-policy)))
            admitted {:datalevin/write-token token}
            transaction
            [[:db/add second-eid :guarded/value "from-second"]
             [:db/add second-eid :guarded/gen :db/current-tx]]]
        (d/transact!
         first-conn
         [[:db/add first-eid :guarded/value "advanced"]
          [:db/add first-eid :guarded/gen :db/current-tx]]
         admitted)
        (is (= :datalevin/stale-generation
               (:type
                (throwable-data
                 #(d/transact! second-conn transaction admitted)))))
        (d/refresh-connection! second-conn)
        (let [report (d/transact! second-conn transaction admitted)]
          (is (= (:max-tx (:db-after report))
                 (:guarded/gen
                  (d/entity (:db-after report) second-eid))))))
      (finally
        (d/close first-conn)
        (d/close second-conn)
        (u/delete-files dir)))))

(deftest tuple-and-constant-stamp-selectors-are-exact
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [entities
            (d/transact! conn
                         [{:db/id "endpoint" :app/value "endpoint"}
                          {:db/id "relation" :app/value "relation"}
                          {:db/id "schema" :app/value "schema"}
                          {:db/id "wrong" :app/value "wrong"}])
            endpoint (get (:tempids entities) "endpoint")
            relation (get (:tempids entities) "relation")
            schema-eid (get (:tempids entities) "schema")
            wrong-eid (get (:tempids entities) "wrong")
            policy
            {:guarded-attributes
             #{:tuple/value :definition/value :guarded/gen}
             :frozen-attributes
             #{:tuple/value :definition/value :guarded/gen}
             :commit-generation-attributes #{:guarded/gen}
             :stamp-rules
             [{:when-attribute :tuple/value
               :stamp-attribute :guarded/gen
               :stamp-entity [:tuple-position 1]}
              {:when-attribute :definition/value
               :stamp-attribute :guarded/gen
               :stamp-entity [:constant schema-eid]}]}
            token (:write-token (d/install-write-policy! conn policy))
            admitted {:datalevin/write-token token}]
        (is (= :datalevin/missing-stamp
               (:type
                (throwable-data
                 #(d/transact!
                   conn
                   [[:db/add endpoint :tuple/value [:subject relation]]
                    [:db/add wrong-eid :guarded/gen :db/current-tx]]
                   admitted)))))
        (is (= :datalevin/missing-stamp
               (:type
                (throwable-data
                 #(d/transact!
                   conn
                   [[:db/add relation :definition/value "changed"]
                    [:db/add wrong-eid :guarded/gen :db/current-tx]]
                   admitted)))))
        (is (some?
             (d/transact!
              conn
              [[:db/add endpoint :tuple/value [:subject relation]]
               [:db/add relation :guarded/gen :db/current-tx]
               [:db/add relation :definition/value "changed"]
               [:db/add schema-eid :guarded/gen :db/current-tx]]
              admitted))))
      (finally
        (close-delete! conn dir)))))

(deftest retract-entity-expansion-cannot-bypass-policy
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [report (d/transact! conn [{:guarded/value "before"}])
            eid (:e (first (:tx-data report)))]
        (d/install-write-policy! conn (same-entity-policy))
        (is (= :datalevin/guarded-attribute-write
               (:type
                (throwable-data
                 #(d/transact! conn [[:db/retractEntity eid]])))))
        (is (= "before" (:guarded/value (d/entity (d/db conn) eid)))))
      (finally
        (close-delete! conn dir)))))

(deftest frozen-schema-and-destructive-admin-paths-require-the-token
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [token (:write-token
                   (d/install-write-policy! conn (same-entity-policy)))]
        (is (= :datalevin/frozen-attribute-write
               (:type
                (throwable-data
                 #(d/update-schema
                   conn {:guarded/value {:db/index true}})))))
        (is (map?
             (d/update-schema
              conn {:guarded/value {:db/index true}} nil nil
              {:datalevin/write-token token})))
        (is (= :datalevin/frozen-attribute-write
               (:type
                (throwable-data
                 #(d/clear-dbi (d/datalog-kv conn) c/eav)))))
        (is (= :datalevin/frozen-attribute-write
               (:type
                (throwable-data
                 #(d/drop-dbi (d/datalog-kv conn) c/ave))))))
        (is (= :datalevin/frozen-attribute-write
               (:type
                (throwable-data
                 #(d/re-index conn {})))))
      (finally
        (close-delete! conn dir)))))

(deftest schema-at-open-touches-only-the-attributes-it-changes
  (let [dir (temp-store)
        first-conn (d/create-conn dir base-schema)]
    (try
      (let [policy (:policy
                    (d/install-write-policy! first-conn (same-entity-policy)))
            stored (d/schema first-conn)]
        (d/close first-conn)
        (testing "a changed frozen attribute is rejected and named"
          (let [data (rejected-open dir redefining-schema)]
            (is (= :datalevin/frozen-attribute-write (:type data)))
            (is (= #{:guarded/value} (:attributes data))))
          (is (= stored (stored-schema dir))))
        (testing "unchanged frozen attributes are admitted beside a new one"
          (let [conn (d/create-conn dir grown-schema)]
            (try
              (let [schema (d/schema conn)]
                (is (= stored (dissoc schema :extension/value)))
                (is (= :db.type/string
                       (get-in schema [:extension/value :db/valueType]))))
              (testing "which stays outside the persisted policy"
                (is (= policy (d/write-policy conn)))
                (is (some? (d/transact! conn [{:extension/value "open"}]))))
              (finally
                (d/close conn))))))
      (finally
        (close-delete! first-conn dir)))))

(deftest update-schema-touches-only-the-attributes-it-changes
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [policy (:policy
                    (d/install-write-policy! conn (same-entity-policy)))
            stored (d/schema conn)]
        (testing "a changed frozen attribute is rejected and named"
          (let [data (throwable-data
                      #(d/update-schema conn redefining-schema))]
            (is (= :datalevin/frozen-attribute-write (:type data)))
            (is (= #{:guarded/value} (:attributes data))))
          (is (= stored (d/schema conn))))
        (testing "unchanged frozen attributes are admitted beside a new one"
          (let [schema (d/update-schema conn grown-schema)]
            (is (= stored (dissoc schema :extension/value)))
            (is (= :db.type/string
                   (get-in schema [:extension/value :db/valueType]))))
          (testing "which stays outside the persisted policy"
            (is (= policy (d/write-policy conn)))
            (is (some? (d/transact! conn [{:extension/value "open"}]))))))
      (finally
        (close-delete! conn dir)))))

(deftest retyping-a-source-touches-its-frozen-composite-tuple
  (let [dir (temp-store)
        conn (d/create-conn
              dir
              {:pair/left {:db/cardinality :db.cardinality/one}
               :pair/right {:db/valueType :db.type/long}
               :pair/both {:db/tupleAttrs [:pair/left :pair/right]}})]
    (try
      (d/install-write-policy!
       conn
       {:guarded-attributes #{}
        :frozen-attributes #{:pair/both}
        :commit-generation-attributes #{}
        :stamp-rules []})
      (let [data (throwable-data
                  #(d/update-schema
                    conn {:pair/left {:db/valueType :db.type/string}}))]
        (is (= :datalevin/frozen-attribute-write (:type data)))
        (is (= #{:pair/both} (:attributes data))))
      (is (nil? (get-in (d/schema conn) [:pair/left :db/valueType])))
      (finally
        (close-delete! conn dir)))))

(deftest rejected-second-connection-keeps-the-shared-store-reference-count
  (let [dir (temp-store)
        other-spelling (str dir "/.")
        conn (d/create-conn dir base-schema)]
    (try
      (let [token (:write-token
                   (d/install-write-policy! conn (same-entity-policy)))
            kv (d/datalog-kv conn)]
        (is (= 1 (shared-store-refs dir)))
        (testing "a rejected connection is not counted"
          (is (= :datalevin/frozen-attribute-write
                 (:type (rejected-open other-spelling redefining-schema))))
          (is (= 1 (shared-store-refs dir)))
          (is (some? (d/transact! conn [{:app/value "still open"}]))))
        (testing "an admitted connection is counted until it closes"
          (let [second-conn (d/create-conn other-spelling grown-schema)]
            (try
              (is (= 2 (shared-store-refs dir)))
              (finally
                (d/close second-conn))))
          (is (= 1 (shared-store-refs dir))))
        (testing "the last connection closes the environment"
          (d/close conn)
          (is (nil? (shared-store-refs dir)))
          (is (d/closed-kv? kv))
          (let [reopened (d/create-conn dir)]
            (try
              (is (not= token
                        (:write-token
                         (d/install-write-policy!
                          reopened (same-entity-policy)))))
              (finally
                (d/close reopened))))))
      (finally
        (close-delete! conn dir)))))

(deftest current-tx-ref-compatibility-remains-and-other-values-are-rejected
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [ref-report
            (d/transact! conn [[:db/add 100 :ref/value :db/current-tx]])]
        (is (integer? (:v (first (:tx-data ref-report)))))
        (is (= :datalevin/invalid-current-tx-value
               (:type
                (throwable-data
                 #(d/transact!
                   conn [[:db/add 100 :app/value :db/current-tx]]))))))
      (finally
        (close-delete! conn dir)))))

(defn- run-foreign-writer!
  [dir]
  (let [java-bin (str (System/getProperty "java.home") "/bin/java")
        classpath (System/getProperty "java.class.path")
        form (pr-str
              `(let [conn# (datalevin.core/create-conn ~dir)]
                 (try
                   (datalevin.core/transact! conn# [{:app/value "foreign"}])
                   (finally (datalevin.core/close conn#)))))
        process (-> (ProcessBuilder.
                     ^java.util.List
                     [java-bin "-cp" classpath "clojure.main"
                      "-e" (str "(require 'datalevin.core) " form)])
                    (.redirectErrorStream true)
                    (.start))
        output (slurp (.getInputStream process))
        status (.waitFor process)]
    (is (zero? status) output)))

(deftest foreign-writer-between-prepare-and-commit-breaks-continuity
  (let [dir (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (let [before (d/db conn)
            prepared (conn/with before [{:app/value "prepared"}] nil true)]
        (run-foreign-writer! dir)
        (is (= :datalevin/max-tx-continuity-violation
               (:type
                (throwable-data
                 #(db/commit-prepared-tx-data!
                   before (:tx-data prepared) prepared)))))
        (is (= :datalevin/write-unusable
               (:type
                (throwable-data
                 #(d/transact! conn [{:app/value "blocked"}]))))))
      (finally
        (close-delete! conn dir)))))
