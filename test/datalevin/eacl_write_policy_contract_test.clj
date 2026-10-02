(ns datalevin.eacl-write-policy-contract-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.conn :as conn]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.dump :as dump]
   [datalevin.interface :as i]
   [datalevin.util :as u]
   [taoensso.nippy :as nippy])
  (:import
   [java.io DataInputStream DataOutputStream File FileInputStream
    FileOutputStream PushbackReader StringReader]
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

(deftest rejected-frozen-retype-does-not-re-encode-stored-values
  (let [dir (temp-store)
        first-conn (d/create-conn
                    dir {:untyped/value {:db/cardinality :db.cardinality/one}})
        policy {:guarded-attributes #{}
                :frozen-attributes #{:untyped/value}
                :commit-generation-attributes #{}
                :stamp-rules []}
        retype {:untyped/value {:db/valueType :db.type/string}}
        stored-eid (fn [conn]
                     (d/q '[:find ?e .
                            :where [?e :untyped/value "stored"]]
                          (d/db conn)))]
    (try
      (let [eid (:e (first (:tx-data
                            (d/transact! first-conn
                                         [{:untyped/value "stored"}]))))]
        (d/install-write-policy! first-conn policy)
        (testing "a rejection caught inside a transaction that then commits"
          (doseq [submit [#(d/update-schema % retype)
                          ;; the entry a server uses inside a transaction
                          #(i/set-schema (:store @%) retype)]]
            (let [data (d/with-transaction [tx-conn first-conn]
                         (throwable-data #(submit tx-conn)))]
              (is (= :datalevin/frozen-attribute-write (:type data)))
              (is (= #{:untyped/value} (:attributes data))))))
        (d/close first-conn)
        (let [conn (d/create-conn dir)]
          (try
            (testing "leaves the value findable under its stored encoding"
              (is (= eid (stored-eid conn))))
            (d/update-schema
             conn retype nil nil
             {:datalevin/write-token
              (:write-token (d/install-write-policy! conn policy))})
            (finally
              (d/close conn))))
        (testing "while the admitted retype re-encodes it"
          (let [conn (d/create-conn dir)]
            (try
              (is (= :db.type/string
                     (get-in (d/schema conn) [:untyped/value :db/valueType])))
              (is (= eid (stored-eid conn)))
              (finally
                (d/close conn))))))
      (finally
        (close-delete! first-conn dir)))))

(deftest rejected-update-schema-applies-none-of-its-steps
  (let [dir (temp-store)
        first-conn (d/create-conn dir base-schema)
        extension {:extension/value {:db/valueType :db.type/string}}
        caught (fn [& update-args]
                 (d/with-transaction [tx-conn first-conn]
                   (throwable-data
                    #(apply d/update-schema tx-conn update-args))))]
    (try
      (d/install-write-policy! first-conn (same-entity-policy))
      (let [stored (d/schema first-conn)]
        (testing "a frozen rename after an admitted schema map"
          (is (= #{:guarded/value}
                 (:attributes
                  (caught extension nil {:guarded/value :guarded/renamed})))))
        (testing "a frozen deletion after an admitted schema map"
          (is (= #{:guarded/gen}
                 (:attributes (caught extension #{:guarded/gen})))))
        (testing "every frozen attribute the call would touch is named"
          (is (= #{:guarded/value :guarded/gen}
                 (:attributes
                  (caught {:guarded/value
                           {:db/cardinality :db.cardinality/many}}
                          #{:guarded/gen})))))
        (d/close first-conn)
        (testing "the transactions that committed applied nothing"
          (is (= stored (stored-schema dir)))))
      (finally
        (close-delete! first-conn dir)))))

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

(defn- datom-set
  "The datoms of `db` as a set of [e a v]."
  [db]
  (into #{} (map (juxt :e :a :v)) (d/datoms db :eav)))

(defn- persisted-max-tx
  "The generation `conn`'s store has persisted. A store that has committed
  nothing yet is at the first generation."
  ^long [conn]
  (long (or (d/get-value (d/datalog-kv conn) c/meta :max-tx :attr :long)
            c/tx0)))

(defn- store-max-tx
  [conn]
  (i/max-tx (:store (d/db conn))))

(defn- commits-next-generation?
  "True when an ordinary transaction through `conn` commits the generation
  after the persisted one. A store whose in-memory generation differs from
  the persisted one rejects the commit instead."
  [conn]
  (let [generation (persisted-max-tx conn)
        report     (d/transact! conn [{:app/value "after the load"}])]
    (= (inc generation)
       (:max-tx (:db-after report))
       (store-max-tx conn)
       (persisted-max-tx conn))))

(deftest bulk-load-entry-points-commit-and-continue-the-generation
  (let [datoms [(d/datom 1 :app/value "one") (d/datom 2 :app/value "two")]
        loaded #{[1 :app/value "one"] [2 :app/value "two"]}]
    (testing "init-db"
      (let [dir  (temp-store)
            conn (d/conn-from-db (d/init-db datoms dir base-schema))]
        (try
          (is (= loaded (datom-set (d/db conn))))
          (is (commits-next-generation? conn))
          (finally
            (close-delete! conn dir)))))
    (testing "conn-from-datoms"
      (let [dir  (temp-store)
            conn (d/conn-from-datoms datoms dir base-schema)]
        (try
          (is (= loaded (datom-set (d/db conn))))
          (is (commits-next-generation? conn))
          (finally
            (close-delete! conn dir)))))
    (testing "fill-db"
      (let [dir  (temp-store)
            conn (d/create-conn dir base-schema)]
        (try
          (is (= loaded (datom-set (d/fill-db (d/db conn) datoms))))
          (d/refresh-connection! conn)
          (is (commits-next-generation? conn))
          (finally
            (close-delete! conn dir)))))
    (testing "fill-db inside an explicit transaction"
      (let [dir  (temp-store)
            conn (d/create-conn dir base-schema)]
        (try
          (d/with-transaction [tx-conn conn]
            (d/fill-db (d/db tx-conn) datoms))
          (is (= loaded (datom-set (d/db conn))))
          (is (commits-next-generation? conn))
          (finally
            (close-delete! conn dir)))))
    (testing "a loaded dump"
      (let [dir    (temp-store)
            binary (File/createTempFile "eacl-write-policy" ".nippy")
            source (d/conn-from-datoms datoms dir base-schema)
            text   (try
                     (with-open [out (DataOutputStream.
                                      (FileOutputStream. binary))]
                       (dump/dump-datalog source out))
                     (with-out-str (dump/dump-datalog source))
                     (finally
                       (close-delete! source dir)))]
        (try
          (doseq [[format load!]
                  [["as text"
                    #(dump/load-datalog
                      % (PushbackReader. (StringReader. text)) {} {})]
                   ["as nippy"
                    #(with-open [in (DataInputStream.
                                     (FileInputStream. binary))]
                       (dump/load-datalog % in {} {} true))]]]
            (testing format
              (let [target (temp-store)]
                (load! target)
                (let [conn (d/create-conn target)]
                  (try
                    (is (= loaded (datom-set (d/db conn))))
                    (is (commits-next-generation? conn))
                    (finally
                      (close-delete! conn target)))))))
          (finally
            (.delete binary)))))
    (testing "re-index"
      (let [dir        (temp-store)
            conn       (d/conn-from-datoms datoms dir base-schema)
            generation (persisted-max-tx conn)
            reindexed  (d/re-index conn {})]
        (try
          (is (= loaded (datom-set (d/db reindexed))))
          (is (< generation (persisted-max-tx reindexed)))
          (is (commits-next-generation? reindexed))
          (finally
            (close-delete! reindexed dir)))))))

(deftest bulk-load-in-several-batches-keeps-the-open-store-current
  (let [dir   (temp-store)
        conn  (d/create-conn dir base-schema)
        giant (apply str (repeat 2000 "g"))]
    (try
      (d/transact! conn [{:app/value "anchor"}])
      (let [generation (persisted-max-tx conn)
            filled     (binding [c/*fill-db-batch-size* 2]
                         (d/fill-db (d/db conn)
                                    [(d/datom 10 :app/value "first")
                                     (d/datom 11 :app/value "second")
                                     (d/datom 12 :loose/value "untyped")
                                     (d/datom 13 :app/value giant)
                                     (d/datom 14 :app/value "fifth")]))]
        (testing "every batch commits its own generation"
          (is (= (+ generation 3)
                 (:max-tx filled)
                 (store-max-tx conn)
                 (persisted-max-tx conn))))
        (testing "an attribute first seen in the load joins the open schema"
          (is (contains? (d/schema conn) :loose/value)))
        (d/refresh-connection! conn)
        (testing "later commits continue from what the load allocated"
          (let [report (d/transact! conn [{:db/id 15 :app/value (str giant 2)}
                                          {:db/id 16 :loose/value "more"}])]
            (is (= (+ generation 4)
                   (:max-tx (:db-after report))
                   (persisted-max-tx conn))))
          (is (= giant (:app/value (d/entity (d/db conn) 13))))
          (is (= (str giant 2) (:app/value (d/entity (d/db conn) 15))))))
      (finally
        (close-delete! conn dir)))))

(deftest bulk-load-across-a-map-resize-commits-every-batch-once
  (let [dir    (temp-store)
        ;; A map of one megabyte, which the load outgrows several times over.
        conn   (d/create-conn dir base-schema {:kv-opts {:mapsize 1}})
        filler (apply str (repeat 300 "x"))
        datoms (mapv #(d/datom (+ 100 (long %)) :app/value (str filler %))
                     (range 20000))]
    (try
      (d/transact! conn [{:app/value "anchor"}])
      (let [generation (persisted-max-tx conn)
            filled     (binding [c/*fill-db-batch-size* 5000]
                         (d/fill-db (d/db conn) datoms))]
        (is (= 20001 (count (d/datoms filled :eav))))
        (is (< (* 1024 1024) (.length (File. ^String dir "data.mdb"))))
        (testing "the load starts over when a resized map restarts it"
          (is (= (+ generation 4)
                 (:max-tx filled)
                 (store-max-tx conn)
                 (persisted-max-tx conn))))
        (d/refresh-connection! conn)
        (is (commits-next-generation? conn)))
      (finally
        (close-delete! conn dir)))))

(deftest foreign-writer-before-a-bulk-load-breaks-continuity
  (let [dir  (temp-store)
        conn (d/create-conn dir base-schema)]
    (try
      (d/transact! conn [{:app/value "anchor"}])
      (run-foreign-writer! dir)
      (is (= :datalevin/max-tx-continuity-violation
             (:type
              (throwable-data
               #(d/fill-db (d/db conn) [(d/datom 10 :app/value "bulk")])))))
      (is (= :datalevin/write-unusable
             (:type
              (throwable-data
               #(d/transact! conn [{:app/value "blocked"}])))))
      (finally
        (close-delete! conn dir)))))

(deftest bulk-load-entry-points-enforce-a-persisted-policy
  (let [dir       (temp-store)
        conn      (d/create-conn dir base-schema)
        guarded   [(d/datom 20 :app/value "first batch")
                   (d/datom 21 :guarded/value "unadmitted")]
        rejection #(:type (throwable-data %))]
    (try
      (d/transact! conn [{:app/value "anchor"}])
      (let [token      (:write-token
                        (d/install-write-policy! conn (same-entity-policy)))
            policy     (d/write-policy conn)
            stored     (datom-set (d/db conn))
            generation (persisted-max-tx conn)]
        (testing "fill-db rejects a guarded datom with the batches before it"
          (is (= :datalevin/guarded-attribute-write
                 (rejection #(binding [c/*fill-db-batch-size* 1]
                               (d/fill-db (d/db conn) guarded)))))
          (is (= stored (datom-set (d/db conn))))
          (is (= generation (store-max-tx conn) (persisted-max-tx conn))))
        (testing "the administrative token does not admit datoms"
          (is (= :datalevin/guarded-attribute-write
                 (rejection #(d/with-write-policy-token
                              conn token
                              (fn [] (d/fill-db (d/db conn) guarded)))))))
        (testing "fill-db loads datoms that the policy does not guard"
          (is (= (conj stored [22 :app/value "ordinary"])
                 (datom-set
                  (d/fill-db (d/db conn) [(d/datom 22 :app/value "ordinary")]))))
          (d/refresh-connection! conn)
          (is (some? (d/transact! conn
                                  [{:guarded/value "admitted"
                                    :guarded/gen :db/current-tx}]
                                  {:datalevin/write-token token}))))
        (let [stored (datom-set (d/db conn))]
          (d/close conn)
          (testing "a closed store rejects a guarded datom from"
            (testing "init-db"
              (is (= :datalevin/guarded-attribute-write
                     (rejection #(d/init-db guarded dir)))))
            (testing "conn-from-datoms"
              (is (= :datalevin/guarded-attribute-write
                     (rejection #(d/conn-from-datoms guarded dir)))))
            (testing "a loaded dump"
              (is (thrown?
                   Exception
                   (dump/load-datalog
                    dir
                    (PushbackReader.
                     (StringReader.
                      "{} {} [21 :guarded/value \"unadmitted\"]"))
                    {} {})))))
          (testing "and is left unchanged and closed by the rejected load"
            (let [reopened (d/create-conn dir)
                  kv       (d/datalog-kv reopened)]
              (is (= stored (datom-set (d/db reopened))))
              (is (= policy (d/write-policy reopened)))
              (d/close reopened)
              (is (d/closed-kv? kv))))
          (testing "init-db loads datoms that the policy does not guard"
            (let [reopened (d/conn-from-db
                            (d/init-db [(d/datom 23 :app/value "more")] dir))]
              (try
                (is (= (conj stored [23 :app/value "more"])
                       (datom-set (d/db reopened))))
                (is (= policy (d/write-policy reopened)))
                (is (commits-next-generation? reopened))
                (finally
                  (d/close reopened)))))))
      (finally
        (close-delete! conn dir)))))

(deftest bulk-load-inside-a-kv-transaction-fails-instead-of-stalling
  (let [dir     (temp-store)
        conn    (d/create-conn dir base-schema)
        kv      (d/datalog-kv conn)
        nested  (future
                  (throwable-data
                   #(d/with-transaction-kv [_ kv]
                      (d/fill-db (d/db conn)
                                 [(d/datom 1 :app/value "nested")]))))
        outcome (deref nested 60000 ::stalled)]
    ;; A load that stalled holds the environment's write lock for good, so
    ;; the store can be written and closed only when the load returned.
    (if (= ::stalled outcome)
      (is false "the nested bulk load did not return")
      (try
        (is (= :datalevin/nested-write-transaction (:type outcome)))
        (is (empty? (datom-set (d/db conn))))
        (is (commits-next-generation? conn))
        (finally
          (close-delete! conn dir))))))

(defn- definitions
  "The schema of `conn` without the attribute ids that storage assigns."
  [conn]
  (update-vals (d/schema conn) #(dissoc % :db/aid)))

(defn- contents
  "What a re-index has to keep of the store behind `conn`."
  [conn]
  {:datoms (datom-set (d/db conn))
   :schema (definitions conn)
   :policy (d/write-policy conn)})

(defn- stamped-store
  "Open a new store at `dir` that holds an ordinary datom and, under `policy`,
  a guarded entity with its commit-generation stamp."
  [dir policy]
  (let [conn    (d/create-conn dir base-schema)
        _       (d/transact! conn [{:app/value "ordinary"}])
        token   (:write-token (d/install-write-policy! conn policy))
        stamped (:e (first (:tx-data
                            (d/transact!
                             conn
                             [{:guarded/value "guarded"
                               :guarded/gen :db/current-tx}]
                             {:datalevin/write-token token}))))]
    {:conn conn :token token :stamped stamped}))

(defn- restamp!
  [conn stamped tx-meta]
  (d/transact! conn
               [[:db/add stamped :guarded/value "changed"]
                [:db/add stamped :guarded/gen :db/current-tx]]
               tx-meta))

(deftest admitted-re-index-keeps-data-schema-policy-and-generation
  (let [dir (temp-store)
        {:keys [conn token stamped]} (stamped-store dir (same-entity-policy))]
    (try
      (let [kept       (contents conn)
            generation (persisted-max-tx conn)
            reindexed  (d/with-write-policy-token
                        conn token #(d/re-index conn {}))]
        (try
          (is (= kept (contents reindexed)))
          (testing "a stored commit generation is loaded back as it was"
            (is (= generation
                   (:guarded/gen (d/entity (d/db reindexed) stamped)))))
          (testing "the store continues its generation instead of restarting"
            (is (< generation (persisted-max-tx reindexed)))
            (is (= (persisted-max-tx reindexed)
                   (store-max-tx reindexed)
                   (:max-tx (d/db reindexed)))))
          (testing "the reopened environment guards the store under a new token"
            (let [new-token (:write-token
                             (d/install-write-policy!
                              reindexed (same-entity-policy)))]
              (is (not= token new-token))
              (is (= :datalevin/guarded-attribute-write
                     (:type (throwable-data
                             #(restamp! reindexed stamped
                                        {:datalevin/write-token token})))))
              (is (< generation
                     (:guarded/gen
                      (d/entity
                       (:db-after
                        (restamp! reindexed stamped
                                  {:datalevin/write-token new-token}))
                       stamped))))
              (testing "which also admits a schema that changes a frozen one"
                (let [datoms  (datom-set (d/db reindexed))
                      changed (d/with-write-policy-token
                               reindexed new-token
                               #(d/re-index
                                 reindexed
                                 {:guarded/value {:db/valueType :db.type/string
                                                  :db/index     true}}
                                 {}))]
                  (try
                    (is (= datoms (datom-set (d/db changed))))
                    (is (true? (get-in (d/schema changed)
                                       [:guarded/value :db/index])))
                    (finally
                      (d/close changed)))))))
          (finally
            (d/close reindexed))))
      (finally
        (close-delete! conn dir)))))

(deftest re-index-without-frozen-attributes-needs-no-token
  (let [dir (temp-store)
        {:keys [conn stamped]}
        (stamped-store dir (assoc (same-entity-policy) :frozen-attributes #{}))]
    (try
      (let [kept       (contents conn)
            generation (persisted-max-tx conn)
            reindexed  (d/re-index conn {})]
        (try
          (is (= kept (contents reindexed)))
          (is (= generation
                 (:guarded/gen (d/entity (d/db reindexed) stamped))))
          (is (< generation (persisted-max-tx reindexed)))
          (finally
            (d/close reindexed))))
      (finally
        (close-delete! conn dir)))))

(deftest re-index-is-admitted-before-its-first-write
  (let [dir (temp-store)
        {:keys [conn]} (stamped-store dir (same-entity-policy))]
    (try
      (let [kept (contents conn)]
        (testing "also when entered below the public function, as a server does"
          (is (= :datalevin/frozen-attribute-write
                 (:type (throwable-data
                         #(dump/re-index-datalog conn {} {}))))))
        (is (not (d/closed? conn)))
        (is (= kept (contents conn)))
        (is (not (.exists (File. ^String dir "dl-dump")))))
      (finally
        (close-delete! conn dir)))))

(deftest failed-re-index-puts-the-previous-contents-back
  (doseq [[cause schema opts]
          [["a schema that the stored values do not satisfy"
            {:app/value {:db/valueType :db.type/long}} {}]
           ["options that the store cannot be opened with"
            {} {:search-opts :not-a-map}]]]
    (testing cause
      (let [dir (temp-store)
            {:keys [conn token stamped]}
            (stamped-store dir (same-entity-policy))]
        (try
          (let [kept       (contents conn)
                generation (persisted-max-tx conn)]
            (is (thrown? Exception
                         (d/with-write-policy-token
                          conn token #(d/re-index conn schema opts))))
            (let [reopened (d/create-conn dir)]
              (try
                (is (= kept (contents reopened)))
                (testing "at a generation that is not behind the stored ones"
                  (is (<= generation (persisted-max-tx reopened))))
                (testing "and still writable by an admitted writer"
                  (is (some?
                       (restamp!
                        reopened stamped
                        {:datalevin/write-token
                         (:write-token
                          (d/install-write-policy!
                           reopened (same-entity-policy)))}))))
                (finally
                  (d/close reopened)))))
          (finally
            (close-delete! conn dir)))))))

(deftest re-index-that-cannot-put-the-contents-back-names-their-dump
  (let [dir (temp-store)
        {:keys [conn token]} (stamped-store dir (same-entity-policy))]
    (try
      (let [datoms (datom-set (d/db conn))
            ;; No batch size below one can load anything, so neither the
            ;; reload nor putting the dump back succeeds.
            data   (binding [c/*fill-db-batch-size* 0]
                     (throwable-data
                      #(d/with-write-policy-token
                        conn token (fn [] (d/re-index conn {})))))]
        (is (= :datalevin/re-index-unrestored (:type data)))
        (is (= dir (:dir data)))
        (testing "a dump that holds every datom of the cleared store"
          (is (= datoms
                 (with-open [in (DataInputStream.
                                 (FileInputStream. ^String (:dump data)))]
                   (set (nth (nippy/thaw-from-in! in) 2)))))))
      (finally
        (close-delete! conn dir)))))
