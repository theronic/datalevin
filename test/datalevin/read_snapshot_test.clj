(ns datalevin.read-snapshot-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.interpret :as interpret]
   [datalevin.lmdb :as l]
   [datalevin.query-optimizer :as qo]
   [datalevin.server :as server]
   [datalevin.storage :as s]
   [datalevin.storage.schema :as ss]
   [datalevin.test.core :as tdc :refer [allocate-port db-fixture]]
   [datalevin.tx-state.lifetime :as lifetime]
   [datalevin.util :as u])
  (:import
   [clojure.lang ExceptionInfo]
   [datalevin.db DB]
   [datalevin.utl LRUCache]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicReference]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  db-fixture
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "read-snapshot-" (random-uuid)))]
      (try (f)
           (finally (u/delete-files *dir*))))))

(def schema
  {:name   {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :age    {:db/valueType :db.type/long}
   :friend {:db/valueType :db.type/ref}
   :tag    {:db/valueType   :db.type/string
            :db/cardinality :db.cardinality/many}})

(defn- open-conn
  ([] (open-conn schema nil))
  ([schema] (open-conn schema nil))
  ([schema opts]
   (d/create-conn (str *dir* "/" (random-uuid)) schema opts)))

(defmacro ^:private thrown-data
  "The ex-data of what body throws, nil when it returns"
  [& body]
  `(try ~@body nil (catch ExceptionInfo e# (ex-data e#))))

(defn- in-other-thread [f] @(future (f)))

(defn- keep-reading
  "Read db in another thread until a read throws, return the ex-data of that.
  `started` is counted down after the first read."
  [db read ^CountDownLatch started]
  (let [expected (read db)]
    (future
      (thrown-data
        (loop []
          (when-not (= expected (read db))
            (throw (IllegalStateException. "A read changed")))
          (.countDown started)
          (recur))))))

(def ^:private state-0
  [{:db/id -1 :name "Ann" :age 30 :friend -2 :tag ["a" "b"]}
   {:db/id -2 :name "Bob" :age 40}
   {:db/id -3 :name "Cid" :age 50 :friend -1}])

(def ^:private state-1
  [[:db/add [:name "Ann"] :age 31]
   [:db/retract [:name "Ann"] :tag "a"]
   [:db/retractEntity [:name "Bob"]]
   {:db/id -1 :name "Dee" :age 20 :friend [:name "Ann"] :tag ["d"]}])

(def ^:private state-2
  [[:db/retractEntity [:name "Cid"]]
   {:name "Eve" :age 35}])

(def ^:private rules
  '[[(knows ?a ?b) [?a :friend ?b]]
    [(knows ?a ?b) [?a :friend ?m] (knows ?m ?b)]])

(defn- eav [datoms] (mapv (juxt :e :a :v) datoms))

(defn- reads
  "What the Datalog read functions return for db, as plain data"
  [db]
  {:q           (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] db)
   :q-join      (d/q '[:find ?n ?fn
                       :where [?e :friend ?f] [?e :name ?n] [?f :name ?fn]]
                     db)
   :q-aggregate (d/q '[:find [(count ?e) (max ?a)] :where [?e :age ?a]] db)
   :q-input     (d/q '[:find ?a .
                       :in $ ?n
                       :where [?e :name ?n] [?e :age ?a]]
                     db "Ann")
   :q-pull      (set (d/q '[:find [(pull ?e [:name :age {:friend [:name]}]) ...]
                            :where [?e :name]]
                          db))
   :q-rule      (d/q '[:find ?n ?fn
                       :in $ %
                       :where (knows ?e ?f) [?e :name ?n] [?f :name ?fn]]
                     db rules)
   :q-not       (set (d/q '[:find [?n ...]
                            :where [?e :name ?n] (not [?e :friend _])]
                          db))
   :pull        (d/pull db '[* {:friend [:name]}] [:name "Ann"])
   :pull-many   (d/pull-many db [:name :age] [[:name "Ann"] [:name "Cid"]])
   :entity      (tdc/entity-map db [:name "Ann"])
   :entid       (d/entid db [:name "Bob"])
   :datoms-eav  (eav (d/datoms db :eav))
   :datoms-ave  (eav (d/datoms db :ave :age))
   :seek        (eav (d/seek-datoms db :ave :age 35))
   :rseek       (eav (d/rseek-datoms db :ave :age 35))
   :search      (eav (d/search-datoms db nil :name nil))
   :count       (d/count-datoms db nil :name nil)
   :cardinality (d/cardinality db :age)
   :index-range (eav (d/index-range db :age 30 40))
   :max-eid     (d/max-eid db)
   :schema      (d/schema db)
   :bounds      (select-keys db [:max-tx :max-eid])})

(deftest reads-keep-the-state-at-acquisition-test
  (let [conn (open-conn)]
    (try
      (d/transact! conn state-0)
      (let [before (reads (d/db conn))
            snap   (d/open-read-snapshot conn)]
        (try
          (is (d/read-snapshot? snap))
          (is (d/db? snap))
          (is (not (d/read-snapshot? (d/db conn))))
          (is (not (d/read-snapshot? conn)))
          (is (= before (reads snap)))
          (testing "a commit from another thread"
            (in-other-thread #(d/transact! conn state-1))
            (is (not= before (reads (d/db conn))))
            (is (= before (reads snap))))
          (testing "a commit from the thread that holds the snapshot"
            (d/transact! conn state-2)
            (is (= before (reads snap))))
          (testing "another snapshot sees what was committed since"
            (d/with-read-snapshot [later conn]
              (is (= (reads (d/db conn)) (reads later)))
              (is (= before (reads snap)))))
          (finally (d/close-read-snapshot snap))))
      (finally (d/close conn)))))

(deftest schema-is-the-one-at-acquisition-test
  (let [conn (open-conn (assoc schema
                               :note {}
                               :unused {:db/valueType :db.type/string}))]
    (try
      (d/transact! conn [{:name "Ann" :note 42 :old/attr "x"}])
      (let [before (d/schema conn)
            snap   (d/open-read-snapshot conn)]
        (try
          (is (= before (d/schema snap)))
          ;; attributes are added, by schema and by data
          (d/update-schema conn {:email {:db/valueType :db.type/string}})
          (d/transact! conn [{:name "Bob" :nick "bobby"}])
          ;; an attribute is renamed, another deleted
          (d/update-schema conn nil #{:unused} {:old/attr :new/attr})
          ;; the stored values of an untyped attribute are encoded again
          (d/update-schema conn {:note {:db/valueType :db.type/long}})
          (let [after (d/schema conn)]
            (is (= #{:email :nick :new/attr}
                   (set (remove (set (keys before)) (keys after)))))
            (is (not (contains? after :unused)))
            (is (= :db.type/long (get-in after [:note :db/valueType]))))
          (is (= before (d/schema snap)))
          (testing "the old schema reads the old data"
            (is (= [[1 :name "Ann"] [1 :note 42] [1 :old/attr "x"]]
                   (eav (d/datoms snap :eav 1))))
            (is (= "x" (d/q '[:find ?v . :where [_ :old/attr ?v]] snap)))
            (is (= 42 (d/q '[:find ?v . :where [_ :note ?v]] snap)))
            (is (= {:name "Ann" :note 42 :old/attr "x"}
                   (d/pull snap [:name :note :old/attr] 1))))
          (testing "the new schema reads the new data"
            (is (= {:name "Ann" :note 42 :new/attr "x"}
                   (d/pull (d/db conn) [:name :note :new/attr] 1)))
            (d/with-read-snapshot [later conn]
              (is (= (d/schema conn) (d/schema later)))
              (is (= {:name "Ann" :note 42 :new/attr "x"}
                     (d/pull later [:name :note :new/attr] 1)))))
          (finally (d/close-read-snapshot snap))))
      (testing "the schema is read from the snapshot, not from the connection"
        ;; committed as another process would, the connection does not know
        (let [kv (d/datalog-kv conn)]
          (ss/transact-schema
            kv (ss/update-schema (ss/load-schema kv)
                                 {:elsewhere {:db/valueType :db.type/long}})))
        (d/with-read-snapshot [snap conn]
          (is (= :db.type/long
                 (get-in (d/schema snap) [:elsewhere :db/valueType])))))
      (finally (d/close conn)))))

(def ^:private mid-query (atom nil))

(defn run-mid-query
  "A query predicate that runs the pending action, once, from inside the
  query that calls it."
  [_]
  (when-let [f (first (reset-vals! mid-query nil))] (f))
  true)

(deftest one-query-reads-one-state-test
  (let [versions {:name (:name schema) :ver {:db/valueType :db.type/long}}
        ;; The predicate belongs to the scan of :name, :ver is scanned later.
        q        '[:find ?n ?v
                   :where
                   [?e :name ?n]
                   [(datalevin.read-snapshot-test/run-mid-query ?n)]
                   [?e :ver ?v]]
        before   [{:name "a" :ver 0} {:name "b" :ver 0} {:name "c" :ver 0}]
        change   [[:db/add [:name "a"] :ver 1]
                  [:db/add [:name "c"] :ver 1]
                  [:db/retractEntity [:name "b"]]
                  {:name "d" :ver 1}]
        result-0 #{["a" 0] ["b" 0] ["c" 0]}
        result-1 #{["a" 1] ["c" 1] ["d" 1]}]
    (testing "a commit in the middle of a query on a snapshot"
      (let [conn (open-conn versions)]
        (try
          (d/transact! conn before)
          (d/with-read-snapshot [snap conn]
            (reset! mid-query
                    (fn [] (in-other-thread #(d/transact! conn change))))
            (is (= result-0 (d/q q snap)))
            (is (nil? @mid-query))
            (is (= result-1 (d/q q (d/db conn))))
            (is (= result-0 (d/q q snap))))
          (finally (d/close conn)))))
    (testing "the same commit in the middle of a query on the live db"
      (let [conn (open-conn versions)]
        (try
          (d/transact! conn before)
          (reset! mid-query
                  (fn [] (in-other-thread #(d/transact! conn change))))
          (is (not= result-0 (d/q q (d/db conn))))
          (is (nil? @mid-query))
          (finally (d/close conn)))))))

(deftest revision-bounds-test
  (let [conn (open-conn)]
    (try
      (testing "of an empty database"
        (d/with-read-snapshot [snap conn]
          (is (= (select-keys (d/db conn) [:max-tx :max-eid])
                 (select-keys snap [:max-tx :max-eid])))
          (is (= 0 (:max-eid snap)))
          (is (empty? (d/datoms snap :eav)))))
      (let [report (d/transact! conn state-0)
            snap   (d/open-read-snapshot conn)]
        (try
          (testing "are those of the last transaction in the snapshot"
            (is (= (get-in report [:tempids :db/current-tx]) (:max-tx snap)))
            (is (= (:max-tx (:db-after report)) (:max-tx snap)))
            (is (= 3 (:max-eid snap) (d/max-eid snap))))
          (testing "stay, while those of the connection move"
            (d/transact! conn state-1)
            (d/transact! conn state-2)
            (is (= (:max-tx (:db-after report)) (:max-tx snap)))
            (is (= 3 (:max-eid snap) (d/max-eid snap)))
            (d/with-read-snapshot [later conn]
              (is (= (+ 2 (long (:max-tx snap))) (:max-tx later)))
              (is (= 5 (:max-eid later) (d/max-eid later)))))
          (finally (d/close-read-snapshot snap))))
      (finally (d/close conn)))))

(deftest revision-bounds-match-the-data-under-commits-test
  ;; Each transaction adds one entity, so in any committed state the number
  ;; of entities, the max entity id and the max tx all move together.
  (let [conn   (open-conn {:seq {:db/valueType :db.type/long}})
        base   (select-keys (d/db conn) [:max-tx :max-eid])
        total  300
        writer (future
                 (dotimes [n total]
                   (d/transact! conn [{:seq (inc n)}])))
        state  (fn [snap]
                 (let [seqs (mapv :v (d/datoms snap :ave :seq))
                       n    (count seqs)]
                   {:n     n
                    :seqs? (= (range 1 (inc n)) seqs)
                    :tx    (- (long (:max-tx snap)) (long (:max-tx base)))
                    :eid   (- (long (:max-eid snap)) (long (:max-eid base)))
                    :query (or (d/q '[:find (count ?e) . :where [?e :seq]] snap)
                               0)
                    :count (d/count-datoms snap nil :seq nil)}))
        sound? (fn [{:keys [n seqs? tx eid query count]}]
                 (and seqs? (= n tx eid query count)))]
    (try
      (loop [seen #{}]
        (let [done? (realized? writer)
              found (d/with-read-snapshot [snap conn] (state snap))
              seen  (conj seen found)]
          (if done?
            (do (is (= [] (remove sound? seen)))
                (is (= total (:n found)))
                (is (< 2 (count seen)) "snapshots were taken between commits"))
            (recur seen))))
      (finally @writer (d/close conn)))))

(defn- cached-reads
  [db]
  {:q      (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] db)
   :count  (d/q '[:find (count ?e) . :where [?e :name]] db)
   :pull   (d/pull db [:name :age] [:name "Ann"])
   :datoms (mapv :v (d/datoms db :ave :age))
   :seek   (mapv :v (d/seek-datoms db :ave :age 0 nil 2))
   :entity (:age (d/entity db [:name "Ann"]))
   :size   (d/count-datoms db nil :name nil)})

(def ^:private cached-0
  {:q      #{["Ann" 30] ["Bob" 40]}
   :count  2
   :pull   {:name "Ann" :age 30}
   :datoms [30 40]
   :seek   [30 40]
   :entity 30
   :size   2})

(def ^:private cached-1
  {:q      #{["Ann" 31] ["Bob" 40] ["Cid" 50]}
   :count  3
   :pull   {:name "Ann" :age 31}
   :datoms [31 40 50]
   :seek   [31 40]
   :entity 31
   :size   3})

(deftest caches-are-left-alone-test
  (let [tx-0 [{:name "Ann" :age 30} {:name "Bob" :age 40}]
        tx-1 [[:db/add [:name "Ann"] :age 31] {:name "Cid" :age 50}]]
    (testing "a snapshot is not served what the live db cached later"
      (let [conn (open-conn)]
        (try
          (d/transact! conn tx-0)
          (d/with-read-snapshot [snap conn]
            (d/transact! conn tx-1)
            (dotimes [_ 2]
              (is (= cached-1 (cached-reads (d/db conn)))))
            (dotimes [_ 2]
              (is (= cached-0 (cached-reads snap)))))
          (finally (d/close conn)))))
    (testing "the live db is not served what a snapshot read"
      (let [conn (open-conn)]
        (try
          (d/transact! conn tx-0)
          (d/with-read-snapshot [snap conn]
            (d/transact! conn tx-1)
            (dotimes [_ 2]
              (is (= cached-0 (cached-reads snap))))
            (dotimes [_ 2]
              (is (= cached-1 (cached-reads (d/db conn))))))
          (finally (d/close conn)))))
    (testing "a snapshot publishes no query plan"
      (let [conn  (open-conn)
            q     '[:find ?n :where [?e :age ?a] [(< 35 ?a)] [?e :name ?n]]
            plans #(set (.keys ^LRUCache qo/*plan-cache*))]
        (try
          (d/transact! conn tx-0)
          (d/with-read-snapshot [snap conn]
            (let [before (plans)]
              (is (= #{["Bob"]} (d/q q snap)))
              (is (= before (plans)))
              (is (= #{["Bob"]} (d/q q (d/db conn))))
              (is (not= before (plans)))
              (d/transact! conn tx-1)
              (is (= #{["Bob"]} (d/q q snap)))
              (is (= #{["Bob"] ["Cid"]} (d/q q (d/db conn))))))
          (finally (d/close conn)))))))

(defn- names [db] (set (d/q '[:find [?n ...] :where [_ :name ?n]] db)))

(deftest closing-test
  (let [conn (open-conn)]
    (try
      (d/transact! conn state-0)
      (testing "is idempotent, and reads fail afterwards"
        (let [snap   (d/open-read-snapshot conn)
              entity (d/entity snap [:name "Ann"])]
          (is (= #{"Ann" "Bob" "Cid"} (names snap)))
          (is (nil? (d/close-read-snapshot snap)))
          (is (nil? (d/close-read-snapshot snap)))
          (is (d/read-snapshot? snap))
          (doseq [read [#(names snap)
                        #(d/datoms snap :eav)
                        #(d/seek-datoms snap :ave :age 0)
                        #(d/pull snap '[*] 1)
                        #(d/entid snap [:name "Ann"])
                        #(d/count-datoms snap nil :name nil)
                        #(d/max-eid snap)
                        #(:age entity)
                        #(d/get-value (d/datalog-kv snap) "datalevin/meta"
                                      :max-tx :attr :long)]]
            (is (= {:error :read-snapshot/closed} (thrown-data (read)))))))
      (testing "happens at the end of with-read-snapshot"
        (let [escaped (atom nil)]
          (is (= #{"Ann" "Bob" "Cid"}
                 (d/with-read-snapshot [snap conn]
                   (reset! escaped snap)
                   (names snap))))
          (is (= {:error :read-snapshot/closed} (thrown-data (names @escaped))))
          (is (thrown-with-msg?
                IllegalStateException #"body"
                (d/with-read-snapshot [snap conn]
                  (reset! escaped snap)
                  (throw (IllegalStateException. "body")))))
          (is (= {:error :read-snapshot/closed}
                 (thrown-data (names @escaped))))))
      (testing "is refused to what is not a snapshot"
        (is (thrown-with-msg? ExceptionInfo #"Expected a read snapshot"
                              (d/close-read-snapshot (d/db conn))))
        (is (thrown-with-msg? ExceptionInfo #"Expected a read snapshot"
                              (d/close-read-snapshot conn))))
      (testing "is refused from inside a read of the snapshot"
        (let [snap   (d/open-read-snapshot conn)
              inside (atom nil)]
          (d/visit (d/datalog-kv snap) "datalevin/meta"
                   (fn [_ _]
                     (reset! inside (thrown-data (d/close-read-snapshot snap)))
                     :datalevin/terminate-visit)
                   [:all] :raw :raw false)
          (is (= {:error :read-snapshot/in-use} @inside))
          (is (= #{"Ann" "Bob" "Cid"} (names snap)))
          (d/close-read-snapshot snap)))
      (testing "by close-db releases the snapshot and nothing else"
        (let [snap (d/open-read-snapshot conn)]
          (is (nil? (d/close-db snap)))
          (is (= {:error :read-snapshot/closed} (thrown-data (names snap))))
          (is (not (d/closed? conn)))
          (d/transact! conn [{:name "Dee"}])
          (is (= #{"Ann" "Bob" "Cid" "Dee"} (names (d/db conn))))))
      (testing "waits for a read in another thread, which then fails"
        (let [snap    (d/open-read-snapshot conn)
              started (CountDownLatch. 1)
              reader  (keep-reading snap names started)]
          (is (.await started 10 TimeUnit/SECONDS))
          (d/close-read-snapshot snap)
          (is (= {:error :read-snapshot/closed} (deref reader 10000 :stuck)))))
      (finally (d/close conn)))))

(deftest connection-close-closes-snapshots-test
  (let [dir     (str *dir* "/closing")
        conn    (d/create-conn dir schema)
        _       (d/transact! conn state-0)
        idle    (d/open-read-snapshot conn)
        busy    (d/open-read-snapshot conn)
        started (CountDownLatch. 1)
        reader  (keep-reading busy names started)]
    (is (.await started 10 TimeUnit/SECONDS))
    (d/close conn)
    (is (d/closed? conn))
    (is (= {:error :read-snapshot/closed} (deref reader 10000 :stuck)))
    (doseq [snap [idle busy]]
      (is (= {:error :read-snapshot/closed} (thrown-data (names snap))))
      (is (= {:error :read-snapshot/closed} (thrown-data (d/datoms snap :eav))))
      (is (nil? (d/close-read-snapshot snap))))
    (testing "the store opens again, and takes snapshots"
      (let [conn (d/create-conn dir)]
        (try
          (d/with-read-snapshot [snap conn]
            (is (= #{"Ann" "Bob" "Cid"} (names snap))))
          (finally (d/close conn)))))))

(deftest reader-slots-test
  (let [conn (open-conn schema {:kv-opts {:max-readers 8}})]
    (try
      (d/transact! conn state-0)
      (testing "a closed snapshot gives its reader slot back"
        (is (= #{3}
               (set (repeatedly
                      100 #(d/with-read-snapshot [snap conn]
                             (:max-eid snap)))))))
      (testing "running out of slots is reported, and leaves no slot taken"
        (let [open    (atom [])
              failure (loop []
                        (let [data (thrown-data
                                     (swap! open conj
                                            (d/open-read-snapshot conn)))]
                          (if (and (nil? data) (< (count @open) 100))
                            (recur)
                            data)))]
          (is (= :lmdb/read-transaction (:type failure)))
          (is (= :acquire (:operation failure)))
          (is (<= 1 (count @open) 8))
          (is (some? (thrown-data (d/open-read-snapshot conn))))
          (is (= #{#{"Ann" "Bob" "Cid"}} (set (map names @open))))
          (d/close-read-snapshot (peek @open))
          (d/with-read-snapshot [snap conn]
            (is (= #{"Ann" "Bob" "Cid"} (names snap))))
          (run! d/close-read-snapshot @open)
          (is (= #{3}
                 (set (repeatedly
                        20 #(d/with-read-snapshot [snap conn]
                              (:max-eid snap))))))))
      (testing "the environment keeps no track of closed snapshots"
        (is (empty? (.get ^AtomicReference
                          (:pinned-readers
                           @(i/kv-info (d/datalog-kv conn)))))))
      (finally (d/close conn)))))

(deftest unsupported-stores-test
  (testing "a transaction"
    (let [conn (open-conn)]
      (try
        (d/transact! conn state-0)
        (d/with-transaction [cn conn]
          (d/transact! cn [{:name "Dee"}])
          (is (= {:error :read-snapshot/unsupported :reason :transaction}
                 (thrown-data (d/open-read-snapshot cn))))
          (testing "whose connection still takes snapshots of what is committed"
            (d/with-read-snapshot [snap conn]
              (is (= #{"Ann" "Bob" "Cid"} (names snap)))
              (is (= #{"Ann" "Bob" "Cid" "Dee"} (names @cn))))))
        (is (= #{"Ann" "Bob" "Cid" "Dee"} (names (d/db conn))))
        (finally (d/close conn)))))
  (testing "a read snapshot"
    (let [conn (open-conn)]
      (try
        (d/with-read-snapshot [snap conn]
          (is (= {:error :read-snapshot/unsupported :reason :read-snapshot}
                 (thrown-data (db/read-snapshot snap))
                 (thrown-data (l/read-snapshot (d/datalog-kv snap))))))
        (finally (d/close conn)))))
  (testing "an environment whose reader slots belong to threads"
    (let [conn (open-conn schema {:kv-opts {:flags #{:nordahead}}})]
      (try
        (is (= {:error :read-snapshot/unsupported :reason :tls-readers}
               (thrown-data (d/open-read-snapshot conn))))
        (finally (d/close conn)))))
  (testing "an HA store"
    (let [conn (open-conn)]
      (try
        (let [^DB db (d/db conn)
              store  (.-store db)
              ha     (s/with-open-opts
                       store (assoc (i/opts store) :ha-mode :consensus-lease))]
          (is (= {:error :read-snapshot/unsupported :reason :ha}
                 (thrown-data
                   (d/open-read-snapshot
                     (d/conn-from-db (db/transfer db ha)))))))
        (finally (d/close conn)))))
  (testing "a KV store whose WAL writes are pending outside LMDB"
    (let [kv (d/open-kv (str *dir* "/pending"))]
      (try
        (vswap! (i/kv-info kv) assoc :native-lifetime (lifetime/create))
        (is (= {:error :read-snapshot/unsupported :reason :pending-wal}
               (thrown-data (l/read-snapshot kv))))
        (finally (d/close-kv kv)))))
  (testing "a remote store"
    (let [root (str *dir* "/server")
          port (allocate-port)
          srv  (server/create {:root root :port port})]
      (server/start srv)
      (try
        (let [conn (d/create-conn
                     (str "dtlv://datalevin:datalevin@localhost:" port "/db")
                     schema)]
          (try
            (is (= {:error :read-snapshot/unsupported :reason :remote}
                   (thrown-data (d/open-read-snapshot conn))))
            (is (not (d/read-snapshot? (d/db conn))))
            (finally (d/close conn))))
        (finally (server/stop srv))))))

(deftest snapshot-is-read-only-test
  (let [conn (open-conn)
        live (d/datalog-kv conn)]
    (try
      (d/transact! conn state-0)
      (d/open-dbi live "app")
      (d/open-list-dbi live "list")
      (d/transact-kv live "app" [[:put "k" 0]])
      (d/put-list-items live "list" "k" [0 1] :string :long)
      (d/with-read-snapshot [snap conn]
        (let [kv (d/datalog-kv snap)]
          (doseq [write [#(d/conn-from-db snap)
                         #(d/db-with snap [{:name "Dee"}])
                         #(d/fill-db snap [(d/datom 9 :name "Dee")])
                         #(d/datalog-index-cache-limit snap 8)
                         #(d/transact-kv kv "app" [[:put "k" 1]])
                         #(d/with-transaction-kv [tx kv]
                            (d/transact-kv tx "app" [[:put "k" 1]]))
                         #(d/update-kv kv "app" "k" inc)
                         #(d/put-list-items kv "list" "k" [2] :string :long)
                         #(d/del-list-items kv "list" "k" :string)
                         #(d/open-dbi kv "other")
                         #(d/open-list-dbi kv "other")
                         #(d/clear-dbi kv "app")
                         #(d/drop-dbi kv "app")
                         #(d/set-env-flags kv #{:nosync} true)
                         #(d/copy kv (str *dir* "/copy"))
                         #(d/re-index kv {})]]
            (is (= :read-snapshot/read-only (:error (thrown-data (write))))))
          (testing "the KV handle reads the snapshot too"
            (d/transact-kv live "app" [[:put "k" 2]])
            (d/put-list-items live "list" "k" [2] :string :long)
            (is (= 0 (d/get-value kv "app" "k")))
            (is (= [["k" 0]] (d/get-range kv "app" [:all])))
            (is (= [0 1] (d/get-list kv "list" "k" :string :long)))
            (is (= 2 (d/get-value live "app" "k")))
            (is (= [0 1 2] (d/get-list live "list" "k" :string :long))))
          (is (= #{"Ann" "Bob" "Cid"} (names snap)))))
      (testing "refused writes leave the connection as it was"
        (is (= #{"Ann" "Bob" "Cid"} (names (d/db conn))))
        (d/transact! conn [{:name "Dee"}])
        (is (= #{"Ann" "Bob" "Cid" "Dee"} (names (d/db conn)))))
      (finally (d/close conn)))))

(deftest snapshot-joins-no-write-batch-test
  ;; Of the writers that arrive together, the first runs the writes of all of
  ;; them in its transaction. It is not handed a write that came through a
  ;; snapshot.
  (let [conn (open-conn schema {:kv-opts {:write-batch-delay-us 300000}})
        live (d/datalog-kv conn)]
    (try
      (d/open-dbi live "app")
      (d/with-read-snapshot [snap conn]
        (let [kv     (d/datalog-kv snap)
              first  (future (d/transact-kv live "app" [[:put "a" 1]]))
              writes [#(d/update-kv kv "app" "k" (fnil inc 0))
                      #(d/transact-kv kv "app" [[:put "k" 1]])]]
          ;; by now it waits for writers to join it
          (Thread/sleep 100)
          (doseq [write writes]
            (is (= :read-snapshot/read-only (:error (thrown-data (write))))))
          (is (= :transacted @first))
          (is (= [["a" 1]] (d/get-range live "app" [:all])))))
      (finally (d/close conn)))))

(deftest secondary-index-searches-are-refused-test
  (let [conn (open-conn
               (assoc schema
                      :text {:db/valueType :db.type/string :db/fulltext true}
                      :doc  {:db/valueType :db.type/idoc}
                      :vec  {:db/valueType :db.type/vec})
               {:vector-opts {:dimensions 2 :metric-type :euclidean}})
        tx   {:name "Ann" :text "red fox" :doc {:k 1} :vec [1.0 2.0]}
        fulltext '[:find ?e . :in $ ?q :where [(fulltext $ ?q) [[?e ?a ?v]]]]
        idoc     '[:find ?e . :where [(idoc-match $ :doc {:k 1}) [[?e ?a ?v]]]]
        vector   '[:find ?e .
                   :in $ ?q
                   :where [(vec-neighbors $ :vec ?q) [[?e ?a ?v]]]]]
    (try
      (d/transact! conn [tx])
      (d/with-read-snapshot [snap conn]
        (d/transact! conn [{:name "Bob" :text "red hen" :doc {:k 1}
                            :vec [1.0 2.5]}
                           [:db/add [:name "Ann"] :text "blue fox"]])
        (testing "their searches follow the latest commit, so are refused"
          (is (= {:error :read-snapshot/unsupported :reason :fulltext}
                 (thrown-data (d/q fulltext snap "red"))
                 (thrown-data (d/fulltext-datoms snap "red"))
                 (thrown-data
                   (d/q '[:find ?e .
                          :in $ ?q
                          :where [(fulltext $ :text ?q) [[?e ?a ?v]]]]
                        snap "red"))))
          (is (= {:error :read-snapshot/unsupported :reason :idoc}
                 (thrown-data (d/q idoc snap))))
          (is (= {:error :read-snapshot/unsupported :reason :vector}
                 (thrown-data (d/q vector snap [1.0 2.0]))))
          (is (= {:error :read-snapshot/unsupported :reason :embedding}
                 (thrown-data
                   (d/q '[:find ?e .
                          :in $ ?q
                          :where
                          [(embedding-neighbors $ :text ?q) [[?e ?a ?v]]]]
                        snap "red")))))
        (testing "the datoms of indexed attributes are read as any other"
          (is (= (assoc tx :vec [1.0 2.0])
                 (update (d/pull snap [:name :text :doc :vec] [:name "Ann"])
                         :vec vec)))
          (is (= #{"Ann"} (names snap))))
        (testing "the live db searches as before"
          (is (= 2 (d/q fulltext (d/db conn) "hen")))
          (is (= #{1 2}
                 (set (d/q '[:find [?e ...]
                             :where [(idoc-match $ :doc {:k 1}) [[?e ?a ?v]]]]
                           (d/db conn)))))
          (is (some? (d/q vector (d/db conn) [1.0 2.0])))))
      (finally (d/close conn)))))

(deftest any-thread-reads-a-snapshot-test
  (let [conn  (open-conn)
        total 200]
    (try
      (d/transact! conn (mapv (fn [n] {:name (str "n" n) :age n})
                              (range total)))
      (let [snap (in-other-thread #(d/open-read-snapshot conn))]
        (testing "opened, read and closed by different threads"
          (is (= total (in-other-thread #(count (names snap)))))
          (let [result (promise)]
            (Thread/startVirtualThread
              #(deliver result (try (count (names snap))
                                    (catch Throwable e e))))
            (is (= total (deref result 10000 :stuck)))))
        (testing "read by many threads, which also read the live db"
          (let [writer  (future
                          (dotimes [n 200]
                            (d/transact! conn [{:name (str "w" n) :age -1}])))
                failed  (atom [])
                check   (fn [label ok?]
                          (when-not ok? (swap! failed conj label)))
                reader  (fn []
                          (try
                            (dotimes [round 1500]
                              (let [n    (rand-int total)
                                    ref  [:name (str "n" n)]
                                    live (d/db conn)]
                                (check :snapshot-pull
                                       (= {:name (str "n" n) :age n}
                                          (d/pull snap [:name :age] ref)))
                                (check :live-entity
                                       (= n (:age (d/entity live ref))))
                                (check :snapshot-new
                                       (nil? (d/entid snap [:name "w0"])))
                                (check :live-datoms
                                       (= [(str "n" n)]
                                          (mapv :v (d/datoms
                                                     live :eav
                                                     (d/entid live ref)
                                                     :name))))
                                (when (zero? (rem round 100))
                                  (check :snapshot-scan
                                         (= (range total)
                                            (map :v (d/datoms snap :ave
                                                              :age))))
                                  (check :snapshot-query
                                         (= total (count (names snap)))))))
                            (catch Throwable e
                              (swap! failed conj e))))
                threads (mapv (fn [_] (future (reader))) (range 6))
                virtual (promise)]
            (Thread/startVirtualThread #(deliver virtual (reader)))
            (run! deref threads)
            (deref virtual 60000 :stuck)
            @writer
            (is (= [] @failed))
            (is (= (+ total 200) (count (names (d/db conn)))))
            (is (= total (count (names snap))))))
        (in-other-thread #(d/close-read-snapshot snap)))
      (finally (d/close conn)))))

(def ^:private callers (atom #{}))

(defn note-caller
  "A query predicate that notes the thread that calls it."
  [_]
  (swap! callers conj (Thread/currentThread))
  true)

(defn- run-alone
  "Call f in a thread of its own, and give up on it after a minute. Return
  what it returned, and whether that thread alone called `note-caller`."
  [f]
  (reset! callers #{})
  (let [thread (promise)
        run    (future (deliver thread (Thread/currentThread)) (f))
        result (deref run 60000 :stuck)]
    (when (= :stuck result) (future-cancel run))
    [result (= #{@thread} @callers)]))

(deftest queries-stay-in-the-calling-thread-test
  ;; The live db spreads the work of these queries over several threads. A
  ;; snapshot has one reader, which a thread would hold while it waits for
  ;; another thread that needs it.
  (let [conn    (open-conn (assoc schema
                                  :score {:db/valueType :db.type/long}
                                  :next  {:db/valueType :db.type/ref}))
        ;; enough for a scan to be divided among threads
        total   9000
        ;; the name of a kind of query, the query, its results before and
        ;; after the commit below, and its other inputs
        queries [["a plan of several steps"
                  '[:find (count ?e) .
                    :where
                    [?e :friend ?f] [?f :age ?a]
                    [(datalevin.read-snapshot-test/note-caller ?a)]
                    [?f :friend ?g] [?g :score ?s]
                    [(datalevin.read-snapshot-test/note-caller ?s)]]
                  total (- total 3)]
                 ["plans of unconnected parts"
                  '[:find (count ?a) .
                    :where
                    [?a :name "n1"] [?a :friend ?a1] [?a1 :age ?x]
                    [(datalevin.read-snapshot-test/note-caller ?x)]
                    [?b :name "n2"] [?b :friend ?b1] [?b1 :age ?y]
                    [(datalevin.read-snapshot-test/note-caller ?y)]
                    [?c :name "n3"] [?c :friend ?c1] [?c1 :age ?z]
                    [(datalevin.read-snapshot-test/note-caller ?z)]]
                  1 1]
                 ["rules that depend on each other"
                  '[:find (count ?b) .
                    :in $ %
                    :where [?a :name "t0"] (odd ?a ?b)]
                  5 4
                  '[[(odd ?a ?b)
                     [?a :next ?b]
                     [(datalevin.read-snapshot-test/note-caller ?b)]]
                    [(odd ?a ?b)
                     (even ?a ?m) [?m :next ?b]
                     [(datalevin.read-snapshot-test/note-caller ?b)]]
                    [(even ?a ?b)
                     (odd ?a ?m) [?m :next ?b]
                     [(datalevin.read-snapshot-test/note-caller ?b)]]]]]
        before  (mapv #(nth % 2) queries)
        after   (mapv #(nth % 3) queries)
        run-all (fn [db]
                  (mapv (fn [[_ q _ _ & inputs]] (apply d/q q db inputs))
                        queries))]
    (try
      ;; a ring of entities, and a short chain for the rules
      (doseq [part (partition-all 1000 (range total))]
        (d/transact! conn (mapv (fn [n]
                                  {:db/id  (inc (long n))
                                   :name   (str "n" n)
                                   :age    n
                                   :score  n
                                   :friend (inc (rem (inc (long n)) total))})
                                part)))
      (d/transact! conn (mapv (fn [n]
                                (cond-> {:db/id (+ total 1 (long n))
                                         :name  (str "t" n)}
                                  (< (long n) 9)
                                  (assoc :next (+ total 2 (long n)))))
                              (range 10)))
      (d/with-read-snapshot [snap conn]
        (doseq [[kind q result _ & inputs] queries]
          (testing kind
            (is (= [result false]
                   (run-alone #(apply d/q q (d/db conn) inputs)))
                "the live db uses other threads")
            (is (= [result true] (run-alone #(apply d/q q snap inputs))))))
        (testing "run from inside another read of the same snapshot"
          (let [inside #(let [found (volatile! nil)]
                          (d/visit (d/datalog-kv snap) "datalevin/meta"
                                   (fn [_ _]
                                     (vreset! found
                                              [(thrown-data
                                                 (d/close-read-snapshot snap))
                                               (run-all snap)])
                                     :datalevin/terminate-visit)
                                   [:all] :raw :raw false)
                          @found)]
            (is (= [[{:error :read-snapshot/in-use} before] true]
                   (run-alone inside)))))
        (testing "after a commit"
          (d/transact! conn [[:db/retractEntity [:name "n7"]]
                             [:db/retractEntity [:name "t9"]]])
          (is (= after (run-all (d/db conn))))
          (is (= before (run-all snap)))))
      (finally (d/close conn)))))

(deftest transaction-in-the-thread-of-a-snapshot-test
  (let [conn (open-conn)]
    (try
      (d/transact! conn state-0)
      (d/with-read-snapshot [snap conn]
        (d/with-transaction [cn conn]
          (d/transact! cn [{:name "Dee"}])
          (is (= #{"Ann" "Bob" "Cid" "Dee"} (names @cn)))
          (is (= #{"Ann" "Bob" "Cid"} (names snap))))
        (is (= #{"Ann" "Bob" "Cid" "Dee"} (names (d/db conn))))
        (is (= #{"Ann" "Bob" "Cid"} (names snap)))
        (testing "an aborted one as well"
          (d/with-transaction [cn conn]
            (d/transact! cn [{:name "Eve"}])
            (d/abort-transact cn))
          (is (= #{"Ann" "Bob" "Cid" "Dee"} (names (d/db conn))))
          (is (= #{"Ann" "Bob" "Cid"} (names snap)))))
      (finally (d/close conn)))))

(deftest snapshot-outlives-map-growth-test
  (let [dir    (str *dir* "/growth")
        conn   (d/create-conn
                 dir (assoc schema :bio {:db/valueType :db.type/string})
                 {:kv-opts {:mapsize 1}})
        random (java.util.Random. 42)
        ;; random text, as what compresses well does not fill the map
        bio    (fn []
                 (apply str (repeatedly
                              16000 #(char (+ 33 (.nextInt random 90))))))
        before (mapv (fn [n] {:name (str "n" n) :age n :bio (bio)}) (range 20))
        size   #(.length (java.io.File. (str dir "/data.mdb")))]
    (try
      (d/transact! conn before)
      (d/with-read-snapshot [snap conn]
        (is (< (long (size)) (* 1024 1024)))
        (dotimes [round 5]
          (d/transact! conn (mapv (fn [n]
                                    {:name (str "m" round "-" n) :bio (bio)})
                                  (range 40))))
        (is (< (* 2 1024 1024) (long (size))) "the map has grown")
        (is (= (set (map :name before)) (names snap)))
        (is (= (mapv :bio before)
               (mapv #(:bio (d/pull snap [:bio] [:name (:name %)])) before)))
        (is (= 220 (count (names (d/db conn))))))
      (finally (d/close conn)))))

(deftest giant-values-test
  (let [conn  (open-conn (assoc schema :bio {:db/valueType :db.type/string}))
        giant (fn [c] (apply str (repeat 100000 c)))]
    (try
      (d/transact! conn [{:name "Ann" :bio (giant "a")}
                         {:name "Bob" :bio (giant "b")}])
      (d/with-read-snapshot [snap conn]
        (d/transact! conn [[:db/add [:name "Ann"] :bio (giant "c")]
                           [:db/retractEntity [:name "Bob"]]])
        (is (= (giant "a") (:bio (d/pull snap [:bio] [:name "Ann"]))))
        (is (= (giant "b") (:bio (d/entity snap [:name "Bob"]))))
        (is (= #{(giant "a") (giant "b")}
               (set (d/q '[:find [?b ...] :where [_ :bio ?b]] snap))))
        (is (= #{(giant "c")}
               (set (d/q '[:find [?b ...] :where [_ :bio ?b]] (d/db conn))))))
      (finally (d/close conn)))))

(deftest custom-type-values-test
  (let [conn (open-conn)
        task {:index {:type     :long
                      :order-fn (interpret/inter-fn [task] (:priority task))}}
        make {:priority 3 :name "Make" :options {:debug true}}
        test {:priority 3 :name "Test"}
        fix  {:priority 1 :name "Fix"}]
    (try
      (d/register-type conn :app/task task)
      (d/update-schema conn {:task  {:db/valueType :app/task}
                             :tasks {:db/valueType   :app/task
                                     :db/cardinality :db.cardinality/many}})
      (d/transact! conn [{:name "Ann" :task make :tasks [make test]}])
      (d/with-read-snapshot [snap conn]
        (let [reads  (fn [db]
                       {:pull   (update (d/pull db [:task :tasks] [:name "Ann"])
                                        :tasks set)
                        :q      (d/q '[:find ?n
                                       :in $ ?t
                                       :where [?e :task ?t] [?e :name ?n]]
                                     db make)
                        :range  (set (map :v (d/index-range
                                               db :tasks {:priority 3}
                                               {:priority 3})))
                        :datoms (mapv :v (d/datoms db :ave :task))})
              before (reads snap)]
          (is (= {:pull   {:task make :tasks #{make test}}
                  :q      #{["Ann"]}
                  :range  #{make test}
                  :datoms [make]}
                 before))
          (d/transact! conn [[:db/add [:name "Ann"] :task fix]
                             [:db/retract [:name "Ann"] :tasks test]
                             {:name "Bob" :task test}])
          (testing "a type that is registered later"
            (d/register-type conn :app/note
                             {:index {:type     :string
                                      :order-fn (interpret/inter-fn [note]
                                                  (:title note))}})
            (d/update-schema conn {:note {:db/valueType :app/note}})
            (d/transact! conn [{:name "Cid" :note {:title "a" :text "b"}}]))
          (is (= {:pull   {:task fix :tasks #{make}}
                  :q      #{}
                  :range  #{make}
                  :datoms [fix test]}
                 (reads (d/db conn))))
          (is (= before (reads snap)))
          (is (= :read-snapshot/read-only
                 (:error (thrown-data
                           (d/register-type (d/datalog-kv snap) :app/other
                                            task)))))))
      (finally (d/close conn)))))

(deftest wal-store-test
  (let [dir  (str *dir* "/wal")
        conn (d/create-conn dir schema {:wal? true})]
    (try
      (is (true? (:wal? (d/opts conn))))
      (d/transact! conn state-0)
      (let [before (reads (d/db conn))
            snap   (d/open-read-snapshot conn)]
        (is (= before (reads snap)))
        (in-other-thread #(d/transact! conn state-1))
        (d/update-schema conn {:email {:db/valueType :db.type/string}})
        (d/transact! conn state-2)
        (is (not= before (reads (d/db conn))))
        (is (= before (reads snap)))
        (testing "closed with its connection"
          (d/close conn)
          (is (= {:error :read-snapshot/closed} (thrown-data (names snap))))))
      (let [conn (d/create-conn dir)]
        (try
          (d/with-read-snapshot [snap conn]
            (is (= #{"Ann" "Dee" "Eve"} (names snap))))
          (finally (d/close conn))))
      (finally (d/close conn)))))

(deftest prepared-reads-test
  (let [conn (open-conn)]
    (try
      (d/transact! conn state-0)
      (let [q     '[:find ?a . :in $ ?n :where [?e :name ?n] [?e :age ?a]]
            live  (d/db conn)
            query (d/prepare-q live q)
            pull  (d/prepare-pull live [:name :age])
            value (d/prepare-get-value (d/datalog-kv live) "datalevin/meta"
                                       :attr :long)
            ann   (d/entid live [:name "Ann"])]
        (d/with-read-snapshot [snap conn]
          (let [snap-query (d/prepare-q snap q)
                snap-pull  (d/prepare-pull snap [:name :age])]
            (is (= 30 (d/execute-prepared query ["Ann"])))
            (is (= {:name "Ann" :age 30} (d/execute-prepared pull ann)))
            (d/transact! conn state-1)
            (testing "prepared with the snapshot"
              (is (= 30 (d/execute-prepared snap-query ["Ann"])))
              (is (= {:name "Ann" :age 30} (d/execute-prepared snap-pull ann))))
            (testing "prepared with the live db"
              (is (= 31 (d/execute-prepared query ["Ann"])))
              (is (= {:name "Ann" :age 31} (d/execute-prepared pull ann)))
              (is (= (:max-tx (d/db conn))
                     (d/execute-prepared value :max-tx))))
            (testing "prepared with the live db, run with the snapshot"
              (is (= {:name "Ann" :age 30} (d/execute-prepared pull snap ann)))
              (is (= (:max-tx snap)
                     (d/execute-prepared value (d/datalog-kv snap) :max-tx)))
              (is (= {:name "Ann" :age 31} (d/execute-prepared pull ann)))))))
      (finally (d/close conn)))))

(deftest simulated-transaction-test
  (let [conn (open-conn)]
    (try
      (d/transact! conn state-0)
      (d/with-read-snapshot [snap conn]
        (d/transact! conn state-1)
        (let [report (d/tx-data->simulated-report
                       snap [[:db/add [:name "Ann"] :age 33]])]
          (is (= 33 (d/q '[:find ?a . :where [?e :name "Ann"] [?e :age ?a]]
                         (:db-after report))))
          (is (= 30 (d/q '[:find ?a . :where [?e :name "Ann"] [?e :age ?a]]
                         snap)))
          (is (= 31 (d/q '[:find ?a . :where [?e :name "Ann"] [?e :age ?a]]
                         (d/db conn))))))
      (finally (d/close conn)))))
