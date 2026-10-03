(ns datalevin.query.transaction-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u])
  (:import [java.util UUID]))

(use-fixtures :once db-fixture)

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

(deftest queries-stay-in-the-thread-of-the-transaction-test
  ;; A write transaction belongs to the thread that opened it. Without one,
  ;; the work of these queries is spread over several threads.
  (let [root    (u/tmp-dir (str "query-transaction-" (UUID/randomUUID)))
        conn    (d/create-conn
                  root {:name   {:db/valueType :db.type/string
                                 :db/unique    :db.unique/identity}
                        :age    {:db/valueType :db.type/long}
                        :score  {:db/valueType :db.type/long}
                        :friend {:db/valueType :db.type/ref}
                        :next   {:db/valueType :db.type/ref}})
        total   100
        ;; the name of a kind of query, the query, its result and its other
        ;; inputs
        queries [["a plan of several steps"
                  '[:find (count ?e) .
                    :where
                    [?e :friend ?f] [?f :age ?a]
                    [(datalevin.query.transaction-test/note-caller ?a)]
                    [?f :friend ?g] [?g :score ?s]
                    [(datalevin.query.transaction-test/note-caller ?s)]]
                  total]
                 ["plans of unconnected parts"
                  '[:find (count ?a) .
                    :where
                    [?a :name "n1"] [?a :friend ?a1] [?a1 :age ?x]
                    [(datalevin.query.transaction-test/note-caller ?x)]
                    [?b :name "n2"] [?b :friend ?b1] [?b1 :age ?y]
                    [(datalevin.query.transaction-test/note-caller ?y)]
                    [?c :name "n3"] [?c :friend ?c1] [?c1 :age ?z]
                    [(datalevin.query.transaction-test/note-caller ?z)]]
                  1]
                 ["rules that depend on each other"
                  '[:find (count ?b) .
                    :in $ %
                    :where [?a :name "t0"] (odd ?a ?b)]
                  5
                  '[[(odd ?a ?b)
                     [?a :next ?b]
                     [(datalevin.query.transaction-test/note-caller ?b)]]
                    [(odd ?a ?b)
                     (even ?a ?m) [?m :next ?b]
                     [(datalevin.query.transaction-test/note-caller ?b)]]
                    [(even ?a ?b)
                     (odd ?a ?m) [?m :next ?b]
                     [(datalevin.query.transaction-test/note-caller ?b)]]]]]]
    (try
      ;; a ring of entities, and a short chain for the rules
      (d/transact! conn (mapv (fn [n]
                                {:db/id  (inc (long n))
                                 :name   (str "n" n)
                                 :age    n
                                 :score  n
                                 :friend (inc (rem (inc (long n)) total))})
                              (range total)))
      (d/transact! conn (mapv (fn [n]
                                (cond-> {:db/id (+ total 1 (long n))
                                         :name  (str "t" n)}
                                  (< (long n) 9)
                                  (assoc :next (+ total 2 (long n)))))
                              (range 10)))
      (doseq [[kind q result & inputs] queries]
        (testing kind
          (is (= [result false]
                 (run-alone #(apply d/q q (d/db conn) inputs)))
              "without a transaction, other threads are used")
          (is (= [result true]
                 (run-alone #(d/with-transaction [cn conn]
                               (apply d/q q @cn inputs)))))))
      (finally
        (d/close conn)
        (u/delete-files root)))))
