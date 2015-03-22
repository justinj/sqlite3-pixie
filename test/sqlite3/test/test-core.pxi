(ns sqlite3.test
  (require pixie.test :as t)
  (require sqlite3.core :as sqlite))

(def db-name "/tmp/test.db")

(defn remove-table []
  (sqlite/with-connection db-name [conn]
      (sqlite/query conn "DROP TABLE ssbm_players;")
      (sqlite/close-connection conn)))

(defn create-table []
  (sqlite/with-connection db-name [conn]
    (sqlite/query conn "CREATE TABLE ssbm_players
                           (name STRING,
                           character STRING,
                           ssbmrank INTEGER PRIMARY KEY,
                           score FLOAT);")))

(defn insert-row [data]
  (sqlite/with-connection db-name [conn]
      (sqlite/query conn "INSERT INTO ssbm_players (name, character, ssbmrank, score)
                             VALUES (?, ?, ?, ?)"
                        (:name      data)
                        (:character data)
                        (:ssbmrank  data)
                        (:score     data))))

(defn insert-test-data []
  (insert-row {:name "Mango"     :character "Fox"        :ssbmrank 1 :score 10.00})
  (insert-row {:name "Armada"    :character "Peach"      :ssbmrank 2 :score 9.893})
  (insert-row {:name "PPMD"      :character "Falco"      :ssbmrank 3 :score 9.750})
  (insert-row {:name "Mew2King"  :character "Marth"      :ssbmrank 4 :score 9.717})
  (insert-row {:name "Hungrybox" :character "Jigglypuff" :ssbmrank 5 :score 9.622})
  (insert-row {:name "Leffen"    :character "Fox"        :ssbmrank 6 :score 9.422}))

(defn set-up-testdb []
  (remove-table)
  (create-table)
  (insert-test-data))

(defmacro test-query-result [name query expected-result]
  `(t/deftest ~name
     (set-up-testdb)
     (sqlite/with-connection "/tmp/test.db" [conn]
         (let [results (sqlite/query conn ~@query)]
           (t/assert= results ~expected-result)))))

(test-query-result no-binding
  ["SELECT name FROM ssbm_players;"]
  [{:name "Mango"}
   {:name "Armada"}
   {:name "PPMD"}
   {:name "Mew2King"}
   {:name "Hungrybox"}
   {:name "Leffen"}])

(test-query-result with-binding-string
  ["SELECT name FROM ssbm_players WHERE character = ?;" "Fox"]
  [{:name "Mango"} {:name "Leffen"}])

(test-query-result with-binding-int
  ["SELECT name FROM ssbm_players WHERE ssbmrank = ?;" 3]
  [{:name "PPMD"}])

(test-query-result with-binding-float
  ["SELECT name FROM ssbm_players WHERE score = ?;" 10.00]
  [{:name "Mango"}])

(test-query-result with-binding-multiple
  ["SELECT name FROM ssbm_players WHERE character = ? AND ssbmrank = ?;" "Fox" 1]
  [{:name "Mango"}])

(test-query-result with-retrieving-float
  ["SELECT score FROM ssbm_players WHERE name = 'PPMD';"]
  [{:score 9.750}])

; this guy isn't actually used yet...
(t/deftest create-table-ddl
  (t/assert= "CREATE TABLE ssbm_players (name STRING, character STRING, ssbmrank INTEGER, score FLOAT);"
             (sqlite/create-table-ddl :ssbm_players
                                      [:name      :string]
                                      [:character :string]
                                      [:ssbmrank  :integer]
                                      [:score     :float])))

(t/deftest throws-on-too-many-params
  (t/assert-throws?
    (sqlite/with-connection db-name [conn]
      (sqlite/query conn "SELECT name FROM ssbm_players WHERE name = 'PPMD'" "PPMD"))))

(t/deftest throws-on-not-enough-params
  (t/assert-throws?
    (sqlite/with-connection db-name [conn]
      (sqlite/query conn "SELECT name FROM ssbm_players WHERE name = ?"))))

(t/deftest throws-on-opening-invalid-filename
  (t/assert-throws?
    RuntimeException
    "Sqlite Error: unable to open database file"
    (sqlite/with-connection "." [_])))

; TODO:
; (t/deftest throws-on-using-invalid-connection
;   (t/assert-throws?
;     RuntimeException
;     "who the hell knows"
;     (do 
;       (let [conn (sqlite/connect "test.db")]
;         (sqlite/close-connection conn)
;         (prn (sqlite/query conn "SELECT * FROM ssbm_players"))))))
