(ns sqlite3.test
  (require pixie.test :as t)
  (require sqlite3.core :as sqlite))

(def db-name "/tmp/test.db")

(defn remove-table []
  (sqlite/with-connection db-name
    (fn [conn]
      (sqlite/run-query conn "DROP TABLE ssbm_players;")
      (sqlite/close-connection conn))))

(defn create-table []
  (sqlite/with-connection db-name
    (fn [conn]
      (sqlite/run-query conn "CREATE TABLE ssbm_players
                             (name STRING, character STRING, ssbmrank INTEGER PRIMARY KEY);"))))

(defn insert-row [data]
  (sqlite/with-connection db-name
    (fn [conn]
      (sqlite/run-query conn "INSERT INTO ssbm_players (name, character, ssbmrank)
                             VALUES (?, ?, ?)"
                        (:name data)
                        (:character data)
                        (:ssbmrank data)))))

(defn insert-test-data []
  (insert-row {:name "Mango"     :character "Fox"        :ssbmrank 1})
  (insert-row {:name "Armada"    :character "Peach"      :ssbmrank 2})
  (insert-row {:name "PPMD"      :character "Falco"      :ssbmrank 3})
  (insert-row {:name "Mew2King"  :character "Marth"      :ssbmrank 4})
  (insert-row {:name "Hungrybox" :character "Jigglypuff" :ssbmrank 5})
  (insert-row {:name "Leffen"    :character "Fox"        :ssbmrank 6}))

(defn set-up-testdb []
  (remove-table)
  (create-table)
  (insert-test-data))

(t/deftest basic
  (set-up-testdb)
  (sqlite/with-connection "/tmp/test.db"
    (fn [conn]
      (let [results (sqlite/run-query conn "SELECT * FROM ssbm_players;")]
        (t/assert= (count results) 6)))))

(t/deftest create-table-ddl
  (t/assert= "CREATE TABLE ssbm_players (name STRING, character STRING, ssbmrank INTEGER);"
             (sqlite/create-table-ddl :ssbm_players
                                      [:name :string]
                                      [:character :string]
                                      [:ssbmrank :integer])))
