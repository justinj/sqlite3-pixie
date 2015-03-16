(ns sqlite
  (require pixie.ffi :as ffi)
  (require pixie.ffi-infer :as f)
  (require pixie.string :as string)
  )

(def libsqlite-name "/usr/lib/sqlite3/libtclsqlite3.dylib")
(def libsqlite (ffi-library libsqlite-name))

(def cb-type (ffi/ffi-callback [CVoidP CInt32 CVoidP CVoidP] CInt))
(f/with-config {:library "sqlite3"
                :includes ["sqlite3.h"]}
  (f/defcfn sqlite3_open)
  ; last arg to exec is a buffer for errors, maybe useful
  (f/defcfn sqlite3_exec)
  (f/defcfn sqlite3_close))

(defn make-map-of-call [argc argv cols]
  (into {}
        (for [i (range 0 argc)]
          (let [offset (* i 8)
                col-name (keyword (ffi/unpack cols offset CCharP))
                value (ffi/unpack argv offset CCharP)]
            [col-name value]))))

(defn make-setter-callback [result-atom]
  (ffi/ffi-prep-callback
    cb-type
    (fn [_ argc argv cols]
      (let [row (make-map-of-call argc argv cols)]
        (swap! result-atom conj row))
      0)))

(defn sqlite-connect [db-name]
  (let [conn (buffer 255)]
    (sqlite3_open db-name conn)
    (ffi/unpack conn 0 CVoidP)))

; TODO: check the connection is valid
(defn run-raw-query [conn query]
  (let [result (atom [])
        set-result (make-setter-callback result)]
    (sqlite3_exec conn query set-result nil nil)
    @result))

; TODO: this should escape stuff
(defn sqlize-val [value]
  (pr-str value))

(defn run-query [conn query & args]
  (let [query (reduce
                (fn [query arg]
                  (let [replaced (string/replace-first query "?" (sqlize-val arg))]
                    (if (= query replaced)
                      (throw "arity mismatch in run-query")
                      replaced)))
                  query
                  args)]
    (run-raw-query conn query)))

(let [conn (sqlite-connect "test.db")]
  (prn (run-query conn     "select a from testtable where a = ?;" "poop"))
  (prn (run-raw-query conn "select a from testtable where a = \"poop\";")))
