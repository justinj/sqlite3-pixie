(ns sqlite
  (require pixie.ffi :as ffi)
  (require pixie.ffi-infer :as f)
  (require pixie.string :as string)
  )

; TODO: Make sure we are freeing the stuff from sqlite when we need to...

(def libsqlite-name "/usr/lib/sqlite3/libtclsqlite3.dylib")
(def libsqlite (ffi-library libsqlite-name))

(def cb-type (ffi/ffi-callback [CVoidP CInt32 CVoidP CVoidP] CInt))
(f/with-config {:library "sqlite3"
                :includes ["sqlite3.h"]}
  (f/defcfn sqlite3_open)
  ; last arg to exec is a buffer for errors, maybe useful
  (f/defcfn sqlite3_exec)
  (f/defcfn sqlite3_close)

  ; https://www.sqlite.org/c3ref/bind_parameter_index.html
  (f/defcfn sqlite3_bind_parameter_count)
  ; we SHOULDNT need sqlite_stmt struct, we should just have an opaque pointer (duh why was this
  ; not obvious)

  ; int sqlite3_prepare_v2(
  ;   sqlite3 *db,            /* Database handle */
  ;   const char *zSql,       /* SQL statement, UTF-8 encoded */
  ;   int nByte,              /* Maximum length of zSql in bytes. */
  ;   sqlite3_stmt **ppStmt,  /* OUT: Statement handle */
  ;   const char **pzTail     /* OUT: Pointer to unused portion of zSql */
  ; );
  ; see https://www.sqlite.org/c3ref/prepare.html
  (f/defcfn sqlite3_prepare_v2)
  (f/defcfn sqlite3_errmsg)
  )

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

; TODO: figure out an appropriate size for this
(defn new-ptr []
  (buffer 255))

(defn deref-ptr [ptr]
  (ffi/unpack ptr 0 CVoidP))
; https://github.com/sparklemotion/sqlite3-ruby/blob/master/lib/sqlite3/statement.rb#L22
; this is relevant for prepared statements
; ruby calls into sqlite to bind parameters so I guess we should do the same

(defn prepare-query [conn query & args]
  (let [ptr (new-ptr)]
    (sqlite3_prepare_v2
      conn
      query
      -1 ; read up to the first null terminator
      ptr
      nil)
  (prn "it is" (sqlite3_bind_parameter_count (deref-ptr ptr)))
    )
  ; (let [split (string/split query "?")]
  ;   ; (prn split)
  ;   split
  ;   )
  )

(defn run-query [conn query & args]
  (prn
    (apply prepare-query conn query args)
    )
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
  (prn (run-query conn "select a from testtable where a = ?;" "poop"))
  (comment prn (run-raw-query conn "select a from testtable where a = \"poop\";")))
