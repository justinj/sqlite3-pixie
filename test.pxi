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


  ; binding functions
  ; 1st arg: pointer to statement
  ; 2nd arg: index to be set
  ; 3rd arg: value to bind
  ; 4th arg: num bytes in parameter (if present), negative implies up to null terminator
  ; TODO: ensure the stuff we pass in gets freed...
  ; 5th arg (for blob and string): destructor
  ; 6th arg: encoding for text64
  (f/defcfn sqlite3_bind_int64)
  
  (f/defcfn sqlite3_step)

  (f/defcfn sqlite3_column_bytes)
  (f/defcfn sqlite3_column_type)
  (f/defcfn sqlite3_column_count)
  (f/defcfn sqlite3_column_name)

  (f/defcfn sqlite3_column_text)
  (f/defcfn sqlite3_column_text16)
  (f/defcfn sqlite3_column_int64)

  (f/defccallback sqlite3_destructor_type)
  (f/defconst SQLITE_TRANSIENT)

  (f/defconst SQLITE_ROW)
  (f/defconst SQLITE_DONE)

  (f/defconst SQLITE_INTEGER)
  (f/defconst SQLITE_FLOAT)
  (f/defconst SQLITE_TEXT)
  (f/defconst SQLITE_BLOB)
  (f/defconst SQLITE_NULL))


; this is declared on its own because ffi-infer infers it to ask for a function
; pointer for the last arg, but we want to pass SQLITE_TRANSIENT
; TODO: check if we *actually* need to do this? seems sketch
(def sqlite3_bind_text (ffi/ffi-fn libsqlite "sqlite3_bind_text" [CVoidP CInt CCharP CInt CInt] CInt))

(def column-type-name
  {SQLITE_INTEGER "SQLITE_INTEGER"
   SQLITE_FLOAT   "SQLITE_FLOAT"
   SQLITE_TEXT    "SQLITE_TEXT"
   SQLITE_BLOB    "SQLITE_BLOB"
   SQLITE_NULL    "SQLITE_NULL"})

(def no-op-type (ffi/ffi-callback [CVoidP] CVoidP))
; TODO: this should somehow use SQLITE_TRANSIENT
(def no-op (ffi/ffi-prep-callback sqlite3_destructor_type (fn [_] 0)))

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

; TODO: this should escape stuff
(defn sqlize-val [value]
  (pr-str value))

; TODO: figure out an appropriate size for this
(defn new-ptr []
  (buffer 255))

(defn deref-str-ptr [ptr]
  (ffi/unpack ptr 0 CCharP))

(defn deref-ptr [ptr]
  (ffi/unpack ptr 0 CVoidP))
; https://github.com/sparklemotion/sqlite3-ruby/blob/master/lib/sqlite3/statement.rb#L22
; this is relevant for prepared statements
; ruby calls into sqlite to bind parameters so I guess we should do the same

(defmulti bind-arg (fn [_ _ arg] (type arg)))

(defmethod bind-arg String
  [statement column value]
  (sqlite3_bind_text
    (deref-ptr statement)
    column ; index to set
    value
    (count value)
    -1))

(defmethod bind-arg Integer
  [statement column value]
  (sqlite3_bind_int64
    (deref-ptr statement)
    column ; index to set
    value))

(defn prepare-query [conn query & args]
  (let [statement (new-ptr)]
    (sqlite3_prepare_v2
      conn
      query
      -1 ; read up to the first null terminator
      statement
      nil)
    (dotimes [i (count args)]
      (bind-arg statement (inc i) (nth args i))
      )
    statement
    ))

; 15:05 < justinjaffray> when doing ffi with pixie, is there a nice way to handle a function that returns a not-null-terminated string?
; 15:10 < tbaldrid_> justinjaffray: if you define the function as returning a CVoidP, then you can use pixie.ffi/pack! and /unpack to read and write to data at that pointer.
; 15:11 < tbaldrid_> e.g. (pixie.ffi/unpack ptr offset CUInt8)
; 15:12 < tbaldrid_> that's pretty much the same as this C code: x = (char* ptr)[offset]

(defn read-n-chars [ptr n]
  (apply str (map #(char (ffi/unpack ptr % CUInt8))
       (range 0 n))))

(defmulti load-value #(sqlite3_column_type (deref-ptr %1) %2))

(defmethod load-value SQLITE_TEXT
  [statement column]
  (let [size (sqlite3_column_bytes (deref-ptr statement) column)]
    (read-n-chars (sqlite3_column_text (deref-ptr statement) column) size)))

(defmethod load-value SQLITE_INTEGER
  [statement column]
  (sqlite3_column_int64 (deref-ptr statement) column))

(defn get-row [conn statement]
  (for [i (range 0 (sqlite3_column_count (deref-ptr statement)))]
    (load-value statement i)))

(defn run-prepared-statement [conn statement]
  (loop [result []]
  (let [step-value (sqlite3_step (deref-ptr statement))]
    (cond
      (= step-value SQLITE_ROW) (recur (conj result (get-row conn statement)))
      (= step-value SQLITE_DONE) result))))

(defn get-column-names [statement]
  (let [num-cols (sqlite3_column_count (deref-ptr statement))]
    (map keyword
         (map #(sqlite3_column_name (deref-ptr statement) %) (range 0 num-cols)))))

; TODO: this should maybe go in stdlib
(defn zipmap [as bs]
  (loop [m {}
         as as
         bs bs]
    (cond (empty? as) m
          (empty? bs) m
          :else (recur (assoc m (first as) (first bs))
                       (rest as)
                       (rest bs)))))

; TODO: we should make sure the connection is valid
(defn run-query [conn query & args]
  (let [statement (apply prepare-query conn query args)
        rows (run-prepared-statement conn statement)
        cols (get-column-names statement)]
    (map #(zipmap cols %) rows)
    )) 

(let [conn (sqlite-connect "test.db")]
  (prn (run-query conn "select * from testtable where a = ? and b = ?;" "poop" 3)))
