(ns sqlite3.core
  (require pixie.ffi :as ffi)
  (require pixie.ffi-infer :as f)
  (require pixie.string :as string))

(def libsqlite-name "/usr/lib/sqlite3/libtclsqlite3.dylib")
(def libsqlite (ffi-library libsqlite-name))

(f/with-config {:library "sqlite3"
                :includes ["sqlite3.h"]}
  (f/defcfn sqlite3_open)
  ; last arg to exec is a buffer for errors, maybe useful
  (f/defcfn sqlite3_close_v2)

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

  ; int sqlite3_finalize(sqlite3_stmt *pStmt);
  (f/defcfn sqlite3_finalize)

  ; const char *sqlite3_errmsg(sqlite3*);
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
  (f/defcfn sqlite3_bind_double)
  
  ; When executing a prepared statement, called to request a new row of results.
  ; Returns SQLITE_ROW when there is another row available and SQLITE_DONE when
  ; there are no more rows.
  (f/defcfn sqlite3_step)

  ; The row data can be extracted with the following:
  ; Generally these functions take a statement, and some of the take a column index.

  ; How many columns are there in the returned data
  (f/defcfn sqlite3_column_count)

  ; Name of the ith column
  (f/defcfn sqlite3_column_name)

  ; Type of data stored in the column, one of five
  (f/defcfn sqlite3_column_type)

  ; Extract the value of a text column. The strings are *not* NULL-terminated,
  ; so the size of the data to be extracted must first be obtained with
  ; sqlite3_column_bytes.
  (f/defcfn sqlite3_column_text)
  (f/defcfn sqlite3_column_text16)
  (f/defcfn sqlite3_column_bytes)

  (f/defcfn sqlite3_column_int64)
  (f/defcfn sqlite3_column_double)

  (f/defconst SQLITE_OK)

  ; the possible return values from sqlite3_step
  (f/defconst SQLITE_ROW)
  (f/defconst SQLITE_DONE)

  ; possible value types, currently we only use the first 3
  (f/defconst SQLITE_INTEGER)
  (f/defconst SQLITE_FLOAT)
  (f/defconst SQLITE_TEXT)
  (f/defconst SQLITE_BLOB)
  (f/defconst SQLITE_NULL))


; this is declared on its own because ffi-infer infers it to ask for a function
; pointer for the last arg, but we want to pass SQLITE_TRANSIENT
; this seems fixable, but I'm not sure how right now
(def SQLITE_TRANSIENT -1)
(def sqlite3_bind_text (ffi/ffi-fn libsqlite "sqlite3_bind_text" [CVoidP CInt CCharP CInt CInt] CInt))

; TODO: figure out an appropriate size for this
(defn new-ptr []
  (buffer 255))

(defn connect [db-name]
  (let [conn-buffer (new-ptr)
        error-code (sqlite3_open db-name conn-buffer)
        conn (ffi/unpack conn-buffer 0 CVoidP)]
    (ffi/dispose! conn-buffer)
    (when-not (= error-code SQLITE_OK)
      (throw (str "Sqlite Error: "
                  (sqlite3_errmsg conn))))
    conn))

(defn close-connection [conn]
  (sqlite3_close_v2 conn))

(defn- deref-ptr [ptr]
  (ffi/unpack ptr 0 CVoidP))

(defn prepare-query [conn query args]
  (let [statement (new-ptr)]
    (sqlite3_prepare_v2
      conn
      query
      -1 ; read up to the first null terminator
      statement
      nil)
    (let [required-arg-count (sqlite3_bind_parameter_count (deref-ptr statement))
          provided-arg-count (count args)]
      (assert (= provided-arg-count required-arg-count)
              (str "Arity mismatch in sqlite query: "
                   query " expects "
                   required-arg-count " arguments, "
                   provided-arg-count " provided.")))
    (dotimes [i (count args)]
      (bind-param statement (inc i) (nth args i)))
    statement))

(defmulti
  ^{:doc "Binds the parameter to the prepared statement at the specified index"
    :private true}
  bind-param
  (fn [_ _ arg] (type arg)))

(defmethod bind-param String
  [statement column value]
  (sqlite3_bind_text (deref-ptr statement) column value (count value) SQLITE_TRANSIENT))

(defmethod bind-param Integer
  [statement column value]
  (sqlite3_bind_int64 (deref-ptr statement) column value))

(defmethod bind-param Float
  [statement column value]
  (sqlite3_bind_double (deref-ptr statement) column value))

(defn- read-n-chars [ptr n]
  (apply str (map #(char (ffi/unpack ptr % CUInt8))
       (range 0 n))))

(defmulti
  ^{:doc "Extracts the value for the specified column in a particular statement
following a call to sqlite3_step"
    :private true}
  load-value #(sqlite3_column_type (deref-ptr %1) %2))

(defmethod load-value SQLITE_TEXT [statement column]
  (let [size (sqlite3_column_bytes (deref-ptr statement) column)]
    (read-n-chars (sqlite3_column_text (deref-ptr statement) column) size)))

(defmethod load-value SQLITE_INTEGER [statement column]
  (sqlite3_column_int64 (deref-ptr statement) column))

(defmethod load-value SQLITE_FLOAT [statement column]
  (sqlite3_column_double (deref-ptr statement) column))


(defn- get-row [conn statement]
  "Get a seq of all the column values for a row after a call to sqlite3_step"
  (for [i (range 0 (sqlite3_column_count (deref-ptr statement)))]
    (load-value statement i)))

(defn- run-prepared-statement [conn statement]
  (loop [result []]
    (let [step-value (sqlite3_step (deref-ptr statement))]
      (cond
        (= step-value SQLITE_ROW) (recur (conj result (get-row conn statement)))
        (= step-value SQLITE_DONE) result))))

(defn- get-column-names [statement]
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
(defn query [conn query & args]
  (let [statement (prepare-query conn query args)
        rows (run-prepared-statement conn statement)
        cols (vec (get-column-names statement))]
    (sqlite3_finalize (deref-ptr statement))
    (ffi/dispose! statement)
    (vec (map #(zipmap cols %) rows)))) 

(defn- with-connection-fn [filename handler]
  (let [conn (connect filename)]
    (handler conn)
    (close-connection conn)))

(defmacro with-connection [db-name bindings & body]
  `(with-connection-fn ~db-name
                       (fn ~bindings ~@body)))

(def symbol->column-type
  ^{:private true}
  {:string  "STRING"
   :integer "INTEGER"
   :float   "FLOAT"})

(defn- pair->ddl [[col-name type]]
  (str (name col-name) " " (symbol->column-type type)))

; (defn create-table-ddl [table & specs]
;   (str "CREATE TABLE " (name table) " "
;        "(" (apply str (interpose ", " (map pair->ddl specs))) ");"))
