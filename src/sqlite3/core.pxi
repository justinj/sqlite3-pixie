(ns sqlite3.core
  (require pixie.ffi :as ffi)
  (require pixie.ffi-infer :as f)
  (require pixie.string :as string))

(def libsqlite-name (f/full-lib-name "sqlite3"))
(def libsqlite (ffi-library libsqlite-name))

(f/with-config {:library "sqlite3"
                :includes ["sqlite3.h"]}
  ; last arg to exec is a buffer for errors, maybe useful
  (f/defcfn sqlite3_exec)

  (f/defcfn sqlite3_open)
  (f/defcfn sqlite3_close_v2)

  ; https://www.sqlite.org/c3ref/bind_parameter_index.html
  (f/defcfn sqlite3_bind_parameter_count)

  ; takes connection, sql statement, number of bytes in the sql statement,
  ; OUTPUT parameter to a statement, and an OUTPUT parameter to the unused
  ; portion of the second argument
  (f/defcfn sqlite3_prepare_v2)

  ; Destroys a statement
  (f/defcfn sqlite3_finalize)

  ; Takes a connection and returns a string error
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

  ; The following functions deal with extracting data after a query
  ; Generally these functions take a statement, and some of them take a column index.

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

  ; returned by a number of function to signal no error
  (f/defconst SQLITE_OK)

  ; the possible return values from sqlite3_step
  ; means there is another row of data available
  (f/defconst SQLITE_ROW)
  ; means no more rows of data
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
; SQLITE_TRANSIENT tells bind_text that the string we are passing in can
; potentially be garbage collected, and so it should make an internal copy
; TODO: this is what should realllly be fixed. I feel like this is a bug in
; pixie's ffi-infer, since I get some weird unspecified error when I try to use
; the inferred one.
(def SQLITE_TRANSIENT -1)
; (prn "we got " SQLITE_TRANSIENT)
(def sqlite3_bind_text (ffi/ffi-fn libsqlite "sqlite3_bind_text" [CVoidP CInt CCharP CInt CInt] CInt))

; TODO: figure out an appropriate size for this
(defn new-ptr []
  (buffer 8))

(defn connect [db-name]
  (let [conn-buffer (new-ptr)
        error-code (sqlite3_open db-name conn-buffer)
        conn (ffi/unpack conn-buffer 0 CVoidP)]
    (ffi/dispose! conn-buffer)
    (when-not (= error-code SQLITE_OK)
      (throw {:msg "Sqlite Error"
              :data (sqlite3_errmsg conn)}))
    conn))

(defn close-connection [conn]
  (sqlite3_close_v2 conn))

; TODO: I feel like the fact that this is used so often implies that in some
; cases we should just pass around the deref'd ptr
(defn- deref-ptr [ptr]
  (ffi/unpack ptr 0 CVoidP))

(defmulti
  ^{:doc "Extracts the value for the specified column in a particular statement
following a call to sqlite3_step"
    :private true}
  load-value #(sqlite3_column_type (deref-ptr %1) %2))

(defmethod load-value SQLITE_TEXT [statement column]
  (let [size (sqlite3_column_bytes (deref-ptr statement) column)]
    (sqlite3_column_text (deref-ptr statement) column)))

(defmethod load-value SQLITE_INTEGER [statement column]
  (sqlite3_column_int64 (deref-ptr statement) column))

(defmethod load-value SQLITE_FLOAT [statement column]
  (sqlite3_column_double (deref-ptr statement) column))

(defn- get-row [statement]
  "Get a seq of all the column values for a row after a call to sqlite3_step"
  (for [i (range 0 (sqlite3_column_count (deref-ptr statement)))]
    (load-value statement i)))

(defn- reduce-prepared-statement [statement f init]
  (loop [result init]
    (let [step-value (sqlite3_step (deref-ptr statement))]
      (cond
        (= step-value SQLITE_ROW) (recur (f result (get-row statement)))
        (= step-value SQLITE_DONE) result))))

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

(defn- run-prepared-statement [statement]
  (reduce-prepared-statement statement conj []))

(defn- get-column-names [statement]
  (let [num-cols (sqlite3_column_count (deref-ptr statement))]
    (map keyword
         (map #(sqlite3_column_name (deref-ptr statement) %) (range 0 num-cols)))))

; TODO: we should make sure the connection is valid
(defn query [conn query & args]
  (let [statement (prepare-query conn query args)
        rows (run-prepared-statement statement)
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
