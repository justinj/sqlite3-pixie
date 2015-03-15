(ns sqlite
  (require pixie.ffi :as ffi)
  (require pixie.ffi-infer :as f)
  )

(def libsqlite-name "/usr/lib/sqlite3/libtclsqlite3.dylib")
(def libsqlite (ffi-library libsqlite-name))


(def buf (buffer 1024))
(def coolbuf (buffer 1024))
(def errbuf (buffer 1024))


; int sqlite3_exec(
;                  sqlite3*,                                  /* An open database */
;                  const char *sql,                           /* SQL to be evaluated */
;                  int (*callback)(void*,int,char**,char**),  /* Callback function */
;                  void *,                                    /* 1st argument to callback */
;                  char **errmsg                              /* Error msg written here */
;                  );

; (def cb-type (ffi/ffi-callback [CVoidP CInt CCharP CCharP] CInt))
(def cb-type (ffi/ffi-callback [CVoidP CInt32 CVoidP CVoidP] CInt))

(f/with-config {:library "sqlite3"
                :includes ["sqlite3.h"]}
  (f/defcfn sqlite3_open)
  (f/defcfn sqlite3_exec))

(def callback (ffi/ffi-prep-callback cb-type
                                     (fn [_ argc argv cols]
                                       (prn (ffi/unpack argv 0 CCharP))
                                     0)))

(dotimes [i 10]
  (print (nth buf i) " "))
(prn)

(prn (sqlite3_open "/Users/justin/dev/pixie/sqlite/test.db" buf))

(dotimes [i 10]
  (print (nth buf i) " "))
(prn)

(let [res (ffi/unpack buf 0 CVoidP)]

(prn (sqlite3_exec res "select * from testtable;" callback nil errbuf))
)

(let [res (ffi/unpack errbuf 0 CCharP)]
  (prn res))

(dotimes [i 10]
  (print (nth errbuf i) " "))
(prn)
