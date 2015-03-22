sqlite3-pixie
=============

Sqlite3 bindings for Pixie.

This library is not ready to be used in general,
unless you like bugs and probable interface changes.

Presumably in the future Pixie will have a more
uniform interface across different SQL database, like
Clojure has jdbc, so this is sort of temporary (for now)

TODO
====

* transactions
* make api similar to jdbc
* make the finding of the sqlite library cross platform, this might entail packaging a sqlite tarball with this library
* maybe experiment to see if we can detect memory leaks? running stuff a bunch of times?
* reorganize the ffi-infer stuff
* benchmarks would be cool
