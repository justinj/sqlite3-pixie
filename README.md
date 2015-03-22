sqlite3-pixie
=============

Sqlite3 bindings for Pixie.

TODO
====

* transactions
* make api similar to jdbc
* make the finding of the sqlite library cross platform, this might entail packaging a sqlite tarball with this library
* check for errors when opening a database - it's not clear to me what can go wrong, but an error code is returned so we should check it
* maybe experiment to see if we can detect memory leaks? running stuff a bunch of times?
* should comment signatures all the ffi-infer stuff
* reorganize the ffi-infer stuff
