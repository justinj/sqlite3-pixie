sqlite3-pixie
=============

Sqlite3 bindings for [Pixie](https://github.com/pixie-lang/pixie).

This library is not ready to be used in general unless you like bugs and probable interface changes.

In particular, this almost certainly does not work on Linux at the moment (due to the hardcoded location of the sqlite dsym).

Presumably in the future Pixie will have a more uniform interface across different SQL databases, like Clojure has jdbc, so this is sort of temporary (in its current form, at least).

Pull requests welcome!

Running the tests
=================

```bash
$ dust get-deps
$ dust test
```

TODO
====

* transactions
* make api similar to jdbc
* make the finding of the sqlite library cross platform, this might entail packaging a sqlite tarball with this library
* maybe experiment to see if we can detect memory leaks? running stuff a bunch of times?
* benchmarks would be cool
