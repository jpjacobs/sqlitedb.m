## SQLiteDB for Matlab

sqlitedb.m provides a solution to the problem encountered with generating
results from testing scripts, where afterwards it's hard to keep track of which
script(s) generated which results with which options.

The SQLite interface itself used is [this one](https://github.com/kyamagu/matlab-sqlite3-driver).

The idea is to declare a number of fields and types, and those will be the
columns in the SQLite database.  All Matlab types can be saved; the ones
natively supported by sqlite are stored as is, others are serialized by saving
the object to a temporary file.

The most convenient interface is the sqlitedb/addFrom(str) function, which adds
all fields in a scalar struct str which have corresponding names declared for
the sqlitedb object.

Arbitrary (watch out, no safeguards against DROP or DELETE) SQL commands can be
executed by sqlitedb/execute().

More documentation can be found in the scripts comments, and more might be
written here in the future.
