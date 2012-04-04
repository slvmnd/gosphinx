About
-----

A sphinx client package for the Go programming language.

Installation
------------

`go get github.com/yunge/gosphinx`

Testing
-------

import "documents.sql" to "test" database in mysql, start sphinx searchd with "sphinx.conf".

Then "cd" to gosphinx,

`go test .`

Differs from other languages' lib
-------------------------------

No GetLastError()

Go can return multi values, it's unnecessary to set a "error" field, gosphinx just return error as another return values.

But GetLastWarning() is still remained, and still has IsConnectError() to "Checks whether the last error was a network error on API side".


