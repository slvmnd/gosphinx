About
-----

A sphinx client package for the Go programming language.
Implemented by https://github.com/yunge

A few bugfixes added by slvmnd

Installation
------------

`go get github.com/slvmnd/goshpinx`

Testing
-------

import "documents.sql" to "test" database in mysql, start sphinx searchd with "sphinx.conf".

Then "cd" to gosphinx,

`make test`

Differs from other languages' lib
-------------------------------

No GetLastError()

Go can return multi values, it's unnecessary to set a "error" field, gosphinx just return error as another return values.

But GetLastWarning() is still remained, and still has IsConnectError() to "Checks whether the last error was a network error on API side".


