sqle
=====

An OTP library for building SQL statements for postgresql.

Usage
======

The basic usage is through the `select/1,2,3`, `insert/2,3`, `delete/1,2`, `update/2,3` and
`to_iolist/2`/`to_binary/2`.

To understand how they are used, please examine the test suites.

The second argument to the `to_iolist` and `to_binary` function should be a function that takes one
argument and returns `iodata()`. This should convert any valid erlang value to something that is
safe to put in the SQL itself as a value.
