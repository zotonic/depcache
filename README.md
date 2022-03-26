![Test](https://github.com/zotonic/depcache/workflows/Test/badge.svg)

depcache
========

depcache is an in-memory caching server for Erlang with dependency
checks, cache expiration and local in process memoization of
lookups. It is used by the Zotonic project for all memory-related
caching strategies.

For a detailed explanation, see the chapter on depcache in the book
`The Performance of Open Source Applications` on [this page](http://aosabook.org/en/posa/zotonic.html#posa.zotonic.depcache).

Usage
-----

Start a depcache server like this:

    {ok, Server} = depcache:start_link([]).

Now you can get and set values using the returned `Server` pid.


License
-------

Like Zotonic, depcache is licensed under the Apache 2.0 license.
