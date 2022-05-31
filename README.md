![Test](https://github.com/zotonic/depcache/workflows/Test/badge.svg)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg?logo=apache&logoColor=red)](https://www.apache.org/licenses/LICENSE-2.0)

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

```
    {ok, Server} = depcache:start_link([]).
```

Now you can get and set values using the returned `Server` pid.


## Documentation generation

### Edoc

#### Generate public API
`rebar3 edoc`

#### Generate private API
`rebar3 as edoc_private edoc`

### ExDoc

`rebar3 ex_doc --logo doc/img/logo.png --output edoc`

License
-------

Like Zotonic, depcache is licensed under the Apache 2.0 license.
