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

Add the Hex package in your config file https://hex.pm/packages/depcache

Start a depcache server like this:

```
    {ok, Server} = depcache:start_link([]).
```

Now you can get and set values using the returned `Server` pid.


## Memoization keys and failures

When no explicit key is supplied, `memo` uses the zero-argument function term
(including its captured environment), or a key derived from the
`{Module, Function, Args}` / `{Module, Function}` tuple. Deriving a key never
executes the function.

For MFA calls, top-level argument tuples tagged `context` are deliberately
excluded from the implicit key, allowing results to be shared across contexts.
The function still receives the original arguments. If its result depends on
context fields (such as user, site, or language), supply an explicit key that
includes those fields. Other arguments, including nested context tuples, remain
part of the implicit key.

Concurrent `memo` callers receive the computation's original exception class,
reason, and stack trace. Direct `get_wait/2` callers receive failed computations
as `{exception, Class, Reason, Stack}` instead of the older `{throw, Reason}`
reply that discarded the class and stack trace.


## Invalidation and memory limits

Setting or flushing a dependency key invalidates entries that depend on it.
Dependency records are retained while referenced and reclaimed when their last
cached dependent is removed. Cleanup checks entry generations so an eviction
selected before a replacement write cannot delete the replacement.

`memory_max` is an approximate limit in MiB. It counts all cache ETS tables plus
retained binary payloads. Shared binaries are counted conservatively, so the
estimate may exceed actual memory use. `depcache:size/1` returns this estimate
in machine words; caller heaps and queued messages are not included.

Insertion admission reserves an estimated footprint before storing an entry.
Oversized entries and entries that cannot fit are not retained; `set` still
returns `ok`, invalidates the old value and its dependents, and delivers the new
value to memo waiters. Thus a successful `set` does not guarantee a subsequent
cache hit. A zero budget disables retention. Random eviction wakes at 90% of
the budget and processes bounded batches; expiry sweeps scale with table size
and have a time budget. Fixed table overhead and hash-table allocation mean
this is not a strict process or VM memory limit.

Ordinary `get/2` lookups use ETS directly for both PID and named servers.
Concurrent replacements retain the old entry until publication; readers that
cross generations resolve through the server. Payload accounting runs in the
inserting caller, leaving the single writer free to process other clients.

Optional process-local caching refreshes on the next lookup after the clock
second changes. Updates from another process can therefore remain invisible
within that interval. `flush_process_dict/0` clears local results immediately;
`in_process(false)` clears them and disables local caching.


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
