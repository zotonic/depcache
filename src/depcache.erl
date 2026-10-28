%% @author Arjan Scherpenisse
%% @copyright 2009-2026 Marc Worrell, Arjan Scherpenisse
%% @doc Depcache API
%%
%% == depcache API ==
%% {@link flush/1}, {@link flush/2}, {@link get/2},	{@link get/3}, {@link get_subkey/3},
%% {@link get_wait/2}, {@link set/3}, {@link set/4},	{@link set/5}, {@link size/1}
%% <br />
%% {@link memo/2}, {@link memo/3}, {@link memo/4}, {@link memo/5}
%% <br />
%% {@link flush_process_dict/0}, {@link in_process/1}, {@link in_process_server/1}
%%
%% === Internal ===
%% {@link cleanup/1}, {@link cleanup/5}
%%
%% @end
%% Copyright 2009-2026 Marc Worrell, Arjan Scherpenisse
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(depcache).
-author("Marc Worrell <marc@worrell.nl>").
-behaviour(gen_server).

-include_lib("depcache/include/depcache.hrl").

%% gen_server API
-export([start_link/1, start_link/2]).

%% depcache API
-export([set/3, set/4, set/5, get/2, get_wait/2, get/3, get_subkey/3, flush/2, flush/1, size/1]).
-export([memo/2, memo/3, memo/4, memo/5]).
-export([in_process_server/1, in_process/1, flush_process_dict/0]).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


%% internal export
-export([cleanup/1, cleanup/5]).

-ifdef(fun_stacktrace).
-define(WITH_STACKTRACE(T, R, S), T:R -> S = erlang:get_stacktrace(),).
-else.
-define(WITH_STACKTRACE(T, R, S), T:R:S ->).
-endif.

-type mfargs() :: {module(), atom(), list()}.
-type memo_fun() :: fun(() -> any()) | mfargs() | {module(), atom()}.
-type exception_reply() :: {exception, error | exit | throw, term(), list()}.
-type depcache_server() :: pid() | atom().
-type sec() :: non_neg_integer().
-type max_age_secs() :: sec().
-type key() :: any().
-type dependencies() :: list( key() ).
-type proplist() :: proplists:proplist().
-type callback() :: mfargs().

-export_type([
    memo_fun/0,
    depcache_server/0,
    key/0,
    max_age_secs/0
]).

-record(tables, {
    meta_table :: ets:tab(),
    deps_table :: ets:tab(),
    data_table :: ets:tab(),
    memory_table :: ets:tab()
}).

-record(state,  {now :: sec(), serial :: non_neg_integer(), tables :: tables(), wait_pids :: map(), writers :: map(), cleanup_pid :: pid(), memory_max :: non_neg_integer(), pressured = false :: boolean() }).
-record(meta,   {key :: key(), expire :: sec(), serial :: non_neg_integer(), depend :: dependencies(), binary_bytes = 0 :: non_neg_integer(), memory_words = 0 :: non_neg_integer()}).
-record(depend, {key :: key(), serial :: non_neg_integer(), refs = 0 :: non_neg_integer()}).

-record(cleanup_state, {
    pid :: pid(),
    tables :: #tables{},
    name :: atom(),
    memory_max :: non_neg_integer(),
    callback :: callback() | undefined
}).

-type tables() :: #tables{meta_table :: ets:tab(), deps_table :: ets:tab(), data_table :: ets:tab(), memory_table :: ets:tab()}.
-type state() :: #state{now :: sec(), serial :: non_neg_integer(), tables :: tables(), wait_pids :: map(), writers :: map()}.
-type depend() :: #depend{key :: key(), serial :: non_neg_integer()}.
-type cleanup_state() :: #cleanup_state{pid :: pid(), tables :: tables(), name :: atom(), memory_max :: non_neg_integer(), callback :: callback() | undefined}.
-type config() :: config_map() | config_proplist().
-type config_map() :: #{memory_max => non_neg_integer() | undefined,
                        callback => callback() | undefined}.
-type config_proplist() :: list({memory_max, non_neg_integer | undefined}
                              | {callback, callback() | undefined}).


%% @doc Start a depcache process.
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#start_link-3 gen_server:start_link/3].
%%
%% For Config, you can pass:
%% <dl>
%%   <dt>`callback => {Module, Function, Arguments}'</dt>
%%   <dd>depcache event callback</dd>
%%   <dt>`memory_max => MaxMemoryInMB'</dt>
%%   <dd>number of MB to limit depcache size at</dd>
%% </dl>
%% @param Config configuration options
%% @returns Result of starting gen_server item.

-spec start_link(Config) -> Result when
    Config :: config(),
    Result :: {ok, pid()} | ignore | {error, term()}.
start_link(Config) when is_map(Config) ->
    ensure_valid_config(Config),
    gen_server:start_link(?MODULE, Config, []);
start_link(Config) ->
    start_link(proplists_to_map(Config)).


%% @doc Start a named depcache process.
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#start_link-4 gen_server:start_link/4].
%%
%% For Config, you can pass:
%% <dl>
%%   <dt>`callback => {Module, Function, Arguments}'</dt>
%%   <dd>depcache event callback</dd>
%%   <dt>`memory_max => MaxMemoryInMB'</dt>
%%   <dd>number of MB to limit depcache size at</dd>
%% </dl>
%% @param Name of process
%% @param Config configuration options
%% @returns Result of starting gen_server item.

-spec start_link(Name, Config) -> Result when
    Name :: atom(),
    Config :: config(),
    Result :: {ok, pid()} | ignore | {error, term()}.
start_link(Name, Config) when is_map(Config) ->
    ensure_valid_config(Config),
    gen_server:start_link({local, Name}, ?MODULE, Config#{name => Name}, []);
start_link(Name, Config) ->
    start_link(Name, proplists_to_map(Config)).

-ifdef(OTP_RELEASE).
    -if(?OTP_RELEASE >= 24).
        -define(has_proplists_to_map, true).
    -endif.
-endif.

-spec proplists_to_map(PropList) -> Map when
    PropList :: proplist(),
    Map :: map().
-ifdef(has_proplists_to_map).
%% proplists:to_map was introduced in OTP 24.
proplists_to_map(List) ->
    proplists:to_map(List).
-else.
%% Backport of proplists:to_map.
%% Copied from https://github.com/erlang/otp/blob/master/lib/stdlib/src/proplists.erl#L704-L720
proplists_to_map(List) ->
    lists:foldr(
        fun
            ({K, V}, M) ->
                M#{K => V};
            %% if tuples with arity /= 2 appear before atoms or
            %% tuples with arity == 2, proplists:get_value/2,3 returns early
            (T, M) when 1 =< tuple_size(T) ->
                maps:remove(element(1, T), M);
            (K, M) when is_atom(K) ->
                M#{K => true};
            (_, M) ->
                M
        end,
        #{},
        List
    ).
-endif.

-spec ensure_valid_config(Config) -> ok when
    Config :: config_map().
ensure_valid_config(Config) ->
    maps:fold(
        fun
            (memory_max, undefined, Acc) ->
                Acc;
            (memory_max, MemoryMax, Acc) when is_integer(MemoryMax), MemoryMax >= 0 ->
                Acc;
            (callback, undefined, Acc) ->
                Acc;
            (callback, {M, F, Args}, Acc) when is_atom(M), is_atom(F), is_list(Args) ->
                Acc;
            (callback, {M, F, _Arg}, Acc) when is_atom(M), is_atom(F) ->
                %% LEGACY: depcache handles non-list arguments as if a list of
		%%         that single argument was given, which is ambiguous and
		%%         should be discouraged
                Acc;
            (_, _, _) ->
                error(badarg)
        end,
	ok,
        Config
    ).

-define(META_TABLE_PREFIX, $m).
-define(DEPS_TABLE_PREFIX, $p).
-define(DATA_TABLE_PREFIX, $d).

-define(NAMED_TABLE(Name,Prefix), list_to_atom([Prefix,$:|atom_to_list(Name)])).

%% One hour, in seconds.
-define(HOUR,     3600).

%% Default max size, in Mbs, of the stored data in the depcache before the gc kicks in.
-define(MEMORY_MAX, 100).

% Maximum time to wait for a get_wait/2 call before a timout failure (in secs).
-define(MAX_GET_WAIT, 30).

% Number of slots visited for each gc iteration.
-define(CLEANUP_BATCH, 100).

% Number of entries we keep in the local process dictionary for fast lookups.
-define(PROCESS_DICT_THRESHOLD, 10000).


%% @doc Cache the result of the function for an hour.
%% @param Fun a function for producing a value
%% @returns cached value

-spec memo( Fun, Server ) -> Result when
    Fun :: memo_fun(),
    Server :: depcache_server(),
    Result :: any().
memo(Fun, Server) ->
    memo(Fun, undefined, ?HOUR, [], Server).


%% @doc If Fun is a function then cache for an hour given the key. If
%%      Fun is a {M,F,A} tuple then derive the key from the tuple and
%%      cache for `MaxAge' seconds.
%%
%% @param Fun a function for producing a value1
%% @param MaxAge a caching time
%% @param Key a cache item key
%% @returns cached value

-spec memo( Fun, MaxAge_Key, Server ) -> Result when
    Fun :: memo_fun(),
    MaxAge_Key :: MaxAge | Key,
    MaxAge :: max_age_secs(),
    Key :: key(),
    Server :: depcache_server(),
    Result :: any().
memo(Fun, MaxAge, Server) when is_tuple(Fun) ->
    memo(Fun, undefined, MaxAge, [], Server);
memo(Fun, Key, Server) when is_function(Fun) ->
    memo(Fun, Key, ?HOUR, [], Server).


%% @doc Cache the result of the function as Key for `MaxAge' seconds.
%% @returns cached value
%% @equiv memo(Fun, Key, MaxAge, [], Server)

-spec memo( Fun, Key, MaxAge, Server ) -> Result when
    Fun :: memo_fun(),
    Key :: key(),
    MaxAge :: max_age_secs(),
    Server :: depcache_server(),
    Result :: any().
memo(Fun, Key, MaxAge, Server) ->
    memo(Fun, Key, MaxAge, [], Server).


%% @doc Cache the result of the function as Key for `MaxAge' seconds, flush
%%      the cached result if any of the dependencies is changed.
%% @returns cached value

-spec memo( Fun, Key, MaxAge, Dep, Server ) -> Result when
    Fun :: memo_fun(),
    Key :: undefined | key(),
    MaxAge :: max_age_secs(),
    Dep :: dependencies(),
    Server :: depcache_server(),
    Result :: any().
memo(Fun, Key, MaxAge, Dep, Server) ->
    Key1 = case Key of
               undefined -> memo_key(Fun);
               _ -> Key
           end,
    case ?MODULE:get_wait(Key1, Server) of
        {ok, Value} ->
            Value;
        {exception, Class, Reason, Stack} ->
            erlang:raise(Class, Reason, Stack);
        {throw, R} ->
            %% Accept replies from computations started by an older version.
            throw(R);
        undefined ->
            memo_key(Fun, Key1, MaxAge, Dep, Server);
        {error, premature_exit} ->
            ?MODULE:memo(Fun, Key, MaxAge, Dep, Server)
    end.

%% @private
%% @param MaxAge maximum lifetime of an element in the cache
%% @param Dep list of subkeys
%% @returns cached value
%% @see memo/5

-spec memo_key( Fun, Key, MaxAge, Dep, Server ) -> Result when
    Fun :: memo_fun(),
    Key :: key(),
    MaxAge :: max_age_secs(),
    Dep :: dependencies(),
    Server :: depcache_server(),
    Result :: any().
memo_key(Fun, Key, MaxAge, Dep, Server) ->
    try
        {Value1, MaxAge1, Dep1} = case apply_fun(Fun) of
                                      #memo{value=V, max_age=MA, deps=D} ->
                                          MA1 = case is_integer(MA) of
                                                    true -> MA;
                                                    false -> MaxAge
                                                end,
                                          {V, MA1, Dep++D};
                                      Value ->
                                          {Value, MaxAge, Dep}
                                  end,
        case MaxAge1 of
            0 -> memo_send_replies(Key, Value1, Server);
            _ -> set(Key, Value1, MaxAge1, Dep1, Server)
        end,

        Value1
    catch
        ?WITH_STACKTRACE(Class, R, S)
            memo_send_errors(Key, {exception, Class, R, S}, Server),
            erlang:raise(Class, R, S)
    end.


%% @private
%% @doc Execute the memo function
%% Returns the result value
apply_fun({M,F,A}) -> erlang:apply(M,F,A);
apply_fun({M,F}) -> M:F();
apply_fun(Fun) when is_function(Fun) -> Fun().


%% @private
%% @doc Derive a key without executing the computation. Context records in MFA
%% arguments are deliberately excluded, allowing reuse across contexts. Callers
%% must provide an explicit key when the result depends on the context.
-spec memo_key(memo_fun()) -> key().
memo_key(Fun) when is_function(Fun, 0) ->
    Fun;
memo_key({M,F,A}) ->
    WithoutContext = lists:filter(fun(T) when is_tuple(T) andalso element(1, T) =:= context -> false; (_) -> true end, A),
    {M,F,WithoutContext};
memo_key({M,F}) ->
    {M,F}.


%% @private
%% @doc Send the calculated value to the processes waiting for the result.
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#reply-2 gen_server:reply/2].
%%

-spec memo_send_replies( Key, Value, Server ) -> Result when
    Key :: key(),
	Value :: any(),
	Server :: depcache_server(),
	Result :: ok.
memo_send_replies(Key, Value, Server) ->
    Pids = get_waiting_pids(Key, Server),
    [ catch gen_server:reply(Pid, {ok, Value}) || Pid <- Pids ],
    ok.


%% @private
%% @doc Send an error to the processes waiting for the result.
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#reply-2 gen_server:reply/2].
%%

-spec memo_send_errors( Key, Exception, Server ) -> Result when
    Key :: key(),
    Exception :: exception_reply(),
    Server :: depcache_server(),
    Result :: list().
memo_send_errors(Key, Exception, Server) ->
    Pids = get_waiting_pids(Key, Server),
    [ catch gen_server:reply(Pid, Exception) || Pid <- Pids ].


%% @doc Add the key to the depcache, hold it for `3600' seconds and no dependencies.
%% @equiv set(Key, Data, 3600, [], Server)

-spec set( Key, Data, Server ) -> Result when
    Key :: key(),
    Data :: any(),
    Server :: depcache_server(),
    Result :: ok.

set(Key, Data, Server) ->
    set(Key, Data, ?HOUR, [], Server).


%% @doc Add the key to the depcache, hold it for `MaxAge' seconds and no dependencies.
%% @equiv set(Key, Data, MaxAge, [], Server)

-spec set( Key, Data, MaxAge, Server ) -> Result when
    Key :: key(),
    Data :: any(),
    MaxAge :: max_age_secs(),
    Server :: depcache_server(),
    Result :: ok.
set(Key, Data, MaxAge, Server) ->
    set(Key, Data, MaxAge, [], Server).


%% @doc Add the key to the depcache, hold it for `MaxAge' seconds and check the dependencies.
%% @param MaxAge maximum lifetime of an element in the cache
%% @param Depend list of subkeys
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#call-2 gen_server:call/2].
%%

-spec set( Key, Data, MaxAge, Depend, Server ) -> Result when
    Key :: key(),
    Data :: any(),
    MaxAge :: max_age_secs(),
    Depend :: dependencies(),
    Server :: depcache_server(),
    Result :: ok.
set(Key, Data, MaxAge, Depend, Server) ->
    flush_process_dict(),
    %% Traverse payloads in the caller, not in the shared writer process.
    {Bytes, Words} = entry_size(Key, Data, Depend),
    gen_server:call(Server, {set, Key, Data, MaxAge, Depend, Bytes, Words}).


%% @doc Fetch the key from the cache, when the key does not exist then lock the entry and let
%% the calling process insert the value. All other processes requesting the key will wait till
%% the key is updated and receive the key's new value.
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#call-3 gen_server:call/3].
%%

-spec get_wait( Key, Server ) -> Result when
    Key :: key(),
    Server :: depcache_server(),
    Result :: {ok, any()} | undefined | exception_reply() | {throw, term()} | {error, premature_exit}.
get_wait(Key, Server) ->
    case get_process_dict(Key, Server) of
        NoValue when NoValue =:= undefined orelse NoValue =:= depcache_disabled ->
            gen_server:call(Server, {get_wait, Key}, ?MAX_GET_WAIT*1000);
        Other ->
            Other
    end.


%% @private
%% @doc Fetch the queue of pids that are waiting for a `get_wait/2'. This flushes the queue and
%% the key from the depcache.
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#call-3 gen_server:call/3].
%%
%% @see get_wait/2

-spec get_waiting_pids( Key, Server ) -> Result when
    Key :: key(),
    Server :: depcache_server(),
    Result :: [{pid(), Tag}],
    Tag :: gen_server:reply_tag().

get_waiting_pids(Key, Server) ->
    gen_server:call(Server, {get_waiting_pids, Key}, ?MAX_GET_WAIT*1000).


%% @doc Fetch the key from the cache, return the data or an `undefined' if not found (or not valid).
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#call-2 gen_server:call/2].
%%

-spec get( Key, Server ) -> Result when
    Key :: key(),
    Server :: depcache_server(),
    Result :: {ok, any()} | undefined.
get(Key, Server) ->
    case get_process_dict(Key, Server) of
        depcache_disabled -> gen_server:call(Server, {get, Key});
        Value -> Value
    end.


%% @doc Fetch the key from the cache, return the data or an `undefined' if not found (or not valid)
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#call-2 gen_server:call/2].
%%

-spec get_subkey( Key, SubKey, Server ) -> Result when
    Key :: key(),
    SubKey :: key(),
    Server :: depcache_server(),
    Result :: {ok, any()} | undefined.
get_subkey(Key, SubKey, Server) ->
    case in_process_server(Server) of
        true ->
            case erlang:get({depcache_subkey, Key, SubKey}) of
                {memo, Value} ->
                    Value;
                undefined ->
                    Value = gen_server:call(Server, {get, Key, SubKey}),
                    memo_put({depcache_subkey, Key, SubKey}, Value),
                    Value
            end;
        false ->
            gen_server:call(Server, {get, Key, SubKey})
    end.


%% @doc Fetch the key from the cache, return the data or an `undefined' if not found (or not valid)
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#call-2 gen_server:call/2].
%%

-spec get( Key, SubKey, Server ) -> Result when
    Key :: key(),
    SubKey :: key(),
    Server :: depcache_server(),
    Result :: {ok, any()} | undefined.
get(Key, SubKey, Server) ->
    case get_process_dict(Key, Server) of
        undefined ->
            undefined;
        depcache_disabled ->
            gen_server:call(Server, {get, Key, SubKey});
        {ok, Value} ->
            {ok, find_value(SubKey, Value)}
    end.


%% @doc Flush the key and all keys depending on that key
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#call-2 gen_server:call/2].
%%

-spec flush( Key, Server ) -> Result when
    Key :: key(),
    Server :: depcache_server(),
    Result :: ok.
flush(Key, Server) ->
    gen_server:call(Server, {flush, Key}),
    flush_process_dict().


%% @doc Flush all keys from the caches
%% <br/>
%% <b>See also:</b>
%% [http://erlang.org/doc/man/gen_server.html#call-2 gen_server:call/2].
%%

-spec flush( Server ) -> Result when
    Server :: depcache_server(),
    Result :: ok.
flush(Server) ->
    gen_server:call(Server, flush),
    flush_process_dict().


%% @doc Return estimated cache memory in words, including ETS overhead and
%% retained binary payloads. Shared binaries are counted conservatively.

-spec size( Server ) -> Result when
    Server :: depcache_server(),
    Result :: non_neg_integer().
size(Server) ->
    gen_server:call(Server, size).


%% @private
%% @doc Fetch the depcache tables.

-spec get_tables( Server ) -> Result when
    Server :: depcache_server(),
    Result :: tuple().
get_tables(Server) ->
    case erlang:get(depcache_tables) of
        {ok, Server, Tables} ->
            Tables;
        {ok, _OtherServer, _Tables} ->
            flush_process_dict(),
            get_tables1(Server);
        undefined->
            get_tables1(Server)
    end.

%% @private
%% @doc Get or init ets-tables.
%% @see get_tables/1

-spec get_tables1( Server ) -> Result when
	Server :: depcache_server(),
	Result :: tuple().
get_tables1(Server) when is_pid(Server) ->
    {ok, Tables} = gen_server:call(Server, get_tables),
    erlang:put(depcache_tables, {ok, Server, Tables}),
    Tables;
get_tables1(Server) when is_atom(Server) ->
    Tables = {
        ?NAMED_TABLE(Server, ?META_TABLE_PREFIX),
        ?NAMED_TABLE(Server, ?DEPS_TABLE_PREFIX),
        ?NAMED_TABLE(Server, ?DATA_TABLE_PREFIX)
    },
    erlang:put(depcache_tables, {ok, Server, Tables}),
    Tables.

%% @private
%% @doc Fetch a value from the dependency cache, using the in-process cached tables.

-spec get_process_dict(Key, Server ) -> Result when
    Key :: key(),
    Server :: depcache_server(),
    Result :: tuple() | depcache_disabled | undefined.
get_process_dict(Key, Server) ->
    case in_process_server(Server) of
        true ->
            case erlang:get({depcache, Key}) of
                {memo, Value} ->
                    Value;
                undefined ->
                    Value = get_ets(Key, Server),
                    memo_put({depcache, Key}, Value),
                    Value
            end;
        false ->
            get_ets(Key, Server)
    end.

%% @private
%% @doc Get cached value by key.

-spec get_ets(Key, Server ) -> Result when
    Key :: key(),
    Server :: depcache_server(),
    Result :: undefined | {ok, any()}.
get_ets(Key, Server) ->
    {MetaTable, DepsTable, DataTable} = get_tables(Server),
    try get_concurrent(Key, get_now(), MetaTable, DepsTable, DataTable) of
        {flush, Serial} ->
            flush_process_dict(),
            gen_server:call(Server, {get_checked, Key, Serial});
        retry ->
            %% A replacement crossed the two table reads. Resolve in the writer
            %% instead of reporting a miss or spinning while writes continue.
            gen_server:call(Server, {get, Key});
        Value -> Value
    catch
        error:badarg ->
            %% A cached table identifier outlived its server. Preserve the normal
            %% gen_server failure semantics and allow named servers to restart.
            erlang:erase(depcache_tables),
            flush_process_dict(),
            gen_server:call(Server, {get, Key})
    end.


%% @doc Check if we use a local process dict cache.

-spec in_process_server( Server ) -> Result when
    Server :: depcache_server(),
    Result :: boolean().
in_process_server(Server) ->
    case erlang:get(depcache_in_process) of
        true ->
            _ = get_tables(Server),
            Now = now_sec(),
            case erlang:get(depcache_now) =/= Now
                 orelse erlang:get(depcache_count) > ?PROCESS_DICT_THRESHOLD of
                true -> flush_process_dict();
                false -> ok
            end,
            erlang:put(depcache_now, Now),
            true;
        _ ->
            false
    end.


%% @doc Enable or disable the in-process caching using the process dictionary

-spec in_process( IsChaching ) -> Result when
    IsChaching :: undefined | boolean(),
    Result :: undefined | boolean().
in_process(true) ->
    erlang:put(depcache_in_process, true);
in_process(false) ->
    Old = erlang:erase(depcache_in_process),
    flush_process_dict(),
    Old;
in_process(undefined) ->
    in_process(false).


%% @doc Flush all items memoized in the process dictionary.

-spec flush_process_dict() -> ok.
flush_process_dict() ->
    case erlang:erase(depcache_keys) of
        undefined -> ok;
        Keys -> lists:foreach(fun erlang:erase/1, Keys)
    end,
    erlang:erase(depcache_now),
    erlang:put(depcache_count, 0),
    ok.

%% Track only our own entries; never scan the application's process dictionary.
-spec memo_put(term(), term()) -> ok.
memo_put(Key, Value) ->
    Keys = case erlang:get(depcache_keys) of undefined -> []; Ks -> Ks end,
    erlang:put(Key, {memo, Value}),
    erlang:put(depcache_keys, [Key|Keys]),
    erlang:put(depcache_count, incr(erlang:get(depcache_count))),
    ok.

%% @private
%% @doc Get the current system time in seconds

-spec get_now() -> sec().
get_now() ->
    now_sec().



%% gen_server callbacks

%% @private
%% @doc Initialize the depcache.  Creates ets tables for the deps, meta and data.  Spawns garbage collector.

-spec init(Config) -> Result when
    Config :: config_map(),
    State :: state(),
    Result :: {ok, State}.
init(Config) ->
    MemoryMaxMbs = case maps:get(memory_max, Config, undefined) of
                       undefined -> ?MEMORY_MAX;
                       MemoryMaxMbs1 -> MemoryMaxMbs1
                   end,
    MemoryMaxWords = 1024 * 1024 * MemoryMaxMbs div erlang:system_info(wordsize),

    Tables0 = case maps:get(name, Config, none) of
        none ->
            #tables{
                meta_table=ets:new(meta_table, [set, {keypos, 2}, protected, {read_concurrency, true}]),
                deps_table=ets:new(deps_table, [set, {keypos, 2}, protected, {read_concurrency, true}]),
                data_table=ets:new(data_table, [set, {keypos, 1}, protected, {read_concurrency, true}])
            };
        Name when is_atom(Name) ->
            #tables{
                meta_table=ets:new(?NAMED_TABLE(Name, ?META_TABLE_PREFIX),
                                   [set, named_table, {keypos, 2}, protected, {read_concurrency, true}]),
                deps_table=ets:new(?NAMED_TABLE(Name, ?DEPS_TABLE_PREFIX),
                                   [set, named_table, {keypos, 2}, protected, {read_concurrency, true}]),
                data_table=ets:new(?NAMED_TABLE(Name, ?DATA_TABLE_PREFIX),
                                   [set, named_table, {keypos, 1}, protected, {read_concurrency, true}])
            }
    end,
    MemoryTable = ets:new(memory_table, [set, protected]),
    ets:insert(MemoryTable, {binary_bytes, 0}),
    Tables = Tables0#tables{memory_table = MemoryTable},
    timer:send_after(1000, tick),
    CleanupPid = spawn_link(?MODULE,
               cleanup,
               [#cleanup_state{
                   pid = self(),
                   tables = Tables,
                   name = maps:get(name, Config, none),
                   memory_max = MemoryMaxWords,
                   callback = maps:get(callback, Config, undefined)
               }]),
    {ok, #state{
        tables=Tables,
        now=now_sec(),
        serial=0,
        wait_pids=#{},
        writers=#{},
        cleanup_pid=CleanupPid,
        memory_max=MemoryMaxWords
    }}.

%% @private
%% @doc Handling call messages

-spec handle_call(Request, From, State) -> Result when
    Request :: size,
    From :: {pid(), gen_server:reply_tag()},
    State :: state(),
    Result :: {reply, non_neg_integer(), State};
(Request, From, State) -> Result when
    Request :: get_tables,
    From :: {pid(), gen_server:reply_tag()},
    State :: state(),
    Meta_table :: ets:tab(),
    Deps_table :: ets:tab(),
    Data_table :: ets:tab(),
    Result :: {reply, {ok, {Meta_table, Deps_table, Data_table}}, State};
(Request, From, State) -> Result when
    Request :: get_wait,
    From :: {pid(), gen_server:reply_tag()},
    State :: state(),
    Result :: {reply, Reply, state()} | {noreply, state()},
    Reply :: undefined | {ok, term()};
(Request, From, State) -> Result when
    Request :: {get_waiting_pids, Key},
    Key :: key(),
    From :: {pid(), gen_server:reply_tag()},
    State :: state(),
    Result :: {reply, [{pid(), Tag}], state()},
    Tag :: gen_server:reply_tag();
(Request, From, State) -> Result when
    Request :: {get, Key} | {get_checked, Key, non_neg_integer()},
    Key :: key(),
    From :: {pid(), gen_server:reply_tag()},
    State :: state(),
    Result :: {reply, Reply, State},
    Reply :: undefined | {ok, term()};
(Request, From, State) -> Result when
    Request :: {get, Key, SubKey},
    Key :: key(),
    SubKey :: key(),
    From :: {pid(), gen_server:reply_tag()},
    State :: state(),
    Result :: {reply, Reply, State},
    Reply :: undefined | {ok, term()};
(Request, From, State) -> Result when
    Request :: {set, Key, Data, MaxAge, Depend} | {set, Key, Data, MaxAge, Depend, non_neg_integer(), non_neg_integer()},
    Key :: key(),
    Data :: any(),
    MaxAge :: max_age_secs(),
    Depend :: dependencies(),
    From :: {pid(), gen_server:reply_tag()},
    State :: state(),
    Result :: {reply, Reply, State},
    Reply :: ok;
(Request, From, State) -> Result when
    Request :: {flush, Key} | {flush, Key, non_neg_integer()} | {flush_batch, [{key(), non_neg_integer()}]},
    Key :: key(),
    From :: {pid(), gen_server:reply_tag()},
    State :: state(),
    Result :: {reply, ok, State};
(Request, From, State) -> Result when
	Request :: flush,
	From :: {pid(), gen_server:reply_tag()},
	State :: state(),
	Result :: {reply, ok, State}.
handle_call(size, _From, #state{tables=Tables}=State) ->
    {reply, memory_words(Tables), State};

handle_call(get_tables, _From, State) ->
	handle_call_get_tables(State);

handle_call({get_wait, Key}, From, State) ->
	handle_call_get_wait(Key, From, State);

handle_call({get_waiting_pids, Key}, _From, State) ->
	handle_call_get_waiting_pids(Key, State);

handle_call({get, Key}, _From, State) ->
	handle_call_get(Key, State);

handle_call({get, Key, SubKey}, _From, State) ->
    handle_call_get_sub_key({Key, SubKey}, State);

handle_call({set, Key, Data, MaxAge, Depend}, From, State) ->
    {Bytes, Words} = entry_size(Key, Data, Depend),
    handle_call({set, Key, Data, MaxAge, Depend, Bytes, Words}, From, State);

handle_call({set, Key, Data, MaxAge, Depend, Bytes, Words}, _From, State) ->
    handle_call_set({Key, Data, MaxAge, Depend, Bytes, Words}, State);

handle_call({get_checked, Key, Serial}, _From, State) ->
    flush_if_current(Key, Serial, State),
    handle_call_get(Key, State);

handle_call({flush_batch, Entries}, _From, State) ->
    lists:foreach(fun({Key, Serial}) -> flush_if_current(Key, Serial, State) end, Entries),
    {reply, ok, State};

handle_call({flush, Key, Serial}, _From, State) ->
    flush_if_current(Key, Serial, State),
    {reply, ok, State};

handle_call({flush, Key}, _From, State) ->
	handle_call_flush(Key, State);

handle_call(flush, _From, State) ->
	handle_call_flush_all(State).

%% @private
%% @doc Handling cast messages

-spec handle_cast(Request, State) -> Result when
    Request :: {flush, Key} | flush | any(),
    Key :: key(),
    State :: state(),
    Result :: {noreply, State}.

handle_cast({flush, Key, Serial}, State) ->
    flush_if_current(Key, Serial, State),
    {noreply, State};

handle_cast({flush, Key}, State) ->
    flush_key(Key, State),
    {noreply, State};

handle_cast(flush, #state{tables = Tables} = State) ->
    ets:delete_all_objects(Tables#tables.data_table),
    ets:delete_all_objects(Tables#tables.meta_table),
    ets:delete_all_objects(Tables#tables.deps_table),
    ets:insert(Tables#tables.memory_table, {binary_bytes, 0}),
    erase_process_dict(),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
%% @doc This function is called by a `gen_server' process when when it receives `tick' or
%% any other message than a synchronous or asynchronous request (or a system message).

-spec handle_info(Info, State) -> Result when
    Info :: tick | any(),
    State :: state(),
    Result :: {noreply, State}.

handle_info(tick, State) ->
    timer:send_after(1000, tick),
    erase_process_dict(),
    {noreply, State#state{now=now_sec()}};

handle_info({'DOWN', Ref, process, _Pid, _Reason}, #state{ writers = Writers }=State) ->
    case maps:take(Ref, Writers) of
        error ->
            {noreply, State};
        {Key, Writers1} ->
            WaitPids1 = case maps:take(Key, State#state.wait_pids) of
                           {{_MaxAge, List, Ref}, WaitPids} ->
                               _ = [ catch gen_server:reply(From, {error, premature_exit}) || From <- List ],
                               WaitPids;
                           _ -> State#state.wait_pids
                       end,
            {noreply, State#state{ writers = Writers1, wait_pids = WaitPids1 }}
    end;

handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
%% @doc This function is called by a `gen_server' process when it is about to terminate.

-spec terminate(Reason, State) -> Result when
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: state(),
    Result :: ok.

terminate(_Reason, #state{cleanup_pid=Pid}) ->
    %% A normal server exit does not terminate linked processes automatically.
    unlink(Pid),
    exit(Pid, shutdown),
    ok.


%% @doc This function is called by a gen_server process when it is to update
%% its internal state during a release upgrade/downgrade.
%% @private

-spec code_change(OldVersion, State, Extra) -> Result when
	OldVersion :: (term() | {down, term()}),
	State :: state(),
	Extra :: term(),
	Result :: {ok, State}.

code_change(_OldVersion, State, _Extra) -> {ok, State}.


%% @private
%% @doc Return the ets-tables used by the cache
%% @see handle_call/3
%%

-spec handle_call_get_tables(State) -> Result when
	State :: state(),
	Meta_table :: ets:tab(),
	Deps_table :: ets:tab(),
	Data_table :: ets:tab(),
	Result :: {reply, {ok, {Meta_table, Deps_table, Data_table}}, State}.
handle_call_get_tables(#state{tables = Tables} = State) ->
    {reply, {ok, {Tables#tables.meta_table, Tables#tables.deps_table, Tables#tables.data_table}}, State}.


%% @private
%% @doc Fetch a key from the cache. When the key is not available then let processes wait till the
%% key is available.
%% @see handle_call/3
%%

-spec handle_call_get_wait(Key, From, State) -> Result when
	Key :: key(),
	From :: {pid(), atom()},
	State :: state(),
	Result :: {reply, Reply, state()} | {noreply, state()},
	Reply :: undefined | {ok, term()}.
handle_call_get_wait(Key, From, #state{tables = Tables} = State) ->
    case get_concurrent(Key, State#state.now, Tables#tables.meta_table, Tables#tables.deps_table, Tables#tables.data_table) of
        NotFound when is_tuple(NotFound), element(1, NotFound) =:= flush; NotFound =:= undefined ->
            case NotFound of
                {flush, Serial} -> flush_if_current(Key, Serial, State);
                undefined -> State
            end,
            case State#state.wait_pids of
                #{Key := {MaxAge, List, WriterRef}} when State#state.now < MaxAge ->
                    %% Another process is already calculating the value, let the caller wait.
                    WaitPids = maps:update(Key, {MaxAge, [From|List], WriterRef}, State#state.wait_pids),
                    {noreply, State#state{wait_pids=WaitPids}};
                _ ->
                    %% de-monitor an old writer, if any.
                    Writers = case maps:find(Key, State#state.wait_pids) of
                                  error ->
                                      State#state.writers;
                                  {ok, {_, _WaitPids, OldWriter}} ->
                                      erlang:demonitor(OldWriter, [flush]),
                                      maps:without([OldWriter], State#state.writers)
                              end,

                    %% Monitor and register the writer
                    {Pid, _} = From,
                    Ref = erlang:monitor(process, Pid),
                    Writers1 = maps:put(Ref, Key, Writers),

                    %% Nobody waiting or we hit a timeout, let next requestors wait for this caller.
                    WaitPids = maps:put(Key, {State#state.now+?MAX_GET_WAIT, [], Ref}, State#state.wait_pids),

                    {reply, undefined, State#state{wait_pids=WaitPids, writers=Writers1}}
            end;
        {ok, _Value} = Found ->
            {reply, Found, State}
    end.


%% @private
%% @doc Return the list of processes waiting for the rendering of a key. Flush the queue and the key.
%% @see handle_call/3
%%

-spec handle_call_get_waiting_pids(Key, State) -> Result when
	Key :: key(),
	State :: state(),
	Result :: {reply, [{pid(), Tag}], state()},
	Tag :: atom().
handle_call_get_waiting_pids(Key, State) ->
    {State1, Pids} = case maps:take(Key, State#state.wait_pids) of
                         {{_MaxAge, List, WriterRef}, WaitPids} ->
                             erlang:demonitor(WriterRef, [flush]),
                             Writers = maps:remove(WriterRef, State#state.writers),
                             {State#state{wait_pids=WaitPids, writers=Writers}, List};
                         error ->
                             {State, []}
                     end,
    flush_key(Key, State1),
    {reply, Pids, State1}.


%% @private
%% @doc Fetch a key from the cache, returns `undefined' when not found.
%% @see handle_call/3
%%

-spec handle_call_get(Key, State) -> Result when
	Key :: key(),
	State :: state(),
	Result :: {reply, Reply, State},
	Reply :: undefined | {ok, term()}.
handle_call_get(Key, State) ->
    {reply, get_in_depcache(Key, State), State}.


%% @private
%% @doc Fetch a subkey from a key from the cache, returns `undefined' when not found.
%% This is useful when the cached data is very large and the fetched data is small in comparison.
%% @see handle_call/3
%%

-spec handle_call_get_sub_key(Request, State) -> Result when
	Request :: {Key, SubKey},
	Key :: key(),
	SubKey :: key(),
	State :: state(),
	Result :: {reply, Reply, State},
	Reply :: undefined | {ok, term()}.
handle_call_get_sub_key({Key, SubKey}, State) ->
    case get_in_depcache(Key, State) of
        undefined -> {reply, undefined, State};
        {ok, Value} -> {reply, {ok, find_value(SubKey, Value)}, State}
    end.


%% @private
%% @doc Add an entry to the cache table.
%% @see handle_call/3
%%

-spec handle_call_set({key(), term(), max_age_secs(), dependencies(),
    non_neg_integer(), non_neg_integer()}, state()) -> {reply, ok, state()}.
handle_call_set({Key, Data, MaxAge, Depend, Bytes, Words}, #state{tables=Tables}=State) ->
    erase_process_dict(),
    Serial = State#state.serial + 1,
    Old = case ets:lookup(Tables#tables.meta_table, Key) of
        [Meta] -> Meta;
        [] -> #meta{expire=0, serial=0, depend=[]}
    end,
    Memory = memory_words(Tables),
    %% Admission bounds growth even when writers outpace the eviction worker.
    %% Rejected writes still invalidate old results and satisfy memo waiters.
    Admit = MaxAge > 0 andalso
        Memory + admission_words(Words, Depend, Old) < State#state.memory_max,
    case Admit of
        true ->
            {Added, Removed} = dependency_changes(Depend, Old#meta.depend),
            add_dependencies(Added, Serial, Tables),
            update_binary_bytes(Bytes - Old#meta.binary_bytes, Tables),
            %% Keep the old entry visible until its replacement is published.
            ets:insert(Tables#tables.data_table, {Key, Serial, Data}),
            ets:insert(Tables#tables.meta_table, #meta{key=Key,
                expire=now_sec()+MaxAge, serial=Serial, depend=Depend,
                binary_bytes=Bytes, memory_words=Words}),
            release_dependencies(Removed, Tables);
        false -> remove_entry(Key, Tables)
    end,
    invalidate_dependency(Key, Serial, Tables),
    Pressure = Memory + Words >= State#state.memory_max * 9 div 10,
    case Pressure andalso not State#state.pressured of
        true -> State#state.cleanup_pid ! pressure;
        false -> ok
    end,
    State1 = State#state{serial=Serial, pressured=Pressure},

    %% Check if other processes are waiting for this key, send them the data
    case maps:take(Key, State1#state.wait_pids) of
        {{_MaxAge, List, WriterRef}, WaitPids} ->
            _ = [ catch gen_server:reply(From, {ok, Data}) || From <- List ],
            _ = erlang:demonitor(WriterRef, [flush]),
            Writers = maps:without([WriterRef], State#state.writers),
            {reply, ok, State1#state{writers=Writers, wait_pids=WaitPids}};
        error ->
            {reply, ok, State1}
    end.


%% @private
%% @doc Flush cached data by key.
%% @see handle_call/3
%%

-spec handle_call_flush(Key, State) -> Result when
	Key :: key(),
	State :: state(),
	Result :: {reply, ok, State}.
handle_call_flush(Key, State) ->
    flush_key(Key, State),
    {reply, ok, State}.


%% @private
%% @doc Flush all cached data.
%% @see handle_call/3
%%

-spec handle_call_flush_all(State) -> Result when
	State :: state(),
	Result :: {reply, ok, State}.
handle_call_flush_all(#state{tables = Tables} = State) ->
    ets:delete_all_objects(Tables#tables.data_table),
    ets:delete_all_objects(Tables#tables.meta_table),
    ets:delete_all_objects(Tables#tables.deps_table),
    ets:insert(Tables#tables.memory_table, {binary_bytes, 0}),
    erase_process_dict(),
    {reply, ok, State}.


%% @private
%% @doc Fetch a value, from within the depcache process.  Cache the value in the process dictionary of the depcache.

-spec get_in_depcache(Key, State) -> Result when
	Key :: key(),
	State :: state(),
	Result :: term().
get_in_depcache(Key, State) ->
    case erlang:get({depcache, Key}) of
        {memo, Value} ->
            Value;
        undefined ->
            % Prevent the process dict memo from blowing up the process size
            case erlang:get(depcache_count) > ?PROCESS_DICT_THRESHOLD of
                true -> erase_process_dict();
                false -> nop
            end,
            Value = get_in_depcache_ets(Key, State),
            erlang:put({depcache, Key}, {memo, Value}),
            erlang:put(depcache_count, incr(erlang:get(depcache_count))),
            Value
    end.


%% @private
%% @doc Increment input value.

-spec incr(Value) -> Result when
	Value :: undefined,
	Result :: 1;
(Value) -> Result when
	Value :: integer(),
	Result :: integer().
incr(undefined) -> 1;
incr(N) -> N+1.


%% @private
%% @doc Get decache value.

-spec get_in_depcache_ets(Key, State) -> Result when
	Key :: key(),
	State :: state(),
	Result :: undefined | {ok, Value},
	Value :: term().
get_in_depcache_ets(Key, #state{tables = Tables} = State) ->
    case get_concurrent(Key, State#state.now, Tables#tables.meta_table, Tables#tables.deps_table, Tables#tables.data_table) of
        {flush, Serial} ->
            flush_if_current(Key, Serial, State),
            undefined;
        undefined ->
            undefined;
        {ok, Value} ->
            {ok, Value}
    end.


%% @private
%% @doc Get a value from the depcache.  Called by the depcache and other processes.

-spec get_concurrent(Key, Now, MetaTable, DepsTable, DataTable) -> Result when
	Key :: key(),
	Now :: sec(),
	MetaTable :: ets:tab(),
	DepsTable :: ets:tab(),
	DataTable :: ets:tab(),
	Result :: retry | {flush, non_neg_integer()} | undefined | {ok, term()}.
get_concurrent(Key, Now, MetaTable, DepsTable, DataTable) ->
    case ets:lookup(MetaTable, Key) of
        [] ->
            undefined;
        [#meta{serial=Serial, expire=Expire, depend=Depend}] ->
            get_concurrent(Key, Now, DepsTable, DataTable, Serial, Expire, Depend)
    end.

%% @hidden

-spec get_concurrent(Key, Now, DepsTable, DataTable, Serial, Expire, Depend) -> Result when
	Key :: key(),
	Now :: sec(),
	DepsTable :: ets:tab(),
	DataTable :: ets:tab(),
	Serial :: non_neg_integer(),
	Expire :: sec(),
	Depend :: dependencies(),
	Result :: retry | {flush, non_neg_integer()} | undefined | {ok, term()}.
get_concurrent(Key, Now, DepsTable, DataTable, Serial, Expire, Depend) ->
	%% Check expiration
	case Expire >= Now of
		false ->
			{flush, Serial};
		true ->
			get_concurrent_check_depend(Key, DepsTable, DataTable, Serial, Depend)
	end.

%% @hidden

-spec get_concurrent_check_depend(Key, DepsTable, DataTable, Serial, Depend) -> Result when
	Key :: key(),
	DepsTable :: ets:tab(),
	DataTable :: ets:tab(),
	Serial :: non_neg_integer(),
	Depend :: dependencies(),
	Result :: retry | {flush, non_neg_integer()} | undefined | {ok, term()}.
get_concurrent_check_depend(Key, DepsTable, DataTable, Serial, Depend) ->
	%% Check dependencies
	case check_depend(Serial, Depend, DepsTable) of
		false ->
			{flush, Serial};
		true ->
			case ets:lookup(DataTable, Key) of
				[] -> undefined;
				[{_Key,Serial,Data}] -> {ok, Data};
                [_Replacement] -> retry
			end
	end.


%% @private
%% Delete only the generation observed by a concurrent reader or cleanup worker.
-spec flush_if_current(key(), non_neg_integer(), state()) -> ok.
flush_if_current(Key, Serial, #state{tables=Tables}=State) ->
    case ets:lookup(Tables#tables.meta_table, Key) of
        [#meta{serial=Serial}] -> flush_key(Key, State);
        _ -> ok
    end.

%% @private
-spec flush_key(key(), state()) -> ok.
flush_key(Key, #state{tables=Tables, serial=Serial}) ->
    remove_entry(Key, Tables),
    invalidate_dependency(Key, Serial+1, Tables),
    erase_process_dict().

%% Keep invalidation generations while dependents still refer to them. Removing
%% the last reference is safe: a future dependent gets a new generation.
-spec invalidate_dependency(key(), non_neg_integer(), tables()) -> ok.
invalidate_dependency(Key, Serial, Tables) ->
    ets:update_element(Tables#tables.deps_table, Key, {#depend.serial, Serial}),
    ok.

-spec remove_entry(key(), tables()) -> ok.
remove_entry(Key, Tables) ->
    case ets:lookup(Tables#tables.meta_table, Key) of
        [#meta{depend=Depend, binary_bytes=Bytes}] ->
            ets:delete(Tables#tables.meta_table, Key),
            ets:delete(Tables#tables.data_table, Key),
            update_binary_bytes(-Bytes, Tables),
            release_dependencies(Depend, Tables);
        [] -> ok
    end.

%% Only credit non-growing replacements with identical dependencies. Otherwise
%% reserve the full incoming footprint; shared old dependencies may stay alive.
-spec admission_words(non_neg_integer(), dependencies(), #meta{}) -> non_neg_integer().
admission_words(Words, Deps, #meta{depend=Deps, memory_words=OldWords}) when Words =< OldWords -> 0;
admission_words(Words, _Deps, _Old) -> Words.

-spec dependency_changes(dependencies(), dependencies()) -> {dependencies(), dependencies()}.
dependency_changes(Deps, Deps) -> {[], []};
dependency_changes(New, Old) -> {New -- Old, Old -- New}.

-spec add_dependencies(dependencies(), non_neg_integer(), tables()) -> ok.
add_dependencies(Deps, Serial, Tables) ->
    lists:foreach(fun(D) ->
        ets:update_counter(Tables#tables.deps_table, D, {#depend.refs, 1},
            #depend{key=D, serial=Serial})
    end, Deps).

-spec release_dependencies(dependencies(), tables()) -> ok.
release_dependencies(Deps, Tables) ->
    lists:foreach(fun(D) ->
        case ets:update_counter(Tables#tables.deps_table, D, {#depend.refs, -1}) of
            0 -> ets:delete(Tables#tables.deps_table, D);
            _ -> ok
        end
    end, Deps).

-spec update_binary_bytes(integer(), tables()) -> ok.
update_binary_bytes(0, _Tables) -> ok;
update_binary_bytes(Delta, Tables) ->
    ets:update_counter(Tables#tables.memory_table, binary_bytes, Delta),
    ok.

%% Conservative incremental footprint: copied ETS terms, binary backing storage,
%% and per-object/hash overhead, including potentially new dependency records.
-spec entry_size(key(), term(), dependencies()) -> {non_neg_integer(), non_neg_integer()}.
entry_size(Key, Data, Depend) ->
    Bytes = binary_bytes({Key, Data, Depend}),
    WordSize = erlang:system_info(wordsize),
    Words = erts_debug:flat_size({Key, 0, Data})
        + erts_debug:flat_size(#meta{key=Key, expire=0, serial=0, depend=Depend}) + 32
        + lists:sum([erts_debug:flat_size(#depend{key=D, serial=0}) + 16 || D <- Depend])
        + (Bytes + WordSize - 1) div WordSize,
    {Bytes, Words}.

%% ETS memory excludes off-heap binary payloads. Count retained backing storage;
%% shared binaries may be counted more than once, deliberately conservatively.
-spec binary_bytes(term()) -> non_neg_integer().
binary_bytes(B) when is_bitstring(B) ->
    case binary:referenced_byte_size(B) of
        N when N > 64 -> N;
        _ -> 0
    end;
binary_bytes([H|T]) -> binary_bytes(H) + binary_bytes(T);
binary_bytes(T) when is_tuple(T) -> binary_bytes(tuple_to_list(T));
binary_bytes(M) when is_map(M) ->
    maps:fold(fun(K, V, N) -> N + binary_bytes(K) + binary_bytes(V) end, 0, M);
binary_bytes(F) when is_function(F) ->
    {env, Env} = erlang:fun_info(F, env),
    binary_bytes(Env);
binary_bytes(_) -> 0.


%% @private
%% @doc Check if all dependencies are still valid, that is they have a serial before or equal to the serial of the entry.

-spec check_depend(Serial, Depend, DepsTable) -> Result when
	Serial :: non_neg_integer(),
	Depend :: [depend()],
	DepsTable :: ets:tab(),
	Result :: true | false.
check_depend(_Serial, [], _DepsTable) ->
    true;
check_depend(Serial, Depend, DepsTable) ->
    CheckDepend = fun
                        (_Dep,false) -> false;
                        (Dep,true) ->
                            case ets:lookup(DepsTable, Dep) of
                                [#depend{serial=DepSerial}] -> DepSerial =< Serial;
                                _ -> false
                            end
                    end,
    lists:foldl(CheckDepend, true, Depend).


%% Don't warn about types in find_value, especially the array lookup
%% triggers warnings from dialyzer about the tuple not being an
%% opaque array() type.
-dialyzer({nowarn_function, find_value/2}).

%% @private
%% @doc Search by value in some set of data.

-spec find_value(Key, Data) -> Result when
	Key :: key() | integer(),
	Data :: map() | List | Rsc_list | tuple() | any(),
	List :: list() | proplist(),
	Rsc_list :: {rsc_list, List},
	Result :: undefined | any().
find_value(Key, M) when is_map(M) ->
    maps:get(Key, M, undefined);
find_value(Key, L) when is_integer(Key) andalso is_list(L) ->
    %% Index of list with an integer like "a[2]"
    try
        lists:nth(Key, L)
    catch
        _:_ -> undefined
    end;
find_value(Key, L) when is_list(L) ->
    %% Regular proplist lookup
    proplists:get_value(Key, L);
find_value(Key, {rsc_list, L}) when is_integer(Key) ->
    %% Resource list handling, special lookups when skipping the index
    try
        lists:nth(Key, L)
    catch
        _:_ -> undefined
    end;
find_value(Key, {rsc_list, [H|_T]}) ->
    find_value(Key, H);
find_value(_Key, {rsc_list, []}) ->
    undefined;
find_value(Key, T) when is_integer(Key) ->
    case array:is_array(T) of
        true ->
            %% Index of array with an integer like "a[2]"
            array:get(Key, T);
        false when is_tuple(T) ->
            %% Index of tuple with an integer like "a[2]"
            try
                element(Key, T)
            catch
                _:_ -> undefined
            end;
        false ->
            undefined
    end;
find_value(_Key, _Data) ->
    %% Any subvalue of a non-existant value is empty
    undefined.


%% @doc Cleanup process for the depcache.  Periodically checks a batch of depcache items for their validity.
%%      Asks the depcache server to delete invalidated items.  When the load of the data table is too high then
%%      This cleanup process starts to delete random entries.  By using a random delete we don't need to keep
%%      a LRU list, which is a bit expensive.
%% @equiv cleanup(CleanUp_state, SlotNr, Now, Mode, Ct)

-spec cleanup(CleanUp_state) -> Result when
	CleanUp_state :: cleanup_state(),
	Result :: no_return().
cleanup(#cleanup_state{pid=Pid}=State) ->
    erlang:monitor(process, Pid),
    try ?MODULE:cleanup(State, 0, now_sec(), normal, 0)
    catch
        ?WITH_STACKTRACE(Class, Reason, Stack)
            case is_process_alive(Pid) of
                true -> erlang:raise(Class, Reason, Stack);
                false -> exit(normal)
            end
    end.

%% @doc Run bounded cleanup batches. Normal sweeps scale with the table size;
%% pressure wakes the worker immediately and removes the delay between batches.
-spec cleanup(cleanup_state(), '$end_of_table' | non_neg_integer(), sec(),
    normal | cache_full, integer()) -> no_return().
cleanup(#cleanup_state{pid=Pid, tables=Tables}=State, Slot, _Now, Mode, SweepLimit) ->
    Delay = case Mode of normal -> 100; cache_full -> 0 end,
    receive
        pressure -> ok;
        {'DOWN', _, process, Pid, _} -> exit(normal)
    after Delay -> ok
    end,
    Mode1 = cleanup_mode(State),
    %% Keep the sweep budget after removals: a sparse hash table can retain
    %% many slots even when few entries remain.
    NormalLimit = max(SweepLimit,
        max(?CLEANUP_BATCH, min(10000, ets:info(Tables#tables.meta_table, size) div 10))),
    Limit = case Mode1 of normal -> NormalLimit; cache_full -> ?CLEANUP_BATCH end,
    Deadline = erlang:monotonic_time(microsecond) + 2000,
    {Next, Expired, Evicted} = cleanup_scan(State, Slot, now_sec(), Mode1, Limit, Deadline, [], []),
    lists:foreach(fun(_) -> callback(eviction, State#cleanup_state.name,
        State#cleanup_state.callback) end, Evicted),
    flush_batches(Pid, Expired ++ Evicted, [], 0),
    ?MODULE:cleanup(State, Next, now_sec(), cleanup_mode(State), NormalLimit).

%% Bound writer work too, even if a normal sweep finds thousands of expired keys.
flush_batches(Pid, Rest, Batch, ?CLEANUP_BATCH) ->
    gen_server:call(Pid, {flush_batch, Batch}, infinity),
    flush_batches(Pid, Rest, [], 0);
flush_batches(_Pid, [], [], 0) -> ok;
flush_batches(Pid, [], Batch, _Count) ->
    gen_server:call(Pid, {flush_batch, Batch}, infinity);
flush_batches(Pid, [Entry|Rest], Batch, Count) ->
    flush_batches(Pid, Rest, [Entry|Batch], Count+1).

cleanup_scan(_State, '$end_of_table', _Now, _Mode, _Left, _Deadline, Expired, Evicted) ->
    {0, Expired, Evicted};
cleanup_scan(_State, Slot, _Now, _Mode, 0, _Deadline, Expired, Evicted) ->
    {Slot, Expired, Evicted};
cleanup_scan(#cleanup_state{tables=Tables}=State, Slot, Now, Mode, Left, Deadline, Expired, Evicted) ->
    case Left rem 32 =:= 0 andalso erlang:monotonic_time(microsecond) >= Deadline of
        true -> {Slot, Expired, Evicted};
        false ->
            Entries = try ets:slot(Tables#tables.meta_table, Slot)
                catch error:badarg -> '$end_of_table' end,
            case Entries of
                '$end_of_table' -> {0, Expired, Evicted};
                _ ->
                    {Expired1, Evicted1} = lists:foldl(fun
                        (#meta{key=Key, serial=Serial, expire=Expire, depend=Deps}, {Es, Vs}) ->
                            case Expire < Now orelse not check_depend(Serial, Deps, Tables#tables.deps_table) of
                                true -> {[{Key, Serial}|Es], Vs};
                                false when Mode =:= cache_full ->
                                    case rand_uniform(10) of
                                        1 -> {Es, [{Key, Serial}|Vs]};
                                        _ -> {Es, Vs}
                                    end;
                                false -> {Es, Vs}
                            end
                    end, {Expired, Evicted}, Entries),
                    cleanup_scan(State, Slot+1, Now, Mode, Left-1, Deadline, Expired1, Evicted1)
            end
    end.

-ifdef(rand_only).
rand_uniform(N) ->
    rand:uniform(N).
-else.
rand_uniform(N) ->
    crypto:rand_uniform(1,N+1).
-endif.


%% @private
%% Include all ETS tables and retained binary payloads. An empty cache cannot
%% reclaim its table overhead, so it must sleep even with a zero memory budget.
-spec cleanup_mode(cleanup_state()) -> cache_full | normal.
cleanup_mode(#cleanup_state{tables=Tables, memory_max=MemoryMax}) ->
    case ets:info(Tables#tables.meta_table, size) > 0
         andalso memory_words(Tables) >= MemoryMax * 9 div 10 of
        true -> cache_full;
        false -> normal
    end.

-spec memory_words(tables()) -> non_neg_integer().
memory_words(Tables) ->
    [{binary_bytes, Bytes}] = ets:lookup(Tables#tables.memory_table, binary_bytes),
    WordSize = erlang:system_info(wordsize),
    (Bytes + WordSize - 1) div WordSize
        + lists:sum([ets:info(T, memory) || T <- [Tables#tables.meta_table,
            Tables#tables.deps_table, Tables#tables.data_table, Tables#tables.memory_table]]).


%% @private
%% @doc Returns the current tick count.

-spec now_sec() -> Result when
	Result :: sec().
now_sec() ->
    {M,S,_M} = os:timestamp(),
    M*1000000 + S.


%% @private
%% @doc Safe erase of process dict, keeps some 'magical' `proc_lib' vars.

-spec erase_process_dict() -> Result when
	Result :: ok.
erase_process_dict() ->
    Values = [ {K, erlang:get(K)} || K <- ['$initial_call', '$ancestors', '$erl_eval_max_line'] ],
    erlang:erase(),
    [ erlang:put(K,V) || {K,V} <- Values, V =/= undefined ],
    ok.


%% @private
%% @doc Returns the result of applying `Function' in `Module' to `Args'.

-dialyzer({no_match, callback/3}).

-spec callback(Type, Name, MFA) -> Result when
	Type :: eviction | atom(),
	Name ::  atom(),
	MFA :: undefined,
	Result :: nop | ok | term();
(Type, Name, MFA) -> Result when
	Type :: eviction | atom(),
	Name ::  atom(),
	MFA :: mfargs(),
	Result :: nop | ok | term().
callback(_Type, _Name, undefined) ->
    ok;
callback(Type, Name, {M, F, A}) when is_list(A) ->
    try
        erlang:apply(M, F, [{Type, Name} | A])
    catch
        _:_  ->
            %% Don't log errors because of high calling frequency.
            nop
    end;
callback(Type, Name, {M, F, A}) ->
    %% LEGACY: treating non-lists A as [A] is ambiguous
    callback(Type, Name, {M, F, [A]}).
