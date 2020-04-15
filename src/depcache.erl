%% @author Marc Worrell <marc@worrell.nl>
%% @copyright 2009-2016 Marc Worrell, Arjan Scherpenisse
%%
%% @doc In-memory caching server with dependency checks and local in process memoization of lookups.

%% Copyright 2009-2016 Marc Worrell, Arjan Scherpenisse
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

-type memo_fun() :: function() | mfa() | {module(), atom()}.
-type depcache_server() :: pid() | atom().
-type max_age_secs() :: non_neg_integer().
-type key() :: any().
-type dependencies() :: list( key() ).

-export_type([
    memo_fun/0,
    depcache_server/0,
    key/0,
    max_age_secs/0
]).

-record(tables, {
    meta_table :: ets:tab(),
    deps_table :: ets:tab(),
    data_table :: ets:tab()
}).

-record(state, {now, serial, tables, wait_pids}).
-record(meta,  {key, expire, serial, depend}).
-record(depend,{key, serial}).

-record(cleanup_state, {
    pid :: pid(),
    tables :: #tables{},
    name :: atom(),
    memory_max :: non_neg_integer(),
    callback :: mfa()
}).

%% @doc Start a depcache process.
%%
%% For Config, you can pass:
%% {callback, {Module, Function, Arguments}}: depcache event callback
%% {memory_max, MaxMemoryInMB}: number of MB to limit depcache size at.
-spec start_link(Config :: proplists:proplist()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

%% @doc Start a named depcache process.
%%
%% For Config, you can pass:
%% {callback, {Module, Function, Arguments}}: depcache event callback
%% {memory_max, MaxMemoryInMB}: number of MB to limit depcache size at.
-spec start_link(atom(), Config :: proplists:proplist()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name, Config) ->
    gen_server:start_link({local, Name}, ?MODULE, [{name,Name}|Config], []).

-define(META_TABLE_PREFIX, $m).
-define(DEPS_TABLE_PREFIX, $p).
-define(DATA_TABLE_PREFIX, $d).

-define(NAMED_TABLE(Name,Prefix), list_to_atom([Prefix,$:|atom_to_list(Name)])).

%% One hour, in seconds
-define(HOUR,     3600).

%% Default max size, in Mbs, of the stored data in the depcache before the gc kicks in.
-define(MEMORY_MAX, 100).

% Maximum time to wait for a get_wait/2 call before a timout failure (in secs).
-define(MAX_GET_WAIT, 30).

% Number of slots visited for each gc iteration
-define(CLEANUP_BATCH, 100).

% Number of entries we keep in the local process dictionary for fast lookups
-define(PROCESS_DICT_THRESHOLD, 10000).


-spec memo( memo_fun(), depcache_server() ) -> any().
memo(Fun, Server) ->
    memo(Fun, undefined, ?HOUR, [], Server).

-spec memo( memo_fun(), max_age_secs() | key(), depcache_server() ) -> any().
memo(Fun, MaxAge, Server) when is_tuple(Fun) ->
    memo(Fun, undefined, MaxAge, [], Server);

memo(Fun, Key, Server) when is_function(Fun) ->
    memo(Fun, Key, ?HOUR, [], Server).

-spec memo( memo_fun(), max_age_secs(), key(), depcache_server() ) -> any().
memo(Fun, Key, MaxAge, Server) ->
    memo(Fun, Key, MaxAge, [], Server).

-spec memo( memo_fun(), max_age_secs(), key(), dependencies(), depcache_server() ) -> any().
memo(Fun, Key, MaxAge, Dep, Server) ->
    Key1 = case Key of
        undefined -> memo_key(Fun);
        _ -> Key
    end,
    case ?MODULE:get_wait(Key1, Server) of
        {ok, Value} ->
            Value;
        {throw, R} ->
            throw(R);
        undefined ->
            try
                Value =
                    case Fun of
                        {M,F,A} -> erlang:apply(M,F,A);
                        {M,F} -> M:F();
                        _ when is_function(Fun) -> Fun()
                    end,
                {Value1, MaxAge1, Dep1} =
                    case Value of
                        #memo{value=V, max_age=MA, deps=D} ->
                            MA1 = case is_integer(MA) of true -> MA; false -> MaxAge end,
                            {V, MA1, Dep++D};
                        _ ->
                            {Value, MaxAge, Dep}
                    end,
                case MaxAge of
                    0 -> memo_send_replies(Key, Value1, Server);
                    _ -> set(Key, Value1, MaxAge1, Dep1, Server)
                end,
                Value1
            catch
                ?WITH_STACKTRACE(Class, R, S)
                    error_logger:error_msg("Error in memo ~p:~p in ~p", [Class, R, S]),
                    memo_send_errors(Key, {throw, R}, Server),
                    throw(R)
            end
    end.

%% @doc Calculate the key used for memo functions.
memo_key({M,F,A}) -> 
    WithoutContext = lists:filter(fun(T) when is_tuple(T) andalso element(1, T) =:= context -> false; (_) -> true end, A),
    {M,F,WithoutContext};
memo_key({M,F}) -> 
    {M,F}.

%% @doc Send the calculated value to the processes waiting for the result.
memo_send_replies(Key, Value, Server) ->
    Pids = get_waiting_pids(Key, Server),
    [ catch gen_server:reply(Pid, {ok, Value}) || Pid <- Pids ],
    ok.

%% @doc Send an error to the processes waiting for the result.
memo_send_errors(Key, Exception, Server) ->
    Pids = get_waiting_pids(Key, Server),
    [ catch gen_server:reply(Pid, Exception) || Pid <- Pids ].


%% @doc Add the key to the depcache, hold it for 3600 seconds and no dependencies
-spec set( key(), any(), depcache_server() ) -> ok.
set(Key, Data, Server) ->
    set(Key, Data, 3600, [], Server).

%% @doc Add the key to the depcache, hold it for MaxAge seconds and no dependencies
-spec set( key(), any(), max_age_secs(), depcache_server() ) -> ok.
set(Key, Data, MaxAge, Server) ->
    set(Key, Data, MaxAge, [], Server).

%% @doc Add the key to the depcache, hold it for MaxAge seconds and check the dependencies
-spec set( key(), any(), max_age_secs(), dependencies(), depcache_server() ) -> ok.
set(Key, Data, MaxAge, Depend, Server) ->
    flush_process_dict(),
    gen_server:call(Server, {set, Key, Data, MaxAge, Depend}).


%% @doc Fetch the key from the cache, when the key does not exist then lock the entry and let 
%% the calling process insert the value. All other processes requesting the key will wait till
%% the key is updated and receive the key's new value.
-spec get_wait( key(), depcache_server() ) -> {ok, any()} | undefined.
get_wait(Key, Server) ->
    case get_process_dict(Key, Server) of
        NoValue when NoValue =:= undefined orelse NoValue =:= depcache_disabled ->
            gen_server:call(Server, {get_wait, Key}, ?MAX_GET_WAIT*1000);
        Other ->
            Other
    end.


%% @doc Fetch the queue of pids that are waiting for a get_wait/1. This flushes the queue and
%% the key from the depcache.
-spec get_waiting_pids( key(), depcache_server() ) -> {ok, list( pid() )} | undefined.
get_waiting_pids(Key, Server) ->
    gen_server:call(Server, {get_waiting_pids, Key}, ?MAX_GET_WAIT*1000).



%% @doc Fetch the key from the cache, return the data or an undefined if not found (or not valid)
-spec get( key(), depcache_server() ) -> {ok, any()} | undefined.
get(Key, Server) ->
    case get_process_dict(Key, Server) of
        depcache_disabled -> gen_server:call(Server, {get, Key});
        Value -> Value
    end.


%% @doc Fetch the key from the cache, return the data or an undefined if not found (or not valid)
-spec get_subkey( key(), key(), depcache_server() ) -> {ok, any()} | undefined.
get_subkey(Key, SubKey, Server) ->
    case in_process_server(Server) of
        true ->
            case erlang:get({depcache, {subkey, Key, SubKey}}) of
                {memo, Value} ->
                    Value;
                undefined ->
                    Value = gen_server:call(Server, {get, Key, SubKey}),
                    erlang:put({depcache, {subkey, Key, SubKey}}, {memo, Value}),
                    Value
            end;
        false ->
            gen_server:call(Server, {get, Key, SubKey})
    end.


%% @doc Fetch the key from the cache, return the data or an undefined if not found (or not valid)
-spec get( key(), key(), depcache_server() ) -> {ok, any()} | undefined.
get(Key, SubKey, Server) ->
    case get_process_dict(Key, Server) of
        undefined -> 
            undefined;
        depcache_disabled ->
            gen_server:call(Server, {get, Key, SubKey});
        {ok, Value} ->
            {ok, find_value(SubKey, Value)}
    end.


%% @doc Flush the key and all keys depending on the key
-spec flush( key(), depcache_server() ) -> ok.
flush(Key, Server) ->
    gen_server:call(Server, {flush, Key}),
    flush_process_dict().


%% @doc Flush all keys from the caches
-spec flush( depcache_server() ) -> ok.
flush(Server) ->
    gen_server:call(Server, flush),
    flush_process_dict().


%% @doc Return the total memory size of all stored terms
-spec size( depcache_server() ) -> non_neg_integer().
size(Server) ->
    {_Meta, _Deps, Data} = get_tables(Server),
    ets:info(Data, memory).


%% @doc Fetch the depcache tables.
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

%% @doc Fetch a value from the dependency cache, using the in-process cached tables.
get_process_dict(Key, Server) ->
    case in_process_server(Server) of
        true ->
            case erlang:get({depcache, Key}) of
                {memo, Value} ->
                    Value;
                undefined ->
                    % Prevent the process dict memo from blowing up the process size
                    case now_sec() > erlang:get(depcache_now)
                          orelse erlang:get(depcache_count) > ?PROCESS_DICT_THRESHOLD of
                        true -> flush_process_dict();
                        false -> nop
                    end,

                    Value = get_ets(Key, Server),
                    erlang:put({depcache, Key}, {memo, Value}),
                    erlang:put(depcache_count, incr(erlang:get(depcache_count))),
                    Value
            end;
        false when is_atom(Server) ->
            get_ets(Key, Server);
        false ->
            depcache_disabled
    end.


get_ets(Key, Server) ->
    {MetaTable, DepsTable, DataTable} = get_tables(Server),
    case get_concurrent(Key, get_now(), MetaTable, DepsTable, DataTable) of
        flush ->
            flush(Key, Server),
            undefined;
        undefined ->
            undefined;
        {ok, _Value} = Found ->
            Found
    end.


%% @doc Check if we use a local process dict cache.
-spec in_process_server( depcache_server() ) -> boolean().
in_process_server(Server) ->
    case erlang:get(depcache_in_process) =:= true of
        true ->
            _ = get_tables(Server),
            true;
        false ->
            false
    end.


%% @doc Enable or disable the in-process caching using the process dictionary
-spec in_process( boolean() ) -> ok.
in_process(true) ->
    erlang:put(depcache_in_process, true);
in_process(false) ->
    erlang:erase(depache_in_process),
    flush_process_dict();
in_process(undefined) ->
    in_process(false).

%% @doc Flush all items memoized in the process dictionary.
flush_process_dict() ->
    [ erlang:erase({depcache, Key}) || {{depcache, Key},_Value} <- erlang:get() ],
    erlang:erase(depache_now),
    erlang:put(depcache_count, 0),
    ok.


%% @doc Get the current system time in seconds
get_now() ->
    case erlang:get(depcache_now) of
        undefined ->
            Now = now_sec(),
            erlang:put(depache_now, Now),
            Now;
        Now ->
            Now
    end.




%% gen_server callbacks

%% @spec init(Config) -> {ok, State}
%% @doc Initialize the depcache.  Creates ets tables for the deps, meta and data.  Spawns garbage collector.
init(Config) ->
    MemoryMaxMbs = case proplists:get_value(memory_max, Config) of undefined -> ?MEMORY_MAX; Mbs -> Mbs end,
    MemoryMaxWords = 1024 * 1024 * MemoryMaxMbs div erlang:system_info(wordsize),

    Tables = case proplists:lookup(name, Config) of
        none ->
            #tables{
                meta_table=ets:new(meta_table, [set, {keypos, 2}, protected, {read_concurrency, true}]),
                deps_table=ets:new(deps_table, [set, {keypos, 2}, protected, {read_concurrency, true}]),
                data_table=ets:new(data_table, [set, {keypos, 1}, protected, {read_concurrency, true}])
            };
        {name, Name} when is_atom(Name) ->
            #tables{
                meta_table=ets:new(?NAMED_TABLE(Name, ?META_TABLE_PREFIX),
                                   [set, named_table, {keypos, 2}, protected, {read_concurrency, true}]),
                deps_table=ets:new(?NAMED_TABLE(Name, ?DEPS_TABLE_PREFIX),
                                   [set, named_table, {keypos, 2}, protected, {read_concurrency, true}]),
                data_table=ets:new(?NAMED_TABLE(Name, ?DATA_TABLE_PREFIX),
                                   [set, named_table, {keypos, 1}, protected, {read_concurrency, true}])
            }
    end,
    State = #state{
        tables = Tables,
        now=now_sec(),
        serial=0,
        wait_pids=dict:new()
    },
    timer:send_interval(1000, tick),
    spawn_link(?MODULE,
               cleanup,
               [#cleanup_state{
                   pid = self(),
                   tables = Tables,
                   name = proplists:get_value(name, Config),
                   memory_max = MemoryMaxWords,
                   callback = proplists:get_value(callback, Config)
               }]),
    {ok, State}.


%%--------------------------------------------------------------------
%% Function: handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------

%% @doc Return the ets tables used by the cache
handle_call(get_tables, _From, #state{tables = Tables} = State) ->
    {reply, {ok, {Tables#tables.meta_table, Tables#tables.deps_table, Tables#tables.data_table}}, State};

%% @doc Fetch a key from the cache. When the key is not available then let processes wait till the
%% key is available.
handle_call({get_wait, Key}, From, #state{tables = Tables} = State) ->
    case get_concurrent(Key, State#state.now, Tables#tables.meta_table, Tables#tables.deps_table, Tables#tables.data_table) of
        NotFound when NotFound =:= flush; NotFound =:= undefined ->
            case NotFound of
                flush -> flush_key(Key, State);
                undefined -> State
            end,
            case dict:find(Key, State#state.wait_pids) of
                {ok, {MaxAge,List}} when State#state.now < MaxAge ->
                    %% Another process is already calculating the value, let the caller wait.
                    WaitPids = dict:store(Key, {MaxAge, [From|List]}, State#state.wait_pids),
                    {noreply, State#state{wait_pids=WaitPids}};
                _ ->
                    %% Nobody waiting or we hit a timeout, let next requestors wait for this caller.
                    WaitPids = dict:store(Key, {State#state.now+?MAX_GET_WAIT, []}, State#state.wait_pids),
                    {reply, undefined, State#state{wait_pids=WaitPids}}
            end;
        {ok, _Value} = Found -> 
            {reply, Found, State}
    end;

%% @doc Return the list of processes waiting for the rendering of a key. Flush the queue and the key.
handle_call({get_waiting_pids, Key}, _From, State) ->
    {State1, Pids} = case dict:find(Key, State#state.wait_pids) of
                        {ok, {_MaxAge, List}} -> 
                            WaitPids = dict:erase(Key, State#state.wait_pids),
                            {State#state{wait_pids=WaitPids}, List};
                        error ->
                            {State, []}
                     end,
    flush_key(Key, State1),
    {reply, Pids, State1};


%% @doc Fetch a key from the cache, returns undefined when not found.
handle_call({get, Key}, _From, State) ->
    {reply, get_in_depcache(Key, State), State};

%% @doc Fetch a subkey from a key from the cache, returns undefined when not found.
%% This is useful when the cached data is very large and the fetched data is small in comparison.
handle_call({get, Key, SubKey}, _From, State) ->
    case get_in_depcache(Key, State) of
        undefined -> {reply, undefined, State};
        {ok, Value} -> {reply, {ok, find_value(SubKey, Value)}, State}
    end;

%% Add an entry to the cache table
handle_call({set, Key, Data, MaxAge, Depend}, _From, #state{tables = Tables} = State) ->
    erase_process_dict(),
    State1 = State#state{serial=State#state.serial+1},
    case MaxAge of
        0 ->
            ets:delete(Tables#tables.meta_table, Key),
            ets:delete(Tables#tables.data_table, Key);
        _ ->
            ets:insert(Tables#tables.data_table, {Key, Data}),
            ets:insert(Tables#tables.meta_table, #meta{key=Key, expire=State1#state.now+MaxAge, serial=State1#state.serial, depend=Depend})
    end,
    
    %% Make sure all dependency keys are available in the deps table
    AddDepend = fun(D) ->
                    ets:insert_new(Tables#tables.deps_table, #depend{key=D, serial=State1#state.serial})
                end,
    lists:foreach(AddDepend, Depend),
    
    %% Flush the key itself in the dependency table - this will invalidate depending items
    case is_simple_key(Key) of
        true  -> ok;
        false -> ets:insert(Tables#tables.deps_table, #depend{key=Key, serial=State#state.serial})
    end,

    %% Check if other processes are waiting for this key, send them the data
    case dict:find(Key, State1#state.wait_pids) of
        {ok, {_MaxAge, List}} ->
            [ catch gen_server:reply(Pid, {ok, Data}) || Pid <- List ],
            WaitPids = dict:erase(Key, State1#state.wait_pids),
            {reply, ok, State1#state{wait_pids=WaitPids}};
        error ->
            {reply, ok, State1}
    end;

handle_call({flush, Key}, _From, State) ->
    flush_key(Key, State),
    {reply, ok, State};

handle_call(flush, _From, #state{tables = Tables} = State) ->
    ets:delete_all_objects(Tables#tables.data_table),
    ets:delete_all_objects(Tables#tables.meta_table),
    ets:delete_all_objects(Tables#tables.deps_table),
    erase_process_dict(),
    {reply, ok, State}.



%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------

handle_cast({flush, Key}, State) ->
    flush_key(Key, State),
    {noreply, State};

handle_cast(flush, #state{tables = Tables} = State) ->
    ets:delete_all_objects(Tables#tables.data_table),
    ets:delete_all_objects(Tables#tables.meta_table),
    ets:delete_all_objects(Tables#tables.deps_table),
    erase_process_dict(),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.
    

handle_info(tick, State) ->
    erase_process_dict(),
    flush_message(tick),
    {noreply, State#state{now=now_sec()}};

handle_info(_Msg, State) -> 
    {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVersion, State, _Extra) -> {ok, State}.


%% @doc Fetch a value, from within the depcache process.  Cache the value in the process dictionary of the depcache.
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
    
incr(undefined) -> 1;
incr(N) -> N+1.

get_in_depcache_ets(Key, #state{tables = Tables} = State) ->
    case get_concurrent(Key, State#state.now, Tables#tables.meta_table, Tables#tables.deps_table, Tables#tables.data_table) of
        flush ->
            flush_key(Key, State),
            undefined;
        undefined ->
            undefined;
        {ok, Value} ->
            {ok, Value}
    end.


%% @doc Get a value from the depache.  Called by the depcache and other processes.
%% @spec get_concurrent(term(), now:int(), tid(), tid(), tid()) -> {ok, term()} | undefined | flush
get_concurrent(Key, Now, MetaTable, DepsTable, DataTable) ->
    case ets:lookup(MetaTable, Key) of
        [] -> 
            undefined;
        [#meta{serial=Serial, expire=Expire, depend=Depend}] ->
            %% Check expiration
            case Expire >= Now of
                false -> 
                    flush;
                true ->
                    %% Check dependencies
                    case check_depend(Serial, Depend, DepsTable) of
                        true ->
                            case ets:lookup(DataTable, Key) of
                                [] -> undefined;
                                [{_Key,Data}] -> {ok, Data}
                            end;
                        false ->
                            flush
                    end
            end
    end.



%% @doc Check if a key is usable as dependency key.  That is a string, atom, integer etc, but not a list of lists.
is_simple_key([]) ->
    true;
is_simple_key([H|_]) ->
    not is_list(H);
is_simple_key(_Key) ->
    true.


%% @doc Flush a key from the cache, reset the in-process cache as well (we don't know if any cached value had a dependency)
flush_key(Key, #state{tables = Tables}) ->
    ets:delete(Tables#tables.data_table, Key),
    ets:delete(Tables#tables.deps_table, Key),
    ets:delete(Tables#tables.meta_table, Key),
    erase_process_dict().


%% @doc Check if all dependencies are still valid, that is they have a serial before or equal to the serial of the entry
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



%% Map lookup
find_value(Key, M) when is_map(M) ->
    maps:get(Key, M, undefined);

%% Index of list with an integer like "a[2]"
find_value(Key, L) when is_integer(Key) andalso is_list(L) ->
    try
        lists:nth(Key, L)
    catch
        _:_ -> undefined
    end;

find_value(Key, {GBSize, GBData}) when is_integer(GBSize) ->
    case gb_trees:lookup(Key, {GBSize, GBData}) of
        {value, Val} ->
            Val;
        _ ->
            undefined
    end;

%% Regular proplist lookup
find_value(Key, L) when is_list(L) ->
    proplists:get_value(Key, L);

%% Resource list handling, special lookups when skipping the index
find_value(Key, {rsc_list, L}) when is_integer(Key) ->
    try
        lists:nth(Key, L)
    catch
        _:_ -> undefined
    end;
find_value(Key, {rsc_list, [H|_T]}) ->
    find_value(Key, H);
find_value(_Key, {rsc_list, []}) ->
    undefined;

% Index of tuple with an integer like "a[2]"
find_value(Key, T) when is_integer(Key) andalso is_tuple(T) ->
    try
        element(Key, T)
    catch 
        _:_ -> undefined
    end;

%% Other cases: context, dict or parametrized module lookup.
find_value(Key, Tuple) when is_tuple(Tuple) ->
    Module = element(1, Tuple),
    case Module of
        dict -> 
            case dict:find(Key, Tuple) of
                {ok, Val} ->
                    Val;
                _ ->
                    undefined
            end;
        Module ->
            Exports = Module:module_info(exports),
            case proplists:get_value(Key, Exports) of
                1 ->
                    Tuple:Key();
                _ ->
                    case proplists:get_value(get, Exports) of
                        1 -> 
                            Tuple:get(Key);
                        _ ->
                            undefined
                    end
            end
    end;

%% Any subvalue of a non-existant value is empty
find_value(_Key, _Data) ->
    undefined.


%% @doc Cleanup process for the depcache.  Periodically checks a batch of depcache items for their validity.
%%      Asks the depcache server to delete invalidated items.  When the load of the data table is too high then
%%      This cleanup process starts to delete random entries.  By using a random delete we don't need to keep
%%      a LRU list, which is a bit expensive.

cleanup(#cleanup_state{} = State) ->
    ?MODULE:cleanup(State, 0, now_sec(), normal, 0).

%% Wrap around the end of table
cleanup(#cleanup_state{tables = #tables{meta_table = MetaTable}} = State, '$end_of_table', Now, _Mode, Ct) ->
    case ets:info(MetaTable, size) of
        0 -> ?MODULE:cleanup(State, 0, Now, cleanup_mode(State), 0);
        _ -> ?MODULE:cleanup(State, 0, Now, cleanup_mode(State), Ct)
    end;

%% In normal cleanup, sleep a second between each batch before continuing our cleanup sweep
cleanup(#cleanup_state{tables = #tables{meta_table = MetaTable}} = State, SlotNr, Now, normal, 0) ->
    timer:sleep(1000),
    case ets:info(MetaTable, size) of
        0 -> ?MODULE:cleanup(State, SlotNr, Now, normal, 0);
        _ -> ?MODULE:cleanup(State, SlotNr, now_sec(), cleanup_mode(State), ?CLEANUP_BATCH)
    end;

%% After finishing a batch in cache_full mode, check if the cache is still full, if so keep deleting entries
cleanup(#cleanup_state{} = State, SlotNr, Now, cache_full, 0) ->
    case cleanup_mode(State) of
        normal     -> ?MODULE:cleanup(State, SlotNr, Now, normal, 0);
        cache_full -> ?MODULE:cleanup(State, SlotNr, now_sec(), cache_full, ?CLEANUP_BATCH)
    end;

%% Normal cleanup behaviour - check expire stamp and dependencies
cleanup(#cleanup_state{tables = #tables{meta_table = MetaTable}} = State, SlotNr, Now, normal, Ct) ->
    Slot =  try
                ets:slot(MetaTable, SlotNr)
            catch
                _M:_E -> '$end_of_table'
            end,
    case Slot of
        '$end_of_table' -> ?MODULE:cleanup(State, '$end_of_table', Now, normal, 0);
        [] -> ?MODULE:cleanup(State, SlotNr + 1, Now, normal, Ct - 1);
        Entries ->
            lists:foreach(fun(Meta) -> flush_expired(Meta, Now, State) end, Entries),
            ?MODULE:cleanup(State, SlotNr + 1, Now, normal, Ct - 1)
    end;

%% Full cache cleanup mode - randomly delete every 10th entry
cleanup(#cleanup_state{pid = Pid, name = Name, callback = Callback, tables = #tables{meta_table = MetaTable}} = State, SlotNr, Now, cache_full, Ct) ->
    Slot =  try
                ets:slot(MetaTable, SlotNr)
            catch
                _M:_E -> '$end_of_table'
            end,
    case Slot of
        '$end_of_table' -> ?MODULE:cleanup(State, '$end_of_table', Now, cache_full, 0);
        [] -> ?MODULE:cleanup(State, SlotNr + 1, Now, cache_full, Ct - 1);
        Entries ->
            FlushExpire = fun (Meta) ->
                                case flush_expired(Meta, Now, State) of
                                    ok -> {ok, Meta};
                                    flushed -> flushed
                                end
                           end,
            RandomDelete = fun
                                ({ok, #meta{key=Key}}) ->
                                    case rand_uniform(10) of
                                        1 ->
                                            callback(eviction, Name, Callback),
                                            gen_server:cast(Pid, {flush, Key});
                                        _  -> ok
                                    end;
                                (flushed) -> flushed
                           end,

            Entries1 = lists:map(FlushExpire, Entries),
            lists:foreach(RandomDelete, Entries1),
            ?MODULE:cleanup(State, SlotNr + 1, Now, cache_full, Ct - 1)
    end.

-ifdef(rand_only).
rand_uniform(N) ->
    rand:uniform(N).
-else.
rand_uniform(N) ->
    crypto:rand_uniform(1,N+1).
-endif.

%% @doc Check if an entry is expired, if so delete it
flush_expired(
    #meta{key=Key, serial=Serial, expire=Expire, depend=Depend},
    Now,
    #cleanup_state{pid = Pid, tables = #tables{deps_table = DepsTable}}
) ->
    Expired = Expire < Now orelse not check_depend(Serial, Depend, DepsTable),
    case Expired of
        true  -> gen_server:cast(Pid, {flush, Key}), flushed;
        false -> ok
    end.


%% @doc When the data table is too large then we start to randomly delete keys.  It also signals the cleanup process
%% that it needs to be more aggressive, upping its batch size.
%% We use erts_debug:size() on the stored terms to calculate the total size of all terms stored.  This
%% is better than counting the number of entries.  Using the process_info(Pid,memory) is not very useful as the
%% garbage collection still needs to be done and then we delete too many entries.
cleanup_mode(#cleanup_state{tables = #tables{data_table = DataTable}, memory_max = MemoryMax}) ->
    cleanup_mode(DataTable, MemoryMax).

cleanup_mode(DataTable, MemoryMax) ->
    Memory = ets:info(DataTable, memory),
    if 
        Memory >= MemoryMax -> cache_full;
        true -> normal
    end.



%% @doc Return the current tick count
now_sec() ->
    {M,S,_M} = os:timestamp(),
    M*1000000 + S.

%% @doc Safe erase of process dict, keeps some 'magical' proc_lib vars
erase_process_dict() ->
    Values = [ {K, erlang:get(K)} || K <- ['$initial_call', '$ancestors', '$erl_eval_max_line'] ],
    erlang:erase(),
    [ erlang:put(K,V) || {K,V} <- Values, V =/= undefined ],
    ok.

%% @doc Flush all incoming messages, used when receiving timer ticks to prevent multiple ticks.
flush_message(Msg) ->
    receive
        Msg -> flush_message(Msg)
    after 0 ->
            ok
    end.

callback(_Type, _Name, undefined) ->
    ok;
callback(Type, Name, {M, F, A}) ->
    try
        erlang:apply(M, F, [{Type, Name} | A])
    catch
        _:_  ->
            %% Don't log errors because of high calling frequency.
            nop
    end.
