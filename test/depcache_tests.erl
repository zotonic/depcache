%% @author Arjan Scherpenisse <arjan@scherpenisse.net>

-module(depcache_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("depcache/include/depcache.hrl").

-ifdef(fun_stacktrace).
-define(WITH_STACKTRACE(T, R, S), T:R -> S = erlang:get_stacktrace(),).
-else.
-define(WITH_STACKTRACE(T, R, S), T:R:S ->).
-endif.

%% simple_test() ->
%% 	C = z_context:new(testsandbox),
%% 	depcache:flush(test_m_key, C),
%% 	[ test_m(C) || _N <- lists:seq(1,100) ],
%% 	ok.

%% test_m(Context) ->
%%     ?DEBUG(depcache:memo(fun test_f/0, test_m_key, Context)).

%% test_f() ->
%%     ?DEBUG(waiting),
%%     receive after 5000 -> y end.

get_set_test() ->
    {ok, C} = depcache:start_link(#{}),

    ?assertEqual(undefined, depcache:get(test_key, C)),
    depcache:set(test_key, 123, C),
    ?assertEqual({ok,123}, depcache:get(test_key, C)),
    depcache:flush(test_key, C),
    ?assertEqual(undefined, depcache:get(test_key, C)).


flush_all_test() ->
    {ok, C} = depcache:start_link(#{}),

    depcache:set(test_key, 123, C),
    depcache:set(test_key2, 123, C),
    depcache:set(test_key3, 123, C),
    ?assertEqual({ok,123}, depcache:get(test_key, C)),
    ?assertEqual({ok,123}, depcache:get(test_key2, C)),
    ?assertEqual({ok,123}, depcache:get(test_key3, C)),
    depcache:flush(C),
    ?assertEqual(undefined, depcache:get(test_key, C)),
    ?assertEqual(undefined, depcache:get(test_key2, C)),
    ?assertEqual(undefined, depcache:get(test_key3, C)).

%% Temporarily disabled - tests works when ran from the zotonic shell, but not from the 'runtests' command.

get_set_maxage_test() ->
    {ok, C} = depcache:start_link(#{}),

    ?assertEqual(undefined, depcache:get(xtest_key, C)),

    %% Set a key and hold it for one second.
    depcache:set(xtest_key, 123, 1, C),
    ?assertEqual({ok,123}, depcache:get(xtest_key, C)),

    %% Let the depcache time out.
    timer:sleep(4000),
    ?assertEqual(undefined, depcache:get(xtest_key, C)).


get_set_maxage0_test() ->
    {ok, C} = depcache:start_link(#{}),

    ?assertEqual(undefined, depcache:get(test_key, C)),

    %% Set a key and hold it for 0 seconds
    depcache:set(test_key, 123, 0, C),
    ?assertEqual(undefined, depcache:get(test_key, C)).


get_set_depend_test() ->
    {ok, C} = depcache:start_link(#{}),

    ?assertEqual(undefined, depcache:get(test_key, C)),

    %% Set a key  and hold it for ten seconds.
    depcache:set(test_key, 123, 10, [test_key_dep], C),
    ?assertEqual({ok,123}, depcache:get(test_key, C)),

    %% flush the dependency; test_key should be gone as well.
    depcache:flush(test_key_dep, C),
    ?assertEqual(undefined, depcache:get(test_key, C)).

increase(X) ->
    I = case erlang:get(X) of
            undefined -> 1;
            Num -> Num + 1
        end,
    erlang:put(X, I),
    I.

map_test() ->
    {ok, C} = depcache:start_link(#{}),
    ?assertEqual(undefined, depcache:get(a, b, C)),
    depcache:set(a, #{ b => 1 }, C),
    ?assertEqual({ok, 1}, depcache:get(a, b, C)),
    ok.


memo_test() ->
    {ok, C} = depcache:start_link(#{}),

    IncreaserFun = fun() ->
                           increase(x)
                   end,

    ?assertEqual(1, depcache:memo(IncreaserFun, test_key, C)), % uncached
    ?assertEqual(1, depcache:memo(IncreaserFun, test_key, C)), % cached (no increase)
    depcache:flush(test_key, C),
    ?assertEqual(2, depcache:memo(IncreaserFun, test_key, C)), % uncached again
    ?assertEqual(2, depcache:memo(IncreaserFun, test_key, C)). % cached again


memo_record_test() ->
    {ok, C} = depcache:start_link(#{}),

    Fun = fun() -> V = increase(y), #memo{value=V, deps=[dep]} end,

    ?assertEqual(1, depcache:memo(Fun, test_key1, C)), % uncached
    ?assertEqual(1, depcache:memo(Fun, test_key1, C)), % cached (no increase)
    depcache:flush(dep, C), %% flush the dep
    ?assertEqual(2, depcache:memo(Fun, test_key1, C)), % uncached again
    ?assertEqual(2, depcache:memo(Fun, test_key1, C)). % cached again


raise_error() ->
    erlang:error(some_error).

memo_raise_test() ->
    {ok, C} = depcache:start_link(#{}),

    Fun = fun() -> raise_error() end,
    try
        depcache:memo(Fun, test_key1, C)
    catch
        ?WITH_STACKTRACE(Class, R, S)
        ?assertEqual(error, Class),
        ?assertEqual(some_error, R),
        ?assertMatch({depcache_tests, raise_error, 0, _}, hd(S))
    end,
    ok.

memo_premature_kill_test() ->
    {ok, C} = depcache:start_link(#{}),

    LongTask = fun() ->
                       Fun = fun() ->
                                     timer:sleep(500),
                                     done 
                             end,
                       depcache:memo(Fun, premature_kill_test, C)
               end,

    Pid = spawn(LongTask),
    timer:kill_after(250, Pid),
    timer:sleep(50),
    ?assertEqual({error, premature_exit}, depcache:get_wait(premature_kill_test, C)),

    % Check if another process takes over processing in case of pre-mature exits 
    Task = spawn(LongTask),
    timer:kill_after(250, Task),
    timer:sleep(50),
    ?assertEqual(done, LongTask()),

    ok.


cache_full_eviction_test() ->
    {ok, C} = depcache:start_link(#{memory_max => 1}),
    {ok, {_Meta, _Deps, Data, _Lru}} = gen_server:call(C, get_tables),
    MemoryMax = 1024 * 1024 div erlang:system_info(wordsize),
    Count = fill_until_full(C, Data, MemoryMax, 0),

    % Simulate a busy depcache server while the cleanup process finds the cache full
    sys:suspend(C),
    timer:sleep(1500),
    sys:resume(C),
    timer:sleep(500),
    _ = sys:get_state(C),

    % Only enough entries to get below memory_max should be evicted, not most of the cache
    ?assert(ets:info(Data, memory) < MemoryMax),
    ?assert(ets:info(Data, size) > Count div 2),
    ok.

fill_until_full(C, Data, MemoryMax, N) ->
    case ets:info(Data, memory) >= MemoryMax of
        true ->
            N;
        false ->
            depcache:set({key, N}, lists:seq(1, 100), C),
            fill_until_full(C, Data, MemoryMax, N + 1)
    end.


%% The lru field is the last field of the #meta record; the record is private to depcache.
-define(META_LRU(Row), element(tuple_size(Row), Row)).

lru_config_test() ->
    ?assertError(badarg, depcache:start_link(#{lru => yes})),
    ?assertError(badarg, depcache:start_link(#{lru_slots => 0})),
    {ok, C} = depcache:start_link([{lru, true}, {lru_slots, 1024}]),
    ok = depcache:set(a, 1, C),
    ?assertEqual({ok, 1}, depcache:get(a, C)),
    ok.

lru_disabled_by_default_test() ->
    {ok, C} = depcache:start_link(#{}),
    {ok, {Meta, _Deps, _Data, Lru}} = gen_server:call(C, get_tables),
    ?assertEqual(undefined, Lru),
    ok = depcache:set(a, 1, C),
    [Row] = ets:lookup(Meta, a),
    ?assertEqual(undefined, ?META_LRU(Row)),
    ?assertEqual({ok, 1}, depcache:get(a, C)),
    ok.

lru_touch_on_read_test() ->
    {ok, C} = depcache:start_link(#{lru => true, lru_slots => 1024}),
    {ok, {Meta, _Deps, _Data, Ref}} = gen_server:call(C, get_tables),
    ok = depcache:set(a, 1, C),
    [Row] = ets:lookup(Meta, a),
    Slot = ?META_LRU(Row),
    ?assert(is_integer(Slot) andalso Slot >= 1 andalso Slot =< 1024),
    % A set is not an access: the slot stays untouched (0 in a fresh array)
    ?assertEqual(0, atomics:get(Ref, Slot)),
    % A read stamps the slot with the reader's clock
    ?assertEqual({ok, 1}, depcache:get(a, C)),
    T0 = atomics:get(Ref, Slot),
    ?assert(abs(T0 - now_sec()) =< 1),
    % A read a second later moves the stamp forward
    timer:sleep(1100),
    ?assertEqual({ok, 1}, depcache:get(a, C)),
    T1 = atomics:get(Ref, Slot),
    ?assert(T1 > T0),
    % Setting the key again does not reset the stamp
    ok = depcache:set(a, 2, C),
    ?assertEqual(T1, atomics:get(Ref, Slot)),
    % A miss or a flush does not touch anything
    ok = depcache:flush(a, C),
    ?assertEqual(undefined, depcache:get(a, C)),
    ?assertEqual(T1, atomics:get(Ref, Slot)),
    ok.

lru_eviction_keeps_hot_keys_test_() ->
    {timeout, 30, fun lru_eviction_keeps_hot_keys/0}.

lru_eviction_keeps_hot_keys() ->
    {ok, C} = depcache:start_link(#{memory_max => 1, lru => true}),
    {ok, {_Meta, _Deps, Data, _Lru}} = gen_server:call(C, get_tables),
    MemoryMax = 1024 * 1024 div erlang:system_info(wordsize),

    % Fill most of the cache with cold entries, then add a few hot ones
    ColdCount = fill_until(C, Data, MemoryMax * 8 div 10, 0),
    HotKeys = [ {hot, N} || N <- lists:seq(1, 10) ],
    [ depcache:set(K, lists:seq(1, 100), C) || K <- HotKeys ],

    % A second later, read the hot keys so their access time is newer than
    % everything inserted so far
    timer:sleep(1100),
    [ ?assertMatch({ok, _}, depcache:get(K, C)) || K <- HotKeys ],

    % Push the cache well over its limit with keys that are set but never read,
    % like a scan over many one-off pages, so that about a third of the cache
    % has to be evicted. The cleanup process checks the size once a second, so
    % this fill mostly completes before it starts evicting.
    Total = fill_until(C, Data, MemoryMax * 3 div 2, ColdCount) + length(HotKeys),
    timer:sleep(2500),
    _ = sys:get_state(C),

    ?assert(ets:info(Data, memory) < MemoryMax),
    ?assert(ets:info(Data, size) < Total * 8 div 10),
    % Only enough entries to get below memory_max are evicted
    ?assert(ets:info(Data, size) > Total div 2),
    % The least recently used entries went first, the hot ones all survived.
    % With random eviction each hot key would have had roughly one chance in
    % three of being evicted.
    [ ?assertMatch({ok, _}, depcache:get(K, C)) || K <- HotKeys ],
    ok.

fill_until(C, Data, Words, N) ->
    case ets:info(Data, memory) >= Words of
        true ->
            N;
        false ->
            depcache:set({key, N}, lists:seq(1, 100), C),
            fill_until(C, Data, Words, N + 1)
    end.

now_sec() ->
    {M, S, _} = os:timestamp(),
    M * 1000000 + S.
