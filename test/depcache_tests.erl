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
    {ok, {_Meta, _Deps, Data}} = gen_server:call(C, get_tables),
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
