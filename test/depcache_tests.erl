%% @author Arjan Scherpenisse <arjan@scherpenisse.net>

-module(depcache_tests).

-include_lib("eunit/include/eunit.hrl").


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
    {ok, C} = depcache:start_link([]),

    ?assertEqual(undefined, depcache:get(test_key, C)),
    depcache:set(test_key, 123, C),
    ?assertEqual({ok,123}, depcache:get(test_key, C)),
    depcache:flush(test_key, C),
    ?assertEqual(undefined, depcache:get(test_key, C)).


flush_all_test() ->
    {ok, C} = depcache:start_link([]),

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
    {ok, C} = depcache:start_link([]),

    ?assertEqual(undefined, depcache:get(xtest_key, C)),

    %% Set a key and hold it for one second.
    depcache:set(xtest_key, 123, 1, C),
    ?assertEqual({ok,123}, depcache:get(xtest_key, C)),

    %% Let the depcache time out.
    timer:sleep(4000),
    ?assertEqual(undefined, depcache:get(xtest_key, C)).


get_set_maxage0_test() ->
    {ok, C} = depcache:start_link([]),

    ?assertEqual(undefined, depcache:get(test_key, C)),

    %% Set a key and hold it for 0 seconds
    depcache:set(test_key, 123, 0, C),
    ?assertEqual(undefined, depcache:get(test_key, C)).


get_set_depend_test() ->
    {ok, C} = depcache:start_link([]),

    ?assertEqual(undefined, depcache:get(test_key, C)),

    %% Set a key  and hold it for one second.
    depcache:set(test_key, 123, 10, [test_key_dep], C),
    ?assertEqual({ok,123}, depcache:get(test_key, C)),

    %% flush the dependency; test_key should be gone as well.
    depcache:flush(test_key_dep, C),
    ?assertEqual(undefined, depcache:get(test_key, C)).


memo_test() ->
    {ok, C} = depcache:start_link([]),

    IncreaserFun = fun() ->
                           I = case erlang:get(incr) of
                                   undefined -> 1;
                                   Num -> Num + 1
                               end,
                           erlang:put(incr, I),
                           I
                   end,
    
    ?assertEqual(1, depcache:memo(IncreaserFun, test_key, C)), % uncached
    ?assertEqual(1, depcache:memo(IncreaserFun, test_key, C)), % cached (no increase)
    depcache:flush(test_key, C),
    ?assertEqual(2, depcache:memo(IncreaserFun, test_key, C)), % uncached again
    ?assertEqual(2, depcache:memo(IncreaserFun, test_key, C)). % cached again

