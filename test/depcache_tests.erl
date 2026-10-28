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
    with_cache(#{memory_max => 1}, fun(C) ->
        {ok, {_, _, Data, _}} = gen_server:call(C, get_tables),
        [depcache:set({key, N}, lists:seq(1, 100), C) || N <- lists:seq(1, 2000)],
        ?assert(depcache:size(C)*erlang:system_info(wordsize) =< 1024*1024),
        ?assert(ets:info(Data, size) > 0)
    end).

%% Regression tests for dependency generations, cleanup and memo ownership.
dependency_update_test() ->
    with_cache(#{}, fun(C) ->
        lists:foreach(fun(Key) ->
            depcache:set(derived, old, 60, [Key], C),
            depcache:set(Key, new, C),
            ?assertEqual(undefined, depcache:get(derived, C))
        end, [source, 42, {source, 1}, <<"source">>, "source", [[source]]])
    end).

dependency_reclamation_test() ->
    with_cache(#{}, fun(C) ->
        {ok, {_, Deps, _, _}} = gen_server:call(C, get_tables),
        lists:foreach(fun(N) ->
            depcache:set(temp, value, 60, [{dep, N}], C)
        end, lists:seq(1, 1000)),
        ?assertEqual(1, ets:info(Deps, size)),
        depcache:flush(temp, C),
        ?assertEqual(0, ets:info(Deps, size)),
        depcache:set(a, a, 60, [shared, shared], C),
        depcache:set(b, b, 60, [shared], C),
        depcache:flush(a, C),
        ?assertEqual({ok, b}, depcache:get(b, C)),
        depcache:flush(shared, C),
        depcache:set(c, c, 60, [shared], C),
        ?assertEqual(undefined, depcache:get(b, C)),
        ?assertEqual({ok, c}, depcache:get(c, C)),
        depcache:flush(c, C),
        ?assertEqual(0, ets:info(Deps, size)),
        depcache:set(zero, value, 0, [unused], C),
        ?assertEqual(0, ets:info(Deps, size))
    end).

implicit_memo_key_test() ->
    with_cache(#{}, fun(C) ->
        Fun = {erlang, unique_integer, []},
        Value = depcache:memo(Fun, C),
        ?assertEqual(Value, depcache:memo(Fun, C)),
        ?assertEqual({ok, Value}, depcache:get(Fun, C)),
        ?assertEqual(undefined, depcache:get(undefined, C)),
        ?assertEqual({monitors, []}, process_info(C, monitors))
    end).

memo_completion_monitor_test() ->
    with_cache(#{}, fun(C) ->
        ?assertEqual(value, depcache:memo(fun() -> value end, zero, 0, C)),
        ?assertEqual({monitors, []}, process_info(C, monitors)),
        ?assertError(failed, depcache:memo(fun() -> error(failed) end, failed, C)),
        ?assertEqual({monitors, []}, process_info(C, monitors)),
        ?assertEqual(value, depcache:memo(fun() ->
            #memo{value=value, max_age=0}
        end, override, C)),
        ?assertEqual(undefined, depcache:get(override, C)),
        ?assertEqual({monitors, []}, process_info(C, monitors))
    end).

completed_writer_exit_test() ->
    with_cache(#{}, fun(C) ->
        Parent = self(),
        Old = spawn(fun() ->
            depcache:memo(fun() -> old end, key, 0, C),
            Parent ! old_done,
            receive stop -> ok end
        end),
        receive old_done -> ok after 1000 -> error(old_writer_timeout) end,
        New = spawn(fun() ->
            undefined = depcache:get_wait(key, C),
            Parent ! new_started,
            receive finish -> depcache:set(key, fresh, C) end
        end),
        receive new_started -> ok after 1000 -> error(new_writer_timeout) end,
        spawn(fun() -> Parent ! {waited, depcache:get_wait(key, C)} end),
        await(fun() ->
            State = sys:get_state(C),
            {_, Waiters, _} = maps:get(key, element(5, State)),
            Waiters =/= []
        end),
        Ref = monitor(process, Old),
        Old ! stop,
        receive {'DOWN', Ref, process, Old, _} -> ok end,
        _ = sys:get_state(C),
        New ! finish,
        receive {waited, Result} -> ?assertEqual({ok, fresh}, Result)
        after 1000 -> error(waiter_timeout)
        end
    end).

process_cache_refresh_test() ->
    with_cache(#{}, fun(C) ->
        depcache:set(key, #{sub => old}, C),
        depcache:in_process(true),
        ?assertEqual({ok, #{sub => old}}, depcache:get(key, C)),
        ?assertEqual({ok, old}, depcache:get_subkey(key, sub, C)),
        Parent = self(),
        spawn(fun() -> depcache:set(key, #{sub => new}, C), Parent ! updated end),
        receive updated -> ok end,
        %% Advance the local cache epoch without a wall-clock dependent sleep.
        put(depcache_now, erlang:system_time(second)-1),
        ?assertEqual({ok, new}, depcache:get_subkey(key, sub, C)),
        ?assertEqual({ok, #{sub => new}}, depcache:get(key, C)),
        ?assertEqual(true, depcache:in_process(false)),
        ?assertEqual(false, depcache:in_process_server(C))
    end).

binary_memory_accounting_test() ->
    with_cache(#{}, fun(C) ->
        Baseline = depcache:size(C),
        Bytes = 2*1024*1024,
        depcache:set(blob, #{nested => [binary:copy(<<42>>, Bytes)]}, C),
        ?assert(depcache:size(C)*erlang:system_info(wordsize) >= Bytes),
        depcache:set(blob, small, C),
        ?assert(depcache:size(C) < Baseline + 1000),
        depcache:flush(C),
        ?assert(depcache:size(C) < Baseline + 1000)
    end).

binary_eviction_test() ->
    with_cache(#{memory_max => 1}, fun(C) ->
        depcache:set(blob, binary:copy(<<42>>, 2*1024*1024), C),
        await(fun() -> depcache:get(blob, C) =:= undefined end),
        ?assert(depcache:size(C)*erlang:system_info(wordsize) < 1024*1024)
    end).

cleanup_generation_test() ->
    with_cache(#{}, fun(C) ->
        {ok, {Meta, _, _, _}} = gen_server:call(C, get_tables),
        depcache:set(key, old, C),
        [OldMeta] = ets:lookup(Meta, key),
        OldSerial = element(4, OldMeta),
        depcache:set(key, fresh, C),
        depcache:set(dependent, valid, 60, [key], C),
        %% Both expiry casts and synchronous random eviction must compare generations.
        gen_server:cast(C, {flush, key, OldSerial}),
        ok = gen_server:call(C, {flush, key, OldSerial}),
        ?assertEqual({ok, fresh}, depcache:get(key, C)),
        ?assertEqual({ok, valid}, depcache:get(dependent, C)),
        [NewMeta] = ets:lookup(Meta, key),
        gen_server:cast(C, {flush, key, element(4, NewMeta)}),
        _ = sys:get_state(C),
        ?assertEqual(undefined, depcache:get(key, C)),
        ?assertEqual(undefined, depcache:get(dependent, C))
    end).

with_cache(Config, Fun) ->
    {ok, C} = depcache:start_link(Config),
    try Fun(C)
    after
        depcache:in_process(false),
        gen_server:stop(C)
    end.

await(Fun) ->
    await(Fun, erlang:monotonic_time(millisecond) + 3000).

await(Fun, Deadline) ->
    case Fun() of
        true -> ok;
        false ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true -> timer:sleep(10), await(Fun, Deadline);
                false -> error(await_timeout)
            end
    end.

retained_binary_memory_test() ->
    with_cache(#{}, fun(C) ->
        Bytes = 2*1024*1024,
        Bin = binary:copy(<<42>>, Bytes),
        depcache:set(closure, fun() -> Bin end, C),
        ?assert(depcache:size(C)*erlang:system_info(wordsize) >= Bytes),
        depcache:flush(C),
        depcache:set(bits, <<Bin/binary, 1:1>>, C),
        ?assert(depcache:size(C)*erlang:system_info(wordsize) >= Bytes)
    end).

zero_budget_and_shutdown_test() ->
    {ok, C} = depcache:start_link(#{memory_max => 0}),
    {links, Links} = process_info(C, links),
    [Cleanup] = Links -- [self()],
    Ref = monitor(process, Cleanup),
    try
        depcache:set(key, value, C),
        await(fun() -> depcache:get(key, C) =:= undefined end),
        await(fun() ->
            process_info(Cleanup, status) =:= {status, waiting}
        end)
    after
        gen_server:stop(C)
    end,
    receive {'DOWN', Ref, process, Cleanup, shutdown} -> ok
    after 1000 -> error(cleanup_not_stopped)
    end.

pid_reads_bypass_server_test() ->
    with_cache(#{}, fun(C) ->
        depcache:set(key, value, C),
        ?assertEqual({ok, value}, depcache:get(key, C)),
        sys:suspend(C),
        try ?assertEqual({ok, value}, depcache:get(key, C))
        after sys:resume(C)
        end
    end).

stale_pid_tables_test() ->
    {ok, C} = depcache:start_link(#{}),
    depcache:set(key, value, C),
    ?assertEqual({ok, value}, depcache:get(key, C)),
    gen_server:stop(C),
    ?assertExit({noproc, _}, depcache:get(key, C)).

replacement_visibility_test() ->
    with_cache(#{}, fun(C) ->
        depcache:set(hot, value, C),
        Parent = self(),
        Readers = [spawn_monitor(fun() ->
            receive go -> ok end,
            Misses = lists:foldl(fun(_, N) ->
                case depcache:get(hot, C) of {ok, value} -> N; _ -> N+1 end
            end, 0, lists:seq(1, 20000)),
            Parent ! {read_result, self(), Misses}
        end) || _ <- lists:seq(1, 8)],
        [P ! go || {P, _} <- Readers],
        [depcache:set(hot, value, C) || _ <- lists:seq(1, 5000)],
        lists:foreach(fun({P, Ref}) ->
            receive {read_result, P, Misses} -> ?assertEqual(0, Misses)
            after 2000 -> error(reader_timeout)
            end,
            receive {'DOWN', Ref, process, P, normal} -> ok end
        end, Readers)
    end).

admission_without_cleanup_test() ->
    with_cache(#{memory_max => 1}, fun(C) ->
        {links, Links} = process_info(C, links),
        [Cleanup] = Links -- [self()],
        erlang:suspend_process(Cleanup),
        try
            Parent = self(),
            Writers = [spawn_monitor(fun() ->
                [depcache:set({I, N}, lists:seq(1, 100), C) || N <- lists:seq(1, 1000)],
                Parent ! {writer_done, self()}
            end) || I <- lists:seq(1, 8)],
            lists:foreach(fun({P, Ref}) ->
                receive {writer_done, P} -> ok after 3000 -> error(writer_timeout) end,
                receive {'DOWN', Ref, process, P, normal} -> ok end
            end, Writers),
            ?assert(depcache:size(C)*erlang:system_info(wordsize) =< 1024*1024),
            {message_queue_len, Queue} = process_info(Cleanup, message_queue_len),
            ?assert(Queue =< 1)
        after erlang:resume_process(Cleanup)
        end
    end).

rejected_write_test() ->
    with_cache(#{memory_max => 1}, fun(C) ->
        depcache:set(key, old, C),
        depcache:set(derived, old, 60, [key], C),
        Large = binary:copy(<<42>>, 2*1024*1024),
        depcache:set(key, Large, C),
        ?assertEqual(undefined, depcache:get(key, C)),
        ?assertEqual(undefined, depcache:get(derived, C)),
        ?assertEqual(Large, depcache:memo(fun() -> Large end, memo_key, C)),
        ?assertEqual(undefined, depcache:get(memo_key, C)),
        ?assertEqual({monitors, []}, process_info(C, monitors)),
        undefined = depcache:get_wait(waited_key, C),
        Parent = self(),
        {Waiter, Ref} = spawn_monitor(fun() ->
            Parent ! {oversized_result, depcache:get_wait(waited_key, C)}
        end),
        await(fun() ->
            {_, Waiting, _} = maps:get(waited_key, element(5, sys:get_state(C))),
            Waiting =/= []
        end),
        depcache:set(waited_key, Large, C),
        receive {oversized_result, Value} -> ?assertEqual({ok, Large}, Value)
        after 1000 -> error(waiter_timeout)
        end,
        receive {'DOWN', Ref, process, Waiter, normal} -> ok end,
        ?assertEqual(undefined, depcache:get(waited_key, C)),
        ?assertEqual({monitors, []}, process_info(C, monitors))
    end).

local_cache_owned_keys_test() ->
    with_cache(#{}, fun(C) ->
        put(application_state, retained),
        depcache:set(key, #{sub => value}, C),
        depcache:in_process(true),
        depcache:get(key, C),
        depcache:get_subkey(key, sub, C),
        ?assertEqual(2, get(depcache_count)),
        depcache:flush_process_dict(),
        ?assertEqual(undefined, get({depcache, key})),
        ?assertEqual(undefined, get({depcache_subkey, key, sub})),
        ?assertEqual(retained, erase(application_state))
    end).

adaptive_expiry_test_() ->
    {timeout, 10, fun() ->
        with_cache(#{}, fun(C) ->
            [depcache:set(N, value, 1, C) || N <- lists:seq(1, 10000)],
            {ok, {_, _, Data, _}} = gen_server:call(C, get_tables),
            await(fun() -> ets:info(Data, size) =:= 0 end,
                erlang:monotonic_time(millisecond) + 6000)
        end)
    end}.

implicit_fun_memo_test() ->
    with_cache(#{}, fun(C) ->
        put(implicit_fun_calls, 0),
        Fun = fun() -> increase(implicit_fun_calls) end,
        ?assertEqual(1, depcache:memo(Fun, C)),
        ?assertEqual(1, depcache:memo(Fun, C)),
        ?assertEqual(1, get(implicit_fun_calls)),
        ?assertEqual({ok, 1}, depcache:get(Fun, C)),
        depcache:flush(Fun, C),
        ?assertEqual(2, depcache:memo(Fun, C)),
        ?assertEqual(2, erase(implicit_fun_calls)),
        ?assertEqual({monitors, []}, process_info(C, monitors))
    end).

captured_fun_keys_test() ->
    with_cache(#{}, fun(C) ->
        A = captured_fun(alice),
        B = captured_fun(bob),
        ?assertEqual(alice, depcache:memo(A, C)),
        ?assertEqual(bob, depcache:memo(B, C)),
        ?assertEqual({ok, alice}, depcache:get(A, C)),
        ?assertEqual({ok, bob}, depcache:get(B, C))
    end).

captured_fun(Value) ->
    fun() -> Value end.

context_memo_keys_test() ->
    with_cache(#{}, fun(C) ->
        A = {erlang, element, [2, {context, alice}]},
        B = {erlang, element, [2, {context, bob}]},
        %% Context arguments are passed to the function but excluded from the key.
        ?assertEqual(alice, depcache:memo(A, C)),
        ?assertEqual(alice, depcache:memo(B, C)),
        ?assertEqual({ok, alice}, depcache:get({erlang, element, [2]}, C)),
        %% Context-dependent results require explicit keys.
        ?assertEqual(alice, depcache:memo(A, {user, alice}, 60, C)),
        ?assertEqual(bob, depcache:memo(B, {user, bob}, 60, C))
    end).

memo_exception_waiters_test_() ->
    [{atom_to_list(Class), fun() -> memo_exception_waiters(Class) end}
        || Class <- [error, exit, throw]].

memo_exception_waiters(Class) ->
    with_cache(#{}, fun(C) ->
        Parent = self(),
        Key = {exception, Class},
        {Writer, WriterRef} = spawn_monitor(fun() ->
            Result = capture_exception(fun() ->
                depcache:memo(fun() ->
                    Parent ! writer_started,
                    receive fail -> raise_memo_exception(Class) end
                end, Key, C)
            end),
            Parent ! {writer_result, Result}
        end),
        receive writer_started -> ok after 1000 -> error(writer_timeout) end,
        {Waiter, WaiterRef} = spawn_monitor(fun() ->
            Result = capture_exception(fun() ->
                depcache:memo(fun() -> unexpected end, Key, C)
            end),
            Parent ! {waiter_result, Result}
        end),
        {RawWaiter, RawRef} = spawn_monitor(fun() ->
            Parent ! {raw_result, depcache:get_wait(Key, C)}
        end),
        await(fun() ->
            {_, Waiting, _} = maps:get(Key, element(5, sys:get_state(C))),
            length(Waiting) =:= 2
        end),
        Writer ! fail,
        Original = receive {writer_result, R} -> R after 1000 -> error(writer_timeout) end,
        ?assertMatch({exception, Class, memo_failure, [_|_]}, Original),
        receive {waiter_result, Received} -> ?assertEqual(Original, Received)
        after 1000 -> error(waiter_timeout)
        end,
        receive {raw_result, Raw} -> ?assertEqual(Original, Raw)
        after 1000 -> error(raw_waiter_timeout)
        end,
        lists:foreach(fun({Pid, Ref}) ->
            receive {'DOWN', Ref, process, Pid, normal} -> ok
            after 1000 -> error(process_timeout)
            end
        end, [{Writer, WriterRef}, {Waiter, WaiterRef}, {RawWaiter, RawRef}]),
        ?assertEqual({monitors, []}, process_info(C, monitors)),
        ?assertEqual(recovered, depcache:memo(fun() -> recovered end, Key, C))
    end).

capture_exception(Fun) ->
    try Fun()
    catch
        ?WITH_STACKTRACE(Class, Reason, Stack)
            {exception, Class, Reason, Stack}
    end.

raise_memo_exception(error) -> error(memo_failure);
raise_memo_exception(exit) -> exit(memo_failure);
raise_memo_exception(throw) -> throw(memo_failure).


%% LRU eviction (the `lru' option).

%% The lru field is the last field of the #meta record; the record is private to depcache.
-define(META_LRU(Row), element(tuple_size(Row), Row)).

lru_config_test() ->
    ?assertError(badarg, depcache:start_link(maps:from_list([{lru, yes}]))),
    ?assertError(badarg, depcache:start_link(maps:from_list([{lru_slots, 0}]))),
    with_cache([{lru, true}, {lru_slots, 1024}], fun(C) ->
        ok = depcache:set(a, 1, C),
        ?assertEqual({ok, 1}, depcache:get(a, C))
    end).

lru_disabled_by_default_test() ->
    with_cache(#{}, fun(C) ->
        {ok, {Meta, _Deps, _Data, Lru}} = gen_server:call(C, get_tables),
        ?assertEqual(undefined, Lru),
        ok = depcache:set(a, 1, C),
        [Row] = ets:lookup(Meta, a),
        ?assertEqual(undefined, ?META_LRU(Row)),
        ?assertEqual({ok, 1}, depcache:get(a, C))
    end).

lru_touch_on_read_test() ->
    with_cache(#{lru => true, lru_slots => 1024}, fun(C) ->
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
        ?assertEqual(T1, atomics:get(Ref, Slot))
    end).

lru_named_server_test() ->
    {ok, C} = depcache:start_link(lru_named_test, #{lru => true, lru_slots => 1024}),
    try
        ok = depcache:set(a, 1, lru_named_test),
        ?assertEqual({ok, 1}, depcache:get(a, lru_named_test)),
        % Readers of a named server find the array without asking the server
        {_Meta, _Deps, _Data, Ref} = erlang:element(3, erlang:get(depcache_tables)),
        ?assert(Ref =/= undefined),
        {ok, {_, _, _, Ref}} = gen_server:call(C, get_tables)
    after
        depcache:flush_process_dict(),
        erlang:erase(depcache_tables),
        gen_server:stop(C)
    end,
    % The array is unpublished when the server stops
    ?assertEqual(undefined, persistent_term:get({depcache, lru_named_test, lru}, undefined)).

lru_eviction_keeps_hot_keys_test_() ->
    {timeout, 30, fun lru_eviction_keeps_hot_keys/0}.

lru_eviction_keeps_hot_keys() ->
    with_cache(#{memory_max => 1, lru => true}, fun(C) ->
        {ok, {Meta, _Deps, _Data, _Lru}} = gen_server:call(C, get_tables),
        MemoryMax = 1024 * 1024 div erlang:system_info(wordsize),
        Pressure = MemoryMax * 9 div 10,

        % Fill well below the pressure point with cold entries that are never
        % read, then add a few hot ones
        ColdCount = fill_until(C, Pressure * 7 div 10, 0),
        HotKeys = [ {hot, N} || N <- lists:seq(1, 10) ],
        [ depcache:set(K, lists:seq(1, 100), C) || K <- HotKeys ],
        ?assert(depcache:size(C) < Pressure),

        % A second later, read the hot keys so they have an access time
        timer:sleep(1100),
        [ ?assertMatch({ok, _}, depcache:get(K, C)) || K <- HotKeys ],

        % Push the cache to its limit with more keys that are set but never
        % read, like a scan over many one-off pages. Admission rejects writes
        % past the budget, and the cleanup process evicts until the cache is
        % back under the pressure point.
        Before = ets:info(Meta, size),
        _ = fill_until(C, MemoryMax, ColdCount),
        await(fun() -> depcache:size(C) < Pressure end),

        ?assert(ets:info(Meta, size) < Before + 1500),
        Evicted = [ N || N <- lists:seq(0, ColdCount - 1), depcache:get({key, N}, C) =:= undefined ],
        ?assert(length(Evicted) > 0),
        % The least recently used entries went first, the hot ones all survived.
        % With random eviction each hot key would have had roughly one chance
        % in ten per sweep of being evicted.
        [ ?assertMatch({ok, _}, depcache:get(K, C)) || K <- HotKeys ]
    end).

fill_until(C, Words, N) ->
    case depcache:size(C) >= Words orelse N > 20000 of
        true ->
            N;
        false ->
            depcache:set({key, N}, lists:seq(1, 100), C),
            fill_until(C, Words, N + 1)
    end.

now_sec() ->
    {M, S, _} = os:timestamp(),
    M * 1000000 + S.
