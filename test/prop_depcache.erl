-module(prop_depcache).
-include_lib("proper/include/proper.hrl").

%%%%%%%%%%%%%%%%%%
%%% Properties %%%
%%%%%%%%%%%%%%%%%%
prop_set_get() ->
	?SETUP(setup(),
	?FORALL({Key,Value}, {key(),value()},
		begin
			Server = whereis(dep),
			undefined = depcache:get(Key, Server),
			ok = depcache:set(Key, Value, Server),
			{ok, Value} = depcache:get(Key, Server),
			ok = depcache:flush(Key, Server),
			undefined =:= depcache:get(Key, Server)
		end 
		)
	).
	
prop_flush_all() ->
	?SETUP(setup(),
	?FORALL(List, ?SIZED(Size, resize(Size*3, list({key(), value()}))),
		begin
			Server = whereis(dep),
			lists:foreach(fun({Key,Value}) ->
					depcache:set(Key, Value, Server)
				end, List),
			ok = depcache:flush(Server),
			lists:all(fun({Key, _Value}) ->
					undefined =:= depcache:get(Key, Server)
				end, List)
		end 
		)
	).

prop_get_set_maxage() ->
	?SETUP(setup(),
	?FORALL({Key,Value}, {key(),value()},
		begin
			Server = whereis(dep),
			%% Set a key and hold it for one second.
			TimeValueSec = 1,
			ok = depcache:set(Key, Value, TimeValueSec, Server),
			{ok, Value} = depcache:get(Key, Server),
			%% Let the depcache time out.
			timer:sleep(3000),
			undefined =:= depcache:get(Key, Server)
		end 
		)
	).

prop_get_set_maxage_0() ->
	?SETUP(setup(),
	?FORALL({Key,Value}, {key(),value()},
		begin
			Server = whereis(dep),
			%% Set a key and hold it for zero second.
			TimeValueSec = 0,
			ok = depcache:set(Key, Value, TimeValueSec, Server),
			undefined =:= depcache:get(Key, Server)
		end 
		)
	).

prop_get_set_depend() ->
	?SETUP(setup(),
	?FORALL({Key,{DepKey, DepValue}}, 
		?SUCHTHAT({Key, {DepKey, _DepValue}}, {key(), {key(), value()}},  
			Key =/= DepKey andalso not is_integer(DepKey) ),
		begin
			Server = whereis(dep),
			TimeValueSec = 2,
			ok = depcache:set(Key, [{DepKey, DepValue}], TimeValueSec, [DepKey], Server),
			{ok, [{DepKey,DepValue}]} = depcache:get(Key, Server),
			{ok, DepValue} = depcache:get(Key, DepKey, Server),
			{ok, DepValue} = depcache:get_subkey(Key, DepKey, Server),
			undefined == depcache:get(DepKey, Server) andalso
			ok == depcache:flush(Key, Server)
		end 
		)
	).
	
prop_get_set_depend_map() ->
	?SETUP(setup(),
	?FORALL({Key,{DepKey, DepValue}}, 
		?SUCHTHAT({Key, {DepKey, _DepValue}}, {key(), {key(), value()}},  
			Key =/= DepKey andalso not is_integer(DepKey) ),
		begin
			Server = whereis(dep),
			ok = depcache:set(Key, #{ DepKey => DepValue }, Server),
			{ok, #{DepKey := DepValue}} = depcache:get(Key, Server),
			{ok, DepValue} = depcache:get(Key, DepKey, Server),
			{ok, DepValue} = depcache:get_subkey(Key, DepKey, Server),
			undefined == depcache:get(DepKey, Server) andalso
			ok == depcache:flush(Key, Server)
		end 
		)
	).

prop_memo() ->
	?SETUP(fun() ->
		IncreaseFun = fun(X) ->
		I = case erlang:get(X) of
				undefined 	-> 1;
				Num 		-> Num + 1
			end,
			erlang:put(X, I),
			I 
		end,
		erlang:put("IncreaseFun", IncreaseFun),
		erlang:put("MemoValue", 0),
		fun() -> ok end
	end,
	?SETUP(setup(),
	?FORALL(Key, key(),  
		begin
			Server = whereis(dep),
			IncreaseFun = erlang:get("IncreaseFun"),
			MemoValue = erlang:get("MemoValue"),
			IncreaserFunX = fun() -> IncreaseFun(ok) end,

			DepCacheMemo1 = depcache:memo(IncreaserFunX, Key, Server),
			DepCacheMemo1 = MemoValue + 1,
			ok = depcache:flush(Key, Server),

			DepCacheMemo2 = depcache:memo(IncreaserFunX, Key, Server),
			DepCacheMemo2 = MemoValue + 2,
			ok = depcache:flush(Key, Server),

			erlang:put("MemoValue", MemoValue + 2),
			
			true
		end 
		)
	)	
	).


%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%

setup() ->
	fun() ->
		{ok, Server} = depcache:start_link([]),
		register(dep, Server),
		fun() -> 
			depcache:flush(Server),
			case whereis(dep) of 
				undefined -> ok;
				_Pid -> unregister(dep)
			end,	
			ok
		end
	end.
	


%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%
key() -> non_empty(any()).
value() -> non_empty(any()).
