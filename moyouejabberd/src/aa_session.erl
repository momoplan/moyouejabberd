-module(aa_session).
-include_lib("stdlib/include/qlc.hrl").
-include("jlib.hrl").

-export([find/1,total_count_user/1,get_user_list/1, pid_find_user/1, check_online/1]).
-record(session, {sid, usr, us, priority, info}).
-record(session_counter, {vhost, count}).

-export([c2s_gc/1, c2s_gc/0, c2s_gc1/0]).

-export([receiver_gc1/0, receiver_gc/1, receiver_gc/0]).


find(Username)->
	Keys = mnesia:dirty_all_keys(session),
	lists:foreach(fun(K)->
						  case mnesia:dirty_read(session,K) of
							  [Session] ->
								  {U,_} = Session#session.us,
								  case Username=:=U of 
									  true->
										  io:format("~p~n",[Session]);
									  false->
										  skip
								  end;
							  _ ->
								  skip
						  end
				  end,Keys).


pid_find_user(Pid) ->
	Keys = mnesia:dirty_all_keys(session),
	lists:filtermap(fun(K)->
						 case mnesia:dirty_read(session, K) of
							 [Session] ->
								 case Session#session.sid of
									 {_, Pid} ->
										 {true, Session};
									 _ ->
										 false
								 end;
							 _ ->
								 false
						 end
				 end,Keys).

check_online(Jid) ->
	#jid{user = User, server = Server} = Jid,
	LUser = jlib:nodeprep(User),
	LServer = jlib:nameprep(Server),
	US = {LUser, LServer},
	case catch mnesia:dirty_index_read(session, US, #session.us) of
		{'EXIT', _Reason} ->
			offline;
		[] ->
			offline;
		_ ->
			online
	end.


c2s_gc() ->
	[ c2s_gc(Node) || Node <- [node()|nodes()]].
c2s_gc(Node) ->
	spawn(fun() ->
				  rpc:call(Node,aa_session,c2s_gc1,[])
		  end).

c2s_gc1() ->
	Pid = erlang:whereis(ejabberd_c2s_sup),
	case is_pid(Pid) of
		true ->
			{links, L} = erlang:process_info(Pid, links),
			[erlang:garbage_collect(Pid) || Pid <- L];
		_ ->
			skip
	end.

receiver_gc() ->
	[ receiver_gc(Node) || Node <- [node()|nodes()]].
receiver_gc(Node) ->
	spawn(fun() ->
				  rpc:call(Node,aa_session,receiver_gc1,[])
		  end).

receiver_gc1() ->
	Pid = erlang:whereis(ejabberd_receiver_sup),
	case is_pid(Pid) of
		true ->
			{links, L} = erlang:process_info(Pid, links),
			[erlang:garbage_collect(Pid) || Pid <- L];
		_ ->
			skip
	end.
	


get_user_list(Json) ->
	{ok,Obj,_} = rfc4627:decode(Json),
	{ok,P} = rfc4627:get_field(Obj, "params"),
	PageSize = case rfc4627:get_field(P, "pageSize") of
			{ok,PSize} when is_binary(PSize) ->
				binary_to_integer(PSize);
			{ok,PSize} when is_list(PSize) ->
			   	list_to_integer(PSize);
			{ok,PSize} ->
			   	PSize;
			_ ->
				20
	end,
	PageNo = case rfc4627:get_field(P, "pageNo") of
			{ok,PNo} when is_binary(PNo) ->
				binary_to_integer(PNo);
			{ok,PNo} when is_list(PNo) ->
			   	list_to_integer(PNo);
			{ok,PNo} ->
			   	PNo;
			_ ->
				0
	end,
	{ok,Domain} = rfc4627:get_field(P, "domain"),
	U = get_user_list(binary_to_list(Domain),PageSize,PageNo),
	case U of
		{ok,L} ->
			UL = build_json_list(L,[]),
			[{TotalCount}] = total_count_user(binary_to_list(Domain)),
			io:format("UL=======~p~nTotalCount=======~p~n", [UL,TotalCount]),
			{obj,[{pageSize,PageSize},{pageNo,PageNo},{totalCount,TotalCount},{data,UL}]};
		_ ->
			{obj,[{pageSize,PageSize},{pageNo,PageNo},{totalCount,0},{data,[]}]} 
	end.



get_user_list(Server,PageSize,PageNo) ->
	io:format("Server=~p ; PageSize=~p ; PageNo=~p~n",[Server,PageSize,PageNo]),
	F = fun() ->
			    Q = qlc:q([
				        {S#session.us}||S<-mnesia:table(session),
					ordsets:is_subset([Server],tuple_to_list(S#session.us))==true
			    ]),
			    C = qlc:cursor(Q),
			    case PageNo of
				    0 -> skip;
				    _ -> qlc:next_answers(C,PageSize*PageNo)
			    end,
			    qlc:next_answers(C,PageSize)
	    end,
	{atomic,R} = mnesia:transaction(F),
	io:format("Result=~p~n",[R]),
	case R of
		[] -> {empty,[]};
		_ -> {ok,R}
	end.

total_count_user(Server) ->
	F = fun() ->
			    Q=qlc:q([{S#session_counter.count}||S<-mnesia:table(session_counter),S#session_counter.vhost==Server]),
			    qlc:e(Q)
	    end,
	{atomic,R} = mnesia:transaction(F),
	R.


build_json_list([E|L],List)->
	{{Name,Domain}} = E,
	JN = {obj,[{name,list_to_binary(Name)},{domain,list_to_binary(Domain)}]},
	build_json_list(L, [JN|List]);
build_json_list([],List)-> List.

