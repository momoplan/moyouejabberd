-module(aa_session).
-include_lib("stdlib/include/qlc.hrl").

-export([find/1,total_count_user/1,get_user_list/1]).
-record(session, {sid, usr, us, priority, info}).
-record(session_counter, {vhost, count}).


find(Username)->
	Keys = mnesia:dirty_all_keys(session),
	lists:foreach(fun(K)->
				      [Session] = mnesia:dirty_read(session,K),
				      {U,_} = Session#session.us,
				      case Username=:=U of 
					      true->
						      io:format("~p~n",[Session]);
					      false->
						      skip
				      end
		      end,Keys).


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

