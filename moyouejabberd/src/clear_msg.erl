
-module(clear_msg).

-include("aa_data.hrl").
-include("jlib.hrl").
-record(session, {sid, usr, us, priority, info}).


-export([clear_unlist_msg/1,
		 status/1,
		 clean_session/0]).

clean_session() ->
	Keys = mnesia:dirty_all_keys(session),
	DNum = lists:foldl(fun(K, DelNum)->
							case mnesia:dirty_read(session, K) of
								[#session{sid = {_, Pid}}] ->
									IsPid = rpc:call(node(Pid), erlang, is_pid, [Pid]),
									IsProcessAlive = rpc:call(node(Pid), erlang, is_process_alive, [Pid]),
									case IsPid andalso IsProcessAlive of
										true ->
											DelNum;
										false ->
											mnesia:dirty_delete(session, K),
											DelNum + 1
									end;
								_ -> 
									DelNum							
							end
					end, 0,Keys),
	{total_delete_session_number, DNum}.

status(NodeName) when is_atom(NodeName) ->
	NodeNameList = atom_to_list(NodeName),
	status(NodeNameList);
status(NodeNameList) when is_list(NodeNameList) ->
	TableName = list_to_atom(NodeNameList ++ "user_message"),
	ListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
	ets:new(from_user, [named_table, public, set]),
	ets:new(to_user, [named_table, public, set]),
	Keys = mnesia:dirty_all_keys(TableName),
	[begin
		 case mnesia:dirty_read(TableName, Key) of
			 [#user_msg{to = User, from = From}] ->
%% 				 #jid{user = ToUser} = User,
				 #jid{user = FromUser} = From,
				 case ets:lookup(from_user, FromUser) of
					 [] ->
						 ets:insert(from_user, {FromUser, 1});
					 [{FromUser, Num}] ->
						 ets:insert(from_user, {FromUser, Num + 1})
				 end,
				 case ets:lookup(to_user, User) of
					 [] ->
						 ets:insert(to_user, {User, 1});
					 [{User, Num1}] ->
						 ets:insert(to_user, {User, Num1 + 1})
				 end;
			 [] ->
				 skip
		 end
	 end || Key <- Keys],
	AllFromStatus = ets:tab2list(from_user),
	SortFrom = lists:keysort(2, AllFromStatus),
	FromRank = lists:sublist(lists:reverse(SortFrom), 10),
	AllToStatus = ets:tab2list(to_user),
	SortTo = lists:keysort(2, AllToStatus),
	ToRank = lists:sublist(lists:reverse(SortTo), 10),
	ets:delete(from_user),
	ets:delete(to_user),
	ToRank1 = lists:map(fun({#jid{user = ToUser} = TUser, TNum}) ->
								case mnesia:dirty_read(ListTableName, TUser) of
									[#user_msg_list{msg_list = MsgList}] ->
										RealLen = length(MsgList);
									_ ->
										RealLen = 0
								end,
								{ToUser, TNum, in_list, RealLen}
						end, ToRank),
	[{from, FromRank}, {to, ToRank1}].
	

clear_unlist_msg(NodeName) when is_atom(NodeName) ->
	NodeNameList = atom_to_list(NodeName),
	clear_unlist_msg(NodeNameList);
clear_unlist_msg(NodeNameList) when is_list(NodeNameList) ->
	TableName = list_to_atom(NodeNameList ++ "user_message"),
	
	case mnesia:dirty_first(TableName) of
		{aborted, Reason} ->
			{aborted, Reason};
		'$end_of_table' ->
			finished;
		Key ->
			clear_unlist_msg(NodeNameList, TableName,Key, 0)
	end.	
clear_unlist_msg(NodeNameList, TableName,Key, DelNum) ->
	Res = mnesia:dirty_next(TableName, Key),
	case check_message_in_list(NodeNameList, TableName, Key, DelNum) of
		skip ->
			DelNum1 = DelNum;
		DelNum1 ->
			ok
	end,
	case Res of
		{aborted, Reason} ->
			{aborted, Reason};
		'$end_of_table' ->
			{finished, total_delete, DelNum};
		NewKey ->			
			clear_unlist_msg(NodeNameList, TableName,NewKey, DelNum1)
	end.

check_message_in_list(NodeNameList, TableName,Key, DelNum) ->
	case mnesia:dirty_read(TableName, Key) of
		[#user_msg{to = User}] ->
			ListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
			case mnesia:dirty_read(ListTableName, User) of
				[#user_msg_list{msg_list = MsgList}] ->
					case lists:member(Key, MsgList) of
						true ->
							skip;
						false ->
							mnesia:dirty_delete(TableName, Key),
							DelNum + 1
					end;
				_ ->
					mnesia:dirty_delete(TableName, Key),
					DelNum + 1
			end;
		_ ->
			skip
	end.