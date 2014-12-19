%% @author songzhiming
%% @doc @todo Add description to aa_usermsg_handler.


-module(aa_usermsg_handler).

-include("ejabberd.hrl").
-include("aa_data.hrl").
-include("jlib.hrl").

-include_lib("stdlib/include/qlc.hrl").

-define(CHECK_EXPIRE_PERIOD, 1800000).%% 半个小时检查一下过期的消息并且清除
-define(MSG_EXPRIRE, check_expire).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1,
		 store_msg/4,
		 del_msg/2,
		 get_offline_msg/1]).

-export([dump/1,
		load/1]).

dump(Jid) ->
	ValidJid = format_user_data(Jid),
	F = fun() ->
				case mnesia:read(?MY_USER_TABLES, ValidJid, write) of
					[#?MY_USER_TABLES{msg_table = TableName, msg_list_table = RamMsgListTableName}] ->
						case mnesia:dirty_read(RamMsgListTableName, ValidJid) of
							[#user_msg_list{msg_list = KeysList}] ->					
								mnesia:dirty_delete(RamMsgListTableName, ValidJid),
								AvaliabelMsgList
									= lists:filtermap(fun(-1) ->
														   false;
													  (Key) ->
														   case mnesia:dirty_read(TableName, Key) of
															   [Msg] ->
																   {true, Msg};
															   _ ->
																   false
														   end 
												   end, KeysList),
								[mnesia:dirty_delete({TableName, Key}) || Key <- KeysList],
								write_messages_to_sql(Jid, AvaliabelMsgList, TableName);
							_ ->
								skip
						end;	
					_ ->
						skip
				end
		end,
	{T1, _} = erlang:statistics(wall_clock),
%% 	mnesia:transaction(F),
	case mnesia:transaction(F) of
		{atomic, Result} ->
			?INFO_MSG("dump player ~p message to db correctly: ~p", [Jid#jid.user, Result]);
		{aborted, Reason} ->
			?ERROR_MSG("Problem dumping player ~p message to db, Reason:~p", [Jid#jid.user,Reason])
	end,
	{T2, _} = erlang:statistics(wall_clock),
	?ERROR_MSG("dump cost time ~p", [T2 - T1]).

load(Jid) ->
	UserJid = format_user_data(Jid),
	F = fun() ->
				#?MY_USER_TABLES{msg_table = T1, msg_list_table = T2} =
					 get_user_tables(UserJid),
				{T1, T2}
		end,
	case mnesia:transaction(F) of
		{aborted, Reason} ->
			?ERROR_MSG("get offline for user ~p, Reason:~p", [UserJid, Reason]),
			{ok, []};
		{atomic, {TableName, RamMsgListTableName}} ->
			load_message_from_mysql(UserJid, TableName, RamMsgListTableName)
	end.

%% 	F = fun() ->
%% 				UserJid = format_user_data(Jid),
%% 				#?MY_USER_TABLES{msg_table = TableName, msg_list_table = RamMsgListTableName} =
%% 					 get_user_tables(UserJid),
%% 				load_message_from_mysql(UserJid, TableName, RamMsgListTableName)
%% 		end,
%% %% 	mnesia:transaction(F),
%% 	case mnesia:transaction(F) of
%% 		{atomic, Result} ->
%% 			?INFO_MSG("load user mssage correctly: ~p", [Result]);
%% 		{aborted, Reason} ->
%% 			?ERROR_MSG("Problem loading message form db, player:~p Reason:~p", [Jid, Reason])
%% 	end.

start_link(_Jid) ->
	ok.

%% 放弃每个用户一个进程，这些代码无用，注释
%% 	case supervisor:start_child(my_usermsg_pid_sup, {aa_usermsg_handler, 
%% 												{aa_usermsg_handler, start_link, [Jid]}, 
%% 												permanent, 
%% 												3000, 
%% 												worker, 
%% 												[aa_usermsg_handler]}) of
%% 		{ok, Pid} ->
%% 			{ok, Pid};
%% 		{ok, Pid, _} ->
%% 			{ok, Pid};
%% 		_ ->
%% 			throw(fail_to_start_user_msg_pid)
%% 	end.


%% 放弃每个用户一个进程，这些代码无用，注释
%% store_msg(Key, From, To, Packet) ->
%% 	{Pid, Node} = get_userpid(To),
%% 	gen_server:call({Pid, Node}, {store_msg, Key, format_user_data(From), format_user_data(To), Packet}),	
%% 	aa_msg_statistic:add().
%% 
%% del_msg(Key, UserJid1) ->
%% 	{Pid, Node} = get_userpid(UserJid1),
%% 	gen_server:call({Pid, Node}, {del_msg, Key,format_user_data(UserJid1)}),
%% 	aa_msg_statistic:del().
%% 
%% get_offline_msg(Range, UserJid1) ->
%% 	load(UserJid1),
%% 	{Pid, Node} = get_userpid(UserJid1),
%% 	get_server:call({Pid, Node}, {get_offline_msg, Range, format_user_data(UserJid1)}).
	
	

store_msg(Key, From, To, Packet) ->
	?INFO_MSG("aa user msg rcv store msg call ~p", [{Key, From, To}]),	
	store_message(Key, format_user_data(From), format_user_data(To), Packet),
	aa_msg_statistic:add(),
	?INFO_MSG("store msg finish ~p", [Key]).

del_msg(Key, UserJid1) ->
	?INFO_MSG("aa user msg rcv del msg call ~p", [Key]),
	delete_message(Key,format_user_data(UserJid1)),
	aa_msg_statistic:del(),
	?INFO_MSG("del msg finish ~p", [Key]).

get_offline_msg(UserJid1) ->
	UserJid = format_user_data(UserJid1),
	{TableName, ListTableName} = init_user_data(UserJid),
	
	aa_usermsg_handler:load(UserJid),
	
	case mnesia:dirty_read(ListTableName, UserJid) of
		[] ->
			{ok, []};
		[#user_msg_list{msg_list = []}] ->
			{ok, []};
		[#user_msg_list{msg_list = MsgsIds}] ->
			%% 保证有消息，保证是倒序的
			Msgs = load_mnesia_messages(MsgsIds, TableName),						
			{ok, Msgs};
		_ ->
			{ok, []}
	end.
%% 	UserJid = format_user_data(UserJid1),
%% 	F = fun() ->
%% 				#?MY_USER_TABLES{msg_table = T1, msg_list_table = T2} =
%% 					 get_user_tables(UserJid),
%% 				{T1, T2}
%% 		end,
%% 	
%% 	case mnesia:transaction(F) of
%% 		{aborted, Reason} ->
%% 			?ERROR_MSG("get offline for user ~p, Reason:~p", [UserJid1, Reason]),
%% 			{ok, []};
%% 		{atomic, {TableName, RamMsgListTableName}} ->
%% 			aa_usermsg_handler:load(UserJid),
%% 			case mnesia:dirty_read(RamMsgListTableName, UserJid) of
%% 				[] ->
%% 					{ok, []};
%% 				[#user_msg_list{msg_list = []}] ->
%% 					{ok, []};
%% 				[UM] ->
%% 					MsgsIds = recheck_message_ids(TableName, RamMsgListTableName, UM),
%% 					%% 保证有消息，保证是倒序的
%% 					Msgs = load_mnesia_messages(MsgsIds, TableName),						
%% 					{ok, Msgs};
%% 				_ ->
%% 					{ok, []}
%% 			end
%% 	end.

load_mnesia_messages(MsgsIds, TableName) ->
	lists:foldl(fun(Key, MList) ->
						case mnesia:dirty_read(TableName, Key) of
							[M] ->
								[M|MList];
							_ ->
								MList
						end
				end, [], MsgsIds).

%% ====================================================================
%% Internal functions
%% ====================================================================


get_user_tables(UserJid) ->
	case mnesia:read(?MY_USER_TABLES, UserJid, write) of
		[TableInfo] ->
			TableInfo;
		[] ->
			NodeNameList = atom_to_list(node()),
			RamMsgTableName = list_to_atom(NodeNameList ++ "user_message"),			
			RamMsgListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
			TableInfo = #?MY_USER_TABLES{id = UserJid,
										 msg_table = RamMsgTableName, 
										 msg_list_table = RamMsgListTableName},
			mnesia:dirty_write(?MY_USER_TABLES, TableInfo),
			TableInfo		
	end.

unixtime() ->
    {M, S, _} = erlang:now(),
    M * 1000000 + S.

index_score()-> {M,S,T} = now(),  M*1000000000000+S*1000000+T.


store_message(Key, From, #jid{server = Domain}=To, Packet) ->
	OfflineExpireDays = case ejabberd_config:get_local_option({offline_expire_days, Domain},
                                       fun(V) -> V end) of
							undefined ->
								1;
							Days ->
								Days
						end,
	Now = unixtime(),
	ExpireTime = Now + OfflineExpireDays * 24 *3600,
	Data = #user_msg{id = Key, 
					 from = From, 
					 to = To, 
					 packat = Packet, 
					 timestamp = Now, 
					 expire_time = ExpireTime,
					 score = index_score()},
	{TableName, ListTableName} = init_user_data(To),
	mnesia:dirty_write(TableName, Data),
	case mnesia:dirty_read(ListTableName, To) of
		[UserMsgList] ->
			OldList = UserMsgList#user_msg_list.msg_list,
			NewListData = UserMsgList#user_msg_list{msg_list = [Key|OldList]};
		_ ->
			NewListData = #user_msg_list{id = To, msg_list = [Key]}
	end,
	mnesia:dirty_write(ListTableName, NewListData).
%% 	F = fun() ->
%% 				case mnesia:read(?MY_USER_TABLES, To,write) of
%% 					[ #?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName}] ->
%% 						skip;
%% 					[] ->
%% 						NodeNameList = atom_to_list(node()),
%% 						TableName = list_to_atom(NodeNameList ++ "user_message"),			
%% 						ListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
%% 						TableInfo = #?MY_USER_TABLES{id = To,
%% 													 msg_table = TableName, 
%% 													 msg_list_table = ListTableName},
%% 						mnesia:dirty_write(?MY_USER_TABLES, TableInfo)
%% 				end,
%% 				mnesia:dirty_write(TableName, Data),
%% 				case mnesia:dirty_read(ListTableName, To) of
%% 					[UserMsgList] ->
%% 						OldList = UserMsgList#user_msg_list.msg_list,
%% 						NewListData = UserMsgList#user_msg_list{msg_list = [Key|OldList]};
%% 					_ ->
%% 						NewListData = #user_msg_list{id = To, msg_list = [Key]}
%% 				end,
%% 				mnesia:dirty_write(ListTableName, NewListData)
%% 		end,
%% 	case mnesia:transaction(F) of
%% 		{atomic, Result} ->
%% 			?INFO_MSG("packet save correctly: ~p", [Result]);
%% 		{aborted, Reason} ->
%% 			?ERROR_MSG("Problem saving packet:~n~p  reason:~p", [Packet,Reason])
%% 	end.

	

%% store_message(Key, From, To, Packet, TimeStamp) ->
%% 	#?MY_USER_TABLES{msg_table = TableName} = get_user_tables(To),
%% 	Data = #user_msg{id = Key, 
%% 					 from = From, 
%% 					 to = To, 
%% 					 packat = Packet, 
%% 					 timestamp = TimeStamp, 
%% 					 expire_time = 0,
%% 					 score = index_score()},	
%% 	mnesia:dirty_write(TableName, Data).

delete_message(Key, UserJid) ->	
	case mnesia:dirty_read(?MY_USER_TABLES, UserJid) of
		[TableInfo] ->
			#?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName} =TableInfo,
			mnesia:dirty_delete(TableName, Key),
			case mnesia:dirty_read(ListTableName, UserJid) of
				[#user_msg_list{msg_list = KeyList}] ->
					case KeyList of
						[Key|Rest] ->
							NewListData = #user_msg_list{id = UserJid, msg_list = Rest};
						_ ->
							NewListData = #user_msg_list{id = UserJid, msg_list = lists:delete(Key, KeyList)}
					end,
					mnesia:dirty_write(ListTableName, NewListData);
				_ ->
					skip
			end;
		_ ->
			skip
	end.
%% 	F = fun() ->
%% 				case mnesia:read(?MY_USER_TABLES, UserJid,write) of
%% 					[TableInfo] ->
%% 						#?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName} =TableInfo,
%% 						mnesia:dirty_delete(TableName, Key),
%% 						case mnesia:dirty_read(ListTableName, UserJid) of
%% 							[#user_msg_list{msg_list = KeyList}] ->
%% 								case KeyList of
%% 									[Key|Rest] ->
%% 										NewListData = #user_msg_list{id = UserJid, msg_list = Rest};
%% 									_ ->
%% 										NewListData = #user_msg_list{id = UserJid, msg_list = lists:delete(Key, KeyList)}
%% 								end,
%% 								mnesia:dirty_write(ListTableName, NewListData);
%% 							_ ->
%% 								skip
%% 						end;
%% 					_ ->
%% 						skip
%% 				end
%% 		end,	
%% 	case mnesia:transaction(F) of
%% 		{atomic, Result} ->
%% 		    ?INFO_MSG("packet delete correctly: ~p", [Result]);
%% 		{aborted, Reason} ->
%% 		    ?ERROR_MSG("Problem deleting message key ~p for user ~p  reason:~p", [Key, UserJid,Reason])
%% 	    end.

format_user_data(Jid) ->
	Jid#jid{resource = [], lresource = []}.


get_userpid_name(#jid{user = Uid, server = Domain}) ->
	list_to_atom(Uid ++ "@" ++ Domain).

%% 直接传表进来是因为外层直接做了锁，内层不需要关心锁的事情
load_message_from_mysql(Jid, MsgTableName, ListTableName) ->
	LoadKeyList = load_msg_to_mnesia(MsgTableName, Jid),
	rebuild_user_msglist(Jid, ListTableName, LoadKeyList),
	clear_user_mysql_data(Jid),
	ok.

load_msg_to_mnesia(MsgTableName, Jid) ->
	Name = get_userpid_name(Jid),
	Sql = io_lib:format("select * from messages where jid='~s' order by id desc",[Name]),
	case db_sql:get_all(Sql) of
		[] ->
			LoadKeyList = [];
		DataList when is_list(DataList) ->
			LoadKeyList = deal_mysql_datas(DataList, MsgTableName)
	end,
	LoadKeyList.

deal_mysql_datas(DataList, MsgTableName) ->
	F = fun(Data, KList) ->
				Msg = format_msg(Data),
				mnesia:dirty_write(MsgTableName, Msg),
				[Msg#user_msg.id|KList]
		end,
	lists:foldl(F, [], DataList).

format_msg([_Id, _JId, Content, TimeStamp]) ->
	{Key, From, To, Packet1} = bitstring_to_term(Content),
	Packet = binary_to_term(Packet1),
	#user_msg{id = Key, 
			  from = From, 
			  to = To, 
			  packat = Packet, 
			  timestamp = TimeStamp, 
			  expire_time = 0, 
			  score = index_score()
			 }.

clear_user_mysql_data(Jid) ->	
	Name = get_userpid_name(Jid),
	Sql1 = io_lib:format("delete from messages where jid='~s'",[Name]),
	db_sql:execute(Sql1).

rebuild_user_msglist(Jid, ListTableName, LoadKeyList) ->
	case mnesia:dirty_read(ListTableName, Jid) of
		[#user_msg_list{msg_list = MList} = Data] ->			
			NewData = Data#user_msg_list{msg_list = MList ++ LoadKeyList};
		_ ->
			NewData = #user_msg_list{id = Jid, msg_list = LoadKeyList}
	end,
	?DEBUG("new list data ~p", [NewData]),
	mnesia:dirty_write(ListTableName, NewData).

write_messages_to_sql(_Jid, [], _Tablename)->
	ok;
write_messages_to_sql(Jid, AvaliabelMsgList, Tablename) ->
	Name = get_userpid_name(Jid),
	Count = length(AvaliabelMsgList),
	if Count > 50 ->
		   {WriteList, Rest} = lists:split(50, AvaliabelMsgList);
	   true ->
		   WriteList = AvaliabelMsgList,
		   Rest = []
	end,
	F = fun(#user_msg{id = Key, from = From, to = To, packat = Packet, timestamp = TimeStamp}) ->
%% 				?ERROR_MSG("time stapm ~p", [TimeStamp]),
				case TimeStamp of
					{datetime, _} ->
						Ts = 0;
					_ ->
						Ts = TimeStamp
				end,
				Content = term_to_bitstring({Key, From, To, term_to_binary(Packet)}),
				io_lib:format("('~s', '~s', ~p)", [Name, Content, Ts])
		end,
	Datas = [ F(Message) || Message <- WriteList],
	Bodys = implode(",", Datas),
	Sql = "insert into messages(`jid`, `content`, `createDate`) values" ++ Bodys,
	db_sql:execute(Sql),
	write_messages_to_sql(Jid, Rest, Tablename).

%% 在List中的每两个元素之间插入一个分隔符
implode(_S, [])->
	[<<>>];
implode(S, L) when is_list(L) ->
    implode(S, L, []).
implode(_S, [H], NList) ->
    lists:reverse([H | NList]);
implode(S, [H | T], NList) ->
    L = [H| NList],
    implode(S, T, [S | L]).



%% term序列化，term转换为bitstring格式，e.g., [{a},1] => <<"[{a},1]">>
term_to_bitstring(Term) ->
    erlang:list_to_bitstring(io_lib:format("~p", [Term])).


%% term反序列化，string转换为term，e.g., "[{a},1]"  => [{a},1]
string_to_term(String) ->
    case erl_scan:string(String++".") of
        {ok, Tokens, _} ->
            case catch erl_parse:parse_term(Tokens) of
                {ok, Term} -> Term;
                _Err -> undefined
            end;
        _Error ->
            undefined
    end.

%% term反序列化，bitstring转换为term，e.g., <<"[{a},1]">>  => [{a},1]
bitstring_to_term(undefined) -> undefined;
bitstring_to_term(BitString) ->
    string_to_term(binary_to_list(BitString)).

init_user_data(User) ->
	NodeNameList = atom_to_list(node()),
	case mnesia:dirty_read(?MY_USER_TABLES, User) of
		[ #?MY_USER_TABLES{msg_table = TableNameOld, msg_list_table = ListTableNameOld}] ->
			OldNameList = lists:sublist(ListTableNameOld, length(ListTableNameOld) - 12),
			if NodeNameList == OldNameList ->
					TableName = TableNameOld,
					NodeNameList = ListTableNameOld;
				true ->
					{TableName, NodeNameList} = init_table_with_node(User),
					transfer_data(TableNameOld, ListTableNameOld, TableName, NodeNameList, User)
			end;
		[] ->
			{TableName, NodeNameList} = init_table_with_node(User)
	end,
	{TableName, NodeNameList}.

init_table_with_node(User) ->
	NodeNameList = atom_to_list(node()),
	TableName = list_to_atom(NodeNameList ++ "user_message"),			
	ListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
	TableInfo = #?MY_USER_TABLES{id = User,
								 msg_table = TableName, 
								 msg_list_table = ListTableName},
	mnesia:dirty_write(?MY_USER_TABLES, TableInfo),
	{TableName, NodeNameList}.

transfer_data(TableNameOld, ListTableNameOld, TableName, NodeNameList, UserJid) ->
	case mnesia:dirty_read(ListTableNameOld, UserJid) of
		[] ->
			skip;
		[#user_msg_list{msg_list = []}] ->
			mnesia:dirty_delete(ListTableNameOld, UserJid),
			skip;
		[#user_msg_list{msg_list = MsgsIds}] ->
			%% 保证有消息，保证是倒序的
			[begin
				 case mnesia:dirty_read(TableNameOld, Key) of
							[M] ->
								mnesia:dirty_delete(TableNameOld, Key),
								mnesia:dirty_write(TableName, M);
							_ ->
								skip
				 end
			 end || Key <- MsgsIds],
			mnesia:dirty_write(NodeNameList, #user_msg_list{id = UserJid, msg_list = MsgsIds}),
			mnesia:dirty_delete(ListTableNameOld, UserJid),
			ok;
		_ ->
			skip
	end.
