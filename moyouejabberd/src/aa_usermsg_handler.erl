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
		 message_status_info/0,
		 get_offline_msg/1]).

-export([dump/1,
		load/1,
		 load_all/0,
		 rebuild_list_from_msg/0]).

-export([delete_msg_by_from/1,
		 delete_msg_by_from/2,
		 delete_msg_by_from1/1]).

%% dump(Jid) ->
%% 	{Pid, Node} = get_userpid(Jid),
%% 	gen_server:call({Pid, Node}, {dump, Jid}).
%% 
%% load(Jid) ->
%% 	{Pid, Node} = get_userpid(Jid),
%% 	gen_server:call({Pid, Node}, {load, Jid}).

dump(Jid) ->
	ValidJid = format_user_data(Jid),
	F = fun() ->
				case mnesia:dirty_read(?MY_USER_TABLES, ValidJid, write) of
					[#?MY_USER_TABLES{msg_table = TableName, msg_list_table = RamMsgListTableName}] ->
						case mnesia:dirty_read(RamMsgListTableName, ValidJid) of
							[#user_msg_list{msg_list = KeysList}] ->					
								mnesia:delete({RamMsgListTableName, ValidJid}),
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
								[mnesia:delete({TableName, Key}) || Key <- KeysList],
								write_messages_to_sql(Jid, AvaliabelMsgList, TableName);
							_ ->
								skip
						end;	
					_ ->
						skip
				end
		end,
	mnesia:transaction(F).

load(Jid) ->
	F = fun() ->
				UserJid = format_user_data(Jid),
				#?MY_USER_TABLES{msg_table = TableName, msg_list_table = RamMsgListTableName} =
					 get_user_tables(UserJid),
				load_message_from_mysql(UserJid, TableName, RamMsgListTableName)
		end,
	mnesia:transaction(F).

load_all() ->
	NodeNameList = atom_to_list(node()),
	RamMsgListTableName = list_to_atom(NodeNameList ++ "user_msglist"),

	UserKyes = mnesia:dirty_all_keys(RamMsgListTableName),
	lists:foreach(fun(Key) ->
						  case mnesia:dirty_read(RamMsgListTableName, Key) of
							  [#user_msg_list{msg_list = []}] ->
								  skip;
							  [#user_msg_list{msg_list = KeysList}] ->
								  case lists:reverse(KeysList) of
									  [-1|_] ->%% 有一部分数据被写入数据库了					
										  load(Key);
									  _ ->
										  skip
								  end;
							  _ ->
								  skip
						  end
				  end, UserKyes).

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
	?INFO_MSG("store msg finish", []).

del_msg(Key, UserJid1) ->
	?INFO_MSG("aa user msg rcv del msg call ~p", [Key]),
	delete_message(Key,format_user_data(UserJid1)),
	aa_msg_statistic:del(),
	?INFO_MSG("del msg finish", []).

get_offline_msg(UserJid1) ->
	?WARNING_MSG("get offline msg", []),
	UserJid = format_user_data(UserJid1),
	F = fun() ->
				#?MY_USER_TABLES{msg_table = T1, msg_list_table = T2} =
					 get_user_tables(UserJid),
				{T1, T2}
		end,
	{atomic, {TableName, RamMsgListTableName}} = mnesia:transaction(F),
	case mnesia:dirty_read(RamMsgListTableName, UserJid) of
		[] ->
			%% 内存里没有任何列表的数据，这时候可以认为需要到数据库里查找一下数据
			?WARNING_MSG("get user offline msg loop 1", []),
			aa_usermsg_handler:load(UserJid),
			aa_usermsg_handler:get_offline_msg(UserJid);
		[#user_msg_list{msg_list = []}] ->
			{ok, []};
		[#user_msg_list{msg_list = KeysList} = UM] ->
			case lists:reverse(KeysList) of
				[-1|_] ->%% 有一部分数据被写入数据库了	
					aa_usermsg_handler:load(UserJid),
					?WARNING_MSG("get user offline msg loop 2", []),
					aa_usermsg_handler:get_offline_msg(UserJid);
				_ ->
					MsgsIds = recheck_message_ids(TableName, RamMsgListTableName, UM),
					%% 保证有消息，保证是倒序的
					Msgs = load_mnesia_messages(MsgsIds, TableName),						
					{ok, Msgs}
			end;
		_ ->
			{ok, []}
	end.

recheck_message_ids(TableName, RamMsgListTableName, #user_msg_list{msg_list = KeysList} = OldListData) ->
	AvaliableList = lists:filter(fun(Key) ->
							   case mnesia:dirty_read(TableName, Key) of
								   [_] ->
									   true;
								   _ ->
									   false
							   end 
					   end, KeysList),
	F = fun() ->
				mnesia:write({RamMsgListTableName, OldListData#user_msg_list{msg_list = AvaliableList}, write})
		end,
	mnesia:transaction(F),
	AvaliableList.

load_mnesia_messages(MsgsIds, TableName) ->
	lists:foldl(fun(Key, MList) ->
						case mnesia:dirty_read(TableName, Key) of
							[M] ->
								[M|MList];
							_ ->
								MList
						end
				end, [], MsgsIds).

message_status_info() ->
	NodeNameList = atom_to_list(node()),
	RamMsgTableName = list_to_atom(NodeNameList ++ "user_message"),
	RamMsgListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
	
	ets:new(tmp_from_data, [named_table, public, set]),
	ets:new(tmp_to_data, [named_table, public, set]),
	Keys = mnesia:dirty_all_keys(RamMsgTableName),
	lists:foreach(fun(Key) ->
						  case mnesia:dirty_read(RamMsgTableName, Key) of
							  [#user_msg{from = From}] ->
								  case ets:lookup(tmp_from_data, From) of
									  [] ->
										  ets:insert(tmp_from_data, {From, 1});
									  [{From, Num}] ->
										  ets:insert(tmp_from_data, {From, Num + 1})
								  end;
							  _ ->
								  skip
						  end
				  end, Keys),
	
	UserKyes = mnesia:dirty_all_keys(RamMsgListTableName),
	lists:foreach(fun(Key) ->
						  case mnesia:dirty_read(RamMsgListTableName, Key) of
							  [#user_msg_list{msg_list = MsgList}] ->
								  ets:insert(tmp_to_data, {Key, length(MsgList)});
							  _ ->
								  skip
						  end
				  end, UserKyes),
	
	FromDatas = ets:tab2list(tmp_from_data),
	FromDatas1 = lists:keysort(2, FromDatas),
	FromDatas2 = lists:sublist(lists:reverse(FromDatas1), 50),
	
	ToData = ets:tab2list(tmp_to_data),
	ToData1 = lists:keysort(2, ToData),
	ToData2 = lists:sublist(lists:reverse(ToData1), 50),
	ets:delete(tmp_to_data),
	ets:delete(tmp_from_data),
	
	{ok, FromDatas2, ToData2}.

delete_msg_by_from({Uid, Hours}) ->
	[ delete_msg_by_from(Node, {Uid, Hours}) || Node <- [node()|nodes()]];
delete_msg_by_from(UId) ->
	[ delete_msg_by_from(Node, UId) || Node <- [node()|nodes()]].
delete_msg_by_from(Node, Info) ->
	spawn(fun() ->
				  rpc:call(Node,aa_usermsg_handler,delete_msg_by_from1,[Info])
		  end).

delete_msg_by_from1({UId, Hours}) ->
	Pid = erlang:whereis(aa_usermsg_handler),
	Now = unixtime(),
	ExpireTime = Now - Hours * 3600,
	case is_pid(Pid) of
		true ->
			NodeNameList = atom_to_list(node()),
			RamMsgTableName = list_to_atom(NodeNameList ++ "user_message"),
			Keys = mnesia:dirty_all_keys(RamMsgTableName),
			lists:foreach(fun(Key) ->
								  case mnesia:dirty_read(RamMsgTableName, Key) of
									  [#user_msg{from = UserJid, timestamp = TS}] ->
										  if UserJid#jid.user == UId andalso TS < ExpireTime->
												 mnesia:dirty_delete(RamMsgTableName, Key);
											 true ->
												 skip
										  end;
									  _ ->
										  skip
								  end
						  end, Keys);
		_ ->
			skip
	end;

delete_msg_by_from1(UId) ->
	Pid = erlang:whereis(aa_usermsg_handler),
	case is_pid(Pid) of
		true ->
			NodeNameList = atom_to_list(node()),
			RamMsgTableName = list_to_atom(NodeNameList ++ "user_message"),
			Keys = mnesia:dirty_all_keys(RamMsgTableName),
			lists:foreach(fun(Key) ->
								  case mnesia:dirty_read(RamMsgTableName, Key) of
									  [#user_msg{from = UserJid}] ->
										  if UserJid#jid.user == UId ->
												 mnesia:dirty_delete(RamMsgTableName, Key);
											 true ->
												 skip
										  end;
									  _ ->
										  skip
								  end
						  end, Keys);
		_ ->
			skip
	end.

rebuild_list_from_msg() ->
	NodeNameList = atom_to_list(node()),
	RamMsgTableName = list_to_atom(NodeNameList ++ "user_message"),
	RamMsgListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
	
%% 	ets:new(tmp_from_data, [named_table, public, set]),
	ets:new(tmp_to_data, [named_table, public, set]),
	Keys = mnesia:dirty_all_keys(RamMsgTableName),
	lists:foreach(fun(Key) ->
						  case mnesia:dirty_read(RamMsgTableName, Key) of
							  [#user_msg{to = To} = Msg] ->
								  case ets:lookup(tmp_to_data, To) of
									  [] ->
										  ets:insert(tmp_to_data, {To, [Msg]});
									  [{To, MList}] ->
										  ets:insert(tmp_to_data, {To, [Msg|MList]})
								  end;
							  _ ->
								  skip
						  end
				  end, Keys),
	
	MsgList = [{To, lists:reverse(lists:keysort(#user_msg.timestamp, List))} || {To, List} <- ets:tab2list(tmp_to_data)],
	
	[begin KeysList = [Key || #user_msg{id = Key} <- MList],
		   NewData = #user_msg_list{id = To, msg_list = KeysList},
		   mnesia:dirty_write(RamMsgListTableName, NewData)
	 end || {To, MList} <- MsgList],
	ets:delete(tmp_to_data).

%% ====================================================================
%% Internal functions
%% ====================================================================


get_user_tables(UserJid) ->
	case mnesia:read(?MY_USER_TABLES, UserJid, write) of
		[TableInfo] ->
			TableInfo;
		_ ->
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


store_message(Key, From, To, Packet) ->
	[Domain|_] = ?MYHOSTS, 
	OfflineExpireDays = case ejabberd_config:get_local_option({offline_expire_days, Domain}) of
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
	F = fun() ->
				case mnesia:read(?MY_USER_TABLES, To,write) of
					[ #?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName}] ->
						skip;
					_ ->
						NodeNameList = atom_to_list(node()),
						TableName = list_to_atom(NodeNameList ++ "user_message"),			
						ListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
						TableInfo = #?MY_USER_TABLES{id = To,
													 msg_table = TableName, 
													 msg_list_table = ListTableName},
						mnesia:dirty_write(?MY_USER_TABLES, TableInfo)
				end,
				mnesia:write(TableName, Data, write),
				case mnesia:read(ListTableName, To) of
					[UserMsgList] ->
						OldList = UserMsgList#user_msg_list.msg_list,
						NewListData = UserMsgList#user_msg_list{msg_list = [Key|OldList]};
					_ ->
						%% 内存里没有消息列表
						Status = aa_session:check_online(To),
						case Status of
							online -> %% 如果用户在线，则不需要到数据里拉取消息
								NewListData = #user_msg_list{id = To, msg_list = [Key]};
							offline ->%% 如果用户不在线，则认为很有可能数据被全部写入数据库，做个标记
								NewListData = #user_msg_list{id = To, msg_list = [Key, -1]}
						end
				end,
				mnesia:write(ListTableName, NewListData, write)
		end,
	mnesia:transaction(F).

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
	F = fun() ->
				case mnesia:read(?MY_USER_TABLES, UserJid,write) of
					[TableInfo] ->
						#?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName} =TableInfo,
						mnesia:delete({TableName, Key}),
						case mnesia:read(ListTableName, UserJid) of
							[#user_msg_list{msg_list = KeyList}] ->
								case KeyList of
									[Key|Rest] ->
										NewListData = #user_msg_list{id = UserJid, msg_list = Rest};
									_ ->
										NewListData = #user_msg_list{id = UserJid, msg_list = lists:delete(Key, KeyList)}
								end,
								mnesia:write(ListTableName, NewListData);
							_ ->
								skip
						end;
					_ ->
						skip
				end
		end,	
	mnesia:transaction(F).

format_user_data(Jid) ->
	Jid#jid{resource = [], lresource = []}.


get_userpid_name(#jid{user = Uid, server = Domain}) ->
	list_to_atom(Uid ++ "@" ++ Domain).

%% 直接传表进来是因为外层直接做了锁，内层不需要关心锁的事情
load_message_from_mysql(#jid{user = User} = Jid, MsgTableName, ListTableName) ->
	LoadKeyList = load_msg_to_mnesia(MsgTableName, Jid),
	rebuild_user_msglist(Jid, ListTableName,LoadKeyList),
	clear_user_mysql_data(Jid),
	ok.

load_msg_to_mnesia(MsgTableName, Jid) ->
	Name = get_userpid_name(Jid),
	Sql = io_lib:format("select * from messages where jid='~s' order by id",[Name]),
	case db_sql:get_all(Sql) of
		[] ->
			LoadKeyList = [];
		DataList when is_list(DataList) ->
			KList1 = deal_mysql_datas(DataList, MsgTableName),
			LoadKeyList = lists:reverse(KList1)
	end,
	LoadKeyList.

deal_mysql_datas(DataList, MsgTableName) ->
	F = fun(Data, KList) ->
				Msg = format_msg(Data),
				mnesia:write(MsgTableName, Msg, write),
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
	case mnesia:read(ListTableName, Jid) of
		[#user_msg_list{msg_list = MList} = Data] ->
			case lists:reverse(MList) of
				[-1|Rest] ->
					MList1 = lists:reverse(Rest);
				_ ->
					MList1 = MList
			end,
			NewData = Data#user_msg_list{msg_list = MList1 ++ LoadKeyList};
		_ ->
			NewData = #user_msg_list{id = Jid, msg_list = LoadKeyList}
	end,
	?WARNING_MSG("new list data ~p", [NewData]),
	mnesia:write(ListTableName, NewData, write).

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
term_to_bitstring(Term, Default) ->
	case term_to_bitstring(Term) of
		<<"undefined">> ->
			Default;
		X ->
			X
	end.


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

bitstring_to_term(BitString, Default) ->
	case bitstring_to_term(BitString) of
		undefined ->
			Default;
		R ->
			R
	end.

%% term反序列化，bitstring转换为term，e.g., <<"[{a},1]">>  => [{a},1]
bitstring_to_term(undefined) -> undefined;
bitstring_to_term(BitString) ->
    string_to_term(binary_to_list(BitString)).