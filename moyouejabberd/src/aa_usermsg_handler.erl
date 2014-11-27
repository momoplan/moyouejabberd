%% @author songzhiming
%% @doc @todo Add description to aa_usermsg_handler.


-module(aa_usermsg_handler).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

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
		 get_offline_msg/2]).

-export([dump/1,
		load/1,
		 load_all/0]).

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
	PidName = get_userpid_name(Jid),
	Pid = whereis(PidName),
	case is_pid(Pid) of
		true ->
			Pid1 = Pid;
		false ->
			{ok, Pid1} = start(PidName)
	end,
	gen_server:call(Pid1, {dump, Jid}),
	gen_server:cast(Pid1, stop).

load(Jid) ->
	PidName = get_userpid_name(Jid),
	Pid = whereis(PidName),
	case is_pid(Pid) of
		true ->
			%% 防止出现load 在stop消息之后发出的情况
			case catch gen_server:call(Pid, {load, Jid}) of
				{ok, ok} ->
					ok;
				_ ->
					ok = load_message_from_mysql(Jid)
			end;
		false ->
			ok = load_message_from_mysql(Jid) 
	end,
	ok.

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
	?INFO_MSG("aa user msg rcv store msg call ~p", [{Key, From, To, Packet}]),	
	store_message(Key, format_user_data(From), format_user_data(To), Packet),
	aa_msg_statistic:add(),
	?INFO_MSG("store msg finish", []).

del_msg(Key, UserJid1) ->
	?INFO_MSG("aa user msg rcv del msg call ~p", [Key]),
	delete_message(Key,format_user_data(UserJid1)),
	aa_msg_statistic:del(),
	?INFO_MSG("del msg finish", []).

get_offline_msg(Range, UserJid1) ->
	UserJid = format_user_data(UserJid1),
	#?MY_USER_TABLES{msg_table = TableName, msg_list_table = RamMsgListTableName} =
		 get_user_tables(UserJid),
	case mnesia:dirty_read(RamMsgListTableName, UserJid) of
		[] ->
			load(UserJid),
			get_offline_msg(Range, UserJid);
		[#user_msg_list{msg_list = []}] ->
			{ok, []};
		[#user_msg_list{msg_list = KeysList} = UM] ->
			case lists:reverse(KeysList) of
				[-1|_] ->%% 有一部分数据被写入数据库了					
					load(UserJid),
					get_offline_msg(Range, UserJid);
				_ ->
					AvaliableList 
						= lists:filter(fun(Key) ->
											   case mnesia:dirty_read(TableName, Key) of
												   [_] ->
													   true;
												   _ ->
													   false
											   end 
									   end, KeysList),
					F = fun() ->
								mnesia:write({RamMsgListTableName, UM#user_msg_list{msg_list = AvaliableList}, write})
						end,
					mnesia:transaction(F),
					TotalCount = length(AvaliableList),
					
					if TotalCount > Range andalso Range /=0 ->
						   MsgsIds = lists:sublist(AvaliableList, Range);
					   true ->
						   MsgsIds = AvaliableList
					end,
					?INFO_MSG("aa usermsg offline ids ~p", [MsgsIds]),
					%% 保证有消息，保证是倒序的
					Msgs =
						lists:foldl(fun(Key, MList) ->
											case mnesia:dirty_read(TableName, Key) of
												[M] ->
													[M|MList];
												_ ->
													MList
											end
									end, [], MsgsIds),
					{ok, Msgs}
			end;
		_ ->
			{ok, []}
	end.

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

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {}).

init([]) ->
	
%% 放弃每个用户一个进程，这些代码无用，注释
%% 	PidName = get_userpid_name(Jid),
%% 	Data = #?MY_USER_MSGPID_INFO{user = PidName, pid = self(), node = node()},
%% 	mnesia:transaction(fun() -> mnesia:write(?MY_USER_MSGPID_INFO, Data, write) end),
%% 	erlang:send_after(?CHECK_EXPIRE_PERIOD, self(), ?MSG_EXPRIRE),
    {ok, #state{}}.

handle_call({dump, Jid}, _From, State) ->
	ValidJid = format_user_data(Jid),
	case mnesia:dirty_read(?MY_USER_TABLES, ValidJid) of
		[#?MY_USER_TABLES{msg_table = TableName, msg_list_table = RamMsgListTableName}] ->
			case mnesia:dirty_read(RamMsgListTableName, ValidJid) of
				[#user_msg_list{msg_list = KeysList}] ->					
					mnesia:transaction(fun() -> mnesia:delete({RamMsgListTableName, ValidJid}) end),
					AvaliableList 
						= lists:filter(fun(-1) ->
											   false;
										  (Key) ->
											   case mnesia:dirty_read(TableName, Key) of
												   [_] ->
													   true;
												   _ ->
													   false
											   end 
									   end, KeysList),
					write_messages_to_sql(Jid, AvaliableList),
					CleanMsgF = fun() ->
										[mnesia:delete({TableName, Key}) || Key <- AvaliableList]
								end,
					mnesia:transaction(CleanMsgF),
					F = fun() ->
								case mnesia:dirty_read(RamMsgListTableName, ValidJid) of
									[#user_msg_list{msg_list = KeysList1}] ->
										NewMsgList = KeysList1 ++ [-1];
									_ ->
										NewMsgList = [-1]
								end,
								NewData = #user_msg_list{id = ValidJid, msg_list = NewMsgList},
								mnesia:write(RamMsgListTableName, NewData, write)
						end,
					mnesia:transaction(F);
				_ ->
					skip
			end;	
		_ ->
			skip
	end,
	{reply, ok, State};

handle_call({load, Jid}, _From, State) ->
	ok = load_message_from_mysql(Jid),
	{reply, {ok, ok}, State};

%% 放弃每个用户一个进程，这些代码无用，注释
%% handle_call({store_msg,Key, From, To, Packet}, _From, State) ->
%% 	store_message(Key, From, To, Packet),
%% 	{reply, ok, State};
%% 
%% handle_call({del_msg, Key, UserJid}, _From, State) ->
%% 	delete_message(Key, UserJid),
%% 	{reply, ok, State};
%% 
%% handle_call({get_offline_msg, Range, UserJid}, _From, State) ->
%% 	#?MY_USER_TABLES{msg_table = TableName, msg_list_table = RamMsgListTableName} =
%% 		 get_user_tables(UserJid),
%% 	Msgs = 
%% 		case mnesia:dirty_read(RamMsgListTableName, UserJid) of
%% 			[] ->
%% 				[];
%% 			[#user_msg_list{msg_list = []}] ->
%% 				[];
%% 			[#user_msg_list{msg_list = KeysList} = UM] ->
%% 				AvaliableList 
%% 					= lists:filter(fun(Key) ->
%% 										   case mnesia:dirty_read(TableName, Key) of
%% 											   [_] ->
%% 												   true;
%% 											   _ ->
%% 												   false
%% 										   end 
%% 								   end, KeysList),
%% 				mnesia:dirty_write(RamMsgListTableName, UM#user_msg_list{msg_list = AvaliableList}),
%% 				TotalCount = length(AvaliableList),
%% 				
%% 				if TotalCount > Range andalso Range /=0 ->
%% 					   MsgsIds = lists:sublist(AvaliableList, Range);
%% 				   true ->
%% 					   MsgsIds = AvaliableList
%% 				end,
%% 				?INFO_MSG("aa usermsg offline ids ~p", [MsgsIds]),
%% 				%% 保证有消息，保证是倒序的
%% 				lists:foldl(fun(Key, MList) ->
%% 									case mnesia:dirty_read(TableName, Key) of
%% 											[M] ->
%% 												[M|MList];
%% 											_ ->
%% 												MList
%% 										end
%% 							end, [], MsgsIds);
%% 			_ ->
%% 				[]
%% 		end,
%% 	?INFO_MSG("mnesia:dirty_read(~p, ~p)", [RamMsgListTableName, UserJid]),
%% 	?INFO_MSG("user ~p offline msg ~p",[UserJid, Msgs]),
%% 	{reply, {ok, Msgs}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.




handle_cast(stop, State) ->
	{stop, normal, State};

handle_cast(test_cfg, State) ->
	[Domain|_] = ?MYHOSTS,
	MsgCopyNodes = case ejabberd_config:get_local_option({ram_msg_bak_nodes, Domain}) of
					   undefined ->
						   [];
					   N ->
						   N
				   end,
	io:format("BakNodes ~p", [MsgCopyNodes]),
	{noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(?MSG_EXPRIRE, State) ->
%% 	TableName = State#state.tabel_name,
%% 	CurTime = unixtime(),
%% 	Q = qlc:q([Id || #user_msg{id = Id, expire_time = ETime} <- mnesia:table(TableName),
%% 					 ETime =< CurTime]),
%% 	ExprieMsgIds = qlc:e(Q),
%% 	[delete_message(Id) || Id <- ExprieMsgIds],
%% 	erlang:send_after(?CHECK_EXPIRE_PERIOD, self(), ?MSG_EXPRIRE),
%% 	?INFO_MSG("aa user msg check expire finish", []),
	{noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_user_tables(UserJid) ->
	case mnesia:dirty_read(?MY_USER_TABLES, UserJid) of
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
	#?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName} =
		 get_user_tables(To),
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
				mnesia:dirty_write(TableName, Data),
				case mnesia:dirty_read(ListTableName, To) of
					[UserMsgList] ->
						OldList = UserMsgList#user_msg_list.msg_list,
						NewListData = UserMsgList#user_msg_list{msg_list = [Key|OldList]};
					_ ->
						NewListData = #user_msg_list{id = To, msg_list = [Key]}
				end,
				mnesia:dirty_write(ListTableName, NewListData)
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
	#?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName} =
		 get_user_tables(UserJid),
	F = fun() ->
				mnesia:delete({TableName, Key}),
				case mnesia:read(ListTableName, UserJid) of
					[#user_msg_list{msg_list = KeyList}] ->
						case KeyList of
							[Key|Rest] ->
								NewListData = #user_msg_list{id = UserJid, msg_list = Rest};
							_ ->
								NewListData = #user_msg_list{id = UserJid, msg_list = lists:delete(Key, KeyList)}
						end,
						mnesia:write(ListTableName, NewListData, write);
					_ ->
						skip
				end
		end,
	mnesia:transaction(F).

format_user_data(Jid) ->
	Jid#jid{resource = [], lresource = []}.


get_userpid_name(#jid{user = Uid, server = Domain}) ->
	list_to_atom(Uid ++ "@" ++ Domain).

%% get_userpid(Jid) ->
%% 	PidName = get_userpid_name(Jid),
%% 	case mnesia:dirty_read(?MY_USER_MSGPID_INFO, PidName) of
%% 		[#?MY_USER_MSGPID_INFO{pid = Pid, node = Node}] ->
%% 			case rpc:call(Node, erlang, is_process_alive, Pid) of
%% 				true ->
%% 					{Pid, Node};
%% 				_ ->
%% 					mnesia:sync_dirty(fun() -> mnesia:delete({?MY_USER_MSGPID_INFO, PidName}) end),
%% 					{ok, Pid1} = start_link(Jid),
%% 					{Pid1, node()}
%% 			end;
%% 		_ ->
%% 			{ok, Pid1} = start_link(Jid),
%% 			{Pid1, node()}
%% 	end.

load_message_from_mysql(Jid) ->
	Name = get_userpid_name(Jid),
	ValidJid = format_user_data(Jid),
	#?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName} =
		 get_user_tables(Jid),
	Sql = io_lib:format("select * from messages where jid='~s' order by id",[Name]),
	F = fun([_Id, _JId, Content, TimeStamp], KList) ->
				%%Jid1 = binary_to_term(JId),
				{Key, From, To, Packet} = binary_to_term(Content),
				Data = #user_msg{id = Key, 
								 from = From, 
								 to = To, 
								 packat = Packet, 
								 timestamp = TimeStamp, 
								 expire_time = 0,
								 score = index_score()},	
				mnesia:dirty_write(TableName, Data),
				[Key|KList]
		end,
	case db_sql:get_all(Sql) of
		[] ->
			LoadKeyList = [],
			ok;
		DataList when is_list(DataList) ->
			KList1 = lists:foldl(F, [], DataList),
			LoadKeyList = lists:reverse(KList1),
			ok
	end,
	F1 = fun() ->
				case mnesia:read(ListTableName, ValidJid) of
					[#user_msg_list{msg_list = MList} = Data] ->
						case lists:reverse(MList) of
							[-1|Rest] ->
								MList1 = lists:reverse(Rest);
							_ ->
								MList1 = MList
						end,
						mnesia:write(ListTableName, Data#user_msg_list{msg_list = MList1 ++ LoadKeyList}, write);
					_ ->
						if LoadKeyList == [] ->
							   skip;
						   true ->
							   NewData = #user_msg_list{id = ValidJid, msg_list = LoadKeyList},
							   mnesia:write(ListTableName, NewData, write)
						end
				end
		end,
	mnesia:transaction(F1),
	Sql1 = io_lib:format("select from messages where jid='~s'",[Name]),
	db_sql:execute(Sql1),
	ok.

write_messages_to_sql(_Jid, [])->
	ok;
write_messages_to_sql(Jid, KeyList) ->
	Name = get_userpid_name(Jid),
	#?MY_USER_TABLES{msg_table = TableName} = get_user_tables(Jid),
	Count = length(KeyList),
	if Count > 50 ->
		   {WriteList, Rest} = lists:split(50, KeyList);
	   true ->
		   WriteList = KeyList,
		   Rest = []
	end,
	F = fun(#user_msg{id = Key, from = From, to = To, packat = Packet, timestamp = TimeStamp}) ->
				Content = term_to_binary({Key, From, To, Packet}),
				io_lib:format("('~s', '~s', ~p)", [Name, Content, TimeStamp])
		end,
	Datas = [begin
				 [Message] = mnesia:dirty_read(TableName, Key), 
				 F(Message)
			 end || Key <- WriteList],
	Bodys = implode(",", Datas),
	Sql = "insert into messages(`jid`, `content`, `createDate`) values" ++ Bodys,
	db_sql:execute(Sql),
	write_messages_to_sql(Jid, Rest).

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


start(Name) ->
	gen_server:start({local, Name}, ?MODULE, [], []).