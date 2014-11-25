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
-export([start_link/0,
		 store_msg/4,
		 del_msg/2,
		 refresh_bak_info/0,
		 message_status_info/0,
		 test_cfg/0,
		 get_offline_msg/2]).

-export([delete_msg_by_from/1,
		 delete_msg_by_from/2,
		 delete_msg_by_from1/1]).

refresh_bak_info() ->
	gen_server:call(?MODULE, refresh_bak).

test_cfg() ->
	gen_server:call(?MODULE, test_cfg).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
	Msgs = 
		case mnesia:dirty_read(RamMsgListTableName, UserJid) of
			[] ->
				[];
			[#user_msg_list{msg_list = []}] ->
				[];
			[#user_msg_list{msg_list = KeysList} = UM] ->
				AvaliableList 
					= lists:filter(fun(Key) ->
										   case mnesia:dirty_read(TableName, Key) of
											   [_] ->
												   true;
											   _ ->
												   false
										   end 
								   end, KeysList),
				mnesia:dirty_write(RamMsgListTableName, UM#user_msg_list{msg_list = AvaliableList}),
				TotalCount = length(AvaliableList),
				
				if TotalCount > Range andalso Range /=0 ->
					   MsgsIds = lists:sublist(AvaliableList, Range);
				   true ->
					   MsgsIds = AvaliableList
				end,
				?INFO_MSG("aa usermsg offline ids ~p", [MsgsIds]),
				%% 保证有消息，保证是倒序的
				lists:foldl(fun(Key, MList) ->
									case mnesia:dirty_read(TableName, Key) of
											[M] ->
												[M|MList];
											_ ->
												MList
										end
							end, [], MsgsIds);
			_ ->
				[]
		end,
	?INFO_MSG("mnesia:dirty_read(~p, ~p)", [RamMsgListTableName, UserJid]),
	?INFO_MSG("user ~p offline msg ~p",[UserJid, Msgs]),
	{ok, Msgs}.

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
-record(state, {tabel_name, msg_list_table, bak_table_name, bak_nodes}).

init([]) ->
	[Domain|_] = ?MYHOSTS, 
	NodeNameList = atom_to_list(node()),
	RamMsgTableName = list_to_atom(NodeNameList ++ "user_message"),
	MsgCopyNodes = case ejabberd_config:get_local_option({ram_msg_bak_nodes, Domain}) of
					   undefined ->
						   [];
					   N ->
						   N
				   end,
	mnesia:create_table(RamMsgTableName, [{record_name, user_msg},
										  {attributes, record_info(fields,user_msg)}, 
										  {ram_copies, [node()]}]),
	
	?ERROR_MSG("local config ~p", [ets:tab2list(local_config)]),
	
	[begin mnesia:add_table_copy(RamMsgTableName, CopyNode, ram_copies),
		   spawn(fun() ->
						 net_adm:ping(CopyNode)
				 end)
	 end || CopyNode <- MsgCopyNodes],
	
	RamMsgListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
	
	mnesia:create_table(RamMsgListTableName, [{record_name, user_msg_list},
										  {attributes, record_info(fields,user_msg_list)}, 
										  {ram_copies, [node()]}]),
	
	[mnesia:add_table_copy(RamMsgListTableName, CopyNode, ram_copies)|| CopyNode <- MsgCopyNodes],
	
	init_mnesia_user_table_info(Domain),
	
	
	
%% 	erlang:send_after(?CHECK_EXPIRE_PERIOD, self(), ?MSG_EXPRIRE),
    {ok, #state{tabel_name = RamMsgTableName,
				msg_list_table = RamMsgListTableName,
				bak_nodes = MsgCopyNodes}}.

%% handle_call({get_offline_msg, Range, UserJid}, _From, State) ->
%% 	TableName = State#state.tabel_name,
%% 	AllUserMsgs = mnesia:dirty_index_read(TableName, UserJid, to),
%% 	TotalCount = length(AllUserMsgs),
%% 	if TotalCount > Range andalso Range /=0 ->
%% 		   OrderList = lists:keysort(AllUserMsgs, #user_msg.score),
%% 		   Msgs1 = lists:nthtail(OrderList, TotalCount - Range),
%% 		   Msgs = lists:reverse(Msgs1),
%% 		   ok;
%% 	   true ->
%% 		   Msgs = AllUserMsgs
%% 	end,
%% 	{reply,{ok, Msgs}, State};

handle_call(refresh_bak, _From, State) ->
	ejabberd_config:reload_config(),
	State1 = refresh_mnesia_table(State),
	{reply, ok, State1};

handle_call(test_cfg, _From, State) ->
	ejabberd_config:reload_config(),
	[Domain|_] = ?MYHOSTS,
	MsgCopyNodes = case ejabberd_config:get_local_option({mysql_config, Domain}) of
					   undefined ->
						   [];
					   N ->
						   N
				   end,
	{reply, {ok, MsgCopyNodes}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.



handle_cast({store_msg, Key, From, To, Packet}, State) ->
	store_message(Key, From, To, Packet),
	?INFO_MSG("store msg finish", []),
	{noreply, State};

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



refresh_mnesia_table(#state{tabel_name = RamMsgTableName,
							msg_list_table = RamMsgListTableName,
							bak_nodes = OldMsgCopyNodes} = State) ->
	[Domain|_] = ?MYHOSTS,
	MsgCopyNodes = case ejabberd_config:get_local_option({ram_msg_bak_nodes, Domain}) of
					   undefined ->
						   [];
					   N ->
						   N
				   end,
	{AddNodes, DelNodes} = lists:foldl(fun(Node, {ANodes, DNodes}) ->
											   case lists:member(Node, DNodes) of
												   true ->
													   ANodes1 = ANodes,
													   DNodes1 = lists:delete(Node, DNodes);
												   false ->
													   ANodes1 = [Node|ANodes],
													   DNodes1 = DNodes
											   end,
											   {ANodes1, DNodes1}
									   end, {[], OldMsgCopyNodes}, MsgCopyNodes),
	
	%% 删除表复制
	[begin mnesia:del_table_copy(RamMsgTableName, CopyNode),
		   spawn(fun() ->
						 net_adm:ping(CopyNode)
				 end)
	 end || CopyNode <- DelNodes],
	[mnesia:del_table_copy(RamMsgListTableName, CopyNode)|| CopyNode <- DelNodes],
	
	%% 添加表复制
	[begin mnesia:add_table_copy(RamMsgTableName, CopyNode, ram_copies),
		   spawn(fun() ->
						 net_adm:ping(CopyNode)
				 end)
	 end || CopyNode <- AddNodes],
	
	[mnesia:add_table_copy(RamMsgListTableName, CopyNode, ram_copies)|| CopyNode <- AddNodes],
	State#state{bak_nodes = MsgCopyNodes}.
	

init_mnesia_user_table_info(Domain) ->
	case ejabberd_config:get_local_option({store_user_tables_info, Domain}) of
		1 ->
			create_or_copy_table(?MY_USER_TABLES, [{attributes, record_info(fields,?MY_USER_TABLES)}, 
												   {ram_copies, [node()]}], ram_copies);
		_ ->
			skip
	end.

create_or_copy_table(TableName, Opts, Copy) ->
	case mnesia:create_table(TableName, Opts) of
		{aborted,{already_exists,_}} ->
			mnesia:add_table_copy(TableName, node(), Copy);
		_ ->
			skip
	end.

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
	mnesia:dirty_write(TableName, Data),
	case mnesia:dirty_read(ListTableName, To) of
		[UserMsgList] ->
			OldList = UserMsgList#user_msg_list.msg_list,
			NewListData = UserMsgList#user_msg_list{msg_list = [Key|OldList]};
		_ ->
			NewListData = #user_msg_list{id = To, msg_list = [Key]}
	end,
	?INFO_MSG("storem msg update list ~p", [NewListData]),
	mnesia:dirty_write(ListTableName, NewListData).

delete_message(Key, UserJid) ->
	#?MY_USER_TABLES{msg_table = TableName, msg_list_table = ListTableName} =
		 get_user_tables(UserJid),
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
	end.

format_user_data(Jid) ->
	Jid#jid{resource = [], lresource = []}.