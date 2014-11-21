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

-define(ETS_TABLENAME, ets_tablename).
-define(ETS_KEY_MSGTABLE, msg_table).
-define(ETS_KEY_USER_MSGLIST, user_msglist_table).
-record(tablename, {key, name}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
		 store_msg/4,
		 del_msg/1,
		 get_offline_msg/2]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

store_msg(Key, From, To, Packet) ->
	?INFO_MSG("aa user msg rcv store msg call ~p", [{Key, From, To, Packet}]),	
	store_message(Key, format_user_data(From), format_user_data(To), Packet),
	?INFO_MSG("store msg finish", []).

del_msg(Key) ->
	?INFO_MSG("aa user msg rcv del msg call ~p", [Key]),
	delete_message(Key),
	?INFO_MSG("del msg finish", []).

get_offline_msg(Range, UserJid1) ->
	UserJid = format_user_data(UserJid1),
	[#tablename{name = TableName}] = ets:lookup(?ETS_TABLENAME, ?ETS_KEY_MSGTABLE),
	[#tablename{name = RamMsgListTableName}] = ets:lookup(?ETS_TABLENAME, ?ETS_KEY_USER_MSGLIST),
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

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {tabel_name, msg_list_table, bak_table_name}).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
	Result :: {ok, State}
			| {ok, State, Timeout}
			| {ok, State, hibernate}
			| {stop, Reason :: term()}
			| ignore,
	State :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
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
	
	
	
%% 	erlang:send_after(?CHECK_EXPIRE_PERIOD, self(), ?MSG_EXPRIRE),
	
	%% ets存储表的名字，避免以后频繁的合成
	ets:new(?ETS_TABLENAME, [{keypos, #tablename.key}, named_table, public, set]),
	ets:insert(?ETS_TABLENAME, #tablename{key = ?ETS_KEY_MSGTABLE, name = RamMsgTableName}),
	ets:insert(?ETS_TABLENAME, #tablename{key = ?ETS_KEY_USER_MSGLIST, name = RamMsgListTableName}),
    {ok, #state{tabel_name = RamMsgTableName,
				msg_list_table = RamMsgListTableName}}.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
	Result :: {reply, Reply, NewState}
			| {reply, Reply, NewState, Timeout}
			| {reply, Reply, NewState, hibernate}
			| {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason, Reply, NewState}
			| {stop, Reason, NewState},
	Reply :: term(),
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity,
	Reason :: term().
%% ====================================================================

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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================

handle_cast({del_msg, Key}, State) ->
	delete_message(Key),
	?INFO_MSG("del msg finish", []),
	{noreply, State};

handle_cast({store_msg, Key, From, To, Packet}, State) ->
	store_message(Key, From, To, Packet),
	?INFO_MSG("store msg finish", []),
	{noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================

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


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(_Reason, _State) ->
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

unixtime() ->
    {M, S, _} = erlang:now(),
    M * 1000000 + S.

index_score()-> {M,S,T} = now(),  M*1000000000000+S*1000000+T.


store_message(Key, From, To, Packet) ->
	[#tablename{name = TableName}] = ets:lookup(?ETS_TABLENAME, ?ETS_KEY_MSGTABLE),
	[#tablename{name = ListTableName}] = ets:lookup(?ETS_TABLENAME, ?ETS_KEY_USER_MSGLIST),
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

delete_message(Key) ->
	[#tablename{name = TableName}] = ets:lookup(?ETS_TABLENAME, ?ETS_KEY_MSGTABLE),
	case mnesia:dirty_read(TableName, Key) of
		[#user_msg{to = ToJid}] ->
			mnesia:dirty_delete(TableName, Key),
			
			%% 有较大的概率，被删除的元素是列表里唯一一个元素
			[#tablename{name = ListTableName}] = ets:lookup(?ETS_TABLENAME, ?ETS_KEY_USER_MSGLIST),
			
			case mnesia:dirty_read(ListTableName, ToJid) of
				[#user_msg_list{msg_list = KeyList}] ->
					case KeyList of
						[Key|Rest] ->
							NewListData = #user_msg_list{id = ToJid, msg_list = Rest};
						_ ->
							NewListData = #user_msg_list{id = ToJid, msg_list = lists:delete(Key, KeyList)}
					end,
					mnesia:dirty_write(ListTableName, NewListData);
				_ ->
					skip
			end;
		_ ->
			skip
	end.

format_user_data(Jid) ->
	Jid#jid{resource = [], lresource = []}.