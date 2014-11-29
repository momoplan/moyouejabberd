-module(aa_hookhandler).
-behaviour(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("aa_data.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-define(HTTP_HEAD,"application/x-www-form-urlencoded").
-define(TIME_OUT,1000*5).

-define(PUSH_PID_NUM, 20).

-define(DBCONNNUM, 20).

-define(ETS_ACK_TASK, ets_ack_task).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,get_id/0]).

%% ====================================================================
%% API functions
%% ====================================================================

-export([
	 start_link/0,
	 user_send_packet_handler/3,
	 offline_message_hook_handler/3,
	 roster_in_subscription_handler/6,
	 user_receive_packet_handler/4,
	 refresh_bak_info/0,
	 rlcfg/0
	]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Host) ->
	lists:foreach(
	  fun(Host) ->
			  ejabberd_hooks:delete(user_send_packet,Host,?MODULE, user_send_packet_handler ,80),
			  ejabberd_hooks:delete(roster_in_subscription,Host,?MODULE, roster_in_subscription_handler ,90),
			  ejabberd_hooks:delete(offline_message_hook, Host, ?MODULE, offline_message_hook_handler, 45),
			  ejabberd_hooks:delete(user_receive_packet, Host, ?MODULE, user_receive_packet_handler, 45)

	  end, ?MYHOSTS),
    exit(whereis(?MODULE), stop),
	ok.



refresh_bak_info() ->
	gen_server:call(?MODULE, refresh_bak).

rlcfg() ->
	gen_server:call(?MODULE, reload_config).

%% Message 有时是长度大于1的列表，所以这里要遍历
%% 如果列表中有多个要提取的关键字，我就把他们组合成一个 List
%% 大部分时间 List 只有一个元素
feach_message([Element|Message],List) ->
	case Element of 
		{xmlelement,"body",_,_} ->
			feach_message(Message,[get_text_message_form_packet_result(Element)|List]);
		_ ->
			feach_message(Message,List)
	end;
feach_message([],List) ->
	List.

%% 获取消息包中的文本消息，用于离线消息推送服务
get_text_message_from_packet( Packet )->
	{xmlelement,"message",_,Message } = Packet,
	%% Message 结构不固定，需要遍历
	List = feach_message(Message,[]),
	?DEBUG("~p ==== ~p ",[liangc_debug_offline_message,List]),
	List.

%% 获取消息包中的文本消息，用于离线消息推送服务
get_text_message_form_packet_result( Body )->
	{xmlelement,"body",_,List} = Body,
	Res = lists:map(fun({_,V})-> binary_to_list(V) end,List),                                       
	ResultMessage = binary_to_list(list_to_binary(Res)), 
	ResultMessage.	

%% 离线消息处理器
%% 钩子回调
offline_message_hook_handler(From, To, Packet) ->
	gen_server:cast(?MODULE, {deal_offline_msg, From, To, Packet}).

deal_offline_msg(From, To, Packet) ->
	try
		?DEBUG("FFFFFFFFFFFFFFFFF===From=~p~nTo=~p~nPacket=~p~n",[From, To, Packet]),
		{xmlelement,"message",Header,_ } = Packet,

		D = dict:from_list(Header),
		V = dict:fetch("msgtype", D),
		case V of
			"msgStatus" ->
				ok;
			_->
				MID = case dict:is_key("id", D) of
						  true ->
							  dict:fetch("id", D);
						  _ -> ""
					  end,
				send_offline_message( From, To, Packet, MID,V )
		end
	catch
		_:_ -> ok
	end.

%% 将 Packet 中的 Text 消息 Post 到指定的 Http 服务
%% IOS 消息推送功能
send_offline_message(From ,To ,Packet,MID,MsgType )->
	send_offline_message(From,To,Packet,MID,MsgType,0).	
send_offline_message(From ,To ,Packet,MID,MsgType,N) when N < 3 ->
	{jid,FromUser,Domain,_,_,_,_} = From ,	
	{jid,ToUser,_,_,_,_,_} = To ,	
	%% 取自配置文件 ejabberd.cfg
	HTTPServer =  ejabberd_config:get_local_option({http_server,Domain}),
	%% 取自配置文件 ejabberd.cfg
	HTTPService = ejabberd_config:get_local_option({http_server_service_client,Domain}),
	HTTPTarget = string:concat(HTTPServer,HTTPService),
	Msg = get_text_message_from_packet( Packet ),
	{Service,Method,FN,TN,MSG,MSG_ID,MType} = {
				      list_to_binary("service.uri.pet_user"),
				      list_to_binary("pushMsgApn"),
				      list_to_binary(FromUser),
				      list_to_binary(ToUser),
				      list_to_binary(Msg),
				      list_to_binary(MID),
				      list_to_binary(MsgType)
				     },
	Gid = case MsgType of
		"groupchat" ->
			{xmlelement,"message",Header,_ } = Packet,
			D = dict:from_list(Header),
			GroupID = dict:fetch("groupid", D),
			list_to_binary(GroupID);
		_ ->
			<<"">>
	end,
	ParamObj={obj,[ 
		       {"service",Service},
		       {"method",Method},
		       {"channel",list_to_binary("9")},
		       {"params",{obj,[{"msgtype",MType},{"fromname",FN},{"toname",TN},{"msg",MSG},{"id",MSG_ID},{"groupid",Gid}]} } 
		      ]},
	Form = "body="++http_uri:encode( rfc4627:encode(ParamObj) ),
	try
		?DEBUG("MMMMMMMMMMMMMMMMM===Form=~p~n",[Form]),
		case httpc:request(post,{ HTTPTarget ,[], ?HTTP_HEAD , Form },[],[] ) of   
			{ok, {_,_,Body}} ->
				case rfc4627:decode(Body) of
					{ok,Obj,_Re} -> 
						case rfc4627:get_field(Obj,"success") of
							{ok,false} ->
								{ok,Entity} = rfc4627:get_field(Obj,"entity"),
								?ERROR_MSG("liangc-push-msg error: ~p~n",[binary_to_list(Entity)]);
							_ ->
								?INFO_MSG("liangc_push_offline_ok_id=~p ; Obj=~p",[MID,Obj]),
								ok
						end;
					Other -> 
						?ERROR_MSG("liangc_push_msg_error_id=~p ; Other=~p",[MID,Other]),
						false
				end ;
			{error, Reason} ->
				?INFO_MSG("[ERROR] cause N=~p~nErr=~p~nForm=~p~n",[N,Reason,Form]),
				timer:sleep(200),
				send_offline_message(From,To,Packet,MID,MsgType,N+1)
		end 
	catch 
		_:_ ->
			Err0 = erlang:get_stacktrace(),
			?ERROR_MSG("[ERROR] offline_message_hook_handler N=~p~nErr=~p~nForm=~p~n",[N,Err0,Form]),
			timer:sleep(200),
			send_offline_message(From,To,Packet,MID,MsgType,N+1)	
	end,
	ok;
send_offline_message(From ,To ,Packet,MID,MsgType,3) ->
	?ERROR_MSG("[ERROR] offline_message_hook_handler_lost ~p",[{From ,To ,Packet,MID,MsgType,3}]),
	ok.

%roster_in_subscription(Acc, User, Server, JID, SubscriptionType, Reason) -> bool()
roster_in_subscription_handler(Acc, User, Server, JID, SubscriptionType, Reason) ->
	?DEBUG("~n~p; Acc=~p ; User=~p~n Server=~p ; JID=~p ; SubscriptionType=~p ; Reason=~p~n ", [liangchuan_debug,Acc, User, Server, JID, SubscriptionType, Reason] ),
	{jid,ToUser,Domain,_,_,_,_}=JID,
	?DEBUG("XXXXXXXX===~p",[SubscriptionType]),
	case lists:member(SubscriptionType,[subscribe,subscribed,unsubscribed]) of 
		true -> 
			sync_user(Domain,User,ToUser,SubscriptionType);
		_ ->
			ok
	end,
	true.

%% 好友同步
sync_user(Domain,FromUser,ToUser,SType) ->
	HTTPServer =  ejabberd_config:get_local_option({http_server,Domain}),
	HTTPService = ejabberd_config:get_local_option({http_server_service_client,Domain}),
	HTTPTarget = string:concat(HTTPServer,HTTPService),
	{Service,Method,Channel} = {list_to_binary("service.uri.pet_user"),list_to_binary("addOrRemoveFriend"),list_to_binary("9")},
	{BID,AID,ST} = {list_to_binary(FromUser),list_to_binary(ToUser),list_to_binary(atom_to_list(SType))},
	%% 2013-10-22 : 新的请求协议如下，此处不必关心，success=true 即成功
	%% INPUT {"SubscriptionType":"", "aId":"", "bId":""}
	%% OUTPUT {"success":true,"entity":"OK" }
	PostBody = {obj,[{"service",Service},{"method",Method},{"channel",Channel},{"params",{obj,[{"SubscriptionType",ST},{"aId",AID},{"bId",BID}]}}]},	
	JsonParam = rfc4627:encode(PostBody),
	ParamBody = "body="++JsonParam,
	URL = HTTPServer++HTTPService++"?"++ParamBody,
	?DEBUG("~p: ~p~n ",[liangchuan_debug,URL]),
	Form = lists:concat([ParamBody]),
	case httpc:request(post,{ HTTPTarget, [], ?HTTP_HEAD, Form },[],[] ) of   
		{ok, {_,_,Body}} ->
			case rfc4627:decode(Body) of
				{ok , Obj , _Re } ->
					%% 请求发送出去以后，如果返回 success=false 那么记录一个异常日志就可以了，这个方法无论如何都要返回 ok	
					case rfc4627:get_field(Obj,"success") of
						{ok,false} ->	
							{ok,Entity} = rfc4627:get_field(Obj,"entity"),
							?DEBUG("liangc-sync-user error: ~p~n",[binary_to_list(Entity)]);
						_ ->
							false
					end;
				_ -> 
					false
			end ;
		{error, Reason} ->
			?DEBUG("[ERROR] cause ~p~n",[Reason])
	end,
	?DEBUG("[--OKOKOKOK--] ~p was done.~n",[addOrRemoveFriend]),
	ok.

%roster_out_subscription(Acc, User, Server, JID, SubscriptionType, Reason) -> bool()
%roster_out_subscription_handler(Acc, User, Server, JID, SubscriptionType, Reason) ->
%	true.

%user_send_packet(From, To, Packet) -> ok
user_send_packet_handler(#jid{user=FU,server=FD}=From, To, Packet) ->
	try
		?DEBUG("~n************** my_hookhandler user_send_packet_handler >>>>>>>>>>>>>>>~p~n ",[zhiming_debug]),
		?DEBUG("~n~pFrom=~p ; To=~p ; Packet=~p~n ", [liangchuan_debug,From, To, Packet] ),
		%% From={jid,"cc","test.com","Smack","cc","test.com","Smack"}
		[_,E|_] = tuple_to_list(Packet),
		Domain = FD,
		case E of 
			"message" ->
				{_,"message",Attr,_} = Packet,
				?DEBUG("Attr=~p", [Attr] ),
				D = dict:from_list(Attr),
				T = dict:fetch("type", D),
				MT = case dict:is_key("msgtype",D) of true-> dict:fetch("msgtype",D); _-> "" end,
				%% 理论上讲，这个地方一定要有一个ID，不过如果没有，其实对服务器没影响，但客户端就麻烦了
				SRC_ID_STR = case dict:is_key("id", D) of true -> dict:fetch("id", D); _ -> "" end,
				?DEBUG("SRC_ID_STR=~p", [SRC_ID_STR] ),
				?DEBUG("Type=~p", [T] ),
				ACK_FROM = case catch ejabberd_config:get_local_option({ack_from ,Domain}) of true -> true; _ -> false end,
				?DEBUG("ack_from=~p ; Domain=~p ; T=~p ; MT=~p",[ACK_FROM,Domain,T,MT]),
				SYNCID = SRC_ID_STR++"@"++Domain,
				
				server_ack(From,To,Packet),
				%% 判断是否群聊消息，不是根据 msgtype 判断的，是根据收消息人判断，这个逻辑很关键
				IS_GROUP_CHAT = case aa_group_chat:is_group_chat(To) of  
									true when MT=/="msgStatus" ->
										?DEBUG("###### send_group_chat_msg ###### From=~p ; Domain=~p",[From,Domain]),
										aa_group_chat:route_group_msg(From,To,Packet),
										true;
									true when MT=:="msgStatus" -> false;
									false -> false 
								end,
				?DEBUG("IS_GROUP_CHAT=~p ; SRCID=~p; MT=~p, FU=~p; ACK_FROM=~p",[IS_GROUP_CHAT,SYNCID,MT,FU, ACK_FROM]),
				if IS_GROUP_CHAT=:=false,ACK_FROM,MT=/=[],MT=/="msgStatus", MT=/="frienddynamicmsg",FU=/="messageack" ->
%% 						if IS_GROUP_CHAT=:=false,ACK_FROM,MT=/="msgStatus", MT=/="frienddynamicmsg",FU=/="messageack" ->
					   {M,S,SS} = os:timestamp(),
					   MsgTime = lists:sublist(erlang:integer_to_list(M*1000000000000+S*1000000+SS),1,13),
					   {Tag,E,Attr,Body} = Packet,
					   RAttr0 = [{K,V} || {K, V} <- Attr, K=/="msgTime"],
					   RAttr1 = [{"msgTime",MsgTime}|RAttr0],
					   RPacket = {Tag,E,RAttr1,Body},
					   aa_usermsg_handler:store_msg(SYNCID, From, To, RPacket);
				   IS_GROUP_CHAT=:=false,ACK_FROM,MT=:="msgStatus" ->
					   aa_usermsg_handler:del_msg(SYNCID, From),
					   ack_task({ack,SYNCID});
				   true ->
					   skip
				end;
			_ ->
				?DEBUG("~p", [skip_00] ),
				skip
		end,
		?DEBUG("~n************** my_hookhandler user_send_packet_handler <<<<<<<<<<<<<<<~p~n ",[liangchuan_debug]) 
	catch
		ErrType:Reason ->
			Err = erlang:get_stacktrace(),
			?ERROR_MSG("user_send_packet_handler_error ~p:~p:> ~p",[ErrType, Reason, Err])
	end,
	ok.

user_receive_packet_handler(_JID, #jid{user = FU, server=FD}=From, To, Packet) ->
	[_,E|_] = tuple_to_list(Packet),
	Domain = FD,
	if FU == "messageack" ->
		   skip;
	   true ->
		   case E of 
			   "message" ->
				   ?INFO_MSG("user receive packet ~p", [{From, To, Packet}]),
				   {_,"message",Attr,_} = Packet,
				   D = dict:from_list(Attr),
				   %% 理论上讲，这个地方一定要有一个ID，不过如果没有，其实对服务器没影响，但客户端就麻烦了
				   SRC_ID_STR = case dict:is_key("id", D) of true -> dict:fetch("id", D); _ -> "" end,
				   SYNCID = SRC_ID_STR++"@"++Domain,
				   TPid = erlang:spawn(fun()-> ack_task(SYNCID,From,To,Packet) end),
				   ets:insert(?ETS_ACK_TASK, {SYNCID, TPid});
			   _ ->
				   skip
		   end
	end,
	ok.




%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {
	  ecache_node,
	  ecache_mod=ecache_server,
	  ecache_fun=cmd,
	  push_pids = [],
	  bak_nodes = []
}).

init([]) ->
	?DEBUG("INIT_START >>>>>>>>>>>>>>>>>>>>>>>> ~p",[liangchuan_debug]),  
	lists:foreach(
	  fun(Host) ->
			  ?INFO_MSG("#### _begin Host=~p~n",[Host]),
			  ejabberd_hooks:add(user_send_packet,Host,?MODULE, user_send_packet_handler ,80),
			  ?INFO_MSG("#### user_send_packet Host=~p~n",[Host]),
			  ejabberd_hooks:add(roster_in_subscription,Host,?MODULE, roster_in_subscription_handler ,90),
			  ?INFO_MSG("#### roster_in_subscription Host=~p~n",[Host]),
			  ejabberd_hooks:add(offline_message_hook, Host, ?MODULE, offline_message_hook_handler, 45),
			  ?INFO_MSG("#### offline_message_hook Host=~p~n",[Host]),
			  ejabberd_hooks:add(user_receive_packet, Host, ?MODULE, user_receive_packet_handler, 45),
			  ?INFO_MSG("#### user_receive_packet Host=~p~n",[Host])

	  end, ?MYHOSTS),
	%% 2014-3-4 : 在这个 HOOK 初始化时，启动一个thrift 客户端，同步数据到缓存服务器
	%% 启动5281端口，接收内网回调
	aa_inf_server:start(),
	State = #state{},
	
	ets:new(?ETS_ACK_TASK, [named_table, public, set]),
	
	%% 初始化mnesia表
	State1 = init_mnesia_tables(State),
	
	PushPids = [spawn(fun() ->
							  local_handle_offline_message()
					  end) || _ <- lists:duplicate(?PUSH_PID_NUM, 1)],
	%% init_mysql_connection
	init_msyql_conn(),
	{ok, State1#state{push_pids = PushPids}}.



handle_call(refresh_bak, _From, State) ->
	ejabberd_config:reload_config(),
	State1 = refresh_mnesia_table(State),
	{reply, ok, State1};

handle_call(reload_config, _From, State) ->
	ejabberd_config:reload_config(),
	{reply, reload_ok, State};

handle_call(_Call, _From, State)->
	{reply, ok, State}.

handle_cast({server_ack,#jid{server=FD},_To,Packet},State)->
	Domain = FD,
	{_,"message",Attr,_} = Packet,
	D = dict:from_list(Attr),
	MT = case dict:is_key("msgtype",D) of true-> dict:fetch("msgtype",D); _-> "" end,
	SRC_ID_STR = case dict:is_key("id", D) of true -> dict:fetch("id", D); _ -> "" end,
	ACK_FROM = case ejabberd_config:get_local_option({ack_from ,Domain}) of 
			   true -> true;
			   _ -> false
	end,

	if ACK_FROM and ( (MT=:="normalchat") or (MT=:="groupchat") ) ->
		   %% IS_GROUP_CHAT = aa_group_chat:is_group_chat(To),
		   case dict:is_key("from", D) of 
			   true -> 
				   Attributes = [
						 {"id",get_id()},
						 {"to",dict:fetch("from", D)},
						 {"from","messageack@"++Domain},
						 {"type","normal"},
						 {"msgtype",""},
						 {"action","ack"}
				   ],
				   Child = [{xmlelement, "body", [], [
						{xmlcdata, list_to_binary("{'src_id':'"++SRC_ID_STR++"','received':'true'}")}
				   ]}],
				   %%Answer = {xmlelement,"message",Attributes, []},
				   Answer = {xmlelement, "message", Attributes , Child},
				   FF = jlib:string_to_jid(xml:get_tag_attr_s("from", Answer)),
				   TT = jlib:string_to_jid(xml:get_tag_attr_s("to", Answer)),
				   ?DEBUG("Answer ::::> FF=~p ; TT=~p ; P=~p ", [FF,TT,Answer] ),
				   case catch ejabberd_router:route(FF, TT, Answer) of
					   ok -> 
						   ?DEBUG("Answer ::::> ~p ", [ok] );
					   _ERROR ->
						   ?DEBUG("Answer ::::> error=~p ", [_ERROR] )
				   end,
				   answer;
			   _ ->
				   ?DEBUG("~p", [skip_01] ),
				   skip
		   end;
	   true ->
		   ?DEBUG("~p", [skip_02] ),
		   skip
	end,
	{noreply, State};

handle_cast({deal_offline_msg, From,To,Packet}, State) ->
	case State#state.push_pids of
		[_pid|_] ->
			PushPids = State#state.push_pids;
		_ ->
			PushPids = [spawn(fun() ->
							  local_handle_offline_message()
					  end) || _ <- lists:duplicate(?PUSH_PID_NUM, 1)]
	end,
	
	Pid = random_pushpid(PushPids),
	
	case is_process_alive(Pid) of
		true ->
			Pid ! {offline_msg, From, To, Packet},
			{noreply, #state{push_pids = PushPids}};
		false ->
			NewPids = lists:delete(Pid, PushPids),
			deal_offline_msg(From, To, Packet),
			{noreply, State#state{push_pids = NewPids}}
	end;
handle_cast(_Msg, State) -> 
	{noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%% ====================================================================
%% Internal functions
%% ====================================================================

ack_task({ack,ID})->
	ack_task({do,ack,ID});

ack_task({do,M,ID})->
	try
		OBJ = ets:lookup(?ETS_ACK_TASK, ID),
		case OBJ of
			[{ID, ACKTaskPid}] ->
				ACKTaskPid!M;
			_ ->
				skip
		end
	catch 
		_:_-> 
			Error = erlang:get_stacktrace(),
			?ERROR_MSG("DO_ACK_TASK_ID=~p ; M=~p ; ERROR=~p.",[ID,M,Error]),
			ack_err
	end.

ack_task(ID,From,To,Packet)->
	?INFO_MSG("ACK_TASK_~p ::::> START.",[ID]),
	receive 
		ack ->
			ets:delete(?ETS_ACK_TASK, ID),
			?INFO_MSG("ACK_TASK_~p ::::> ACK.",[ID])
	after ?TIME_OUT -> 
		gen_server:cast(?MODULE, {deal_offline_msg, From, To, Packet})
	end.


server_ack(From,To,Packet)->
	gen_server:cast(?MODULE,{server_ack,From,To,Packet}).


get_id()-> 
	{M,S,SS} = now(), 
	atom_to_list(node())++"_"++integer_to_list(M)++integer_to_list(S)++integer_to_list(SS).


init_mnesia_tables(State) ->
	[Domain|_] = ?MYHOSTS,
	
	case ejabberd_config:get_local_option({store_group_members, Domain}) of
		1 ->
			create_or_copy_table(?GOUPR_MEMBER_TABLE, [{attributes, record_info(fields,?GOUPR_MEMBER_TABLE)}, 
											   {ram_copies, [node()]}], ram_copies);
		_ ->
			skip
	end,
	
	NodeNameList = atom_to_list(node()),
	RamMsgTableName = list_to_atom(NodeNameList ++ "user_message"),
	
	%% 消息表，备份节点添加
	MsgCopyNodes = case ejabberd_config:get_local_option({ram_msg_bak_nodes, Domain}) of
					   undefined ->
						   [];
					   N ->
						   N
				   end,
	mnesia:create_table(RamMsgTableName, [{record_name, user_msg},
										  {attributes, record_info(fields,user_msg)}, 
										  {ram_copies, [node()]},
										  {index, [to]}]),
%% 	mnesia:add_table_index(RamMsgTableName, to),	
	[begin mnesia:add_table_copy(RamMsgTableName, CopyNode, ram_copies),
		   spawn(fun() ->
						 net_adm:ping(CopyNode)
				 end)
	 end || CopyNode <- MsgCopyNodes],
	
	RamMsgListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
	
	%% 用户消息列表
	mnesia:create_table(RamMsgListTableName, [{record_name, user_msg_list},
										  {attributes, record_info(fields,user_msg_list)}, 
										  {ram_copies, [node()]}]),
	
	[mnesia:add_table_copy(RamMsgListTableName, CopyNode, ram_copies)|| CopyNode <- MsgCopyNodes],
	
	%% 用户数据存储表
	case ejabberd_config:get_local_option({store_user_tables_info, Domain}) of
		1 ->
			create_or_copy_table(?MY_USER_TABLES, [{attributes, record_info(fields,?MY_USER_TABLES)}, 
												   {ram_copies, [node()]}], ram_copies);
		_ ->
			skip
	end,
	State#state{bak_nodes = MsgCopyNodes}.

create_or_copy_table(TableName, Opts, Copy) ->
	case mnesia:create_table(TableName, Opts) of
		{aborted,{already_exists,_}} ->
			mnesia:add_table_copy(TableName, node(), Copy);
		_ ->
			skip
	end.

refresh_mnesia_table(#state{bak_nodes = OldMsgCopyNodes} = State) ->
	[Domain|_] = ?MYHOSTS,
	NodeNameList = atom_to_list(node()),
	RamMsgTableName = list_to_atom(NodeNameList ++ "user_message"),
	RamMsgListTableName = list_to_atom(NodeNameList ++ "user_msglist"),
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

init_msyql_conn() ->
	[Domain|_] = ?MYHOSTS,
	case ejabberd_config:get_local_option({mysql_config, Domain}) of
		undefined ->
			throw(no_mysql_connection);
		DBCfg ->
%% 			?ERROR_MSG("dbcfg ~p", [DBCfg]),
			{_, User} = lists:keyfind(user, 1, DBCfg),
			{_, Password} = lists:keyfind(password, 1, DBCfg),
			{_, Host} = lists:keyfind(host, 1, DBCfg),
			{_, DB} = lists:keyfind(db, 1, DBCfg),
			{_, Encode} = lists:keyfind(encode, 1, DBCfg),
			{_, Port} = lists:keyfind(port, 1, DBCfg),
			application:start(emysql),
			emysql:add_pool(?DB, ?DBCONNNUM, User, Password, Host, Port, DB, Encode)
	end.

random_pushpid(Pids) ->
	Count = length(Pids),	
	{A, B, C} = os:timestamp(),
	random:seed(A, B,C),
	Index = random:uniform(Count),
	lists:nth(Index, Pids).

local_handle_offline_message() ->
	receive
		{offline_msg, From, To, Packet} ->
			deal_offline_msg(From,To,Packet),
			local_handle_offline_message()
	end.
