-module(aa_hookhandler).
-behaviour(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("aa_data.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-define(HTTP_HEAD,"application/x-www-form-urlencoded").
-define(TIME_OUT,1000*5).

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
	 sm_register_connection_hook_handler/3,
	 sm_remove_connection_hook_handler/3,
	 user_available_hook_handler/1
	]).

-record(dmsg,{mid,pid}).

sm_register_connection_hook_handler(SID, JID, Info) -> ok.
sm_remove_connection_hook_handler(SID, JID, Info) -> ok.
user_available_hook_handler(#jid{server=Domain}=JID) -> 
	%% 统计并发量
	?DEBUG("##### counter_log ::::> ~p",[JID]),
	try
		[{Total}] = aa_session:total_count_user(Domain),		
		log({counter,Domain,Total}) 
	catch
		_:_ ->
			Err = erlang:get_stacktrace(),
			?DEBUG("##### counter_log ERROR ::::> ~p",[Err]) 
	end,
	ok.

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
offline_message_hook_handler(#jid{user=FromUser}=From, #jid{server=Domain}=To, Packet) ->
	try
		?DEBUG("FFFFFFFFFFFFFFFFF===From=~p~nTo=~p~nPacket=~p~n",[From, To, Packet]),
		{xmlelement,"message",Header,_ } = Packet,
		%%这里只推送 msgtype=normalchat 的消息，以下是判断
		D = dict:from_list(Header),
		V = dict:fetch("msgtype", D),
		case V of
			"msgStatus" ->
				ok;
			_->
				if FromUser=/="messageack" ->
					   %% 2014-3-5 : 当消息离线时，要更改存储模块中对应的消息状态
					   MID = case dict:is_key("id", D) of
							 true ->
								 ID = dict:fetch("id", D),
								 ack_task({offline,ID}),
								 ID;
							 _ -> ""
							 end,
					   %% 回调webapp
					   case catch ejabberd_config:get_local_option({ack_from ,Domain}) of
						   true->
							   case aa_group_chat:is_group_chat(To) of
								   true ->
									   skip;
								   false ->
									   offline_message_hook_handler( From, To, Packet, D, MID,V )
							   end,
							   ok;
						   _->
							   %% 宠物那边走这个逻辑
							   case V of 
								   "normalchat" -> 
									   offline_message_hook_handler( From, To, Packet, D, MID,V ); 
								   _-> 
									   skip 
							   end
					   end;
				   true->
					   ok
				end
		end
	catch
		_:_ -> ok
	end.


offline_message_hook_handler(From, To, Packet,D,ID,MsgType ) ->
	try
		V = dict:fetch("fileType", D),
		send_offline_message(From ,To ,Packet,V,ID,MsgType )
	catch
		_:_ -> send_offline_message(From ,To ,Packet,"",ID,MsgType )
	end,
	ok.

%% 将 Packet 中的 Text 消息 Post 到指定的 Http 服务
%% IOS 消息推送功能
send_offline_message(From ,To ,Packet,Type,MID,MsgType )->
	send_offline_message(From,To,Packet,Type,MID,MsgType,0).	
send_offline_message(From ,To ,Packet,Type,MID,MsgType,N) when N < 3 ->
	{jid,FromUser,Domain,_,_,_,_} = From ,	
	{jid,ToUser,_,_,_,_,_} = To ,	
	%% 取自配置文件 ejabberd.cfg
	HTTPServer =  ejabberd_config:get_local_option({http_server,Domain}),
	%% 取自配置文件 ejabberd.cfg
	HTTPService = ejabberd_config:get_local_option({http_server_service_client,Domain}),
	HTTPTarget = string:concat(HTTPServer,HTTPService),
	Msg = get_text_message_from_packet( Packet ),
	{Service,Method,FN,TN,MSG,T,MSG_ID,MType} = {
				      list_to_binary("service.uri.pet_user"),
				      list_to_binary("pushMsgApn"),
				      list_to_binary(FromUser),
				      list_to_binary(ToUser),
				      list_to_binary(Msg),
				      list_to_binary(Type),
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
		       {"params",{obj,[{"msgtype",MType},{"fromname",FN},{"toname",TN},{"msg",MSG},{"type",T},{"id",MSG_ID},{"groupid",Gid}]} } 
		      ]},
	Form = "body="++http_uri:encode( rfc4627:encode(ParamObj) ),
	try
		?DEBUG("MMMMMMMMMMMMMMMMM===Form=~p~n",[Form]),
%% 		case httpc:request(post,{ HTTPTarget ,[], ?HTTP_HEAD , Form },[],[] ) of   
		case httpc:request(post,{ HTTPTarget ,[], ?HTTP_HEAD , Form },[{version, "HTTP/1.0"}],[] ) of   
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
				?ERROR_MSG("[ERROR] cause N=~p~nErr=~p~nForm=~p~n",[N,Reason,Form]),
				timer:sleep(200),
				send_offline_message(From,To,Packet,Type,MID,MsgType,N+1)
		end 
	catch 
		_:_ ->
			Err0 = erlang:get_stacktrace(),
			?ERROR_MSG("[ERROR] offline_message_hook_handler N=~p~nErr=~p~nForm=~p~n",[N,Err0,Form]),
			timer:sleep(200),
			send_offline_message(From,To,Packet,Type,MID,MsgType,N+1)	
	end,
	ok;
send_offline_message(From ,To ,Packet,Type,MID,MsgType,3) ->
	?ERROR_MSG("[ERROR] offline_message_hook_handler_lost ~p",[{From ,To ,Packet,Type,MID,MsgType,3}]),
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
				case dict:is_key("type", D) of
					false ->
						?DEBUG("packet do not have attr `type`,so skip", []);
					true ->
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
%% 							   SyncRes = gen_server:call(?MODULE,{sync_packet,SYNCID,From,To,Packet}),
							   
							   {M,S,SS} = os:timestamp(),
							   MsgTime = lists:sublist(erlang:integer_to_list(M*1000000000000+S*1000000+SS),1,13),
							   {Tag,E,Attr,Body} = Packet,
							   RAttr0 = [{K,V} || {K, V} <- Attr, K=/="msgTime"],
							   RAttr1 = [{"msgTime",MsgTime}|RAttr0],
							   RPacket = {Tag,E,RAttr1,Body},
							   %% add {K,V} to zset
							   aa_usermsg_handler:store_msg(SYNCID, From, To, RPacket),
							   
							   ?DEBUG("==> SYNC_RES new => ID=~p",[SRC_ID_STR]),
							   ack_task({new,SYNCID,From,To,Packet});
						   IS_GROUP_CHAT=:=false,ACK_FROM,MT=:="msgStatus" ->
							   KK = FU++"@"++FD++"/offline_msg",
							   ?DEBUG("==> SYNC_RES ack => ACK_USER=~p ; ACK_ID=~p",[KK,SYNCID]),
							   ?INFO_MSG("call aa usermsg handler del msg", []),
							   aa_usermsg_handler:del_msg(SYNCID),
							   ack_task({ack,SYNCID});
						   true ->
							   skip
						end
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

user_receive_packet_handler(_JID, _From, _To, _Packet) ->
	ok.




%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {
	  ecache_node,
	  ecache_mod=ecache_server,
	  ecache_fun=cmd
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
			  ?INFO_MSG("#### user_receive_packet Host=~p~n",[Host]),

			  ejabberd_hooks:add(sm_register_connection_hook, Host, ?MODULE, sm_register_connection_hook_handler, 45),
			  ?INFO_MSG("#### sm_register_connection_hook_handler Host=~p~n",[Host]),
			  ejabberd_hooks:add(sm_remove_connection_hook, Host, ?MODULE, sm_remove_connection_hook_handler, 45),
			  ?INFO_MSG("#### sm_remove_connection_hook_handler Host=~p~n",[Host]),
			  ejabberd_hooks:add(user_available_hook, Host, ?MODULE, user_available_hook_handler, 45),
			  ?INFO_MSG("#### user_available_hook_handler Host=~p~n",[Host])

	  end, ?MYHOSTS),
	%% 2014-3-4 : 在这个 HOOK 初始化时，启动一个thrift 客户端，同步数据到缓存服务器
	%% 启动5281端口，接收内网回调
	aa_inf_server:start(),
	mnesia:create_table(dmsg,[{attributes,record_info(fields,dmsg)},{ram_copies,[node()]}]),
	case mnesia:create_table(?GOUPR_MEMBER_TABLE, [{attributes, record_info(fields,?GOUPR_MEMBER_TABLE)}, 
										  {ram_copies, [node()]}]) of
		{aborted,{already_exists,_}} ->
			mnesia:add_table_copy(?GOUPR_MEMBER_TABLE, node(), ram_copies);
		_ ->
			skip
	end,
	{ok, #state{}}.

handle_call({ecache_cmd,Cmd}, _F, #state{ecache_node=Node,ecache_mod=Mod,ecache_fun=Fun}=State) ->
	?DEBUG("==== ecache_cmd ===> Cmd=~p",[Cmd]),
	R = rpc:call(Node,Mod,Fun,[{Cmd}]),
	{reply, R, State};
handle_call({sync_packet,K,From,To,Packet}, _F, State) ->
	%% insert {K,V} 
	%% reset msgTime
	{M,S,SS} = now(),
	MsgTime = lists:sublist(erlang:integer_to_list(M*1000000000000+S*1000000+SS),1,13),
	{Tag,E,Attr,Body} = Packet,
	RAttr0 = lists:map(fun({K,V})-> case K of "msgTime" -> skip; _-> {K,V} end end,Attr),
	RAttr1 = lists:append([X||X<-RAttr0,X=/=skip],[{"msgTime",MsgTime}]),
	RPacket = {Tag,E,RAttr1,Body},
	%% add {K,V} to zset
	?INFO_MSG("call aa_usermsg_handler store msg", []),
	aa_usermsg_handler:store_msg(K, From, To, RPacket),
	log(RPacket),
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
	%% 一个标记，如果有值，则表示不需要 server_ack 回弹此消息
	G = case dict:is_key("g", D) of true -> dict:fetch("g", D); _ -> true end,
	?DEBUG("G=~p ; Packet=~p",[G,Packet]),
	if G and ACK_FROM and ( (MT=:="normalchat") or (MT=:="groupchat") ) ->
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

%% {id,from,to,msgtype,body}
log({counter,Domain,Total}) ->
	try
		N = ejabberd_config:get_local_option({log_node,Domain}),
		case net_adm:ping(N) of 
			pang -> 
				?INFO_MSG("write_log ::::> ~p",[{counter,Domain,Total}]);
			pong ->
				{logbox,N}!{counter,Domain,Total}
		end 
	catch
		E:I ->
			Err = erlang:get_stacktrace(),
			?ERROR_MSG("write_log_error ::::> E=~p ; I=~p~n Error=~p",[E,I,Err]),
			{error,E,I}
	end;
log(Packet) ->
	[Domain|_] = ?MYHOSTS, 
	try
		N = ejabberd_config:get_local_option({log_node,Domain}),
		{xmlelement,"message",Attr,_} = Packet,
		D = dict:from_list(Attr),
		ID 	= case dict:is_key("id",D) of true-> dict:fetch("id",D); false-> "" end,
		From 	= case dict:is_key("from",D) of true-> dict:fetch("from",D); false-> "" end,
		To 	= case dict:is_key("to",D) of true-> dict:fetch("to",D); false-> "" end,
		MsgType = case dict:is_key("msgtype",D) of true-> dict:fetch("msgtype",D); false-> "" end,
		Msg 	= erlang:list_to_binary(get_text_message_from_packet(Packet)),
		Message = {ID,From,To,MsgType,Msg},
		case net_adm:ping(N) of 
			pang -> 
				?INFO_MSG("write_log ::::> ~p",[Message]),
				Message;
			pong ->
				{logbox,N}!Message
		end 
	catch
		E:I ->
			Err = erlang:get_stacktrace(),
			?ERROR_MSG("write_log_error ::::> E=~p ; I=~p~n Error=~p",[E,I,Err]),
			{error,E,I}
	end.

ack_task({new,ID,From,To,Packet})->
        TPid = erlang:spawn(fun()-> ack_task(ID,From,To,Packet) end),
        ?DEBUG("ack_task_new ~p",[{ID,From,To,Packet,TPid}]),
        Dmsg = #dmsg{mid=ID,pid=TPid},
        RTN = mnesia:dirty_write(dmsg,Dmsg),
        ?DEBUG("ack_task_new_rtn=~p ; dmsg=~p",[RTN,Dmsg]);
	
ack_task({ack,ID})->
	ack_task({do,ack,ID});
ack_task({offline,ID})->
	ack_task({do,offline,ID});
ack_task({do,M,ID})->
	?INFO_MSG("DO_ACK_TASK_ID=~p ; M=~p.",[ID,M]),
	try
		OBJ = mnesia:dirty_read(dmsg,ID),
		?DEBUG("DO_ACK_TASK_ID=~p ; M=~p ; OBJ=~p.",[ID,M,OBJ]),
		case OBJ of
			[{_,_,ResendPid}] ->
				ResendPid!M;
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
		offline->
			mnesia:dirty_delete(dmsg,ID),
			?INFO_MSG("ACK_TASK_~p ::::> OFFLINE.",[ID]);
		ack ->
			mnesia:dirty_delete(dmsg,ID),
			?INFO_MSG("ACK_TASK_~p ::::> ACK.",[ID])
	after ?TIME_OUT -> 
		?DEBUG("ACK_TASK_~p ::::> AFTER",[ID]),
		%% 2014-06-18 : 离线消息统统上桥,如果开启桥接模式
		mnesia:dirty_delete(dmsg,ID),
%% 		gen_server:cast(?MODULE,{offline_message,ID,From,To,Packet}),
		offline_message_hook_handler(From,To,Packet)
	end.


server_ack(From,To,Packet)->
	gen_server:cast(?MODULE,{server_ack,From,To,Packet}).


get_id()-> 
	{M,S,SS} = now(), 
	atom_to_list(node())++"_"++integer_to_list(M)++integer_to_list(S)++integer_to_list(SS).
