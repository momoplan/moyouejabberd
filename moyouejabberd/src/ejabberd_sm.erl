%%%----------------------------------------------------------------------
%%% File    : ejabberd_sm.erl
%%% Author  : Alexey Shchepin <alexey@process-one.net>
%%% Purpose : Session manager
%%% Created : 24 Nov 2002 by Alexey Shchepin <alexey@process-one.net>
%%%
%%%
%%% ejabberd, Copyright (C) 2002-2013   ProcessOne
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License
%%% along with this program; if not, write to the Free Software
%%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
%%% 02111-1307 USA
%%%
%%%----------------------------------------------------------------------

-module(ejabberd_sm).
-author('alexey@process-one.net').

-behaviour(gen_server).

%% API
-export([start_link/0,
	 route/3,
	 open_session/5, close_session/4,
	 check_in_subscription/6,
	 bounce_offline_message/3,
	 disconnect_removed_user/2,
	 get_user_resources/2,
	 set_presence/7,
	 unset_presence/6,
	 close_session_unset_presence/5,
	 dirty_get_sessions_list/0,
	 dirty_get_my_sessions_list/0,
	 get_vh_session_list/1,
	 get_vh_session_number/1,
	 register_iq_handler/4,
	 register_iq_handler/5,
	 unregister_iq_handler/2,
	 force_update_presence/1,
	 connected_users/0,
	 connected_users_number/0,
	 user_resources/2,
	 get_session_pid/3,
	 get_user_info/3,
	 get_user_ip/3,
	 is_existing_resource/3
    	 %% ack/2,ack/3
	]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("ejabberd_commands.hrl").
-include("mod_privacy.hrl").

%% 2014-2-27 : 加一个表用来记录ACK进程ID，以便应答时使用
%% 2014-3-5 : 这部分逻辑迁移到外部模块完成，此处作废
%% -record(session_ack, {id,pid,ct}).

-record(session, {sid, usr, us, priority, info}).
-record(session_counter, {vhost, count}).
-record(state, {}).

%% default value for the maximum number of user connections
-define(MAX_USER_SESSIONS, infinity).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% 在 ejabberd_local 模块转过来的，精髓都在这里了，问题也在这里
route(From, To, Packet) ->
    case catch do_route(From, To, Packet) of
		{'EXIT', Reason} ->
	    	?ERROR_MSG("~p~nwhen processing: ~p",[Reason, {From, To, Packet}]);
		_ ->
	    	ok
    end.

open_session(SID, User, Server, Resource, Info) ->
%% 可以T掉前一个链接的代码
%%	case mnesia:dirty_index_read(session, {User, Server, Resource}, #session.usr) of
%%		[] ->
%%			?DEBUG("[open_session_ok] User=~p;Server=~p;Resource=~p ::::>",[User,Server,Resource]);
%%		Ss ->
%%			?DEBUG("[open_session_ss] User=~p;Server=~p;Resource=~p ::::>Ss=~p",[User,Server,Resource,Ss]),
%%			lists:foreach(fun(Session) -> 
%%				SID2 = Session#session.sid,
%%				USR = Session#session.usr,
%%				?DEBUG("[open_session_close_first_ok] User=~p;Server=~p ::::>",[User,Server]),
%%				{UUU,SSS,RRR} = USR,
%%				%% TODO 这里不光要关闭 session，还要关闭 c2s 链接
%%				case ejabberd_sm:get_session_pid(UUU,SSS,RRR) of
%%					Pid when is_pid(Pid) ->
%%						?DEBUG("[open_session_stop_ok] User=~p;Server=~p ::::>",[User,Server]),
%%						ejabberd_c2s:stop(Pid);
%%					_ ->
%%						?DEBUG("[open_session_stop_skip_ok] User=~p;Server=~p ::::>",[User,Server]),
%%						ok
%%			    	end,			
%%				close_session(SID2, UUU,SSS,RRR)
%%			end, Ss)
%%	end,
	set_session(SID, User, Server, Resource, undefined, Info),
	mnesia:dirty_update_counter(session_counter,jlib:nameprep(Server), 1),
	check_for_sessions_to_replace(User, Server, Resource),
	JID = jlib:make_jid(User, Server, Resource),
	ejabberd_hooks:run(sm_register_connection_hook, JID#jid.lserver, [SID, JID, Info]).

close_session(SID, User, Server, Resource) ->
    Info = case mnesia:dirty_read({session, SID}) of
	[] -> [];
	[#session{info=I}] -> I
    end,
    F = fun() ->
		mnesia:delete({session, SID}),
		mnesia:dirty_update_counter(session_counter,
					    jlib:nameprep(Server), -1)
	end,
    mnesia:sync_dirty(F),
    JID = jlib:make_jid(User, Server, Resource),
    ejabberd_hooks:run(sm_remove_connection_hook, JID#jid.lserver,
		       [SID, JID, Info]).

check_in_subscription(Acc, User, Server, _JID, _Type, _Reason) ->
    case ejabberd_auth:is_user_exists(User, Server) of
	true ->
	    Acc;
	false ->
	    {stop, false}
    end.

bounce_offline_message(From, To, Packet) ->
	?INFO_MSG("~p~n From=~p~n To=~p~n Packet=~p~n",[bounce_offline_message, From, To, Packet]),
    Err = jlib:make_error_reply(Packet, ?ERR_SERVICE_UNAVAILABLE),
    ejabberd_router:route(To, From, Err),
    stop.

disconnect_removed_user(User, Server) ->
    ejabberd_sm:route(
	  			jlib:make_jid("", "", ""),
				jlib:make_jid(User, Server, ""),
				{xmlelement, "broadcast", [],[{exit, "User removed"}]}
	).

get_user_resources(User, Server) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    US = {LUser, LServer},
    case catch mnesia:dirty_index_read(session, US, #session.us) of
	{'EXIT', _Reason} ->
	    [];
	Ss ->
	    [element(3, S#session.usr) || S <- clean_session_list(Ss)]
    end.

get_user_ip(User, Server, Resource) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    LResource = jlib:resourceprep(Resource),
    USR = {LUser, LServer, LResource},
    case mnesia:dirty_index_read(session, USR, #session.usr) of
	[] ->
	    undefined;
	Ss ->
	    Session = lists:max(Ss),
	    proplists:get_value(ip, Session#session.info)
    end.

get_user_info(User, Server, Resource) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    LResource = jlib:resourceprep(Resource),
    USR = {LUser, LServer, LResource},
    case mnesia:dirty_index_read(session, USR, #session.usr) of
	[] ->
	    offline;
	Ss ->
	    Session = lists:max(Ss),
	    Node = node(element(2, Session#session.sid)),
	    Conn = proplists:get_value(conn, Session#session.info),
	    IP = proplists:get_value(ip, Session#session.info),
	    [{node, Node}, {conn, Conn}, {ip, IP}]
    end.

set_presence(SID, User, Server, Resource, Priority, Presence, Info) ->
    set_session(SID, User, Server, Resource, Priority, Info),
    ejabberd_hooks:run(set_presence_hook, jlib:nameprep(Server),
		       [User, Server, Resource, Presence]).

unset_presence(SID, User, Server, Resource, Status, Info) ->
    set_session(SID, User, Server, Resource, undefined, Info),
    ejabberd_hooks:run(unset_presence_hook, jlib:nameprep(Server),
		       [User, Server, Resource, Status]).

close_session_unset_presence(SID, User, Server, Resource, Status) ->
    close_session(SID, User, Server, Resource),
    ejabberd_hooks:run(unset_presence_hook, jlib:nameprep(Server),
		       [User, Server, Resource, Status]).

get_session_pid(User, Server, Resource) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    LResource = jlib:resourceprep(Resource),
    USR = {LUser, LServer, LResource},
    case catch mnesia:dirty_index_read(session, USR, #session.usr) of
	[#session{sid = {_, Pid}}] -> Pid;
	_ -> none
    end.

dirty_get_sessions_list() ->
    mnesia:dirty_select(
      session,
      [{#session{usr = '$1', _ = '_'},
	[],
	['$1']}]).

dirty_get_my_sessions_list() ->
    mnesia:dirty_select(
      session,
      [{#session{sid = {'_', '$1'}, _ = '_'},
	[{'==', {node, '$1'}, node()}],
	['$_']}]).

get_vh_session_list(Server) ->
    LServer = jlib:nameprep(Server),
    mnesia:dirty_select(
      session,
      [{#session{usr = '$1', _ = '_'},
	[{'==', {element, 2, '$1'}, LServer}],
	['$1']}]).

get_vh_session_number(Server) ->
    LServer = jlib:nameprep(Server),
    Query = mnesia:dirty_select(
		session_counter,
		[{#session_counter{vhost = LServer, count = '$1'},
		  [],
		  ['$1']}]),
    case Query of
	[Count] ->
	    Count;
	_ -> 0
    end.
    
register_iq_handler(Host, XMLNS, Module, Fun) ->
    ejabberd_sm ! {register_iq_handler, Host, XMLNS, Module, Fun}.

register_iq_handler(Host, XMLNS, Module, Fun, Opts) ->
    ejabberd_sm ! {register_iq_handler, Host, XMLNS, Module, Fun, Opts}.

unregister_iq_handler(Host, XMLNS) ->
    ejabberd_sm ! {unregister_iq_handler, Host, XMLNS}.


%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    update_tables(),
    mnesia:create_table(session,[{ram_copies, [node()]},{attributes, record_info(fields, session)}]),
    mnesia:create_table(session_counter,[{ram_copies, [node()]}, {attributes, record_info(fields, session_counter)}]),
	%% 2014-2-27 : ACK进程信息
    	%% 2014-3-5 : 此逻辑转到外部模块，此处作废 
	%% ?DEBUG("CREATE_TABLE_ACK :::> ~p",[session_ack]),
	%% mnesia:create_table(session_ack,[{ram_copies, [node()]}, {attributes, record_info(fields, session_ack)}]),
	
    mnesia:add_table_index(session, usr),
    mnesia:add_table_index(session, us),
    mnesia:add_table_copy(session, node(), ram_copies),
    mnesia:add_table_copy(session_counter, node(), ram_copies),
    mnesia:subscribe(system),
    ets:new(sm_iqtable, [named_table]),
    lists:foreach(
      fun(Host) ->
	      ejabberd_hooks:add(roster_in_subscription, Host, ejabberd_sm, check_in_subscription, 20),
		  
	      ejabberd_hooks:add(offline_message_hook, Host, ejabberd_sm, bounce_offline_message, 100),
	      
		  ejabberd_hooks:add(remove_user, Host, ejabberd_sm, disconnect_removed_user, 100)
      end, ?MYHOSTS),
    ejabberd_commands:register_commands(commands()),

    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({route, From, To, Packet}, State) ->
    case catch do_route(From, To, Packet) of
	{'EXIT', Reason} ->
	    ?ERROR_MSG("~p~nwhen processing: ~p",
		       [Reason, {From, To, Packet}]);
	_ ->
	    ok
    end,
    {noreply, State};
handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    recount_session_table(Node),
    {noreply, State};
handle_info({register_iq_handler, Host, XMLNS, Module, Function}, State) ->
    ets:insert(sm_iqtable, {{XMLNS, Host}, Module, Function}),
    {noreply, State};
handle_info({register_iq_handler, Host, XMLNS, Module, Function, Opts}, State) ->
    ets:insert(sm_iqtable, {{XMLNS, Host}, Module, Function, Opts}),
    {noreply, State};
handle_info({unregister_iq_handler, Host, XMLNS}, State) ->
    case ets:lookup(sm_iqtable, {XMLNS, Host}) of
	[{_, Module, Function, Opts}] ->
	    gen_iq_handler:stop_iq_handler(Module, Function, Opts);
	_ ->
	    ok
    end,
    ets:delete(sm_iqtable, {XMLNS, Host}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ejabberd_commands:unregister_commands(commands()),
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% 对ACK表的ACID操作
%% 2014-3-5 : 这部分逻辑，挪到外部模块实现，此处逻辑作废
%% ack(set,ID,PID) ->
%% 	?INFO_MSG("ack_set,ID=~p,PID=~p",[ID,PID]),
%%     F = fun() ->
%% 		mnesia:write(#session_ack{id=ID,pid=PID,ct=calendar:local_time()})
%% 	end,
%%     mnesia:sync_dirty(F).
%% ack(get,ID) ->
%% 	?INFO_MSG("ack_get,ID=~p",[ID]),
%% 	ACKS = mnesia:dirty_read(session_ack,ID),
%% 	case ACKS of 
%% 		[ACK] ->
%% 			{ok,ACK#session_ack.pid};
%% 		[] ->
%% 			{empty}
%% 	end;
%% ack(del,ID) ->
%% 	?INFO_MSG("ack_del,ID=~p",[ID]),
%%     F = fun() ->
%% 		mnesia:delete({session_ack,ID})
%% 	end,
%%     mnesia:sync_dirty(F).

set_session(SID, User, Server, Resource, Priority, Info) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    LResource = jlib:resourceprep(Resource),
    US = {LUser, LServer},
    USR = {LUser, LServer, LResource},
    F = fun() ->
		mnesia:write(#session{sid = SID,
				      usr = USR,
				      us = US,
				      priority = Priority,
				      info = Info})
	end,
    mnesia:sync_dirty(F).

%% Recalculates alive sessions when Node goes down 
%% and updates session and session_counter tables 
recount_session_table(Node) ->
    F = fun() ->
		Es = mnesia:select(
		       session,
		       [{#session{sid = {'_', '$1'}, _ = '_'},
			 [{'==', {node, '$1'}, Node}],
			 ['$_']}]),
		lists:foreach(fun(E) ->
				      mnesia:delete({session, E#session.sid})
			      end, Es),
		%% reset session_counter table with active sessions
		mnesia:clear_table(session_counter),
		lists:foreach(fun(Server) ->
				LServer = jlib:nameprep(Server),
				Hs = mnesia:select(session,
				    [{#session{usr = '$1', _ = '_'},
				    [{'==', {element, 2, '$1'}, LServer}],
				    ['$1']}]),
				mnesia:write(
				    #session_counter{vhost = LServer, 
						     count = length(Hs)})
			      end, ?MYHOSTS)
	end,
    mnesia:async_dirty(F).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_route(From, To, Packet) ->
    ?DEBUG("session manager~n\tfrom ~p~n\tto ~p~n\tpacket ~P~n",[From, To, Packet, 8]),
    #jid{user = User, server = Server, luser = LUser, lserver = LServer, lresource = LResource} = To,
    {xmlelement, Name, Attrs, _Els} = Packet,
    case LResource of
		"" ->
		    case Name of
				"presence" ->
				    {Pass, _Subsc} = case xml:get_attr_s("type", Attrs) of
						"subscribe" ->
							Reason = xml:get_path_s(
								   Packet,
								   [{elem, "status"}, cdata]),
							{is_privacy_allow(From, To, Packet) andalso
							 ejabberd_hooks:run_fold(
							   roster_in_subscription,
							   LServer,
							   false,
							   [User, Server, From, subscribe, Reason]),
							 true};
					    "subscribed" ->
							{is_privacy_allow(From, To, Packet) andalso
							 ejabberd_hooks:run_fold(
							   roster_in_subscription,
							   LServer,
							   false,
							   [User, Server, From, subscribed, ""]),
							 true};
					    "unsubscribe" ->
							{is_privacy_allow(From, To, Packet) andalso
							 ejabberd_hooks:run_fold(
							   roster_in_subscription,
							   LServer,
							   false,
							   [User, Server, From, unsubscribe, ""]),
							 true};
					    "unsubscribed" ->
							{is_privacy_allow(From, To, Packet) andalso
							 ejabberd_hooks:run_fold(
							   roster_in_subscription,
							   LServer,
							   false,
							   [User, Server, From, unsubscribed, ""]),
							 true};
					    _ ->
							{true, false}
					end,
				    if Pass ->
					    PResources = get_user_present_resources(LUser, LServer),
					    lists:foreach(
					      fun({_, R}) ->
						  	do_route(From,jlib:jid_replace_resource(To, R),Packet)
					      end, PResources);
				       true ->
					    ok
				    end;
				"message" ->
					%% 这是我们关心的
				    route_message(From, To, Packet);
				"iq" ->
				    process_iq(From, To, Packet);
				"broadcast" ->
				    lists:foreach(
				      fun(R) ->
					      do_route(From,
						       jlib:jid_replace_resource(To, R),
						       Packet)
				      end, get_user_resources(User, Server));
				_ ->
				    ok
		    end;
		_ ->
		    USR = {LUser, LServer, LResource},
		    case mnesia:dirty_index_read(session, USR, #session.usr) of
			[] ->
			    case Name of
				"message" ->
				    route_message(From, To, Packet);
				"iq" ->
				    case xml:get_attr_s("type", Attrs) of
					"error" -> ok;
					"result" -> ok;
					_ ->
					    Err =
						jlib:make_error_reply(
						  Packet, ?ERR_SERVICE_UNAVAILABLE),
					    ejabberd_router:route(To, From, Err)
				    end;
				_ ->
				    ?DEBUG("packet droped~n", [])
			    end;
			Ss ->
			    Session = lists:max(Ss),
			    Pid = element(2, Session#session.sid),
			    ?DEBUG("sending to process ~p~n", [Pid]),
			    Pid ! {route, From, To, Packet}
		    end
    end.

%% The default list applies to the user as a whole,
%% and is processed if there is no active list set
%% for the target session/resource to which a stanza is addressed,
%% or if there are no current sessions for the user.
is_privacy_allow(From, To, Packet) ->
    User = To#jid.user,
    Server = To#jid.server,
    PrivacyList = ejabberd_hooks:run_fold(privacy_get_user_list, Server,
					  #userlist{}, [User, Server]),
    is_privacy_allow(From, To, Packet, PrivacyList).

%% Check if privacy rules allow this delivery
%% Function copied from ejabberd_c2s.erl
is_privacy_allow(From, To, Packet, PrivacyList) ->
    User = To#jid.user,
    Server = To#jid.server,
    allow == ejabberd_hooks:run_fold(
	       privacy_check_packet, Server,
	       allow,
	       [User,
		Server,
		PrivacyList,
		{From, To, Packet},
		in]).

%% XXX 2014-3-5 : 这个方法的逻辑，单独做成一个 TASK 模块，分布到外部完成了，此处作废
%% route_message(From, To, Packet) ->
%% 	%% TODO 1002:如果能在这里判断出 session 是否有效，一切问题就都解决了其实。
%% 	%% 在这里做一个 ack 给接收人 
%% 	MsgType = xml:get_tag_attr_s("msgtype", Packet),
%% 	%% 如果收消息人所在的域，需要做ACK，就执行第一段逻辑，否则直接路由消息到目标地址
%% 	#jid{lserver=Domain} = To,
%% 	ACK_TO = case catch ejabberd_config:get_local_option({ack_to,Domain}) of 
%% 		true -> true;
%% 		_ -> false
%% 	end,
%% 	%% 默认接收放的ACK超时是30秒，建议设置4-5分钟，单位毫秒
%% 	ACK_TO_TIMEOUT = case catch ejabberd_config:get_local_option({ack_to_timeout,Domain}) of 
%% 		N when is_integer(N) -> N;
%% 		_ -> 10*1000
%% 	end,
%% 	?DEBUG("[route_message] ~p ::::> ACK_TO=~p ; ACK_TO_TIMEOUT=~p ; MsgType=~p",["01",ACK_TO,ACK_TO_TIMEOUT,MsgType]),
%% 	if 
%% 		%% 140222 : 这里在逻辑上也要做一个比较大的修正；
%% 		%% 140222 : 再此处会拦截一些 msgtype 为指定值的消息，产生监听进程，而不是直接发送ack消息等待响应了
%% 		%% TODO 暂时只拦截 normalchat 消息
%% 		ACK_TO , MsgType =:= "normalchat" ->
%% 			%% 这里还有一个逻辑，看看收件人的 session 是否有效，如果无效，如果有效校验就ACK校验，无效就算了
%% 		    LUser = To#jid.luser,
%% 		    LServer = To#jid.lserver,
%% 		    PrioRes = get_user_present_resources(LUser, LServer),
%% 			?DEBUG("[route_message] ~p ::::>",["02"]),
%% 			if 
%% 				length(PrioRes) > 0 ->
%% 					?DEBUG("[route_message] ~p ::::>",["03"]),
%% 					%% to 的 session 存在，需要 ACK 
%% 					%% 140222 : 这个地方，必须得拿原始消息的ID做进程标识了，因为取消了单独的ACK
%% 					%% ID = randoms:get_string(),
%% 					{_,"message",Attr,_} = Packet,
%% 					D = dict:from_list(Attr),
%% 					case dict:is_key("id", D) of 
%% 						true -> 
%% 						ID = dict:fetch("id", D),
%% 							%% 注册
%% 							%% 2014-2-27 : 注册进程有限制，无法满足要求，此处要用 mnesia内存表重构
%% 							PPP = spawn(fun()-> route_message({ack,ACK_TO_TIMEOUT},From, To, Packet) end),
%% 							ack(set,list_to_atom(ID),PPP);
%% 							%% register(list_to_atom(ID), PPP);
%% 						_ -> 
%% 							skip
%% 					end,
%% 					%% 发送ACK
%% 					%% send_ack(ID, To,);
%% 					%% 140222 : 用一个正常的消息取代 ACK 消息
%% 					route_message(do,From, To, Packet);
%% 				true ->
%% 					?DEBUG("[route_message] after ~p ::::>",["04"]),
%% 					%% to 的 session 不存在，直接就能识别成离线消息
%% 					route_message(do,From, To, Packet)
%% 			end;
%% 		true ->
%% 			?DEBUG("[route_message] after ~p ::::>",["05"]),
%% 			%% 如果发送的是 ACK 的消息，就直接路由就行了
%% 			route_message(do,From, To, Packet)
%% 	end.
%% route_message({ack,ACK_TO_TIMEOUT},From, To, Packet) ->
%% 	%% 非 ACK 的消息，都认为是业务相关的，都在这做一下收信人 session 校验
%% 	?INFO_MSG("AACCKK route_message/4 ack ::::> ~p",{From, To, Packet}),
%% 	receive
%% 		{ack,ID} ->
%% 			?DEBUG("xxxx_receive_ack ::::> ID=~p",[ID]),
%% 			ack(del,ID);
%% 			%% 140222 : 用真实消息代替ACK消息，所以这里不许要再发消息了，确认以后，结束
%% 			%% route_message(do,From, To, Packet)
%% 		stop ->
%% 			stop
%% 	after ACK_TO_TIMEOUT ->
%% 		?DEBUG("[route_message_ack] after 00 ::::> {From, To, Packet} = ~p",[{From, To, Packet}]),
%% 		%% 5秒以后，如果得不到ACK的回应，就关闭这个 session
%% 		%% TODO 2014-02-28 : 我决定，不再T掉收消息人的链接，改成循环发送更稳定，更安全
%% 		%% 要求客户端要处理重复消息，极端情况下会产生ID相同的重复消息
%% 		{_,"message",Attr,_} = Packet,
%% 		D = dict:from_list(Attr),
%% 		ID = dict:fetch("id", D),
%% 		ack(del,list_to_atom(ID)),
%% 		route_message(From, To, Packet),
%% 		%% TODO 重新发送时，要调用离线的回调接口，产生通知；
%% 		aa_hookhandler:offline_message_hook_handler(From,To,Packet)
%% 
%% 	%%	#jid{user = User, server = Server} = To,
%% 	%%    	case mnesia:dirty_index_read(session, {User, Server}, #session.us) of
%% 	%%		[] ->
%% 	%%			?DEBUG("[route_message_ack] after ~p ::::>",["01"]),
%% 	%%		    offline;
%% 	%%		Ss ->
%% 	%%			?DEBUG("[route_message_ack] after ~p ::::> Ss_length=~p",["02",length(Ss)]),
%% 	%%			%% -record(session, {sid, usr, us, priority, info}).
%% 	%%			lists:foreach(fun(Session) -> 
%% 	%%				SID = Session#session.sid,
%% 	%%				USR = Session#session.usr,
%% 	%%		  		?DEBUG("[route_message_ack] after ~p ::::> SID=~p ; USR=~p",["03",SID,USR]),
%% 	%%				{UUU,SSS,RRR} = USR,
%% 	%%				%% TODO 这里不光要关闭 session，还要关闭 c2s 链接
%% 	%%				case ejabberd_sm:get_session_pid(UUU,SSS,RRR) of
%% 	%%					Pid when is_pid(Pid) ->
%% 	%%				  		?DEBUG("[route_message_ack] after ~p ::::>",["04"]),
%% 	%%					    ejabberd_c2s:stop(Pid);
%% 	%%					_ ->
%% 	%%						?DEBUG("[route_message_ack] after ~p ::::>",["05"]),
%% 	%%					    ok
%% 	%%			    end,			
%% 	%%				close_session(SID, UUU,SSS,RRR)
%% 	%%			end, Ss)
%% 	%%    	end,
%% 	%%	route_message(do,From, To, Packet)
%% 	end;
%% 
%% route_message(do,From, To, Packet) ->
route_message(From, To, Packet) ->
    LUser = To#jid.luser,
    LServer = To#jid.lserver,
    PrioRes = get_user_present_resources(LUser, LServer),
    case catch lists:max(PrioRes) of
		%% 如果有多个源，那么每个都要发，多数情况只有一个源存在
		%% TODO 1001:走这个分支，就证明这个 session 是有效的，不会是一个离线消息，问题就出在这里
		{Priority, _R} when is_integer(Priority), Priority >= 0 ->
		    lists:foreach(
		      %% Route messages to all priority that equals the max, if
		      %% positive
		      fun({P, R}) when P == Priority ->
			      LResource = jlib:resourceprep(R),
			      USR = {LUser, LServer, LResource},
			      case mnesia:dirty_index_read(session, USR, #session.usr) of
					  [] ->
					      ok; % Race condition
					  Ss ->
					      Session = lists:max(Ss),
					      Pid = element(2, Session#session.sid),
					      ?DEBUG("sending to process ~p~n", [Pid]),
					      Pid ! {route, From, To, Packet}
			      end;
			 %% Ignore other priority:
			 ({_Prio, _Res}) ->
			      ok
		      end,
		      PrioRes);
	    	%% 以下分支用来处理离线消息
		_ ->
		    case xml:get_tag_attr_s("type", Packet) of
				"error" ->
				    ok;
				"groupchat" ->
				    	?DEBUG("**************** ~p",["groupchat"]),
					bounce_offline_message(From, To, Packet);
				"headline" ->
				    	?DEBUG("**************** ~p",["headline"]),
				    	bounce_offline_message(From, To, Packet);
				_ ->
				    case ejabberd_auth:is_user_exists(LUser, LServer) of
						true ->
				    			?DEBUG("**************** ~p",["is_user_exists"]),
						    case is_privacy_allow(From, To, Packet) of
								true ->
									LOG_MSG = [route_message_offline_message_hook,LServer, From, To, Packet],
									?INFO_MSG("~p~n LServer=~p~n From=~p~n To=~p~n Packet=~p~n",LOG_MSG),
				    					?DEBUG("**************** ~p",["is_privacy_allow"]),
									ejabberd_hooks:run(offline_message_hook,LServer,[From, To, Packet]);
								false ->
								    ok
						    end;
						_ ->
						    Err = jlib:make_error_reply(Packet, ?ERR_SERVICE_UNAVAILABLE),
				    			?DEBUG("**************** ~p; Err=~p",["not is_privacy_allow",Err]),
						    ejabberd_router:route(To, From, Err)
				    end
		    end
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

clean_session_list(Ss) ->
    clean_session_list(lists:keysort(#session.usr, Ss), []).

clean_session_list([], Res) ->
    Res;
clean_session_list([S], Res) ->
    [S | Res];
clean_session_list([S1, S2 | Rest], Res) ->
    if
	S1#session.usr == S2#session.usr ->
	    if
		S1#session.sid > S2#session.sid ->
		    clean_session_list([S1 | Rest], Res);
		true ->
		    clean_session_list([S2 | Rest], Res)
	    end;
	true ->
	    clean_session_list([S2 | Rest], [S1 | Res])
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_user_present_resources(LUser, LServer) ->
    US = {LUser, LServer},
    case catch mnesia:dirty_index_read(session, US, #session.us) of
		{'EXIT', _Reason} ->
		    [];
		Ss ->
		    [{S#session.priority, element(3, S#session.usr)} || S <- clean_session_list(Ss), is_integer(S#session.priority)]
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% On new session, check if some existing connections need to be replace
check_for_sessions_to_replace(User, Server, Resource) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    LResource = jlib:resourceprep(Resource),
    %% TODO: Depending on how this is executed, there could be an unneeded
    %% replacement for max_sessions. We need to check this at some point.
    check_existing_resources(LUser, LServer, LResource),
    check_max_sessions(LUser, LServer).

check_existing_resources(LUser, LServer, LResource) ->
    SIDs = get_resource_sessions(LUser, LServer, LResource),
    if
	SIDs == [] -> ok;
	true ->
	    %% A connection exist with the same resource. We replace it:
	    MaxSID = lists:max(SIDs),
	    lists:foreach(
	      fun({_, Pid} = S) when S /= MaxSID ->
		      Pid ! replaced;
		 (_) -> ok
	      end, SIDs)
    end.

is_existing_resource(LUser, LServer, LResource) ->
    [] /= get_resource_sessions(LUser, LServer, LResource).

get_resource_sessions(User, Server, Resource) ->
    USR = {jlib:nodeprep(User), jlib:nameprep(Server), jlib:resourceprep(Resource)},
    mnesia:dirty_select(
	     session,
	     [{#session{sid = '$1', usr = USR, _ = '_'}, [], ['$1']}]).

check_max_sessions(LUser, LServer) ->
    %% If the max number of sessions for a given is reached, we replace the
    %% first one
    SIDs = mnesia:dirty_select(
	     session,
	     [{#session{sid = '$1', us = {LUser, LServer}, _ = '_'}, [],
	       ['$1']}]),
    MaxSessions = get_max_user_sessions(LUser, LServer),
    if
	length(SIDs) =< MaxSessions ->
	    ok;
	true ->
	    {_, Pid} = lists:min(SIDs),
	    Pid ! replaced
    end.


%% Get the user_max_session setting
%% This option defines the max number of time a given users are allowed to
%% log in
%% Defaults to infinity
get_max_user_sessions(LUser, Host) ->
    case acl:match_rule(
	   Host, max_user_sessions, jlib:make_jid(LUser, Host, "")) of
	Max when is_integer(Max) -> Max;
	infinity -> infinity;
	_ -> ?MAX_USER_SESSIONS
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

process_iq(From, To, Packet) ->
    IQ = jlib:iq_query_info(Packet),
    case IQ of
	#iq{xmlns = XMLNS} ->
	    Host = To#jid.lserver,
	    case ets:lookup(sm_iqtable, {XMLNS, Host}) of
		[{_, Module, Function}] ->
		    ResIQ = Module:Function(From, To, IQ),
		    if
			ResIQ /= ignore ->
			    ejabberd_router:route(To, From,
						  jlib:iq_to_xml(ResIQ));
			true ->
			    ok
		    end;
		[{_, Module, Function, Opts}] ->
		    gen_iq_handler:handle(Host, Module, Function, Opts,
					  From, To, IQ);
		[] ->
		    Err = jlib:make_error_reply(
			    Packet, ?ERR_SERVICE_UNAVAILABLE),
		    ejabberd_router:route(To, From, Err)
	    end;
	reply ->
	    ok;
	_ ->
	    Err = jlib:make_error_reply(Packet, ?ERR_BAD_REQUEST),
	    ejabberd_router:route(To, From, Err),
	    ok
    end.

force_update_presence({LUser, _LServer} = US) ->
    case catch mnesia:dirty_index_read(session, US, #session.us) of
        {'EXIT', _Reason} ->
            ok;
        Ss ->
            lists:foreach(fun(#session{sid = {_, Pid}}) ->
                                  Pid ! {force_update_presence, LUser}
                          end, Ss)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% ejabberd commands

commands() ->
	[
     #ejabberd_commands{name = connected_users,
		       tags = [session],
		       desc = "List all established sessions",
		       module = ?MODULE, function = connected_users,
		       args = [],
		       result = {connected_users, {list, {sessions, string}}}},
     #ejabberd_commands{name = connected_users_number,
		       tags = [session, stats],
		       desc = "Get the number of established sessions",
		       module = ?MODULE, function = connected_users_number,
		       args = [],
		       result = {num_sessions, integer}},
     #ejabberd_commands{name = user_resources,
		       tags = [session],
		       desc = "List user's connected resources",
		       module = ?MODULE, function = user_resources,
		       args = [{user, string}, {host, string}],
		       result = {resources, {list, {resource, string}}}}
	].

connected_users() ->
    USRs = dirty_get_sessions_list(),
    SUSRs = lists:sort(USRs),
    lists:map(fun({U, S, R}) -> [U, $@, S, $/, R] end, SUSRs).

connected_users_number() ->
    length(dirty_get_sessions_list()).

user_resources(User, Server) ->
    Resources =  get_user_resources(User, Server),
    lists:sort(Resources).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Update Mnesia tables

update_tables() ->
    case catch mnesia:table_info(session, attributes) of
	[ur, user, node] ->
	    mnesia:delete_table(session);
	[ur, user, pid] ->
	    mnesia:delete_table(session);
	[usr, us, pid] ->
	    mnesia:delete_table(session);
	[sid, usr, us, priority] ->
	    mnesia:delete_table(session);
	[sid, usr, us, priority, info] ->
	    ok;
	{'EXIT', _} ->
	    ok
    end,
    case lists:member(presence, mnesia:system_info(tables)) of
	true ->
	    mnesia:delete_table(presence);
	false ->
	    ok
    end,
    case lists:member(local_session, mnesia:system_info(tables)) of
	true ->
	    mnesia:delete_table(local_session);
	false ->
	    ok
    end.
