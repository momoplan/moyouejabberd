-module(aa_group_chat_old).

-include("ejabberd.hrl").
-include("jlib.hrl").

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(HTTP_HEAD,"application/x-www-form-urlencoded").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
	start_link/0,
	route_group_msg/3,
	is_group_chat/1,
	append_user/1,
	remove_user/1,
	remove_group/1
]).

-record(state, { ecache_node, ecache_mod=ecache_server, ecache_fun=cmd }).

start() ->
	aa_group_chat_sup:start_child().

stop(Pid)->
	gen_server:cast(Pid,stop).

start_link() ->
	gen_server:start_link(?MODULE,[],[]).

route_group_msg(From,To,Packet)->
	{ok,Pid} = start(),
	?DEBUG("###### route_group_msg_001 ::::> {From,To,Packet}=~p",[{From,To,Packet}]),
	gen_server:cast(Pid,{route_group_msg,From,To,Packet}).

%% {"service":"group_chat","method":"remove_user","params":{"domain":"test.com","gid":"123123","uid":"123123"}}
%% "{\"method\":\"remove_user\",\"params\":{\"domain\":\"test.com\",\"gid\":\"123123\",\"uid\":\"123123\"}}"
append_user(Body)->
	{ok,Pid} = start(),
	?DEBUG("append_user body=~p",[Body]),
	{ok,Obj,_Re} = rfc4627:decode(Body),
	{ok,Params} = rfc4627:get_field(Obj,"params"),
	{ok,Domain} = rfc4627:get_field(Params,"domain"),
	{ok,Gid} = rfc4627:get_field(Params,"gid"),
	{ok,Uid} = rfc4627:get_field(Params,"uid"),
	Key = binary_to_list(Gid)++"@group."++binary_to_list(Domain),
	case gen_server:call(Pid,{ecache_cmd,["ZCARD",Key]}) of 
		[<<"0">>] ->
			skip;
		_ ->
			gen_server:call(Pid,{ecache_cmd,["ZADD",Key,index_score(),Uid]})
	end,	
	stop(Pid),
	ok.
remove_user(Body)->
	{ok,Pid} = start(),
	?DEBUG("remove_user body=~p",[Body]),
	{ok,Obj,_Re} = rfc4627:decode(Body),
	{ok,Params} = rfc4627:get_field(Obj,"params"),
	{ok,Domain} = rfc4627:get_field(Params,"domain"),
	{ok,Gid} = rfc4627:get_field(Params,"gid"),
	{ok,Uid} = rfc4627:get_field(Params,"uid"),
	Key = binary_to_list(Gid)++"@group."++binary_to_list(Domain),
	case gen_server:call(Pid,{ecache_cmd,["ZCARD",Key]}) of 
		[<<"0">>] ->
			skip;
		_ ->
			gen_server:call(Pid,{ecache_cmd,["ZREM",Key,Uid]})
	end,	
	stop(Pid),
	ok.
remove_group(Body)->
	{ok,Pid} = start(),
	?DEBUG("remove_group body=~p",[Body]),
	{ok,Obj,_Re} = rfc4627:decode(Body),
	{ok,Params} = rfc4627:get_field(Obj,"params"),
	{ok,Domain} = rfc4627:get_field(Params,"domain"),
	{ok,Gid} = rfc4627:get_field(Params,"gid"),
	Key = binary_to_list(Gid)++"@group."++binary_to_list(Domain),
	gen_server:call(Pid,{ecache_cmd,["DEL",Key]}), 
	stop(Pid),
	ok.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
init([]) ->
	Conn = conn_ecache_node(),
	{ok,_,Node} = Conn,
	{ok, #state{ecache_node=Node}}.

handle_call({ecache_cmd,Cmd},_From, State) ->
	?DEBUG("##### ecache_cmd_on_group_chat_mod :::> Cmd=~p",[Cmd]),
	{reply,ecache_cmd(Cmd,State),State}.

handle_cast({route_group_msg,#jid{server=Domain,user=FU}=From,#jid{user=GroupId}=To,Packet}, State) ->
	%% 如果关闭组的cache那么就不保存组信息
	Disable_group_cache =  ejabberd_config:get_local_option({disable_group_cache,Domain}),
	Key = GroupId++"@group."++Domain,
	?DEBUG("###### get_user_list_by_group_id_key :::> key=~p ; disable_group_cache=~p",[Key,Disable_group_cache]),
	Result = case ecache_cmd(["ZCARD",Key],State) of
		[<<"0">>] ->
			case get_user_list_by_group_id(Domain,GroupId) of 
				{ok,UserList} ->
					%% -record(jid, {user, server, resource, luser, lserver, lresource}).
					RList = lists:map(fun(User)-> 
						UID = binary_to_list(User), 
						case Disable_group_cache of
							true ->
								disabled;
							_ ->
								Rss = ecache_cmd(["ZADD",Key,index_score(),UID],State),
								?DEBUG("###### add_to_cache :::> Key=~p ; UID=~p ; Rss=~p",[Key,UID,Rss])
						end,
						UID
					end,UserList),
					?DEBUG("###### get_user_list_by_group_id_http :::> GroupId=~p ; Roster=~p",[GroupId,RList]),
					{ok,UserList};
				Err ->
					?ERROR_MSG("ERROR=~p",[Err]),
					error
			end;
		[BN] ->
			?DEBUG("###### get_user_list_by_group_id_cache_n=~p",[BN]),
			RList = ecache_cmd(["ZRANGE",Key,"0","-1"],State), 
			?DEBUG("###### get_user_list_by_group_id_cache :::> GroupId=~p ; Roster=~p",[GroupId,RList]),
			{ok,RList} 
	end,
	case Result of
		{ok,Res} ->
			case lists:member(list_to_binary(FU),Res) of
				true->
					%% -record(jid, {user, server, resource, luser, lserver, lresource}).
					Roster = lists:map(fun(User)-> 
						UID = binary_to_list(User),
						#jid{user=UID,server=Domain,luser=UID,lserver=Domain,resource=[],lresource=[]} 
					end,Res),
					?DEBUG("###### route_group_msg 002 :::> GroupId=~p ; Roster=~p",[GroupId,Roster]),
					lists:foreach(fun(Target)-> route_msg(From,Target,Packet,GroupId) end,Roster);
				_ ->
					?ERROR_MSG("from_user_not_in_group Key=~p ; from_user=~p",[Key,FU]), 
					error
			end;
		_ ->
			error
	end,
	{stop, normal, State};
handle_cast(stop, State) ->
	{stop, normal, State}.

handle_info({cmd,Args},State)->
	{noreply,State}.

terminate(Reason, State) ->
    ok.
code_change(OldVsn, State, Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
get_user_list_by_group_id(Domain,GroupId)->
	?DEBUG("###### get_user_list_by_group_id :::> GroupId=~p",[GroupId]),
	HTTPServer =  ejabberd_config:get_local_option({http_server,Domain}),
	%% 取自配置文件 ejabberd.cfg
	HTTPService = ejabberd_config:get_local_option({http_server_service_client,Domain}),
	HTTPTarget = string:concat(HTTPServer,HTTPService),

	{Service,Method,GID,SN} = {
			list_to_binary("service.groupchat"),
			list_to_binary("getUserList"),
			list_to_binary(GroupId),
			list_to_binary(aa_hookhandler:get_id())
	},
	ParamObj={obj,[ {"sn",SN},{"service",Service},{"method",Method},{"params",{obj,[{"groupId",GID}]} } ]},
	Form = "body="++rfc4627:encode(ParamObj),
	?DEBUG("###### get_user_list_by_group_id :::> HTTP_TARGET=~p ; request=~p",[HTTPTarget,Form]),
	try
		case httpc:request(post,{ HTTPTarget ,[], ?HTTP_HEAD , Form },[],[] ) of   
	        	{ok, {_,_,Body}} ->
				DBody = rfc4627:decode(Body),
				{_,Log,_} = DBody,
				?DEBUG("###### get_user_list_by_group_id :::> response=~p",[rfc4627:encode(Log)]),
	 			case DBody of
	 				{ok,Obj,_Re} -> 
						case rfc4627:get_field(Obj,"success") of
							{ok,true} ->
								{ok,Entity} = rfc4627:get_field(Obj,"entity"),
								?DEBUG("###### get_user_list_by_group_id :::> entity=~p",[Entity]),
								{ok,Entity};
							_ ->
								{ok,Entity} = rfc4627:get_field(Obj,"entity"),
								{fail,Entity}
						end;
	 				Error -> 
						{error,Error}
	 			end ;
	        	{error, Reason} ->
	 			?INFO_MSG("[ ERROR ] cause ~p~n",[Reason]),
				{error,Reason}
		end
	catch
		_:_->
			%% TODO 测试时，可以先固定组内成员
			{ok,[<<"e1">>,<<"e2">>,<<"e3">>]}
	end.

route_msg(#jid{user=FromUser}=From,#jid{user=User,server=Domain}=To,Packet,GroupId) ->
	case FromUser=/=User of
		true->
			{X,E,Attr,Body} = Packet,
			ID = aa_hookhandler:get_id(),
			?DEBUG("##### route_group_msg_003 param :::> {User,Domain,GroupId}=~p",[{User,Domain,GroupId}]),
			RAttr0 = lists:map(fun({K,V})-> 
				case K of 
					"to" -> {K,User++"@"++Domain}; 
					"id" -> {K,ID};	
					"msgtype" -> {K,"groupchat"};	
					_-> {K,V} 
				end 
			end,Attr),
			RAttr1 = lists:append(RAttr0,[{"groupid",GroupId}]),
			RAttr2 = lists:append(RAttr1,[{"g","0"}]),
			RPacket = {X,E,RAttr2,Body},
			?DEBUG("###### route_group_msg 003 input :::> {From,To,RPacket}=~p",[{From,To,RPacket}]),
			case ejabberd_router:route(From, To, RPacket) of
				ok ->
					?DEBUG("###### route_group_msg 003 OK :::> {From,To,RPacket}=~p",[{From,To,RPacket}]),
					aa_hookhandler:user_send_packet_handler(From,To,RPacket),
					{ok,ok};
				Err ->
					?DEBUG("###### route_group_msg 003 ERR=~p :::> {From,To,RPacket}=~p",[Err,{From,To,RPacket}]),
					{error,Err}
			end;
		_ ->
			{ok,skip}
	end.

is_group_chat(#jid{server=Domain}=To)->
	DomainTokens = string:tokens(Domain,"."),
	Rtn = case length(DomainTokens) > 2 of 
		true ->
			[G|_] = DomainTokens,
			G=:="group";
		_ ->
			false
	end,
	?DEBUG("##### is_group_chat ::::>To~p ; Rtn=~p",[To,Rtn]),
	Rtn.


conn_ecache_node() ->
	try 
		[Domain|_] = ?MYHOSTS, 
		N = ejabberd_config:get_local_option({ecache_node,Domain}), 
		{ok,net_adm:ping(N),N} 
	catch E:I -> 
		Err = erlang:get_stacktrace(), 
		log4erl:error("error ::::> E=~p ; I=~p~n Error=~p",[E,I,Err]), 
		{error,E,I} 
	end.  

ecache_cmd(Cmd,#state{ecache_node=Node,ecache_mod=Mod,ecache_fun=Fun}=State) ->
	rpc:call(Node,Mod,Fun,[{Cmd}]).


index_score()-> {M,S,T} = now(), erlang:integer_to_list(M*1000000000000+S*1000000+T).
