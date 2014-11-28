-module(aa_offline_mod).
-behaviour(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include("aa_data.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% 离线消息对象
-define(EXPIRE,60*60*24*7).

%% ====================================================================
%% API functions
%% ====================================================================

-export([
	 start_link/0,
%% 	 offline_message_hook_handler/3,
	 sm_register_connection_hook_handler/3,
	 sm_remove_connection_hook_handler/3,
	 user_available_hook_handler/1
]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

sm_register_connection_hook_handler(SID, JID, Info) -> ok.

user_available_hook_handler(JID) -> send_offline_msg(JID).

send_offline_msg(JID) ->
	try 
		%% JID={jid,"cc","test.com","Smack","cc","test.com","Smack"} 
		{jid,User,Domain,_,_,_,_} = JID,
		KEY = User++"@"++Domain++"/offline_msg",
		Range = case ejabberd_config:get_local_option({max_offline,Domain}) of 
					N when is_integer(N),N > 0 -> N; 
					_ -> 0
				end,
%% 		?WARNING_MSG("user ~p avaliable to send offline msg", [User]),
		{ok,R} = aa_usermsg_handler:get_offline_msg(Range, JID),
		%% TODO 这里，如果发送失败了，是需要重新发送的，但是先让他跑起来
		?INFO_MSG("@@@@ send_offline_msg :::> KEY=~p ; R.size=~p~n",[KEY,length(R)]),
		lists:foreach(fun(#user_msg{id = Id, from = FF, to = TT, packat = PP})->
							  try	
								  case ejabberd_router:route(FF, TT, PP) of
									  ok -> aa_usermsg_handler:del_msg(Id, JID); 
									  Err -> throw("Error: "++Err)
								  end
							  catch
								  E:I ->
									  ?INFO_MSG("~p ; ~p",[E,I])	
							  end,
							  ok
					  end,R) ,
		?INFO_MSG("@@@@ send_offline_message ::>KEY=~p  <<<<<<<<<<<<<<<<<",[KEY]) 
	catch 
		_:_->
			Err = erlang:get_stacktrace(),
			?INFO_MSG("@@ send_offline_message ERROR::> ~p ",[Err])
	end,
	ok.


sm_remove_connection_hook_handler(SID, JID, Info) -> ok.

%% 离线消息事件
%% 保存离线消息
%% offline_message_hook_handler(#jid{user=FromUser}=From, #jid{user=User,server=Domain}=To, Packet) ->
%% 	Type = xml:get_tag_attr_s("type", Packet),
%% 	ID = xml:get_tag_attr_s("id", Packet),
%% 	IS_GROUP = aa_group_chat:is_group_chat(To),
%% 	if IS_GROUP==false,FromUser=/="messageack",User=/="messageack",Type=/="error",Type=/="groupchat",Type=/="headline" ->
%% 			SYNCID = ID++"@"++Domain,
%% 								?ERROR_MSG("CALL  store msg aa offline mod", []),
%% 			aa_usermsg_handler:store_msg(SYNCID, From, To, Packet);
%% 		true ->
%% 			ok
%% 	end.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, { ecache_node, ecache_mod=ecache_server, ecache_fun=cmd }).

init([]) ->
	?INFO_MSG("INIT_START_OFFLINE_MOD >>>>>>>>>>>>>>>>>>>>>>>> ~p",[liangchuan_debug]),  
	lists:foreach(
	  fun(Host) ->
%% 		ejabberd_hooks:add(offline_message_hook, Host, ?MODULE, offline_message_hook_handler, 40),
		ejabberd_hooks:add(sm_remove_connection_hook, Host, ?MODULE, sm_remove_connection_hook_handler, 40),
		ejabberd_hooks:add(sm_register_connection_hook, Host, ?MODULE, sm_register_connection_hook_handler, 60),
		ejabberd_hooks:add(user_available_hook, Host, ?MODULE, user_available_hook_handler, 40)
	  end, ?MYHOSTS),
	?INFO_MSG("INIT_END_OFFLINE_MOD <<<<<<<<<<<<<<<<<<<<<<<<< ~p",[liangchuan_debug]),
	{ok, #state{}}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
%% ====================================================================
%% Internal functions
%% ====================================================================
