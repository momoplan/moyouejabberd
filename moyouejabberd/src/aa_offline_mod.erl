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
	 user_available_hook_handler/1
]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

user_available_hook_handler(JID) -> send_offline_msg(JID).

send_offline_msg(JID) ->
	try 
		{jid,User,Domain,_,_,_,_} = JID,
		KEY = User++"@"++Domain++"/offline_msg",
		?INFO_MSG("@@@ start get offline message for ~p", [KEY]),

		{ok,R} = aa_hookhandler:get_offline_msg(JID),

		?WARNING_MSG("@@@@ send_offline_msg :::> KEY=~p ; R.size=~p~n",[KEY,length(R)]),
%% 		self() ! {aa_offline_msg, R};
		lists:foreach(fun(#user_msg{from = FF, to = TT, packat = PP})->
						 ejabberd_router:route(FF, TT, PP) 
					  end,R) ,
		?INFO_MSG("@@@@ send_offline_message ::>KEY=~p  <<<<<<<<<<<<<<<<<",[KEY]) 
	catch 
		_:_->
			Err = erlang:get_stacktrace(),
			?INFO_MSG("@@ send_offline_message ERROR::> ~p ",[Err])
	end,
	ok.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, { ecache_node, ecache_mod=ecache_server, ecache_fun=cmd }).

init([]) ->
	?INFO_MSG("INIT_START_OFFLINE_MOD >>>>>>>>>>>>>>>>>>>>>>>> ~p",[liangchuan_debug]),  
	lists:foreach(
	  fun(Host) ->
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
