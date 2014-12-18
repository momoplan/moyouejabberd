%% @author songzhiming
%% @doc @todo Add description to my_user_msg_handler.


-module(my_user_msg_handler).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
		 store_msg/3,
		 delete_msg/3,
		 get_offline_msg/2]).

start_link() ->
	gen_server:start_link(?MODULE, [], []).

store_msg(Pid, User, Message) ->
	gen_server:cast(Pid, {store_msg, User, Message}).

delete_msg(Pid, Key, User) ->
	gen_server:cast(Pid, {del_msg, Key, User}).

get_offline_msg(Pid, User) ->
	gen_server:call(Pid, {get_offline_msg, User}).


%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {}).


init([]) ->
    {ok, #state{}}.


handle_call({get_offline_msg, User}, _From, State) ->
	{reply, aa_usermsg_handler:get_offline_msg(User), State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast({store_msg, User, {Key, From, Packet}}, State) ->
	aa_usermsg_handler:store_msg(Key, From, User, Packet),
	{noreply, State};

handle_cast({del_msg, Key, User}, State) ->
	aa_usermsg_handler:del_msg(Key, User),
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


