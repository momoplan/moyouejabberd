%% @author songzhiming
%% @doc @todo Add description to my_msg_cleaner.


-module(my_msg_cleaner).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(CLEAN_CYCLE, 7200000).
-define(CLEAN_MSG, clean_offline_msg).

-include("jlib.hrl").
-include("ejabberd.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
		 clean/0,
		 run_all_nodes/0,
		 clean_user_msg/2,
		 start/0]).


start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

clean() ->
	gen_server:call(?MODULE, clean, infinity).

run_all_nodes() ->
	[ start(Node) || Node <- [node()|nodes()]].
start(Node) ->
	spawn(fun() ->
				  rpc:call(Node,start,start,[])
		  end).

start() ->
	supervisor:start_child(aa_hookhandler_sup, {?MODULE, 
												{?MODULE, start_link, []}, 
												permanent, 
												3000, 
												worker, 
												[?MODULE]}).

clean_user_msg(Uid, Domain) ->
	NodeNameList = atom_to_list(node()),
	RamMsgListTableName = list_to_atom(NodeNameList ++ "user_msglist"),	
	UserJids = mnesia:dirty_all_keys(RamMsgListTableName),
	UserJid =
	lists:filter(fun(#jid{user = Uid1, server = Domain1}) ->
						 if Uid == Uid1 andalso Domain == Domain1 ->
								true;
							true ->
								false
						 end
				 end, UserJids),
	case UserJid of
		[] ->
			user_not_on_this_node;
		[User|_] ->
			clean_user_msg([User])
	end.



%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {clean_timer = none}).


init([]) ->
	Timer = erlang:send_after(?CLEAN_CYCLE, self(), ?CLEAN_MSG),

    {ok, #state{clean_timer = Timer}}.


handle_call(clean, _From, State) ->
	if State#state.clean_timer /= none ->
		   erlang:cancel_timer(State#state.clean_timer);
	   true ->
		   skip
	end,
	Timer = erlang:send_after(?CLEAN_CYCLE, self(), ?CLEAN_MSG),
	clean_message(),
	{reply, ok, State#state{clean_timer = Timer}};

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



handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(?CLEAN_MSG, State) ->
	Timer = erlang:send_after(?CLEAN_CYCLE, self(), ?CLEAN_MSG),
	clean_message(),
	{noreply, State#state{clean_timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.



terminate(_Reason, _State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

clean_message() ->
	[Domain|_] = ?MYHOSTS,
	SelfNode = node(),	
	case ejabberd_config:get_local_option({handle_msglist_tables, Domain}) of
		undefined ->
			skip;
		[] ->
			skip;
		MsgListTables when is_list(MsgListTables) ->
			[begin
				 case catch mnesia:table_info(Table, where_to_write) of
					 [SelfNode|_] ->
						 UserJids = mnesia:dirty_all_keys(Table),
						 clean_user_msg(UserJids);
					 _ ->
						 skip
				 end
			 end || Table <- MsgListTables]
	end.

clean_user_msg([]) ->
	ok;
clean_user_msg([Jid|RestUser]) ->
	Status = aa_session:check_online(Jid),
	case Status of
		online ->
			skip;
		offline ->
			aa_usermsg_handler:dump(Jid)
	end,
	clean_user_msg(RestUser).
	
