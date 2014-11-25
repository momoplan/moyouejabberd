%% @author songzhiming
%% @doc @todo Add description to mysql_msg_driver.


-module(mysql_msg_driver).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("jlib.hrl").
-include("aa_data.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([dump/1,
		 load/1]).

dump(Jid) ->
	PidName = get_userpid_name(Jid),
	Pid = whereis(PidName),
	case is_pid(Pid) of
		true ->
			Pid1 = Pid;
		false ->
			{ok, Pid1} = start(PidName)
	end,
	gen_server:call(Pid1, {dump, Jid}),
	gen_server:cast(Pid1, stop).

load(Jid) ->
	PidName = get_userpid_name(Jid),
	Pid = whereis(PidName),
	case is_pid(Pid) of
		true ->
			%% 防止出现load 在stop消息之后发出的情况
			case catch gen_server:call(Pid, {load, Jid}) of
				{ok, Data} ->
					ok;
				_ ->
					Data = load_message_from_mysql(Jid)
			end;
		false ->
			Data = load_message_from_mysql(Jid) 
	end,
	{ok, Data}.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {}).


init([]) ->
    {ok, #state{}}.


handle_call({dump, Jid}, _From, State) ->
	case mnesia:dirty_read(?MY_USER_TABLES, Jid) of
		[#?MY_USER_TABLES{msg_list_table = RamMsgListTableName}] ->
			case mnesia:dirty_read(RamMsgListTableName, Jid) of
				[#user_msg_list{msg_list = KeysList}] ->					
					mnesia:dirty_delete(RamMsgListTableName, Jid),
					write_messages_to_sql(Jid, KeysList);
				_ ->
					skip
			end;	
		_ ->
			skip
	end,
	{reply, ok, State};

handle_call({load, Jid}, _From, State) ->
	Data = load_message_from_mysql(Jid),
	{reply, {ok, Data}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast(stop, State) ->
	{stop, normal, State};

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

get_userpid_name(#jid{user = Uid, server = Domain}) ->
	list_to_atom(Uid ++ "@" ++ Domain).

start(Name) ->
	gen_server:start({local, Name}, ?MODULE, [], []).


load_message_from_mysql(Jid) ->
	ok.

write_messages_to_sql(Jid, KeyList) ->
	ok.

