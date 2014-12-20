%% @author songzhiming
%% @doc @todo Add description to my_msg_center.


-module(my_msg_center).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("aa_data.hrl").
-include("jlib.hrl").
-include("ejabberd.hrl").

-define(USER_MSD_PID_COUNT, 30).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         add_pool/1,
         store_message/2,
         delete_message/2,
         get_offline_msg/1]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_pool(Num) ->
    gen_server:call(?MODULE, {add_pool, Num}).

store_message(User, Message) ->
    UserPidName = get_user_pid_name(User),
    case ets:lookup(?MY_USER_MSGPID_INFO, UserPidName) of
        [] ->
            gen_server:cast(?MODULE, {fail_deliver, UserPidName, none, User, store_msg, {User, Message}});
        [#?MY_USER_MSGPID_INFO{pid = Pid}] ->
            deliver_task(store_msg, Pid, UserPidName, User, {User, Message})
    end.

delete_message(User, Key) ->
    UserPidName = get_user_pid_name(User),
    case ets:lookup(?MY_USER_MSGPID_INFO, UserPidName) of
        [] ->
            gen_server:cast(?MODULE, {fail_deliver, UserPidName, none, User, del_msg, {Key, User}});
        [#?MY_USER_MSGPID_INFO{pid = Pid}] ->
            deliver_task(del_msg, Pid, UserPidName, User, {Key, User})
    end.

get_offline_msg(User) ->
	UserPidName = get_user_pid_name(User),
	case ets:lookup(?MY_USER_MSGPID_INFO, UserPidName) of
		[] ->
			NewPid = gen_server:call(?MODULE, {attach_new_pid, User, UserPidName, none}),
			sync_deliver_task(get_offline_msg, NewPid, UserPidName, User, User);
		[#?MY_USER_MSGPID_INFO{pid = Pid}] ->
			sync_deliver_task(get_offline_msg, Pid, UserPidName, User, User)
	end.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {user_msd_handlers = []}).


init([]) ->
    ets:new(?MY_USER_MSGPID_INFO, [{keypos, #?MY_USER_MSGPID_INFO.user}, named_table, public, set]),
    Pids = [begin
                {ok, Pid} = my_user_msg_handler:start_link(),
                Pid
            end || _ <- lists:duplicate(?USER_MSD_PID_COUNT, 1)],
    {ok, #state{user_msd_handlers = Pids}}.


handle_call({add_pool, Num}, _From, State) ->
    Pids = [begin
                {ok, Pid} = my_user_msg_handler:start_link(),
                Pid
            end || _ <- lists:duplicate(Num, 1)],
    OrgPids = State#state.user_msd_handlers,
    NewPids = OrgPids ++ Pids,
    {reply, {{old_size, length(OrgPids)}, {new_size, length(NewPids)}}, State#state{user_msd_handlers = NewPids}};

handle_call({attach_new_pid, User, PidName, Pid}, _From, State) ->
    if is_pid(Pid) ->
            State1 = clean_failed_pid(Pid, PidName, State);
        true ->
            State1 = State
    end,
    {reply, {ok, attach_user_to_pid(PidName, User, State1)}, State1};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast({store_msg, User, UserPidName, Message}, State) ->
    Pid = attach_user_to_pid(UserPidName, User, State),
    deliver_task(store_msg, Pid, UserPidName, User, {User, Message}),
    {noreply, State};

handle_cast({fail_deliver, PidName, Pid, User, Task, Args}, State) ->
	if Pid /= none ->
		   State1 = clean_failed_pid(Pid, PidName, State);
	   true ->
		   State1 = State
	end,
	NewPid  = attach_user_to_pid(PidName, User, State1),
	deliver_task(Task, NewPid, PidName, User, Args),
	{noreply, State1};

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

clean_failed_pid(Pid, PidName, State) ->
    ClearedPidList = lists:delete(Pid, State#state.user_msd_handlers),
    ets:delete(?MY_USER_MSGPID_INFO, PidName),
    {ok, NewPid} = my_user_msg_handler:start_link(),
    NewPidList = ClearedPidList ++ [NewPid],
    State#state{user_msd_handlers = NewPidList}.


get_user_pid_name(#jid{user = User, server = Server}) when is_list(User),is_list(Server) ->
	list_to_binary(User ++ "/" ++ Server);

get_user_pid_name(#jid{user = User, server = Server}) when is_binary(User), is_binary(Server) ->
	<<User/binary, <<"/">>/binary, Server/binary>>.

user_id_to_num(#jid{user = User}) when is_list(User) ->
    lists:sum(User);

user_id_to_num(#jid{user = User}) when is_binary(User) ->
    lists:sum(binary_to_list(User)).

attach_user_to_pid(UserPidName, User, State) ->
    Number = user_id_to_num(User),
    Index = Number rem length(State#state.user_msd_handlers) + 1,
    Pid = lists:nth(Index, State#state.user_msd_handlers),
    NewInfo = #?MY_USER_MSGPID_INFO{user= UserPidName, pid = Pid},
    ets:insert(?MY_USER_MSGPID_INFO, NewInfo),
    Pid.

deliver_task(Task, Pid, PidName, User, Args) ->
    try
        deliver(Task, Pid, Args)
    catch
        ErrorType:ErrorReason ->
            ?ERROR_MSG("deliver task to msg handler ~p:~p, ~n call stack ~p", [ErrorType,
                                                                               ErrorReason,
                                                                               erlang:get_stacktrace()]),
            gen_server:cast(?MODULE, {fail_deliver, PidName, Pid, User, Task, Args})
    end.

sync_deliver_task(Task, Pid, PidName, User, Args) ->
    try
        deliver(Task, Pid, Args)
    catch
        ErrorType:ErrorReason ->
            ?ERROR_MSG("deliver task to msg handler ~p:~p, ~n call stack ~p", [ErrorType,
                                                                               ErrorReason,
                                                                               erlang:get_stacktrace()]),
            NewPid = gen_server:call(?MODULE, {attach_new_pid, User, PidName, Pid}),
            sync_deliver_task(Task, NewPid, PidName, User, Args)
    end.

deliver(store_msg, Pid, {User, Message}) ->
    my_user_msg_handler:store_msg(Pid, User, Message);

deliver(del_msg, Pid, {Key, User}) ->
    my_user_msg_handler:delete_msg(Pid, Key, User);

deliver(get_offline_msg, Pid, User) ->
    my_user_msg_handler:get_offline_msg(Pid, User).
