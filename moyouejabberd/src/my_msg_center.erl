%% @author songzhiming
%% @doc @todo Add description to my_msg_center.


-module(my_msg_center).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("aa_data.hrl").
-include("jlib.hrl").
-include("ejabberd.hrl").

-define(USER_MSD_PID_COUNT, 128).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         add_pool/1,
         store_message/2,
         delete_message/2,
         get_offline_msg/1,
         dump/1,
         list/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_pool(Num) ->
    gen_server:call(?MODULE, {add_pool, Num}).


list() ->
    gen_server:call(?MODULE, {list}).


store_message(User, Message) ->
    PidName = get_user_pid_name(User),
    case ets:lookup(?MY_USER_MSGPID_INFO, PidName) of
        [] ->
            {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, PidName}),
            sync_deliver_task(store_msg, Pid, PidName, {User, Message});
        [#?MY_USER_MSGPID_INFO{pid = Pid}] ->
            sync_deliver_task(store_msg, Pid, PidName, {User, Message})
    end.

delete_message(User, Key) ->
    PidName = get_user_pid_name(User),
    case ets:lookup(?MY_USER_MSGPID_INFO, PidName) of
        [] ->
            {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, PidName}),
            sync_deliver_task(del_msg, Pid, PidName, {Key, User});
        [#?MY_USER_MSGPID_INFO{pid = Pid}] ->
            sync_deliver_task(del_msg, Pid, PidName, {Key, User})
    end.

get_offline_msg(User) ->
    PidName = get_user_pid_name(User),
    case ets:lookup(?MY_USER_MSGPID_INFO, PidName) of
        [] ->
            {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, PidName}),
            sync_deliver_task(get_offline_msg, Pid, PidName, User);
        [#?MY_USER_MSGPID_INFO{pid = Pid}] ->
            sync_deliver_task(get_offline_msg, Pid, PidName, User)
    end.

dump(User) ->
    PidName = get_user_pid_name(User),
    case ets:lookup(?MY_USER_MSGPID_INFO, PidName) of
        [] ->
            {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, PidName}),
            sync_deliver_task(dump, Pid, PidName, User);
        [#?MY_USER_MSGPID_INFO{pid = Pid}] ->
            sync_deliver_task(dump, Pid, PidName, User)
    end.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {user_msd_handlers = []}).


init([]) ->
    ets:new(?MY_USER_MSGPID_INFO, [{keypos, #?MY_USER_MSGPID_INFO.user}, named_table, public, set]),
    Pids = [begin
                {ok, Pid} = my_user_msg_handler:start(),
                {Pid, 0}
            end || _ <- lists:duplicate(?USER_MSD_PID_COUNT, 1)],
    {ok, #state{user_msd_handlers = Pids}}.


handle_call({list}, _From, #state{user_msd_handlers = Handler} = State) ->
    {reply, {ok, length(Handler), Handler}, State};

handle_call({add_pool, Num}, _From, State) ->
    Pids = [begin
                {ok, Pid} = my_user_msg_handler:start(),
                {Pid, 0}
            end || _ <- lists:duplicate(Num, 1)],
    OrgPids = State#state.user_msd_handlers,
    NewPids = OrgPids ++ Pids,
    {reply, {{old_size, length(OrgPids)}, {new_size, length(NewPids)}}, State#state{user_msd_handlers = NewPids}};

handle_call({attach_new_pid, PidName}, _From, #state{user_msd_handlers = Handler} = State) ->
    {Pid1, State1} = case ets:lookup(?MY_USER_MSGPID_INFO, PidName) of
                         [] ->
                             [{Pid, Count} | _] = lists:keysort(2, Handler),
                             NewInfo = #?MY_USER_MSGPID_INFO{user= PidName, pid = Pid},
                             ets:insert(?MY_USER_MSGPID_INFO, NewInfo),
                             {Pid, State#state{user_msd_handlers = lists:keyreplace(Pid, 1, Handler, {Pid, Count + 1})}};
                         [#?MY_USER_MSGPID_INFO{pid = Pid}] ->
                             case erlang:is_process_alive(Pid) of
                                 true ->
                                     {Pid, State};
                                 _ ->
                                     Handler1 = lists:keydelete(Pid, 1, Handler),
                                     Size = length(Handler1),
                                     {NewPid1, NewState} = if
                                                               Size < ?USER_MSD_PID_COUNT ->
                                                                   {ok, NewPid} = my_user_msg_handler:start(),
                                                                   {NewPid, State#state{user_msd_handlers = [{NewPid, 0} | Handler1]}};
                                                               true ->
                                                                   [{NewPid, Count} | _] = lists:keysort(2, Handler1),
                                                                   {NewPid, State#state{user_msd_handlers = lists:keyreplace(NewPid, 1, Handler, {NewPid, Count + 1})}}
                                                           end,
                                     ets:delete(?MY_USER_MSGPID_INFO, PidName),
                                     NewInfo = #?MY_USER_MSGPID_INFO{user= PidName, pid = NewPid1},
                                     ets:insert(?MY_USER_MSGPID_INFO, NewInfo),
                                     {NewPid1, NewState}
                             end
                     end,
    {reply, {ok, Pid1}, State1};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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


get_user_pid_name(#jid{user = User, server = Server}) when is_list(User),is_list(Server) ->
    list_to_binary(User ++ "/" ++ Server);

get_user_pid_name(#jid{user = User, server = Server}) when is_binary(User), is_binary(Server) ->
    <<User/binary, <<"/">>/binary, Server/binary>>.

sync_deliver_task(Task, Pid, PidName, Args) ->
    try
        deliver(Task, Pid, Args)
    catch
        _ErrorType:_ErrorReason ->
            {ok, NewPid} = gen_server:call(?MODULE, {attach_new_pid, PidName}),
            sync_deliver_task(Task, NewPid, PidName, Args)
    end.

deliver(store_msg, Pid, {User, Message}) ->
    my_user_msg_handler:store_msg(Pid, User, Message);

deliver(del_msg, Pid, {Key, User}) ->
    my_user_msg_handler:delete_msg(Pid, Key, User);

deliver(get_offline_msg, Pid, User) ->
    my_user_msg_handler:get_offline_msg(Pid, User);

deliver(dump, Pid, User) ->
    my_user_msg_handler:dump(Pid, User).

