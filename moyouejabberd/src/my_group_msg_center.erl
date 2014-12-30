%% @author chenkangmin
%% @doc @todo Add description to my_group_msg_center.


-module(my_group_msg_center).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("aa_data.hrl").
-include("jlib.hrl").
-include("ejabberd.hrl").

-define(USER_MSD_PID_COUNT, 128).


-record(state, {user_msd_handlers = []}).

-record(group_msg, {id, group_id, from, packet, timestamp, expire_time, score}).

-record(group_id_seq, {group_id, sequence = 0}).

-record(user_group_info, {user_id, group_info_list}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         add_pool/1,
         list/0,
         update_user_group_info/3,
         init_user_group_info/2,
         store_message/3
        ]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_pool(Num) ->
    gen_server:call(?MODULE, {add_pool, Num}).

list() ->
    gen_server:call(?MODULE, {list}).


get_offline_msg(User) ->
    case mnesia:dirty_read(user_group_info, User) of
        [] ->
            {ok, []};
        [GroupInfo] ->
            Msgs = lists:fold(fun({GroupId, Seq}, Acc) ->
                                      {ok, Msg1} = case ets:lookup(my_group_msgpid_info, GroupId) of
                                                       [] ->
                                                           {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, GroupId}),
                                                           sync_deliver_task(get_offline_msg, Pid, GroupId, {GroupId, Seq});
                                                       [{GroupId, Pid}] ->
                                                           sync_deliver_task(get_offline_msg, Pid, GroupId, {GroupId, Seq})
                                                   end,
                                      lists:append(Acc, Msg1)
                              end, [], GroupInfo#user_group_info.group_info_list),
            {ok, Msgs}
    end.

update_user_group_info(User, GroupId, Seq) ->
    case ets:lookup(my_group_msgpid_info, GroupId) of
        [] ->
            {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, GroupId}),
            sync_deliver_task(update_user_group_info, Pid, GroupId, {User, GroupId, Seq});
        [{GroupId, Pid}] ->
            sync_deliver_task(update_user_group_info, Pid, GroupId, {User, GroupId, Seq})
    end.

delete_group_msg(GroupId, Sid) ->
    case ets:lookup(my_group_msgpid_info, GroupId) of
        [] ->
            {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, GroupId}),
            sync_deliver_task(delete_group_msg, Pid, GroupId, {GroupId, Sid});
        [{GroupId, Pid}] ->
            sync_deliver_task(delete_group_msg, Pid, GroupId, {GroupId, Sid})
    end.

init_user_group_info(User, GroupId) ->
    case ets:lookup(my_group_msgpid_info, GroupId) of
        [] ->
            {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, GroupId}),
            sync_deliver_task(init_user_group_info, Pid, GroupId, {User, GroupId});
        [{GroupId, Pid}] ->
            sync_deliver_task(init_user_group_info, Pid, GroupId, {User, GroupId})
    end.

store_message(User, GroupId, Packet) ->
    case ets:lookup(my_group_msgpid_info, GroupId) of
        [] ->
            {ok, Pid} = gen_server:call(?MODULE, {attach_new_pid, GroupId}),
            sync_deliver_task(store_msg, Pid, GroupId, {GroupId, User, Packet});
        [{GroupId, Pid}] ->
            sync_deliver_task(store_msg, Pid, GroupId, {GroupId, User, Packet})
    end.



create_or_copy_table(TableName, Opts, Copy) ->
    case mnesia:create_table(TableName, Opts) of
        {aborted,{already_exists,_}} ->
            mnesia:add_table_copy(TableName, node(), Copy);
        _ ->
            skip
    end.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

init([]) ->
    [Domain|_] = ?MYHOSTS,
    Pids = case ejabberd_config:get_local_option({handle_group_msg_tables, Domain}) of
               1 ->
                   ets:new(my_group_msgpid_info, [{keypos, 1}, named_table, public, set]),
                   create_or_copy_table(group_message, [{record_name, group_msg},
                                                        {attributes, record_info(fields, group_msg)},
                                                        {ram_copies, [node()]}], ram_copies),
                   create_or_copy_table(group_id_seq, [{record_name, group_id_seq},
                                                       {attributes, record_info(fields, group_id_seq)},
                                                       {ram_copies, [node()]}], ram_copies),
                   create_or_copy_table(user_group_info, [{record_name, user_group_info},
                                                          {attributes, record_info(fields, user_group_info)},
                                                          {ram_copies, [node()]}], ram_copies),
                   [begin
                        {ok, Pid} = my_group_user_msg_handler:start(),
                        {Pid, 0}
                    end || _ <- lists:duplicate(?USER_MSD_PID_COUNT, 1)];
               _ ->
                   []
           end,
    {ok, #state{user_msd_handlers = Pids}}.


handle_call({list}, _From, #state{user_msd_handlers = Handler} = State) ->
    {reply, {ok, length(Handler), Handler}, State};

handle_call({add_pool, Num}, _From, State) ->
    Pids = [begin
                {ok, Pid} = my_group_user_msg_handler:start(),
                {Pid, 0}
            end || _ <- lists:duplicate(Num, 1)],
    OrgPids = State#state.user_msd_handlers,
    NewPids = OrgPids ++ Pids,
    {reply, {{old_size, length(OrgPids)}, {new_size, length(NewPids)}}, State#state{user_msd_handlers = NewPids}};

handle_call({attach_new_pid, GroupId}, _From, #state{user_msd_handlers = Handler} = State) ->
    {Pid1, State1} = case ets:lookup(my_group_msgpid_info, GroupId) of
                         [] ->
                             [{Pid, Count} | _] = lists:keysort(2, Handler),
                             ets:insert(my_group_msgpid_info, {GroupId, Pid}),
                             {Pid, State#state{user_msd_handlers = lists:keyreplace(Pid, 1, Handler, {Pid, Count + 1})}};
                         [{GroupId, Pid}] ->
                             case erlang:is_process_alive(Pid) of
                                 true ->
                                     {Pid, State};
                                 _ ->
                                     Handler1 = lists:keydelete(Pid, 1, Handler),
                                     Size = length(Handler1),
                                     {NewPid1, NewState} = if
                                                               Size < ?USER_MSD_PID_COUNT ->
                                                                   {ok, NewPid} = my_group_user_msg_handler:start(),
                                                                   {NewPid, State#state{user_msd_handlers = [{NewPid, 0} | Handler1]}};
                                                               true ->
                                                                   [{NewPid, Count} | _] = lists:keysort(2, Handler1),
                                                                   {NewPid, State#state{user_msd_handlers = lists:keyreplace(NewPid, 1, Handler, {NewPid, Count + 1})}}
                                                           end,
                                     ets:delete(my_group_msgpid_info, GroupId),
                                     ets:insert(my_group_msgpid_info, {GroupId, NewPid1}),
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

sync_deliver_task(Task, Pid, GroupId, Args) ->
    try
        deliver(Task, Pid, Args)
    catch
        _ErrorType:_ErrorReason ->
            {ok, NewPid} = gen_server:call(?MODULE, {attach_new_pid, GroupId}),
            sync_deliver_task(Task, NewPid, GroupId, Args)
    end.

deliver(store_msg, Pid, {GroupId, User, Message}) ->
    my_group_user_msg_handler:store_msg(Pid, GroupId, User, Message);


deliver(init_user_group_info, Pid, {User, GroupId}) ->
    my_group_user_msg_handler:init_user_group_info(Pid, GroupId, User);


deliver(update_user_group_info, Pid, {User, GroupId, Seq}) ->
    my_group_user_msg_handler:update_user_group_info(Pid, GroupId, User, Seq);


deliver(get_offline_msg, Pid, {GroupId, Seq}) ->
    my_group_user_msg_handler:get_offline_msg(Pid, GroupId, Seq);

deliver(delete_group_msg, Pid, {GroupId, Sid}) ->
    my_group_user_msg_handler:delete_group_msg(Pid, GroupId, Sid).