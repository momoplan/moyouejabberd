%% @author songzhiming
%% @doc @todo Add description to my_user_msg_handler.


-module(my_group_user_msg_handler).

-include("ejabberd.hrl").

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).



%% ====================================================================
%% API functions
%% ====================================================================
-export([start/0,
         store_msg/4,
         init_user_group_info/3,
         update_user_group_info/4
        ]).

%% start_link() ->
%% 	gen_server:start_link(?MODULE, [], []).

start() ->
    gen_server:start(?MODULE, [], []).

store_msg(Pid, GroupId, User, Message) ->
    gen_server:call(Pid, {store_msg, GroupId, User, Message}).


init_user_group_info(Pid, GroupId, User) ->
    gen_server:cast(Pid, {init_user_group_info, GroupId, User}).


update_user_group_info(Pid, GroupId, User, Seq) ->
    gen_server:cast(Pid, {update_user_group_info, GroupId, User, Seq}).


index_score()-> {M,S,T} = now(),  M*1000000000000+S*1000000+T.


unixtime() ->
    {M, S, _} = erlang:now(),
    M * 1000000 + S.

id_prefix(GroupId) ->
    lists:concat(["mygroup", "_", GroupId, "_"]).

get_id_seq(GroupId) ->
    case mnesia:dirty_read(group_id_seq, GroupId) of
        [] ->
            1;
        [{group_id_seq, GroupId, Sequence}] ->
            Sequence + 1
    end.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {}).


-record(group_msg, {id, group_id, from, packet, timestamp, expire_time, score}).

-record(group_id_seq, {group_id, sequence = 0}).



init([]) ->
    {ok, #state{}}.


handle_call({store_msg, GroupId, User, Packet}, _From, State) ->
    Now = unixtime(),
    ExpireTime = Now + 1 * 24 *3600,
    Seq = get_id_seq(GroupId),
    Id = id_prefix(GroupId) ++ integer_to_list(Seq),
    Data = #group_msg{
        id = Id,
        group_id = GroupId,
        from = User,
        packet = Packet,
        timestamp = Now,
        expire_time = ExpireTime,
        score = index_score()},
    mnesia:dirty_write(group_message, Data),
    mnesia:dirty_write(group_id_seq, #group_id_seq{group_id = GroupId, sequence = Seq}),
    {reply, {ok, Id}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast({init_user_group_info, GroupId, User}, State) ->
    case mnesia:dirty_read(user_group_info, User) of
        [] ->
            GroupInfoList = [{GroupId, 0}],
            GroupInfo =  #user_group_info{user_id = User, group_info_list = GroupInfoList},
            mnesia:dirty_write(user_group_info, GroupInfo);
        [GroupInfo] ->
            GroupInfoList = GroupInfo#user_group_info.group_info_list,
            case lists:keysearch(GroupId, 1, GroupInfoList) of
                false ->
                    GroupInfo1 = GroupInfo#user_group_info{group_info_list = [{GroupId, 0} | GroupInfoList]},
                    mnesia:dirty_write(user_group_info, GroupInfo1);
                _ ->
                    skip
            end
    end,
    {noreply, State};

handle_cast({update_user_group_info, GroupId, User, Seq}, State) ->
    case mnesia:dirty_read(user_group_info, User) of
        [] ->
            GroupInfoList = [{GroupId, Seq}],
            GroupInfo =  #user_group_info{user_id = User, group_info_list = GroupInfoList},
            mnesia:dirty_write(user_group_info, GroupInfo);
        [GroupInfo] ->
            GroupInfoList = GroupInfo#user_group_info.group_info_list,
            case lists:keysearch(GroupId, 1, GroupInfoList) of
                false ->
                    GroupInfo1 = GroupInfo#user_group_info{group_info_list = [{GroupId, Seq} | GroupInfoList]},
                    mnesia:dirty_write(user_group_info, GroupInfo1);
                _ ->
                    lists:keyreplace(GroupId, 1, GroupInfoList, {GroupId, Seq})
            end
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.



handle_info(_Info, State) ->
    {noreply, State}.



terminate(_Reason, _State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


