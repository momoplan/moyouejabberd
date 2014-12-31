%% @author songzhiming
%% @doc @todo Add description to my_user_msg_handler.


-module(my_group_user_msg_handler).

-include("ejabberd.hrl").
-include("aa_data.hrl").
-include("jlib.hrl").

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).



%% ====================================================================
%% API functions
%% ====================================================================
-export([start/0,
         store_msg/4,
         get_offline_msg/2,
         get_offline_msg/4,
         init_user_group_info/3,
         update_user_group_info/4,
         delete_group_msg/3,
         clear_user_group_info/3
        ]).


-record(state, {}).


-record(group_msg, {id, group_id, from, packet, timestamp, expire_time, score}).

-record(group_id_seq, {group_id, sequence = 0}).

-record(user_group_info, {user_id, group_info_list}).


start() ->
    gen_server:start(?MODULE, [], []).

store_msg(Pid, GroupId, User, Message) ->
    gen_server:call(Pid, {store_msg, GroupId, User, Message}).

get_offline_msg(Pid, User) ->
    gen_server:call(Pid, {get_offline_msg, User}).

get_offline_msg(Pid, GroupId, Seq, User) ->
    gen_server:call(Pid, {get_offline_msg, GroupId, Seq, User}).

delete_group_msg(Pid, GroupId, Sid) ->
    gen_server:cast(Pid, {delete_group_msg, GroupId, Sid}).

init_user_group_info(Pid, GroupId, User) ->
    gen_server:cast(Pid, {init_user_group_info, GroupId, User}).


update_user_group_info(Pid, GroupId, User, Seq) ->
    gen_server:cast(Pid, {update_user_group_info, GroupId, User, Seq}).


clear_user_group_info(Pid, Uid, GroupId) ->
    gen_server:cast(Pid, {clear_user_group_info, Uid, GroupId}).

index_score()-> {M,S,T} = now(),  M*1000000000000+S*1000000+T.


unixtime() ->
    {M, S, _} = erlang:now(),
    M * 1000000 + S.

id_prefix(GroupId) ->
    lists:concat(["mygroup", "_", GroupId, "_"]).


get_current_seq(GroupId) ->
    case mnesia:dirty_read(group_id_seq, GroupId) of
        [] ->
            0;
        [{group_id_seq, GroupId, Sequence}] ->
            Sequence
    end.

get_id_seq(GroupId) ->
    case mnesia:dirty_read(group_id_seq, GroupId) of
        [] ->
            1;
        [{group_id_seq, GroupId, Sequence}] ->
            Sequence + 1
    end.

back_id_seq(GroupId) ->
    case mnesia:dirty_read(group_id_seq, GroupId) of
        [] ->
            skip;
        [{group_id_seq, GroupId, Sequence}] ->
            mnesia:dirty_write(group_id_seq, #group_id_seq{group_id = GroupId, sequence = Sequence - 1})
    end.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================




init([]) ->
    {ok, #state{}}.


handle_call({get_offline_msg, GroupId, Seq, User}, _From, State) ->
    CurrentSeq = get_id_seq(GroupId) - 1,
    Msgs = lists:foldl(fun(X, Acc) ->
                               case mnesia:dirty_read(group_message, id_prefix(GroupId) ++ integer_to_list(X)) of
                                   [] ->
                                       Acc;
                                   [Data] ->
                                       Packet = Data#group_msg.packet,
                                       From = Data#group_msg.from,
                                       Id = Data#group_msg.id,
                                       case User of
                                           From  ->
                                               Acc;
                                           _ ->
                                               [#user_msg{id = Id, from = From, to = User, packat = Packet} | Acc]
                                       end
                               end
                       end, [], lists:seq(Seq + 1, CurrentSeq)),
    {reply, {ok, Msgs}, State};

handle_call({get_offline_msg, User}, _From, State) ->
    Msgs = case mnesia:dirty_read(user_group_info, User) of
               [] ->
                   [];
               [GroupInfo] ->
                   lists:foldl(fun({GroupId, Seq}, Acc) ->
                                       {ok, Msg1} = my_group_msg_center:get_offline_msg(GroupId, Seq, User),
                                       lists:append(Acc, Msg1)
                               end, [], GroupInfo#user_group_info.group_info_list)
           end,
    {reply, {ok, Msgs}, State};

handle_call({store_msg, GroupId, User, Packet}, _From, State) ->
    Now = unixtime(),
    ExpireTime = Now + 1 * 24 *3600,
    Seq = get_id_seq(GroupId),
    Id = id_prefix(GroupId) ++ integer_to_list(Seq),
    {Tag, "message", Attr, Body} = Packet,
    Attr1 = case lists:keysearch("id", 1, Attr) of
                false ->
                    [{"id", Id} | Attr];
                _ ->
                    lists:keyreplace("id", 1, Attr, {"id", Id})
            end,
    Attr2 = lists:append(Attr1, [{"groupid", GroupId}]),
    Attr3 = lists:append(Attr2, [{"g","0"}]),
    Packet1 = {Tag, "message", Attr3, Body},
    Data = #group_msg{
        id = Id,
        group_id = GroupId,
        from = User,
        packet = Packet1,
        timestamp = Now,
        expire_time = ExpireTime,
        score = index_score()},
    mnesia:dirty_write(group_message, Data),
    mnesia:dirty_write(group_id_seq, #group_id_seq{group_id = GroupId, sequence = Seq}),
    {reply, {ok, Id}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast({delete_group_msg, GroupId, Sid}, State) ->
    mnesia:drity_delete(group_message, Sid),
    back_id_seq(GroupId),
    {noreply, State};

handle_cast({init_user_group_info, GroupId, User}, State) ->
    case mnesia:dirty_read(user_group_info, User) of
        [] ->
            GroupInfoList = [{GroupId, get_current_seq(GroupId)}],
            GroupInfo =  #user_group_info{user_id = User, group_info_list = GroupInfoList},
            mnesia:dirty_write(user_group_info, GroupInfo);
        [GroupInfo] ->
            GroupInfoList = GroupInfo#user_group_info.group_info_list,
            case lists:keysearch(GroupId, 1, GroupInfoList) of
                false ->
                    GroupInfo1 = GroupInfo#user_group_info{group_info_list = [{GroupId, get_current_seq(GroupId)} | GroupInfoList]},
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
                    GroupInfo1 = GroupInfo#user_group_info{group_info_list = lists:keyreplace(GroupId, 1, GroupInfoList, {GroupId, Seq})},
                    mnesia:dirty_write(user_group_info, GroupInfo1)
            end
    end,
    {noreply, State};

handle_cast({clear_user_group_info, Uid, GroupId}, State) ->
    [begin
         User = #jid{user = Uid, server = Domain, luser = Uid, lserver = Domain, resource = [], lresource = []},
         case mnesia:dirty_read(user_group_info, User) of
             [] ->
                 skip;
             [GroupInfo] ->
                 GroupInfoList = GroupInfo#user_group_info.group_info_list,
                 case lists:keysearch(GroupId, 1, GroupInfoList) of
                     false ->
                         skip;
                     _ ->
                         GroupInfo1 = GroupInfo#user_group_info{group_info_list = lists:keydelete(GroupId, 1, GroupInfoList)},
                         mnesia:dirty_write(user_group_info, GroupInfo1)
                 end
         end
     end || Domain <- ?MYHOSTS],
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.



handle_info(_Info, State) ->
    {noreply, State}.



terminate(_Reason, _State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


