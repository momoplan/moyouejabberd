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
         clear_user_group_info/3,
         query_group_msg/5,
         dump/2
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

query_group_msg(Pid, GroupId, Uid, Seq, Size) ->
    gen_server:call(Pid, {query_group_msg, GroupId, Uid, Seq, Size}).

dump(Pid, GroupId) ->
    gen_server:cast(Pid, {dump, GroupId}).

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


handle_call({get_offline_msg, GroupId, Seq, #jid{server = Domain} = User}, _From, State) ->
    CurrentSeq = get_current_seq(GroupId),
    %%离线消息数量
    Number = CurrentSeq - Seq,
    %%最多返还20条离线群组数据
    List = if
               Number > 20 ->
                   lists:seq(CurrentSeq - 20 + 1, CurrentSeq);
               Seq > CurrentSeq ->
                   %%异常，当用户序列号大于当前群的序列时，更新为当前群的序列
                   my_group_msg_center:update_user_group_info(User, GroupId, CurrentSeq),
                   [];
               true ->
                   lists:seq(Seq + 1, CurrentSeq)
           end,
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
                       end, [], List),
    case Msgs of
        [] ->
            {reply, {ok, Msgs}, State};
        _ ->
            Attr = [{"groupId", GroupId},
                    {"type", "normal"},
                    {"msgtype", "groupMsgNumber"},
                    {"number", integer_to_list(Number)}],
            Packet = {xmlelement, "message", Attr, []},
            From = #jid{user = "groupMsgNumber", server = Domain, resource = ""},
            NumMessage = #user_msg{from = From, to = User, packat = Packet},
            {reply, {ok, [NumMessage | Msgs]}, State}
    end;
    

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

handle_call({query_group_msg, GroupId, Uid, Seq, Size}, _From, State) ->
    List = if
               Seq - Size > 0 ->
                   lists:seq(Seq - Size, Seq - 1);
               true ->
                   lists:seq(1, Seq - 1)
           end,
    [Domain | _] = ?MYHOSTS,
    Msgs = lists:foldl(fun(X, Acc) ->
                               Mid = id_prefix(GroupId) ++ integer_to_list(X),
                               case mnesia:dirty_read(group_message, Mid) of
                                   [] ->
                                       Sql = lists:flatten(io_lib:format("select content from group_message where mid = '~s'", [Mid])),
                                       case catch db_sql:get_row(Sql) of
                                           {'EXIT', Reason} ->
                                               ?ERROR_MSG("Sql : ~p get row error, reason : ~p~n", [Sql, Reason]),
                                               Acc;
                                           [] ->
                                               Acc;
                                           [Content]  ->
                                               {_Key, From, PacketBin} = bitstring_to_term(Content),
                                               Packet = binary_to_term(PacketBin),
                                               To = #jid{user = Uid, server = Domain, luser = Uid, lserver = Domain, resource = [], lresource = []},
                                               [#user_msg{id = Mid, from = From, to = To, packat = Packet} | Acc]
                                       end;
                                   [Data] ->
                                       Packet = Data#group_msg.packet,
                                       From = Data#group_msg.from,
                                       To = #jid{user = Uid, server = Domain, luser = Uid, lserver = Domain, resource = [], lresource = []},
                                       [#user_msg{id = Mid, from = From, to = To, packat = Packet} | Acc]
                               end
                       end, [], List),
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
    my_group_msg_center:update_user_group_info(User, GroupId, Seq),
    {reply, {ok, Id}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast({dump, GroupId}, State) ->
    Messages = mnesia:dirty_match_object(group_message, #group_msg{group_id = GroupId, _ = '_'}),
    if
        length(Messages) > 100 ->
            CurrentSeq = get_current_seq(GroupId),
            KeepList = [id_prefix(GroupId) ++ integer_to_list(Seq) || Seq <- lists:seq(CurrentSeq - 99, CurrentSeq)],
            [begin
                 case lists:member(Message#group_msg.id, KeepList) of
                     true ->
                         skip;
                     _ ->
                         Sql = "insert into group_message(`group_id`, `mid`, `content`, `createDate`) values",
                         #group_msg{id = Key, from = From, packet = Packet, timestamp = TimeStamp} = Message,
                         Ts = case TimeStamp of
                                  {datetime, _} ->
                                      0;
                                  _ ->
                                      TimeStamp
                              end,
                         Content = term_to_bitstring({Key, From, term_to_binary(Packet)}),
                         Values = lists:flatten(io_lib:format("('~s', '~s', '~s', ~p)", [GroupId, Key, Content, Ts])),
                         catch db_sql:execute(Sql ++ Values),
                         mnesia:dirty_delete(group_message, Key)
                 end
             end || Message <- Messages];
        true ->
            skip
    end,
    {noreply, State};

handle_cast({delete_group_msg, GroupId, Sid}, State) ->
    mnesia:dirty_delete(group_message, Sid),
    back_id_seq(GroupId),
    {noreply, State};

handle_cast({init_user_group_info, GroupId, User}, State) ->
    case mnesia:dirty_read(user_group_info, User) of
        [] ->
            CurrentSeq = get_current_seq(GroupId),
            GroupInfoList = [{GroupId, CurrentSeq, CurrentSeq}],
            GroupInfo =  #user_group_info{user_id = User, group_info_list = GroupInfoList},
            mnesia:dirty_write(user_group_info, GroupInfo);
        [GroupInfo] ->
            GroupInfoList = GroupInfo#user_group_info.group_info_list,
            case lists:keysearch(GroupId, 1, GroupInfoList) of
                false ->
                    CurrentSeq = get_current_seq(GroupId),
                    GroupInfo1 = GroupInfo#user_group_info{group_info_list = [{GroupId, CurrentSeq, CurrentSeq} | GroupInfoList]},
                    mnesia:dirty_write(user_group_info, GroupInfo1);
                {value, {GroupId, Seq}} ->
                    GroupInfo1 = GroupInfo#user_group_info{group_info_list = lists:keyreplace(GroupId, 1, GroupInfoList, {GroupId, Seq, Seq})},
                    mnesia:dirty_write(user_group_info, GroupInfo1);
                _ ->
                    skip
            end
    end,
    {noreply, State};

handle_cast({update_user_group_info, GroupId, User, Seq}, State) ->
    case mnesia:dirty_read(user_group_info, User) of
        [] ->
            GroupInfoList = [{GroupId, Seq, Seq}],
            GroupInfo =  #user_group_info{user_id = User, group_info_list = GroupInfoList},
            mnesia:dirty_write(user_group_info, GroupInfo);
        [GroupInfo] ->
            GroupInfoList = GroupInfo#user_group_info.group_info_list,
            case lists:keysearch(GroupId, 1, GroupInfoList) of
                false ->
                    GroupInfo1 = GroupInfo#user_group_info{group_info_list = [{GroupId, Seq} | GroupInfoList]},
                    mnesia:dirty_write(user_group_info, GroupInfo1);
                {value, {GroupId, Value}} ->
                    if
                        Seq > Value ->
                            GroupInfo1 = GroupInfo#user_group_info{group_info_list = lists:keyreplace(GroupId, 1, GroupInfoList, {GroupId, Value, Seq})},
                            mnesia:dirty_write(user_group_info, GroupInfo1);
                        true ->
                            GroupInfo1 = GroupInfo#user_group_info{group_info_list = lists:keyreplace(GroupId, 1, GroupInfoList, {GroupId, Value, Value})},
                            mnesia:dirty_write(user_group_info, GroupInfo1)
                    end;
                {value, {GroupId, Init, Value}} ->
                    if
                        Seq > Value ->
                            GroupInfo1 = GroupInfo#user_group_info{group_info_list = lists:keyreplace(GroupId, 1, GroupInfoList, {GroupId, Init, Seq})},
                            mnesia:dirty_write(user_group_info, GroupInfo1);
                        true ->
                            skip
                    end
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


term_to_bitstring(Term) ->
    erlang:list_to_bitstring(io_lib:format("~p", [Term])).

bitstring_to_term(undefined) -> undefined;
bitstring_to_term(BitString) ->
    string_to_term(binary_to_list(BitString)).


string_to_term(String) ->
    case erl_scan:string(String++".") of
        {ok, Tokens, _} ->
            case catch erl_parse:parse_term(Tokens) of
                {ok, Term} -> Term;
                _Err -> undefined
            end;
        _Error ->
            undefined
    end.