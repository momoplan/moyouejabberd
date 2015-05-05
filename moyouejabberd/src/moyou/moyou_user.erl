-module(moyou_user).

-include("ejabberd.hrl").
-include("jlib.hrl").

-behaviour(gen_server).

-export([
    start_link/1,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).


-record(state, {uid, count = 0, resv1, resv2, resv3}).

-record(moyou_user, {id, session_list, resv1, resv2, resv3}).


start_link(Uid) ->
    gen_server:start_link(?MODULE, Uid, []).


init(Uid) ->
    erlang:send_after(30 * 1000, self(), check),
    case mnesia:dirty_read(moyou_user_tab, Uid) of
        [] ->
            mnesia:dirty_write(moyou_user_tab, #moyou_user{id = Uid, session_list = []});
        _ ->
            skip
    end,
    {ok, #state{uid = Uid}}.

handle_call({get_offline_msg}, _From, State) ->
    [UserInfo] =  mnesia:dirty_read(moyou_user_tab, State#state.uid),
    SessionList = UserInfo#moyou_user.session_list,
    %    OldMessages = moyou_compatible:get_offline_msg(State#state.uid),
    {reply, {ok, {get_offline_msg(SessionList, []), []}}, State#state{count = State#state.count + 1}};

handle_call({get_session_msg, SessionID, Seq, Size}, _From, State) ->
    [UserInfo] =  mnesia:dirty_read(moyou_user_tab, State#state.uid),
    SessionList = UserInfo#moyou_user.session_list,
    SeqList = get_session_seqs(SessionID, SessionList, Seq, Size),
    {reply, {ok, get_messages(SessionID, SeqList, [])}, State#state{count = State#state.count + 1}};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({update_session_all_seq, SessionID, Seq}, State) ->
    [UserInfo] =  mnesia:dirty_read(moyou_user_tab, State#state.uid),
    SessionList = UserInfo#moyou_user.session_list,
    case lists:keysearch(SessionID, 1, SessionList) of
        false ->
            R = case moyou_util:is_group_session(SessionID) of
                    true ->
                        {InitSeq, ReadSeq} = moyou_compatible:migrate_user_group_session_info(State#state.uid, SessionID, Seq),
                        UserInfo#moyou_user{session_list = [{SessionID, InitSeq,  ReadSeq, Seq} | SessionList]};
                    _ ->
                        UserInfo#moyou_user{session_list = [{SessionID, 0,  Seq - 1, Seq} | SessionList]}
                end,
            mnesia:dirty_write(moyou_user_tab, R);
        {value, {SessionID, InitSeq, ReadSeq, AllSeq}} ->
            if
                Seq > AllSeq ->
                    UserInfo1 = UserInfo#moyou_user{session_list = lists:keyreplace(SessionID, 1, SessionList, {SessionID, InitSeq, ReadSeq, Seq})},
                    mnesia:dirty_write(moyou_user_tab, UserInfo1);
                true ->
                    skip
            end
    end,
    {noreply, State#state{count = State#state.count + 1}};
handle_cast({update_session_read_seq, SessionID, Seq}, State) ->
    [UserInfo] =  mnesia:dirty_read(moyou_user_tab, State#state.uid),
    SessionList = UserInfo#moyou_user.session_list,
    case lists:keysearch(SessionID, 1, SessionList) of
        false ->
            skip;
        {value, {SessionID, InitSeq, ReadSeq, AllSeq}} ->
            case moyou_util:is_group_session(SessionID) of
                true ->
                    if
                        Seq > ReadSeq ->
                            UserInfo1 = UserInfo#moyou_user{session_list = lists:keyreplace(SessionID, 1, SessionList, {SessionID, InitSeq, Seq, AllSeq})},
                            mnesia:dirty_write(moyou_user_tab, UserInfo1);
                        true ->
                            skip
                    end;
                _ ->
                    if
                        Seq =:= AllSeq ->
                            UserInfo1 = UserInfo#moyou_user{session_list = lists:keydelete(SessionID, 1, SessionList)},
                            mnesia:dirty_write(moyou_user_tab, UserInfo1);
                        Seq > ReadSeq ->
                            UserInfo1 = UserInfo#moyou_user{session_list = lists:keyreplace(SessionID, 1, SessionList, {SessionID, InitSeq, Seq, AllSeq})},
                            mnesia:dirty_write(moyou_user_tab, UserInfo1);
                        true ->
                            skip
                    end,
                    case moyou_util:is_system_session(SessionID) of
                        true ->
                            case State#state.uid of
                                "10000" ->
                                    skip;
                                [$s, $y, $s | _] ->
                                    skip;
                                _ ->
                                    Mid = lists:concat([SessionID, "_", Seq]),
                                    mnesia:dirty_delete(moyou_message_tab, Mid),
                                    Sql = io_lib:format("delete from moyou_message where mid = '~s'",[Mid]),
                                    db_sql:execute(Sql)
                            end;
                        _ ->
                            skip
                    end
            end
    end,
    {noreply, State#state{count = State#state.count + 1}};
handle_cast({clear_user_session_info, SessionID}, State) ->
    [UserInfo] =  mnesia:dirty_read(moyou_user_tab, State#state.uid),
    SessionList = UserInfo#moyou_user.session_list,
    UserInfo1 = UserInfo#moyou_user{session_list = lists:keydelete(SessionID, 1, SessionList)},
    mnesia:dirty_write(moyou_user_tab, UserInfo1),
    {noreply, State#state{count = State#state.count + 1}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, State) ->
    if
        State#state.count > 0 ->
            erlang:send_after(30 * 1000, self(), check),
            {noreply, State#state{count = 0}};
        true ->  %%30s内没有处理任何消息，自动stop
            {stop, normal, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ets:delete(ets_moyou_user, State#state.uid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


get_offline_msg([], Acc) ->
    Acc;
get_offline_msg([{SessionID, _InitSeq, ReadSeq, AllSeq} | T], Acc) ->
    Seqs = get_offline_seqs(SessionID, AllSeq, ReadSeq),
    Messages = get_messages(SessionID, Seqs, []),
    case moyou_util:is_group_session(SessionID) of
        true ->
            get_offline_msg(T, lists:append(Acc, append_number_message(SessionID, AllSeq - ReadSeq, Messages)));
        _ ->
            get_offline_msg(T, lists:append(Acc, Messages))
    end.
          

get_offline_seqs(SessionID, AllSeq, ReadSeq) ->
    Diff = AllSeq - ReadSeq,
    if
        Diff > 20 ->
            case moyou_util:is_group_session(SessionID) of
                true ->
                    lists:seq(AllSeq - 19, AllSeq);
                _ ->
                    lists:seq(ReadSeq + 1, AllSeq)
            end;
        Diff > 0 andalso Diff =< 20 ->
            lists:seq(ReadSeq + 1, AllSeq);
        true ->
            []
    end.


get_session_seqs(SessionID, SessionList, Seq, Size) ->
    case lists:keysearch(SessionID, 1, SessionList) of
        false ->
            [];
        {value, {SessionID, InitSeq, _ReadSeq, _AllSeq}} ->
            if
                Seq - InitSeq >= Size ->
                    lists:seq(Seq - Size, Seq - 1);
                Seq - InitSeq < Size ->
                    if
                        Seq > InitSeq ->
                            lists:seq(InitSeq + 1, Seq - 1);
                        true ->
                            []
                    end
            end
    end.
           

get_messages(_SessionID, [], Acc) ->
    lists:reverse(Acc);
get_messages(SessionID, [Seq | T], Acc) ->
    Mid = lists:concat([SessionID, "_", Seq]),
    case mnesia:dirty_read(moyou_message_tab, Mid) of
        [] ->
            Sql = lists:flatten(io_lib:format("select `from`, `mt`, `packet`, `time` from moyou_message where mid = '~s'", [Mid])),
            case catch db_sql:get_row(Sql) of
                {'EXIT', Reason} ->
                    ?ERROR_MSG("Sql : ~p get row error~n, reason : ~p~n", [Sql, Reason]),
                    get_messages(SessionID, T, Acc);
                [] ->
                    get_messages(SessionID, T, Acc);
                [From, Mt, Packet, Time]  ->
                    Message = moyou_session:pack_db_message(Mid, SessionID, From, Mt, Packet, Time),
                    get_messages(SessionID, T, [Message | Acc])
            end;
        [Message] ->
            get_messages(SessionID, T, [Message | Acc])
    end.


append_number_message(_SessionID, _Diff, []) ->
    [];
append_number_message(SessionID, Diff, Messages) ->
    GroupID = moyou_util:get_group_id_from_session(SessionID),
    Attr = [{"groupId", GroupID},
            {"type", "normal"},
            {"msgtype", "groupMsgNumber"},
            {"number", integer_to_list(Diff)}],
    Packet = {xmlelement, "message", Attr, []},
    From = #jid{user = "groupMsgNumber", server = "gamepro.com", resource = ""},
    NumberMessage = moyou_session:pack_message("", SessionID, From, "groupMsgNumber", Packet, ""),
    Messages ++ [NumberMessage].
