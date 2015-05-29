-module(moyou_session).

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


-export([
    pack_message/6,
    pack_db_message/6,
    init_session_seq/2
        ]).


-record(state, {session_id, seq, count = 0, resv1, resv2, resv3}).


%%resv1存储最后一条消息的时间
-record(moyou_message, {id, session_id, from, mt, packet, time, resv1, resv2, resv3}).

-record(moyou_session_seq, {session_id, seq = 0, resv1, resv2, resv3}).


pack_message(Mid, SessionID, From, Mt, Packet, Time) ->
    #moyou_message{id = Mid,
                   session_id = SessionID,
                   from = From,
                   mt = Mt,
                   packet = Packet,
                   time = Time
                  }.

pack_db_message(Mid, SessionID, From, Mt, Packet, Time) ->
    #moyou_message{id = Mid,
                   session_id = SessionID,
                   from = moyou_util:bitstring_to_term(From),
                   mt = Mt,
                   packet = binary_to_term(moyou_util:bitstring_to_term(Packet)),
                   time = Time
                  }.


start_link(SessionID) ->
    gen_server:start_link(?MODULE, SessionID, []).


%% ====================================================================
%% Behavioural functions 
%% ====================================================================
init(SessionID) ->
    erlang:send_after(30 * 1000, self(), check),
    case mnesia:dirty_read(moyou_session_seq_tab, SessionID) of
        [] ->
            Sql = io_lib:format("select seq, resv1 from moyou_session_seq where session_id = '~s'", [SessionID]),
            case db_sql:get_row(Sql) of
                [] ->
                    %%群组session,旧版本兼容处理
                    case moyou_util:is_group_session(SessionID) of
                        true ->
                            Seq = moyou_compatible:migrate_group_session_seq(SessionID),
                            init_session_seq(SessionID, Seq),
                            {ok, #state{session_id = SessionID, seq = Seq}};
                        _ ->
                            init_session_seq(SessionID, 0),
                            {ok, #state{session_id = SessionID, seq = 0}}
                    end;
                [Seq, Resv1] ->
                    R = #moyou_session_seq{session_id = SessionID, seq = Seq, resv1 = convert(Resv1)},
                    mnesia:dirty_write(moyou_session_seq_tab, R),
                    {ok, #state{session_id = SessionID, seq = Seq}}
            end;
        [R] ->
            {ok, #state{session_id = SessionID, seq = R#moyou_session_seq.seq}}
    end.

handle_call({query_session_seq}, _From, State) ->
    {reply, {ok, State#state.seq}, State#state{count = State#state.count + 1}};

handle_call({store_message, From, Packet}, _From, #state{session_id = SessionID, seq = Seq} = State) ->
    {Tag, "message", Attrs, Body} = Packet,
    Cid = xml:get_attr_s("id", Attrs),
    Mt = xml:get_attr_s("msgtype", Attrs),
    Now = moyou_util:unixtime(),
    case ets:lookup(ets_cid_and_sid, Cid) of
        [{Cid, Sid, _}] ->
            {reply, {repeat, Sid}, State#state{count = State#state.count + 1}};
        _ ->
            Sid = lists:concat([SessionID, "_", Seq + 1]),
            Attrs1 = [{"id", Sid} | [{K, V} || {K, V} <- Attrs, K =/= "id"]],
            Packet1 = {Tag, "message", Attrs1, Body},
            Message = pack_message(Sid, SessionID, From, Mt, Packet1, Now),
            mnesia:dirty_write(moyou_message_tab, Message),
            R = #moyou_session_seq{session_id = SessionID, seq = Seq + 1, resv1 = Now},
            mnesia:dirty_write(moyou_session_seq_tab, R),
            ets:insert(ets_cid_and_sid, {Cid, Sid, Now}),
            {reply, {ok, Sid, Seq + 1}, State#state{seq = Seq + 1, count = State#state.count + 1}}
    end;

handle_call({get_session_msg, Seq, Size}, _From, #state{session_id = SessionID, seq = Seq} = State) ->
    SeqList = get_session_seqs(Seq, Size),
    {reply, {ok, get_messages(SessionID, SeqList, [])}, State#state{count = State#state.count + 1}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

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
    Now = moyou_util:unixtime(),
    ets:update_element(ets_moyou_session, State#state.session_id, [{2, undefined}, {3, Now}]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


init_session_seq(SessionID, Seq) ->
    R = #moyou_session_seq{session_id = SessionID, seq = Seq},
    mnesia:dirty_write(moyou_session_seq_tab, R).


convert(<<"undefined">>) ->
    undefined;
convert(Data) ->
    list_to_integer(binary_to_list(Data)).


get_session_seqs(Seq, Size) ->
    if
        Seq - Size >= 0 ->
            lists:seq(Seq - Size, Seq - 1);
        true ->
            lists:seq(1, Seq - 1)
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
