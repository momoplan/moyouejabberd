%% @author chenkangmin
%% @doc @todo Add description to moyou_dump_service.


-module(moyou_dump_service).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("jlib.hrl").
-include("ejabberd.hrl").


-export([start_link/0,
         clean/0,
         cancel/0
        ]).


-record(state, {dump_timer = none}).

-record(moyou_session_dump, {session_id, dump_seq = 0, resv1, resv2, resv3}).

-record(moyou_session_seq, {session_id, seq = 0, resv1, resv2, resv3}).

-record(moyou_message, {id, session_id, from, mt, packet, time, resv1, resv2, resv3}).


clean() ->
    gen_server:cast(?MODULE, clean).

cancel() ->
    gen_server:cast(?MODULE, cancel).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    moyou_util:create_or_copy_table(moyou_session_dump_tab, [{record_name, moyou_session_dump},
                                                             {attributes, record_info(fields, moyou_session_dump)},
                                                             {ram_copies, [node()]}], ram_copies),
    Timer = erlang:send_after(1 * 1000, self(), auto_dump),
    {ok, #state{dump_timer = Timer}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(cancel, State) ->
    if State#state.dump_timer /= none ->
            timer:cancel(State#state.dump_timer);
        true ->
            skip
    end,
    {noreply, State#state{dump_timer = none}};
handle_cast(clean, State) ->
    dump(),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(auto_dump, State) ->
    if State#state.dump_timer /= none ->
            auto_dump();
        true ->
            skip
    end,
    Timer = erlang:send_after(1 * 1000, self(), auto_dump),
    {noreply, State#state{dump_timer = Timer}};
handle_info(_Info, State) ->
    {noreply, State}.



terminate(_Reason, _State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


auto_dump() ->
    {_, Time} = erlang:localtime(),
    %%每天凌晨3点执行dump
    case Time of
        {3, 0, 0} ->
            dump();
        _ ->
            skip
    end.


dump() ->
    Now = moyou_util:unixtime(),
    Keys = ets:select(ets_cid_and_sid, [{{'$1', '_', '$2'}, [{'=<', '$2', Now - 7200}], ['$1']}]),
    [ets:delete(ets_cid_and_sid, Key) || Key <- Keys],
    AllSessionID = ets:select(ets_moyou_session, [{{'$1', '_', '_'}, [], ['$1']}]),
    dump_session(AllSessionID).


dump_session([]) ->
    skip;
dump_session([SessionID | T]) ->
    case ets:lookup(ets_moyou_session, SessionID) of
        [] ->
            skip;
        [{SessionID, undefined, Time}] ->  %%离线session
            Seq = get_session_seq(SessionID),
            DumpSeq = get_session_dump_seq(SessionID),
            Now = moyou_util:unixtime(),
            if
                Now - Time > 86400 ->  %%离线24小时以上的session的消息全部存储进库
                    Seqs = dump_all_seqs(DumpSeq, Seq),
                    persistence(SessionID, Seqs),
                    ets:delete(ets_moyou_session, SessionID);
                true ->
                    Seqs = dump_seqs(SessionID, DumpSeq, Seq),
                    persistence(SessionID, Seqs)
            end;
        [{SessionID, _Pid, _}] ->   %%在线session
            {ok, Seq} = moyou_session_manage:query_session_seq(SessionID),
            DumpSeq = get_session_dump_seq(SessionID),
            Seqs = dump_seqs(SessionID, DumpSeq, Seq),
            persistence(SessionID, Seqs)
    end,
    dump_session(T).


get_session_seq(SessionID) ->
    [R] = mnesia:dirty_read(moyou_session_seq_tab, SessionID),
    R#moyou_session_seq.seq.


get_session_dump_seq(SessionID) ->
    case mnesia:dirty_read(moyou_session_dump_tab, SessionID) of
        [] ->
            0;
        [R] ->
            R#moyou_session_dump.dump_seq
    end.


dump_all_seqs(DumpSeq, AllSeq) ->
    lists:seq(DumpSeq + 1, AllSeq).


dump_seqs(SessionID, DumpSeq, AllSeq) ->
    Diff = AllSeq - DumpSeq,
    case moyou_util:is_group_session(SessionID) of
        true ->
            if
                Diff > 20 ->
                    lists:seq(DumpSeq + 1, AllSeq - 20);
                true ->
                    []
            end;
        _ ->
            if
                Diff > 5 ->
                    lists:seq(DumpSeq + 1, AllSeq - 5);
                true ->
                    []
            end
    end.


persistence(_SessionID, []) ->
    skip;
persistence(SessionID, [Seq | T]) ->
    Mid = lists:concat([SessionID, "_", Seq]),
    case mnesia:dirty_read(moyou_message_tab, Mid) of
        [] ->
            persistence(SessionID, T);
        [Message] ->
            Values = value(Message),
            Sql = "insert into moyou_message(`mid`, `session_id`, `from`, `mt`, `packet`, `time`) values" ++ Values,
            case catch db_sql:execute(Sql) of
                {'EXIT', Reason} ->
                    ?ERROR_MSG("Sql : ~p execute error~n, reason : ~p~n", [Sql, Reason]),
                    persistence(SessionID, T);
                _ ->
                    mnesia:dirty_delete(moyou_message_tab, Mid),
                    R = #moyou_session_dump{session_id = SessionID, dump_seq = Seq},
                    mnesia:dirty_write(moyou_session_dump_tab, R),
                    persistence(SessionID, T)
            end
    end.


value(Message) ->
    lists:flatten(io_lib:format("('~s', '~s', '~s', '~s', '~s', ~p)",
                                [Message#moyou_message.id,
                                 Message#moyou_message.session_id,
                                 moyou_util:term_to_bitstring(Message#moyou_message.from),
                                 Message#moyou_message.mt,
                                 moyou_util:term_to_bitstring(term_to_binary(Message#moyou_message.packet)),
                                 Message#moyou_message.time])).
