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


-export([
    tmp_clean_message/0,
    tmp_clean_session/0
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
        {15, 0, 0} ->
            dump();
        _ ->
            skip
    end.

    
dump() ->
    ?INFO_MSG("================~p dump begin===============~n", [?MODULE]),
    Now = moyou_util:unixtime(),
    Keys = ets:select(ets_cid_and_sid, [{{'$1', '_', '$2'}, [{'=<', '$2', Now - 7200}], ['$1']}]),
    [ets:delete(ets_cid_and_sid, Key) || Key <- Keys],
    Node = node(),
    case lists:sort(mnesia:table_info(moyou_session_seq_tab, where_to_write)) of
        [Node | _] ->
            dump_session();
        _ ->
            skip
    end,
    ?INFO_MSG("================~p dump end===============~n", [?MODULE]).

dump_session() ->
    AllKeys = mnesia:dirty_all_keys(moyou_session_seq_tab),
    Size = length(AllKeys),
    Num = Size div 50,
    dump_session(AllKeys, 1, Num, Size).
    

dump_session(List, 50, Num, Size) ->
    Begin = 49 * Num + 1,
    SubKeys = lists:sublist(List, Begin, Size - Begin + 1),
    spawn(fun() ->
                  deal_dump(SubKeys)
          end);
dump_session(List, N, Num, Size) ->
    SubKeys = lists:sublist(List, (N - 1) * Num + 1, Num),
    spawn(fun() ->
                  deal_dump(SubKeys)
          end),
    dump_session(List, N + 1, Num, Size).

deal_dump([]) ->
    skip;
deal_dump([Key | T]) ->
    case mnesia:dirty_read(moyou_session_seq_tab, Key) of
        [] ->
            skip;
        [R] ->
            Seq = R#moyou_session_seq.seq,
            DumpSeq = get_session_dump_seq(Key),
            Now = moyou_util:unixtime(),
            case R#moyou_session_seq.resv1 of
                undefined ->
                    Seqs = dump_all_seqs(DumpSeq, Seq),
                    persistence(Key, Seqs),
                    persistence_session(Key, R),
                    ets:delete(ets_moyou_session, Key);
                Time ->
                    if
                        Now - Time > 43200 ->  %%离线12小时以上的session的消息全部存储进库
                            Seqs = dump_all_seqs(DumpSeq, Seq),
                            persistence(Key, Seqs),
                            persistence_session(Key, R),
                            ets:delete(ets_moyou_session, Key);
                        true ->
                            Seqs = dump_seqs(Key, DumpSeq, Seq),
                            persistence(Key, Seqs)
                    end
            end
    end,
    deal_dump(T).
    


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
                Diff > 3 ->
                    lists:seq(DumpSeq + 1, AllSeq - 3);
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


persistence_message(_Key) ->
    ok.
%    case mnesia:dirty_read(moyou_message_tab, Key) of
%        [] ->
%            skip;
%        [Message] ->
%            Values = value(Message),
%            Sql = "insert into moyou_message(`mid`, `session_id`, `from`, `mt`, `packet`, `time`) values" ++ Values,
%            case catch db_sql:execute(Sql) of
%                {'EXIT', Reason} ->
%                    ?ERROR_MSG("Sql : ~p execute error~n, reason : ~p~n", [Sql, Reason]),
%%                    dump_msg(Key);
%                _ ->
%                    mnesia:dirty_delete(moyou_message_tab, Key),
%                    dump_msg()
%            end
%    end.
                
    
persistence_session(Key, R) ->
    Sql1 = io_lib:format("select id from moyou_session_seq where session_id = '~s'", [Key]),
    case catch db_sql:get_row(Sql1) of
        {'EXIT', Reason1} ->
            ?ERROR_MSG("Sql : ~p get_row error~n, reason : ~p~n", [Sql1, Reason1]);
        [] ->
            Sql = "insert into moyou_session_seq(session_id, seq, resv1, resv2, resv3) values",
            Values = lists:flatten(io_lib:format("('~s', ~p, '~p', '~s', '~s')", [Key, R#moyou_session_seq.seq, R#moyou_session_seq.resv1, R#moyou_session_seq.resv2, R#moyou_session_seq.resv3])),
            case catch db_sql:execute(Sql ++ Values) of
                {'EXIT', Reason} ->
                    ?ERROR_MSG("Sql : ~p execute error~n, reason : ~p~n", [Sql, Reason]);
                _ ->
                    mnesia:dirty_delete(moyou_session_seq_tab, Key)
            end;
        [ID] ->
            Sql = io_lib:format("update moyou_session_seq set seq = ~p, resv1 = '~p', resv2 = '~s', resv3 = '~s' where id = ~p", [R#moyou_session_seq.seq, R#moyou_session_seq.resv1, R#moyou_session_seq.resv2, R#moyou_session_seq.resv3, ID]),
            case catch db_sql:execute(Sql) of
                {'EXIT', Reason} ->
                    ?ERROR_MSG("Sql : ~p execute error~n, reason : ~p~n", [Sql, Reason]);
                _ ->
                    mnesia:dirty_delete(moyou_session_seq_tab, Key)
            end
    end.



tmp_clean_message() ->
    AllKeys = mnesia:dirty_all_keys(moyou_message_tab),
    tmp_clean_message(AllKeys, 1).

tmp_clean_message(_List, 101) ->
    ok;
tmp_clean_message(List, N) ->
    SubKeys = lists:sublist(List, (N - 1) * 20000 + 1, 20000),
    spawn(fun() ->
                  tmp_clean_message(SubKeys)
          end),
    tmp_clean_message(List, N + 1).


tmp_clean_message([]) ->
    ok;
tmp_clean_message([Key | T]) ->
    persistence_message(Key),
    tmp_clean_message(T).


tmp_clean_session() ->
    AllKeys = mnesia:dirty_all_keys(moyou_session_seq_tab),
    tmp_clean_session(AllKeys, 1).

tmp_clean_session(_List, 101) ->
    ok;
tmp_clean_session(List, N) ->
    SubKeys = lists:sublist(List, (N - 1) * 80000 + 1, 80000),
    spawn(fun() ->
                  tmp_clean_session(SubKeys)
          end),
    tmp_clean_session(List, N + 1).


tmp_clean_session([]) ->
    ok;
tmp_clean_session([Key | T]) ->
    case mnesia:dirty_read(moyou_session_seq_tab, Key) of
        [] ->
            skip;
        [R] ->
            persistence_session(Key, R)
    end,
    tmp_clean_session(T).
