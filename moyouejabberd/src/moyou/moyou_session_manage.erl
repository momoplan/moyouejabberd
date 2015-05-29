-module(moyou_session_manage).


-export([
    store_message/3,
    query_session_seq/1,
    get_session_msg/3
        ]).


-include("ejabberd.hrl").
-include("jlib.hrl").

store_message(SessionID, From, Packet) ->
    case safe_call(SessionID, {store_message, From, Packet}, 3) of
        skip ->
            skip;
        {ok, Sid, Seq} ->
            moyou_user_manage:update_session_all_seq([From#jid.user], SessionID, Seq),
            moyou_user_manage:update_session_read_seq(From#jid.user, SessionID, Seq),
            {ok, {Sid, Seq}};
        Data ->
            {ok, Data}
    end.


query_session_seq(SessionID) ->
    safe_call(SessionID, {query_session_seq}, 3).


get_session_msg(SessionID, Seq, Size) ->
    safe_call(SessionID, {get_session_msg, Seq, Size}, 3).


get_session_pid(SessionID) ->
    case ets:lookup(ets_moyou_session, SessionID) of
        [] ->
            {ok, Child} = supervisor:start_child(moyou_session_sup, [SessionID]),
            ets:insert(ets_moyou_session, {SessionID, Child, 0}),
            Child;
        [{SessionID, undefined, _}] ->
            {ok, Child} = supervisor:start_child(moyou_session_sup, [SessionID]),
            ets:update_element(ets_moyou_session, SessionID, [{2, Child}, {3, 0}]),
            Child;
        [{SessionID, Child, _}] ->
            Child
    end.


safe_call(SessionID, Msg, 0) ->
    ?ERROR_MSG("moyou_session_manage safe_call SessionID : ~p, Msg : ~p failed~n", [SessionID, Msg]),
    skip;
safe_call(SessionID, Msg, Retry) ->
    try
        Pid = get_session_pid(SessionID),
        gen_server:call(Pid, Msg, infinity)
    catch
        _Type:_Error ->
            safe_call(SessionID, Msg, Retry - 1)
    end.
