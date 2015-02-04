-module(moyou_user_manage).


-export([
    update_session_all_seq/3,
    update_session_read_seq/3,
    clear_user_session_info/2,
    get_session_msg/4,
    get_offline_msg/1
        ]).


-include("ejabberd.hrl").
-include("jlib.hrl").


update_session_all_seq(Uids, SessionID, Seq) ->
    [begin
         Pid = get_user_pid(Uid),
         gen_server:cast(Pid, {update_session_all_seq, SessionID, Seq})
     end|| Uid <- Uids].


update_session_read_seq(Uid, SessionID, Seq) ->
    Pid = get_user_pid(Uid),
    gen_server:cast(Pid, {update_session_read_seq, SessionID, Seq}).


clear_user_session_info(Uids, SessionID) ->
    [begin
         Pid = get_user_pid(Uid),
         gen_server:cast(Pid, {clear_user_session_info, SessionID})
     end|| Uid <- Uids].


get_session_msg(Uid, SessionID, Seq, Size) ->
    safe_call(Uid, {get_session_msg, SessionID, Seq, Size}, 3).


get_offline_msg(Uid) ->
    safe_call(Uid, {get_offline_msg}, 3).


get_user_pid(Uid) ->
    case ets:lookup(ets_moyou_user, Uid) of
        [] ->
            {ok, Child} = supervisor:start_child(moyou_user_sup, [Uid]),
            ets:insert(ets_moyou_user, {Uid, Child}),
            Child;
        [{Uid, Child}] ->
            Child
    end.


safe_call(Uid, Msg, 0) ->
    ?ERROR_MSG("moyou_user_manage safe_call Uid : ~p, Msg : ~p failed~n", [Uid, Msg]),
    skip;
safe_call(Uid, Msg, Retry) ->
    try
        Pid = get_user_pid(Uid),
        gen_server:call(Pid, Msg)
    catch
        _TYPE:_ERROR ->
            safe_call(Uid, Msg, Retry - 1)
    end.