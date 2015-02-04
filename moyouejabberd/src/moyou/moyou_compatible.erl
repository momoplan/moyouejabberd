%%%----------------------------------------------------------------------
%%% File    : moyou_compatible.erl
%%% 做兼容处理的模块
%%%----------------------------------------------------------------------

-module(moyou_compatible).
-author('chenkangmin').

-include("ejabberd.hrl").
-include("jlib.hrl").

-compile([export_all]).


get_offline_msg(Uid) ->
    Jid = jlib:make_jid({Uid, "gamepro.com", ""}),
    case catch mnesia:dirty_read(my_user_table_info, Jid) of
        {'EXIT', _Reason} ->
            [];
        [] ->
            [];
        [R] ->
            case mnesia:dirty_read(element(4, R), Jid) of
                [] ->
                    case catch get_offline_from_db(Jid) of
                        {'EXIT', Reason} ->
                            ?ERROR_MSG("get_offline_from_db error~n, Jid : ~p~n reason : ~p~n", [Jid, Reason]),
                            [];
                        Messages ->
                            mnesia:dirty_delete(my_user_table_info, Jid),
                            Messages
                    end;
                [R1] ->  %%先load数据库中的数据
                    case catch get_offline_from_mnesia(Jid, element(3, R1), element(3, R)) of
                        {'EXIT', Reason} ->
                            ?ERROR_MSG("get_offline_from_db error~n, Jid : ~p~n reason : ~p~n", [Jid, Reason]),
                            [];
                        Messages ->
                            mnesia:dirty_delete(element(4, R), Jid),
                            mnesia:dirty_delete(my_user_table_info, Jid),
                            Messages
                    end
            end
    end.


migrate_group_session_seq(SessionID) ->
    GroupId = moyou_util:get_group_id_from_session(SessionID),
    case catch mnesia:dirty_read(group_id_seq, GroupId) of
        {'EXIT', _Reason} ->
            0;
        [] ->
            0;
        [R] ->
            Seq = element(3, R),
            migrate_message(SessionID, GroupId, Seq),
            mnesia:dirty_delete(group_id_seq, GroupId),
            Seq
    end.


migrate_user_group_session_info(Uid, SessionID, Seq) ->
    case catch mnesia:dirty_read(user_group_info, jlib:make_jid({Uid, "gamepro.com", ""})) of
        {'EXIT', _Reason} ->
            {Seq - 1, Seq - 1};
        [] ->
            {Seq - 1, Seq - 1};
        [R] ->
            GroupID = moyou_util:get_group_id_from_session(SessionID),
            case lists:keysearch(GroupID, 1, element(3, R)) of
                false ->
                    {Seq - 1, Seq - 1};
                {value, Value} ->
                    case lists:keydelete(GroupID, 1, element(3, R)) of
                        [] ->
                            mnesia:dirty_delete(user_group_info, element(2, R));
                        List ->
                            mnesia:dirty_write(user_group_info, {element(1, R), element(2, R), List})
                    end,
                    {element(2, Value), element(3, Value)}
            end
    end.


migrate_message(SessionID, GroupId, Seq) ->
    Seqs = if
               Seq > 100 ->
                   lists:seq(Seq - 99, Seq);
               true ->
                   lists:seq(1, Seq)
           end,
    migrate_message1(SessionID, GroupId, Seqs),
    delete_old_message(GroupId, Seq).


migrate_message1(_SessionID, _GroupId, []) ->
    skip;
migrate_message1(SessionID, GroupId, [Seq | T]) ->
    case mnesia:dirty_read(group_message, lists:concat(["mygroup_", GroupId, "_" , Seq])) of
        [] ->
            migrate_message1(SessionID, GroupId, T);
        [OldMessage] ->
            From = element(4, OldMessage),
            Packet = element(5, OldMessage),
            Time = element(6, OldMessage),
            {Tag, "message", Attrs, Body} = Packet,
            Mt = xml:get_attr_s("msgtype", Attrs),
            Sid = lists:concat([SessionID, "_", Seq]),
            Attrs1 = [{"id", Sid} | [{K, V} || {K, V} <- Attrs, K =/= "id"]],
            Packet1 = {Tag, "message", Attrs1, Body},
            Message = moyou_session:pack_message(Sid, SessionID, From, Mt, Packet1, Time),
            mnesia:dirty_write(moyou_message_tab, Message),
            migrate_message1(SessionID, GroupId, T)
    end.
    


delete_old_message(GroupId, Seq) ->
    [mnesia:dirty_delete(group_message, lists:concat(["mygroup_", GroupId, "_" , N])) || N <- lists:seq(1, Seq)].


get_offline_from_db(Jid) ->
    Key = jlib:jid_to_string(Jid),
    Sql = io_lib:format("select count(id) from messages where jid = '~s'", [Key]),
    [Count] = db_sql:get_row(Sql),
    if
        Count > 100 ->
            Sql1 = io_lib:format("delete from messages where jid = '~s'", [Key]),
            db_sql:execute(Sql1),
            [];
        Count =:= 0 ->
            [];
        true ->
            Sql1 = io_lib:format("select content, createDate from messages where jid = '~s' order by id desc", [Key]),
            DataList = db_sql:get_all(Sql1),
            Sql2 = io_lib:format("delete from messages where jid = '~s'", [Key]),
            db_sql:execute(Sql2),
            [format_db_message(Data) || Data <-  DataList]
    end.


get_offline_from_mnesia(Jid, Mids, MessageTab) ->
    Messages = get_offline_from_db(Jid),
    Size = length(Mids) + length(Messages),
    if
        Size > 100 ->
            [mnesia:dirty_delete(MessageTab, Mid) || Mid <- Mids],
            [];
        true ->
            Messages1 = Messages ++ read_mnesia(Mids, MessageTab, []),
            [mnesia:dirty_delete(MessageTab, Mid) || Mid <- Mids],
            Messages1
    end.


read_mnesia([], _MessageTab, Acc) ->
    Acc;
read_mnesia([Mid | T], MessageTab, Acc) ->
    case mnesia:dirty_read(MessageTab, Mid) of
        [] ->
            read_mnesia(T, MessageTab, Acc);
        [R] ->
            read_mnesia(T, MessageTab, [R | Acc])
    end.


format_db_message([Content, TimeStamp]) ->
    {Key, From, To, Packet1} = moyou_util:bitstring_to_term(Content),
    Packet = binary_to_term(Packet1),
    {user_msg, Key, From, To, Packet, TimeStamp, undefined, undefined}.


clean_history_message() ->
    Keys = lists:reverse(mnesia:dirty_all_keys(my_user_table_info)),
    [begin
         Jid = jlib:jid_to_string(Key),
         [R] = mnesia:dirty_read(my_user_table_info, Key),
         IdTable = element(4, R),
         case mnesia:dirty_read(IdTable, Key) of
             [] ->
                 Sql = io_lib:format("select count(id) from messages where jid = '~s'",[Jid]),
                 case catch db_sql:get_row(Sql) of
                     {'EXIT', Reason} ->
                         ?ERROR_MSG("Sql : ~p get row error~n, reason : ~p~n", [Sql, Reason]);
                     [Count]  ->
                         if
                             Count > 100 ->
                                 Sql1 = io_lib:format("delete from messages where jid = '~s'",[Jid]),
                                 db_sql:execute(Sql1),
                                 mnesia:dirty_delete(IdTable, Key),
                                 mnesia:dirty_delete(my_user_table_info, Key),
                                 ?INFO_MSG("clear db ~p : ~p history message~n", [Jid, Count]);
                             true ->
                                 skip
                         end
                 end;
             [R1] ->
                 Size = length(element(3, R1)),
                 if
                     Size > 100 ->
                         [mnesia:dirty_delete(element(3, R), Mid) || Mid <- element(3, R1)],
                         mnesia:dirty_delete(IdTable, Key),
                         mnesia:dirty_delete(my_user_table_info, Key),
                         ?INFO_MSG("clear mnesia ~p : ~p history message~n", [Jid, Size]);
                     true ->
                         skip
                 end
         end
     end || Key <- Keys].
