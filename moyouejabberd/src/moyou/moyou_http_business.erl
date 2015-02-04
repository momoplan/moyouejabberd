-module(moyou_http_business).


-include("ejabberd.hrl").
-include("jlib.hrl").

-export([
    business/2
        ]).

-record(moyou_group_member, {gid, members = []}).

business("process_counter", _Obj) ->
    {true, process_counter()};
business("remove_group", Obj) ->
    {ok, Params} = rfc4627:get_field(Obj, "params"),
    {ok, Gid} = rfc4627:get_field(Params, "gid"),
    remove_group(binary_to_list(Gid)),
    {true, ok};
business("remove_user", Obj) ->
    {ok, Params} = rfc4627:get_field(Obj, "params"),
    {ok, Gid} = rfc4627:get_field(Params, "gid"),
    {ok, Uid} = rfc4627:get_field(Params, "uid"),
    remove_user(binary_to_list(Gid), binary_to_list(Uid)),
    {true, ok};
business("append_user", Obj) ->
    {ok, Params} = rfc4627:get_field(Obj, "params"),
    {ok, Gid} = rfc4627:get_field(Params, "gid"),
    {ok, Uid} = rfc4627:get_field(Params, "uid"),
    append_user(binary_to_list(Gid), binary_to_list(Uid)),
    {true, ok};
business("update_group_info", Obj) ->
    {ok, Gid} = rfc4627:get_field(Obj, "gid"),
    {ok, GName} = rfc4627:get_field(Obj, "gname"),
    {ok, NotPush} = rfc4627:get_field(Obj, "notPushUserids"),
    NotPush1 = [binary_to_list(B) || B <- NotPush],
    moyou_rpc_util:update_group_info(binary_to_list(Gid), binary_to_list(GName), NotPush1),
    {true, ok};
business("update_user_info", Obj) ->
    {ok, Uid} = rfc4627:get_field(Obj, "uid"),
    {ok, NickName} = rfc4627:get_field(Obj, "nickname"),
    DeviceToken = case rfc4627:get_field(Obj, "deviceToken") of
                      {ok, null} ->
                          null;
                      {ok, DeviceTokenBin} ->
                          re:replace(binary_to_list(DeviceTokenBin), " ", "", [global, {return, list}])
                  end,
    Imei = case rfc4627:get_field(Obj, "imei") of
               {ok, null} ->
                   null;
               {ok, ImeiBin} ->
                   binary_to_list(ImeiBin)
           end,
    {ok, SilenceConfig} = rfc4627:get_field(Obj, "silenceConfig"),
    {ok, MessageConfig} = rfc4627:get_field(Obj, "messageDetailConfig"),
    Blacks = case rfc4627:get_field(Obj, "blackList") of
                 {ok, BlackList} ->
                     [binary_to_list(Bin) || Bin <- BlackList];
                 _ ->
                     []
             end,
    Friends = case rfc4627:get_field(Obj, "friends") of
                  {ok, FriendsList} ->
                      [{binary_to_list(FriendId), binary_to_list(Alia)} || [FriendId, Alia] <- FriendsList];
                  _ ->
                      []
              end,
    Env = case rfc4627:get_field(Obj, "appType") of
              {ok, null} ->
                  null;
              {ok, AppType} ->
                  binary_to_list(AppType);
              _ ->
                  null
          end,
    moyou_rpc_util:update_user_info(binary_to_list(Uid), Friends, binary_to_list(NickName),
                                    DeviceToken, Imei, Blacks, SilenceConfig, MessageConfig,
                                    Env),
    {true, ok};
business("query_group_id", Obj) ->
    {ok, Gid} = rfc4627:get_field(Obj, "gid"),
    SessionID = moyou_util:get_session_id(binary_to_list(Gid)),
    Seq = moyou_rpc_util:query_group_id(SessionID),
    Entity = {obj, [{seq, Seq}]},
    {true, Entity};
business("query_group_msg", Obj) ->
    {ok, Mid} = rfc4627:get_field(Obj, "msg_id"),
    {ok, Uid} = rfc4627:get_field(Obj, "uid"),
    Size = case rfc4627:get_field(Obj, "size") of
               {ok, SizeTmp} ->
                   list_to_integer(binary_to_list(SizeTmp));
               _ ->
                   20
           end,
    [Prefix, SessionKey, Seq] = re:split(binary_to_list(Mid), "_", [{return, list}]),
    SessionID = case Prefix of
                    "mygroup" ->   %%做老版本兼容,因为在客户端是写死的mygroup
                        lists:concat(["group", "_", SessionKey]);
                    _ ->
                        lists:concat([Prefix, "_", SessionKey])
                end,
    Messages = moyou_rpc_util:get_session_msg(binary_to_list(Uid), SessionID, list_to_integer(Seq), Size),
    Entity = [encode_msg_to_json(Message, binary_to_list(Uid)) ||Message <- Messages],
    {true, Entity};
business(_, _Obj) ->
    {false, list_to_binary("method undefined")}.



process_counter()->
    Counter = lists:map(fun process_counter/1, [node() | nodes()]),
    L = [X || X <- Counter, ordsets:is_subset([ok], tuple_to_list(X)) =:= true],
    process_counter_to_json(L, []).
process_counter(From) ->
    try
        P = rpc:call(From, erlang, whereis, [ejabberd_c2s_sup]),
        case is_pid(P) of
            true ->
                {links, L} = rpc:call(From,erlang,process_info,[P, links]),
                {ok, {From, length(L)}};
            _ ->
                {no_ejabberd_node, From}
        end
    catch
        _:_->
            {no_ejabberd_node,From}
    end.
process_counter_to_json([E | L], List)->
    {ok,{Node, Total}} = E,
    JN = {obj, [{node, Node},{totalCount, Total}]},
    process_counter_to_json(L, [JN | List]);
process_counter_to_json([], List)->
    List.


remove_group(Gid) ->
    Members = mod_customize:query_local_group_member(Gid),
    SessionID = moyou_util:get_session_id(Gid),
    moyou_rpc_util:clear_user_session_info(Members, SessionID),
    mnesia:dirty_delete(moyou_group_member_tab, Gid).


remove_user(Gid, Uid) ->
    SessionID = moyou_util:get_session_id(Gid),
    moyou_rpc_util:clear_user_session_info([Uid], SessionID),
    case mod_customize:query_local_group_member(Gid) of
        [] ->
            skip;
        Members ->
            Members1 = lists:delete(Uid, Members),
            R = #moyou_group_member{gid = Gid, members = Members1},
            mnesia:dirty_write(moyou_group_member_tab, R)
    end.


append_user(Gid, Uid) ->
    case mod_customize:query_local_group_member(Gid) of
        [] ->
            skip;
        Members ->
            case lists:member(Uid, Members) of
                true ->
                    skip;
                false ->
                    Members1 = [Uid | Members],
                    R = #moyou_group_member{gid = Gid, members = Members1},
                    mnesia:dirty_write(moyou_group_member_tab, R)
            end
    end.


encode_msg_to_json(Message, Uid) ->
    {xmlelement, Name, Attrs, Els} = element(6, Message),
    From = element(4, Message),
    To = #jid{user = Uid, server = From#jid.server, resource = "", luser = Uid, lserver = From#jid.lserver, lresource = ""},
    Attrs1 = jlib:replace_from_to_attrs(jlib:jid_to_string(From),
                                        jlib:jid_to_string(To),
                                        Attrs),
    FixedPacket = {xmlelement, Name, Attrs1, Els},
    Text = xml:element_to_binary(FixedPacket),
    {obj, [{item, Text}]}.