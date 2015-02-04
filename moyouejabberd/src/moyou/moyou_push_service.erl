%% @author chenkangmin
%% @doc @todo Add description to my_offline_msg_center.
-module(moyou_push_service).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("jlib.hrl").
-include("ejabberd.hrl").

-record(state, {ios_push_certfile,
                ios_dev_push_certfile,
                ios_push_keyfile,
                ios_dev_push_keyfile,
                ios_push_host,
                ios_dev_push_host,
                ios_push_handlers = [],
                ios_dev_push_handlers = []}).

-record(moyou_push_user_info, {uid, 
                               friends = [],
                               nick_name,
                               device_token,
                               imei,
                               blacks = [], 
                               silence_config = 0,
                               message_config = 1,
                               env}).

-record(moyou_push_group_info, {gid = "",
                                name = "",
                                not_push = []}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         list/0,
         update_group_info/3,
         update_user_info/9,
         send_offline_message/3,
         test_normal_msg/1,
         test_group_msg/0
        ]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

list() ->
    gen_server:call(?MODULE, {list}).

update_group_info(Gid, GroupName, NotPush) ->
    case mnesia:dirty_read(moyou_push_group_info_tab, Gid) of
        [] ->
            GroupInfo = #moyou_push_group_info{gid = Gid, name = GroupName, not_push = NotPush},
            mnesia:dirty_write(moyou_push_group_info_tab, GroupInfo);
        [GroupInfo] ->
            mnesia:dirty_write(moyou_push_group_info_tab, GroupInfo#moyou_push_group_info{name = GroupName, not_push = NotPush})
    end.

update_user_info(Uid, Friends, NickName, DeviceToken, Imei, Blacks, SilenceConfig, MessageConfig, Env) ->
    case mnesia:dirty_read(moyou_push_user_info_tab, Uid) of
        [] ->
            UserInfo = #moyou_push_user_info{uid = Uid, friends = Friends, nick_name = NickName, blacks = Blacks,
                                             imei = Imei, device_token = DeviceToken, silence_config = SilenceConfig,
                                             message_config = MessageConfig, env = Env},
            mnesia:dirty_write(moyou_push_user_info_tab, UserInfo);
        [UserInfo] ->
            mnesia:dirty_write(moyou_push_user_info_tab, UserInfo#moyou_push_user_info{friends = Friends, nick_name = NickName, 
                                                                                       blacks = Blacks, imei = Imei,
                                                                                       device_token = DeviceToken, silence_config = SilenceConfig,
                                                                                       message_config = MessageConfig, env = Env})
    end.

send_offline_message(From, #jid{user = ToUid}, Packet) ->
    ToUserInfo = get_user_info(ToUid),
    if
        ToUserInfo =/= [] ->
            case ToUserInfo#moyou_push_user_info.imei of
                null -> 
                    ios_send_offline_msg(From, ToUserInfo, Packet);
                _ -> 
                    android_send_offline_msg(From, ToUserInfo, Packet)
            end;
        true ->
            skip
    end.


android_send_offline_msg(From, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when ToUserInfo#moyou_push_user_info.imei /= null ->
    Imei = ToUserInfo#moyou_push_user_info.imei,
    PushTo = #jid{user = Imei, server = "push.gamepro.com", resource = [], luser = Imei, lserver = "push.gamepro.com", lresource = []},
    Mt = proplists:get_value("msgtype", Attr, ""),
    PushAttr = [{"id", moyou_util:get_id()},
                {"type", "chat"},
                {"msgTime", proplists:get_value("msgTime", Attr, "")},
                {"msgtype", moyou_util:msg_type_2_push_type(Mt)}],
    BodyJson = get_push_body(Mt, From, ToUserInfo, Packet),
    case BodyJson of
        skip ->
            skip;
        _ ->
            CDATA = rfc4627:encode(BodyJson),
            PushBody = {xmlelement, "body", [], [{xmlcdata, list_to_binary(CDATA)}]},
            PushPacket = {xmlelement, "message", PushAttr, [PushBody]},
            ejabberd_router:route(From, PushTo, PushPacket)
    end;
android_send_offline_msg(_From, _ToUserInfo, _Packet) ->
    skip.

ios_send_offline_msg(From, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when
    ToUserInfo#moyou_push_user_info.device_token /= null ->
    Token = ToUserInfo#moyou_push_user_info.device_token,
    Mt = proplists:get_value("msgtype", Attr, ""),
    Alert = get_push_alert(Mt, From, ToUserInfo, Packet),
    case Alert of
        skip ->
            skip;
        _ ->
            Sound = get_push_sound(ToUserInfo, Mt),
            push_to_ios(Token, Sound, Alert, ToUserInfo#moyou_push_user_info.env)
    end;
ios_send_offline_msg(_From, _ToUserInfo, _Packet) ->
    skip.

get_push_sound(ToUserInfo, Mt) ->
    case ToUserInfo#moyou_push_user_info.silence_config of
        0 ->
            case Mt of
                "startTeamPreparedConfirm" ->
                    "inplace_sound.mp3";
                _ ->
                    "default"
            end;
        _ ->
            null
    end.


get_push_body(Mt, #jid{user = FromUid}, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when Mt =:= "groupchat" ->
    Gid = proplists:get_value("groupid", Attr, ""),
    ID = proplists:get_value("id", Attr, ""),
    GroupInfo = get_group_info(Gid),
    FromUserInfo = get_user_info(FromUid),
    case FromUserInfo of
        [] ->
            skip;
        _ ->
            Content = moyou_util:get_msg_content(Packet),
            {ok, Entity, _} = rfc4627:decode(hd(Content)),
            {ok, Msg} = rfc4627:get_field(Entity, "content"),
            Name = list_to_binary(get_nickname_or_alia(FromUserInfo, ToUserInfo)),
            case lists:member(ToUserInfo#moyou_push_user_info.uid, GroupInfo#moyou_push_group_info.not_push) of
                true ->
                    skip;
                _ ->
                    {obj, [{"id", list_to_binary(ID)},
                           {"expire", <<"604800">>},
                           {"msgtype", list_to_binary(Mt)},
                           {"from", list_to_binary(FromUserInfo#moyou_push_user_info.uid)},
                           {"body", {obj, [{"groupId", list_to_binary(Gid)},
                                           {"groupName", list_to_binary(GroupInfo#moyou_push_group_info.name)},
                                           {"msg", <<Name/binary, <<":">>/binary, Msg/binary>>}]}
                           }]}
            end
    end;
get_push_body(Mt, #jid{user = FromUid}, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet)
when Mt =:= "normalchat" orelse Mt =:= "sayHello" ->
    ID = proplists:get_value("id", Attr, ""),
    FromUserInfo = get_user_info(FromUid),
    Content = moyou_util:get_msg_content(Packet),
    case FromUserInfo of
        [] ->
            skip;
        _ ->
            Name = get_nickname_or_alia(FromUserInfo, ToUserInfo),
            case check_black(FromUserInfo, ToUserInfo) of
                true ->
                    skip;
                _ ->
                    {obj, [{"id", list_to_binary(ID)},
                           {"expire", <<"604800">>},
                           {"body", list_to_binary(lists:concat([Name, ":", Content]))},
                           {"msgtype", list_to_binary(Mt)},
                           {"from", list_to_binary(FromUserInfo#moyou_push_user_info.uid)}]}
            end
    end;
get_push_body(Mt, #jid{user = FromUid}, _ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when Mt =:= "system"
    orelse Mt =:= "joinGroupApplication"
    orelse Mt =:= "groupBillboard"
    orelse Mt =:= "startTeamPreparedConfirm"
    orelse Mt =:= "teamInvite"
    orelse Mt =:= "requestJoinTeam"
    orelse Mt =:= "teamHelper" ->
    ID = proplists:get_value("id", Attr, ""),
    Content = moyou_util:get_msg_content(Packet),
    Name = moyou_util:msg_type_2_push_name(Mt),
    {obj, [{"id", list_to_binary(ID)},
           {"expire", <<"604800">>},
           {"body", list_to_binary(lists:concat([Name, ":", Content]))},
           {"msgtype", <<"normalchat">>},
           {"from", list_to_binary(FromUid)}]};
get_push_body(_Mt, _From, _ToUserInfo, _Packet) ->
    skip.


get_push_alert(Mt, #jid{user = FromUid}, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when Mt =:= "groupchat" ->
    Gid = proplists:get_value("groupid", Attr, ""),
    GroupInfo = get_group_info(Gid),
    FromUserInfo = get_user_info(FromUid),
    case FromUserInfo of
        [] ->
            skip;
        _ ->
            Content = moyou_util:get_msg_content(Packet),
            {ok, Entity, _} = rfc4627:decode(hd(Content)),
            {ok, Msg} = rfc4627:get_field(Entity, "content"),
            Name = get_nickname_or_alia(FromUserInfo, ToUserInfo),
            case lists:member(ToUserInfo#moyou_push_user_info.uid, GroupInfo#moyou_push_group_info.not_push) of
                true ->
                    skip;
                _ ->
                    case ToUserInfo#moyou_push_user_info.message_config of
                        0 ->
                            "您有一条新的陌游消息！";
                        _ ->
                            lists:concat([Name, "(", GroupInfo#moyou_push_group_info.name, "):", binary_to_list(Msg)])
                    end
            end
    end;
get_push_alert(Mt, #jid{user = FromUid}, ToUserInfo, Packet) when Mt =:= "normalchat" ->
    FromUserInfo = get_user_info(FromUid),
    Content = moyou_util:get_msg_content(Packet),
    case FromUserInfo of
        [] ->
            skip;
        _ ->
            Name = get_nickname_or_alia(FromUserInfo, ToUserInfo),
            case check_black(FromUserInfo, ToUserInfo) of
                true ->
                    skip;
                _ ->
                    case ToUserInfo#moyou_push_user_info.message_config of
                        0 ->
                            "您有一条新的陌游消息！";
                        _ ->
                            lists:concat([Name, ":", Content])
                    end
            end
    end;
get_push_alert(Mt, _From, _ToUserInfo, Packet) when Mt =:= "system"
    orelse Mt =:= "joinGroupApplication"
    orelse Mt =:= "groupBillboard"
    orelse Mt =:= "startTeamPreparedConfirm"
    orelse Mt =:= "teamInvite"
    orelse Mt =:= "requestJoinTeam"
    orelse Mt =:= "teamHelper" ->
    Content = moyou_util:get_msg_content(Packet),
    Name = moyou_util:msg_type_2_push_name(Mt),
    lists:concat([Name, ":", Content]);
get_push_alert(_Mt, _From, _ToUserInfo, _Packet) ->
    skip.


push_to_ios(Token, Sound, Alert, Env) ->
    {ok, Pid} = gen_server:call(?MODULE, {random_ios_push_pid, Env}),
    moyou_ios_provider:push(Pid, Token, Sound, Alert).
    

check_black(#moyou_push_user_info{uid = Uid}, #moyou_push_user_info{blacks = Blacks}) ->
    lists:member(Uid, Blacks).


get_nickname_or_alia(#moyou_push_user_info{nick_name = NickName, uid = Uid}, #moyou_push_user_info{friends = Friends}) ->
    case lists:keysearch(Uid, 1, Friends) of
        {value, {Uid, Alia}} ->
            Alia;
        _ ->
            NickName
    end.


get_group_info(Gid) ->
    case mnesia:dirty_read(moyou_push_group_info_tab, Gid) of
        [] ->
            Url = moyou_util:get_config(gamepro_server),
            ParamObj = {obj, [{"method", list_to_binary("getGroupInfo")},
                              {"params", {obj, [{"groupId", list_to_binary(Gid)}]}}
                             ]},
            Params = "body=" ++ rfc4627:encode(ParamObj),
            case moyou_util:http_request(Url, Params) of
                [] ->
                    #moyou_push_group_info{gid = Gid};
                Body ->
                    case rfc4627:decode(Body) of
                        {ok, Obj, _Re} ->
                            case rfc4627:get_field(Obj, "success") of
                                {ok, true} ->
                                    GroupInfo = pack_group_info(Gid, Obj),
                                    mnesia:dirty_write(moyou_push_group_info_tab, GroupInfo),
                                    GroupInfo;
                                _ ->
                                    #moyou_push_group_info{gid = Gid}
                            end;
                        _ ->
                            #moyou_push_group_info{gid = Gid}
                    end
            end;
        [GroupInfo] ->
            GroupInfo
    end.

get_user_info(Uid) when is_list(Uid) ->
    get_user_info(list_to_binary(Uid));
get_user_info(Uid) ->
    case mnesia:dirty_read(moyou_push_user_info_tab, binary_to_list(Uid)) of
        [] ->
            Url = moyou_util:get_config(gamepro_server),
            ParamObj = {obj, [{"method", list_to_binary("getUserInfo")},
                              {"params", {obj, [{"userid", Uid}]}}
                             ]},
            Params = "body=" ++ rfc4627:encode(ParamObj),
            case moyou_util:http_request(Url, Params) of
                [] ->
                    [];
                Body ->
                    case rfc4627:decode(Body) of
                        {ok, Obj, _Re} ->
                            case rfc4627:get_field(Obj, "success") of
                                {ok, true} ->
                                    UserInfo = pack_user_info(Uid, Obj),
                                    mnesia:dirty_write(moyou_push_user_info_tab, UserInfo),
                                    UserInfo;
                                _ ->
                                    []
                            end;
                        _ ->
                            []
                    end
            end;
        [UserInfo] ->
            UserInfo
    end.

pack_user_info(Uid, Obj) ->
    {ok, Entity} = rfc4627:get_field(Obj, "entity"),
    Friends = [{binary_to_list(FriendId), binary_to_list(Alia)} ||
        [FriendId, Alia] <- moyou_util:get_json_field(Entity, "friends", [])],
    NickName = binary_to_list(moyou_util:get_json_field(Entity, "nickname", <<"">>)),
    Blacks = [binary_to_list(Bin) || Bin <- moyou_util:get_json_field(Entity, "blackList", [])],
    DeviceToken = case moyou_util:get_json_field(Entity, "deviceToken", null) of
                      null ->
                          null;
                      DeviceTokenBin ->
                          re:replace(binary_to_list(DeviceTokenBin), " ", "", [global, {return, list}])
                  end,
    Imei = case moyou_util:get_json_field(Entity, "imei", null) of
               null ->
                   null;
               ImeiBin ->
                   binary_to_list(ImeiBin)
           end,
    MessageConfig = moyou_util:get_json_field(Entity, "messageDetailConfig", 1),
    SilenceConfig = moyou_util:get_json_field(Entity, "silenceConfig", 0),
    Env = case moyou_util:get_json_field(Entity, "appType", null) of
              null ->
                  null;
              AppType ->
                  binary_to_list(AppType)
          end,
    #moyou_push_user_info{uid = binary_to_list(Uid), friends = Friends, nick_name = NickName, blacks = Blacks,
                          imei = Imei, device_token = DeviceToken, silence_config = SilenceConfig,
                          message_config = MessageConfig, env = Env}.


pack_group_info(Gid, Obj) ->
    {ok, Entity} = rfc4627:get_field(Obj, "entity"),
    GroupName = binary_to_list(moyou_util:get_json_field(Entity, "groupName", <<"">>)),
    NotPush = [binary_to_list(Bin) || Bin <- moyou_util:get_json_field(Entity, "notPushUserids", [])],
    #moyou_push_group_info{gid = Gid, name = GroupName, not_push = NotPush}.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

init([]) ->
    [CertFile, KeyFile, Host, DevCertFile, DevKeyFile, DevHost] = moyou_util:get_config(ios_push_config),
    PoolSize =  moyou_util:get_config(ios_push_poolsize, 128),
    moyou_util:create_or_copy_table(moyou_push_user_info_tab, [{record_name, moyou_push_user_info},
                                                               {attributes, record_info(fields, moyou_push_user_info)},
                                                               {ram_copies, [node()]}], ram_copies),
    moyou_util:create_or_copy_table(moyou_push_group_info_tab, [{record_name, moyou_push_group_info},
                                                                {attributes, record_info(fields, moyou_push_group_info)},
                                                                {ram_copies, [node()]}], ram_copies),
    ProIosPids = [begin
                      {ok, Pid} = moyou_ios_provider:start(CertFile, KeyFile, Host),
                      Pid
                  end || _ <- lists:duplicate(PoolSize, 1)],
    DevIosPids = [begin
                      {ok, Pid} = moyou_ios_provider:start(DevCertFile, DevKeyFile, DevHost),
                      Pid
                  end || _ <- lists:duplicate(PoolSize, 1)],
    {ok, #state{ios_push_certfile = CertFile,
                ios_push_keyfile = KeyFile,
                ios_push_host = Host,
                ios_push_handlers = ProIosPids,
                ios_dev_push_certfile = DevCertFile,
                ios_dev_push_keyfile = DevKeyFile,
                ios_dev_push_host = DevHost,
                ios_dev_push_handlers = DevIosPids}}.

handle_call({list}, _From, State) ->
    {reply, {ok, State}, State};
handle_call({random_ios_push_pid, Env}, _From, #state{ios_push_certfile = CertFile,
                                                      ios_push_keyfile = KeyFile,
                                                      ios_push_host = Host,
                                                      ios_push_handlers = Handler,
                                                      ios_dev_push_certfile = DevCertFile,
                                                      ios_dev_push_keyfile = DevKeyFile,
                                                      ios_dev_push_host = DevHost,
                                                      ios_dev_push_handlers = DevHandler} = State) ->
    case Env of
        "development" ->
            Index = moyou_util:random(length(DevHandler)),
            Pid = lists:nth(Index, DevHandler),
            case is_process_alive(Pid) of
                true ->
                    {reply, {ok, Pid}, State};
                false ->
                    NewPids = lists:delete(Pid, DevHandler),
                    {ok, Pid1} = moyou_ios_provider:start(DevCertFile, DevKeyFile, DevHost),
                    {reply, {ok, Pid1}, State#state{ios_dev_push_handlers = [Pid1 | NewPids]}}
            end;
        _ ->
            Index = moyou_util:random(length(Handler)),
            Pid = lists:nth(Index, Handler),
            case is_process_alive(Pid) of
                true ->
                    {reply, {ok, Pid}, State};
                false ->
                    NewPids = lists:delete(Pid, Handler),
                    {ok, Pid1} = moyou_ios_provider:start(CertFile, KeyFile, Host),
                    {reply, {ok, Pid1}, State#state{ios_push_handlers = [Pid1 | NewPids]}}
            end
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




test_normal_msg(Uid) ->
    From = {jid, "10000", "gamepro.com", [], "10000", "gamepro.com", []},
    To = {jid, Uid, "gamepro.com", [], Uid, "gamepro.com", []},
    Packet = {xmlelement,"message",
              [{"msgTime","1420812821877"},
               {"id","6GDf3-81"},
               {"to", Uid ++ "@gamepro.com"},
               {"from","10000@gamepro.com/352248062680367"},
               {"type","chat"},
               {"msgtype","normalchat"}],
              [{xmlelement,"body",[],[{xmlcdata,<<"9">>}]}]},
    send_offline_message(From, To, Packet).


test_group_msg() ->
    From = {jid, "10111967", "gamepro.com", [], "10111967", "gamepro.com", []},
    To = {jid, "10244937", "gamepro.com", [], "10244937", "gamepro.com", []},
    Packet = {xmlelement,"message",
              [{"server_id","mygroup_104620_813"},
               {"msgTime","1420811840278"},
               {"type","chat"},
               {"to","10244937@gamepro.com"},
               {"from","10111967@gamepro.com"},
               {"msgtype","groupchat"},
               {"fileType","text"},
               {"id","mygroup_104620_813"},
               {"groupid","104620"},
               {"g","0"}],
              [{xmlelement,"body",[],
                [{xmlcdata,
                  <<"{\"content\":\"M\",\"userNickName\":\"0517\"}">>}]}]},
    send_offline_message(From, To, Packet).