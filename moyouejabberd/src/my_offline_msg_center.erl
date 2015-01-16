%% @author chenkangmin
%% @doc @todo Add description to my_offline_msg_center.


-module(my_offline_msg_center).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("aa_data.hrl").
-include("jlib.hrl").
-include("ejabberd.hrl").

-define(USER_MSD_PID_COUNT, 128).


-record(state, {ios_push_certfile,
                ios_dev_push_certfile,
                ios_push_keyfile,
                ios_dev_push_keyfile,
                ios_push_host,
                ios_dev_push_host,
                ios_push_handlers = [],
                ios_dev_push_handlers = []}).

-record(user_info, {uid, friends = [], nick_name, device_token, imei, blacks = [], silence_config = 0, message_config = 1, env}).

-record(moyou_group_info, {gid, name, not_push = []}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         list/0,
         stop/0,
         update_group_info/3,
         update_user_info/9,
         send_offline_msg/3,
         test_normal_msg/0,
         test_group_msg/0
        ]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

list() ->
    gen_server:call(?MODULE, {list}).

stop() ->
    gen_server:call(?MODULE, {stop}).

update_group_info(Gid, GroupName, NotPush) ->
    case mnesia:dirty_read(group_info_tab, Gid) of
        [] ->
            GroupInfo = #moyou_group_info{gid = Gid, name = GroupName, not_push = NotPush},
            mnesia:dirty_write(group_info_tab, GroupInfo);
        [GroupInfo] ->
            mnesia:dirty_write(group_info_tab, GroupInfo#moyou_group_info{name = GroupName, not_push = NotPush})
    end.

update_user_info(Uid, Friends, NickName, DeviceToken, Imei, Blacks, SilenceConfig, MessageConfig, Env) ->
    case mnesia:dirty_read(user_info_tab, Uid) of
        [] ->
            UserInfo = #user_info{uid = Uid, friends = Friends, nick_name = NickName, blacks = Blacks,
                                  imei = Imei, device_token = DeviceToken, silence_config = SilenceConfig,
                                  message_config = MessageConfig, env = Env},
            mnesia:dirty_write(user_info_tab, UserInfo);
        [UserInfo] ->
            mnesia:dirty_write(user_info_tab, UserInfo#user_info{friends = Friends, nick_name = NickName, blacks = Blacks,
                                                                 imei = Imei, device_token = DeviceToken, silence_config = SilenceConfig,
                                                                 message_config = MessageConfig, env = Env})
    end.

send_offline_msg(#jid{server = Domain} = From, #jid{user = ToUid} = To, Packet) ->
    ToUserInfo = get_user_info(ToUid, Domain),
    if
        ToUserInfo =/= [] ->
            case ToUserInfo#user_info.imei of
                null -> %%ios
                    ios_send_offline_msg(From, ToUserInfo, Packet);
                _ ->  %%android
                    android_send_offline_msg(From, ToUserInfo, Packet)
            end;
        true ->
            %            ?INFO_MSG("skip send_offline_msg From : ~p~n, To : ~p~n, Packet : ~p~n", [From, To, Packet]),
            skip
    end.


android_send_offline_msg(From, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when ToUserInfo#user_info.imei /= null ->
    Imei = ToUserInfo#user_info.imei,
    PushTo = #jid{user = Imei, server = "push.gamepro.com", resource = [], luser = Imei, lserver = "push.gamepro.com", lresource = []},
    Mt = proplists:get_value("msgtype", Attr, ""),
    PushAttr = [{"id", aa_hookhandler:get_id()},
                {"type", "chat"},
                {"msgTime", proplists:get_value("msgTime", Attr, "")},
                {"msgtype", get_push_msg_type(Mt)}],
    BodyJson = get_push_body(Mt, From, ToUserInfo, Packet),
    case BodyJson of
        skip ->
            %            ?INFO_MSG("skip android_send_offline_msg From : ~p~n, To : ~p~n, Packet : ~p~n", [From, ToUserInfo, Packet]),
            skip;
        _ ->
            CDATA = rfc4627:encode(BodyJson),
            PushBody = {xmlelement, "body", [], [{xmlcdata, list_to_binary(CDATA)}]},
            PushPacket = {xmlelement, "message", PushAttr, [PushBody]},
            ejabberd_router:route(From, PushTo, PushPacket)
    end;
android_send_offline_msg(_From, _ToUserInfo, _Packet) ->
    %    ?INFO_MSG("skip android_send_offline_msg From : ~p~n, To : ~p~n, Packet : ~p~n", [From, ToUserInfo, Packet]),
    skip.

ios_send_offline_msg(From, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when ToUserInfo#user_info.device_token /= null ->
    Token = ToUserInfo#user_info.device_token,
    Mt = proplists:get_value("msgtype", Attr, ""),
    Alert = get_push_alert(Mt, From, ToUserInfo, Packet),
    case Alert of
        skip ->
            %            ?INFO_MSG("skip ios_send_offline_msg From : ~p~n, To : ~p~n, Packet : ~p~n", [From, ToUserInfo, Packet]),
            skip;
        _ ->
            Sound = get_push_sound(ToUserInfo, Mt),
            push_to_ios(Token, Sound, Alert, ToUserInfo#user_info.env)
    end;
ios_send_offline_msg(_From, _ToUserInfo, _Packet) ->
    %    ?INFO_MSG("skip ios_send_offline_msg From : ~p~n, To : ~p~n, Packet : ~p~n", [From, ToUserInfo, Packet]),
    skip.

get_push_sound(ToUserInfo, Mt) ->
    case ToUserInfo#user_info.silence_config of
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

get_push_msg_type("system") ->
    "normalchat";
get_push_msg_type("joinGroupApplication") ->
    "normalchat";
get_push_msg_type("groupBillboard") ->
    "normalchat";
get_push_msg_type("startTeamPreparedConfirm") ->
    "normalchat";
get_push_msg_type("teamInvite") ->
    "normalchat";
get_push_msg_type("requestJoinTeam") ->
    "normalchat";
get_push_msg_type("teamHelper") ->
    "normalchat";
get_push_msg_type(Mt) ->
    Mt.


get_push_body(Mt, #jid{user = FromUid, server = Domain}, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when Mt =:= "groupchat" ->
    Gid = proplists:get_value("groupid", Attr, ""),
    ID = proplists:get_value("id", Attr, ""),
    GroupInfo = get_group_info(Gid, Domain),
    FromUserInfo = get_user_info(FromUid, Domain),
    case FromUserInfo of
        [] ->
            skip;
        _ ->
            Content = get_msg_content(Packet),
            {ok, Entity, _} = rfc4627:decode(hd(Content)),
            {ok, Msg} = rfc4627:get_field(Entity, "content"),
            Name = list_to_binary(get_nickname_or_alia(FromUserInfo, ToUserInfo)),
            case lists:member(ToUserInfo#user_info.uid, GroupInfo#moyou_group_info.not_push) of
                true ->
                    skip;
                _ ->
                    {obj, [{"id", list_to_binary(ID)},
                           {"expire", <<"604800">>},
                           {"msgtype", list_to_binary(Mt)},
                           {"from", list_to_binary(FromUserInfo#user_info.uid)},
                           {"body", {obj, [{"groupId", list_to_binary(Gid)},
                                           {"groupName", list_to_binary(GroupInfo#moyou_group_info.name)},
                                           {"msg", <<Name/binary, <<":">>/binary, Msg/binary>>}]}
                           }]}
            end
    end;
get_push_body(Mt, #jid{user = FromUid, server = Domain}, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet)
when Mt =:= "normalchat" orelse Mt =:= "sayHello" ->
    ID = proplists:get_value("id", Attr, ""),
    FromUserInfo = get_user_info(FromUid, Domain),
    Content = get_msg_content(Packet),
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
                           {"from", list_to_binary(FromUserInfo#user_info.uid)}]}
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
    Content = get_msg_content(Packet),
    Name = get_msg_type_name(Mt),
    {obj, [{"id", list_to_binary(ID)},
           {"expire", <<"604800">>},
           {"body", list_to_binary(lists:concat([Name, ":", Content]))},
           {"msgtype", <<"normalchat">>},
           {"from", list_to_binary(FromUid)}]};
get_push_body(_Mt, _From, _ToUserInfo, _Packet) ->
    skip.


get_push_alert(Mt, #jid{user = FromUid, server = Domain}, ToUserInfo, {xmlelement, "message", Attr, _Body} = Packet) when Mt =:= "groupchat" ->
    Gid = proplists:get_value("groupid", Attr, ""),
    GroupInfo = get_group_info(Gid, Domain),
    FromUserInfo = get_user_info(FromUid, Domain),
    case FromUserInfo of
        [] ->
            skip;
        _ ->
            Content = get_msg_content(Packet),
            {ok, Entity, _} = rfc4627:decode(hd(Content)),
            {ok, Msg} = rfc4627:get_field(Entity, "content"),
            Name = get_nickname_or_alia(FromUserInfo, ToUserInfo),
            case lists:member(ToUserInfo#user_info.uid, GroupInfo#moyou_group_info.not_push) of
                true ->
                    skip;
                _ ->
                    case ToUserInfo#user_info.message_config of
                        0 ->
                            "您有一条新的陌游消息！";
                        _ ->
                            lists:concat([Name, "(", GroupInfo#moyou_group_info.name, "):", binary_to_list(Msg)])
                    end
            end
    end;
get_push_alert(Mt, #jid{user = FromUid, server = Domain}, ToUserInfo, Packet) when Mt =:= "normalchat" ->
    FromUserInfo = get_user_info(FromUid, Domain),
    Content = get_msg_content(Packet),
    case FromUserInfo of
        [] ->
            skip;
        _ ->
            Name = get_nickname_or_alia(FromUserInfo, ToUserInfo),
            case check_black(FromUserInfo, ToUserInfo) of
                true ->
                    skip;
                _ ->
                    case ToUserInfo#user_info.message_config of
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
    Content = get_msg_content(Packet),
    Name = get_msg_type_name(Mt),
    lists:concat([Name, ":", Content]);
get_push_alert(_Mt, _From, _ToUserInfo, _Packet) ->
    skip.

       
get_msg_type_name("system") ->
    "系统消息";
get_msg_type_name("joinGroupApplication") ->
    "入群申请";
get_msg_type_name("groupBillboard") ->
    "群公告";
get_msg_type_name("startTeamPreparedConfirm") ->
    "就位确认";
get_msg_type_name("teamInvite") ->
    "组队消息";
get_msg_type_name("requestJoinTeam") ->
    "组队申请";
get_msg_type_name("teamHelper") ->
    "组队助手";
get_msg_type_name(Mt) ->
    Mt.


push_to_ios(Token, Sound, Alert, Env) ->
    {ok, Pid} = gen_server:call(?MODULE, {random_ios_push_pid, Env}),
    my_ios_provider:push(Pid, Token, Sound, Alert).
    

get_msg_content({xmlelement, "message", _, Message}) ->
    feach_message(Message,[]).


feach_message([Element | Message], List) ->
    case Element of
        {xmlelement,"body", _, _} ->
            feach_message(Message, [get_text_message_form_packet_result(Element) | List]);
        _ ->
            feach_message(Message,List)
    end;
feach_message([], List) ->
    List.


%% 获取消息包中的文本消息，用于离线消息推送服务
get_text_message_form_packet_result({xmlelement, "body", _, List})->
    Res = lists:map(fun({_, V})-> binary_to_list(V) end, List),
    binary_to_list(list_to_binary(Res)).


check_black(#user_info{uid = Uid}, #user_info{blacks = Blacks}) ->
    lists:member(Uid, Blacks).


get_nickname_or_alia(#user_info{nick_name = NickName, uid = Uid}, #user_info{friends = Friends}) ->
    case lists:keysearch(Uid, 1, Friends) of
        {value, {Uid, Alia}} ->
            Alia;
        _ ->
            NickName
    end.


get_group_info(Gid, Domain) ->
    case mnesia:dirty_read(group_info_tab, Gid) of
        [] ->
            HTTPServer =  ejabberd_config:get_local_option({http_server, Domain}),
            HTTPService = ejabberd_config:get_local_option({http_server_service_client, Domain}),
            Url = string:concat(HTTPServer, HTTPService),
            ParamObj = {obj, [{"method", list_to_binary("getGroupInfo")},
                              {"params", {obj, [{"groupId", list_to_binary(Gid)}]}}
                             ]},
            Form = "body=" ++ rfc4627:encode(ParamObj),
            try
                case httpc:request(post, {Url ,[], "application/x-www-form-urlencoded" , Form}, [], []) of
                    {ok, {_, _, Body}} ->
                        DBody = rfc4627:decode(Body),
                        case DBody of
                            {ok, Obj, _Re} ->
                                case rfc4627:get_field(Obj, "success") of
                                    {ok, true} ->
                                        {ok, Entity} = rfc4627:get_field(Obj, "entity"),
                                        {ok, GroupName} = rfc4627:get_field(Entity, "groupName"),
                                        NotPush = case rfc4627:get_field(Entity, "notPushUserids") of
                                                      {ok, NotPushList} ->
                                                          [binary_to_list(Bin) || Bin <- NotPushList];
                                                      _ ->
                                                          []
                                                  end,
                                        GroupInfo = #moyou_group_info{gid = Gid, name = binary_to_list(GroupName), not_push = NotPush},
                                        mnesia:dirty_write(group_info_tab, GroupInfo),
                                        GroupInfo;
                                    _ ->
                                        #moyou_group_info{gid = Gid}
                                end;
                            _ ->
                                #moyou_group_info{gid = Gid}
                        end ;
                    {error, _Reason} ->
                        #moyou_group_info{gid = Gid}
                end
            catch
                _ErrType:_ErrReason->
                    #moyou_group_info{gid = Gid}
            end;
        [GroupInfo] ->
            GroupInfo
    end.

get_user_info(Uid, Domain) when is_list(Uid) ->
    get_user_info(list_to_binary(Uid), Domain);
get_user_info(Uid, Domain) ->
    case mnesia:dirty_read(user_info_tab, Uid) of
        [] ->
            try
                HTTPServer =  ejabberd_config:get_local_option({http_server, Domain}),
                HTTPService = ejabberd_config:get_local_option({http_server_service_client, Domain}),
                Url = string:concat(HTTPServer, HTTPService),
                ParamObj = {obj, [{"method", list_to_binary("getUserInfo")},
                                  {"params", {obj, [{"userid", Uid}]}}
                                 ]},
                Form = "body=" ++ rfc4627:encode(ParamObj),
                case httpc:request(post, {Url ,[], "application/x-www-form-urlencoded" , Form}, [], []) of
                    {ok, {_, _, Body}} ->
                        DBody = rfc4627:decode(Body),
                        case DBody of
                            {ok, Obj, _Re} ->
                                case rfc4627:get_field(Obj, "success") of
                                    {ok, true} ->
                                        {ok, Entity} = rfc4627:get_field(Obj, "entity"),
                                        Friends = case rfc4627:get_field(Entity, "friends") of
                                                      {ok, FriendsList} ->
                                                          [{binary_to_list(FriendId), binary_to_list(Alia)} || [FriendId, Alia] <- FriendsList];
                                                      _ ->
                                                          []
                                                  end,
                                        NickName = case rfc4627:get_field(Entity, "nickname") of
                                                       {ok, NickNameBin} ->
                                                           binary_to_list(NickNameBin);
                                                       _ ->
                                                           ""
                                                   end,
                                        Blacks = case rfc4627:get_field(Entity, "blackList") of
                                                     {ok, BlackList} ->
                                                         [binary_to_list(Bin) || Bin <- BlackList];
                                                     _ ->
                                                         []
                                                 end,
                                        DeviceToken = case rfc4627:get_field(Entity, "deviceToken") of
                                                          {ok, null} ->
                                                              null;
                                                          {ok, DeviceTokenBin} ->
                                                              re:replace(binary_to_list(DeviceTokenBin), " ", "", [global, {return, list}]);
                                                          _ ->
                                                              null
                                                      end,
                                        Imei = case rfc4627:get_field(Entity, "imei") of
                                                   {ok, null} ->
                                                       null;
                                                   {ok, ImeiBin} ->
                                                       binary_to_list(ImeiBin);
                                                   _ ->
                                                       null
                                               end,
                                        MessageConfig = case rfc4627:get_field(Entity, "messageDetailConfig") of
                                                            {ok, MessageConfigTmp} ->
                                                                MessageConfigTmp;
                                                            _ ->
                                                                1
                                                        end,
                                        SilenceConfig = case rfc4627:get_field(Entity, "silenceConfig") of
                                                            {ok, SilenceConfigTmp} ->
                                                                SilenceConfigTmp;
                                                            _ ->
                                                                0
                                                        end,
                                        Env = case rfc4627:get_field(Entity, "appType") of
                                                  {ok, null} ->
                                                      null;
                                                  {ok, AppType} ->
                                                      binary_to_list(AppType);
                                                  _ ->
                                                      null
                                              end,
                                        UserInfo = #user_info{uid = binary_to_list(Uid), friends = Friends, nick_name = NickName, blacks = Blacks,
                                                              imei = Imei, device_token = DeviceToken, silence_config = SilenceConfig,
                                                              message_config = MessageConfig, env = Env},
                                        mnesia:dirty_write(user_info_tab, UserInfo),
                                        UserInfo;
                                    _ ->
                                        []
                                end;
                            _ ->
                                []
                        end ;
                    {error, _Reason} ->
                        []
                end
            catch
                _ErrType:_ErrReason->
                    []
            end;
        [UserInfo] ->
            UserInfo
    end.



create_or_copy_table(TableName, Opts, Copy) ->
    case mnesia:create_table(TableName, Opts) of
        {aborted,{already_exists,_}} ->
            mnesia:add_table_copy(TableName, node(), Copy);
        _ ->
            skip
    end.


%% ====================================================================
%% Behavioural functions 
%% ====================================================================

init([]) ->
    [Domain | _] = ?MYHOSTS,
    case ejabberd_config:get_local_option({ios_push_config, Domain}) of
        [CertFile, KeyFile, Host] ->
            ssl:start(),
            create_or_copy_table(user_info_tab, [{record_name, user_info},
                                                 {attributes, record_info(fields, user_info)},
                                                 {ram_copies, [node()]}], ram_copies),
            create_or_copy_table(group_info_tab, [{record_name, moyou_group_info},
                                                  {attributes, record_info(fields, moyou_group_info)},
                                                  {ram_copies, [node()]}], ram_copies),
            IOSPushPid = [begin
                              {ok, Pid} = my_ios_provider:start(CertFile, KeyFile, Host),
                              Pid
                          end || _ <- lists:duplicate(?USER_MSD_PID_COUNT, 1)],
            IOSDevPushPid = [begin
                                 {ok, Pid} = my_ios_provider:start("/etc/ejabberd/key/moyou_dev_cert.pem", "/etc/ejabberd/key/moyou_dev_key.pem", "gateway.sandbox.push.apple.com"),
                                 Pid
                             end || _ <- lists:duplicate(10, 1)],
            {ok, #state{ios_push_certfile = CertFile,
                        ios_push_keyfile = KeyFile,
                        ios_push_host = Host,
                        ios_push_handlers = IOSPushPid,
                        ios_dev_push_certfile = "/etc/ejabberd/key/moyou_dev_cert.pem",
                        ios_dev_push_keyfile = "/etc/ejabberd/key/moyou_dev_key.pem",
                        ios_dev_push_host = "gateway.sandbox.push.apple.com",
                        ios_dev_push_handlers = IOSDevPushPid}};
        _ ->
            {ok, #state{}}
    end.

handle_call({stop}, _From, State) ->
    [Pids ! stop || Pids <- State#state.ios_push_handlers],
    %    [Pids ! stop || Pids <- State#state.ios_dev_push_handlers],
    {reply, {ok, State}, State};

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
            Count = length(DevHandler),
            {A, B, C} = os:timestamp(),
            random:seed(A, B,C),
            Index = random:uniform(Count),
            Pid = lists:nth(Index, DevHandler),
            case is_process_alive(Pid) of
                true ->
                    {reply, {ok, Pid}, State};
                false ->
                    NewPids = lists:delete(Pid, DevHandler),
                    {ok, Pid1} = my_ios_provider:start(DevCertFile, DevKeyFile, DevHost),
                    {reply, {ok, Pid1}, State#state{ios_dev_push_handlers = [Pid1 | NewPids]}}
            end;
        _ ->
            Count = length(Handler),
            {A, B, C} = os:timestamp(),
            random:seed(A, B,C),
            Index = random:uniform(Count),
            Pid = lists:nth(Index, Handler),
            case is_process_alive(Pid) of
                true ->
                    {reply, {ok, Pid}, State};
                false ->
                    NewPids = lists:delete(Pid, Handler),
                    {ok, Pid1} = my_ios_provider:start(CertFile, KeyFile, Host),
                    {reply, {ok, Pid1}, State#state{ios_push_handlers = [Pid1 | NewPids]}}
            end
    end;
    

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




test_normal_msg() ->
    From = {jid, "10111967", "gamepro.com", [], "10111967", "gamepro.com", []},
    To = {jid, "10111087", "gamepro.com", [], "10111087", "gamepro.com", []},
    Packet = {xmlelement,"message",
              [{"msgTime","1420812821877"},
               {"id","6GDf3-81"},
               {"to","10111087@gamepro.com"},
               {"from","10111967@gamepro.com/352248062680367"},
               {"type","chat"},
               {"msgtype","normalchat"}],
              [{xmlelement,"body",[],[{xmlcdata,<<"9">>}]}]},
    send_offline_msg(From, To, Packet).


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
    send_offline_msg(From, To, Packet).