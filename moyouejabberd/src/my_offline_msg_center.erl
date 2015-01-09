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
                ios_push_keyfile,
                ios_push_host,
                ios_push_handlers = []}).

-record(user_info, {uid, friends, nick_name, device_token, imei, app_type, blacks}).

-record(moyou_group_info, {gid, name}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
         send_offline_msg/3
        ]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


send_offline_msg(From, To, {xmlelement, "message", Attr, _Body} = Packet) ->
    FromUserInfo = get_user_info(From),
    ToUserInfo = get_user_info(To),
    if
        FromUserInfo =/= [] andalso ToUserInfo =/= [] ->
            case check_black(FromUserInfo, ToUserInfo) of
                true ->
                    skip;
                _ ->
                    Name = get_nickname_or_alia(FromUserInfo, ToUserInfo),
                    Content = get_msg_content(Packet),
                    case ToUserInfo#user_info.imei of
                        null ->
                            case ToUserInfo#user_info.device_token of
                                null ->
                                    skip;
                                Token ->
                                    %%ios推送
                                    Mt = proplists:get_value("msgtype", Attr, ""),
                                    Alert = case Mt of
                                                "groupchat" ->
                                                    Gid = proplists:get_value("groupid", Attr, ""),
                                                    GName = get_group_name(Gid, From),
                                                    Name ++ "(" ++ GName ++ "):" ++ Content;
                                                _ ->
                                                    Name ++ ":" ++ Content
                                            end,
                                    push_to_ios(Token, Alert)
                            end;
                        Imei ->
                            %%android推送
                            PushTo = #jid{user = Imei, server = "push.gamepro.com", luser = Imei, lserver = "push.gamepro.com"},
                            Mt = proplists:get_value("msgtype", Attr, ""),
                            ID = proplists:get_value("id", Attr, ""),
                            PushAttr = [{"id", aa_hookhandler:get_id()},
                                        {"type", "chat"},
                                        {"msgTime", proplists:get_value("msgTime", Attr, "")},
                                        {"msgtype", Mt}],
                            Json = case Mt of
                                       "groupchat" ->
                                           Gid = proplists:get_value("groupid", Attr, ""),
                                           GName = get_group_name(Gid, From),
                                           {obj, [{"id", list_to_binary(ID)},
                                                  {"expire", <<"604800">>},
                                                  {"body", list_to_binary(Content)},
                                                  {"msgtype", list_to_binary(proplists:get_value("msgtype", Attr, ""))},
                                                  {"from", list_to_binary(FromUserInfo#user_info.uid)},
                                                  {"body", {obj, [{"groupId", list_to_binary(Gid)},
                                                                  {"groupName", list_to_binary(GName)},
                                                                  {"msg", Name ++ ":" ++ Content}]}}]};
                                       _ ->
                                           {obj, [{"id", list_to_binary(ID)},
                                                  {"expire", <<"604800">>},
                                                  {"body", list_to_binary(Content)},
                                                  {"msgtype", list_to_binary(proplists:get_value("msgtype", Attr, ""))},
                                                  {"from", list_to_binary(FromUserInfo#user_info.uid)}]}
                                   end,
                            CDATA = rfc4627:encode(Json),
                            PushBody = {xmlelement, "body", [], [{xmlcdata, CDATA}]},
                            PushPacket = {xmlelement, "message", PushAttr, PushBody},
                            ejabberd_router:route(From, PushTo, PushPacket)
                    end
            end;
        true ->
            skip
    end.


push_to_ios(Token, Alert) ->
    {ok, Pid} = gen_server:call(?MODULE, {random_ios_push_pid}),
    my_ios_provider:push(Pid, Token, Alert).
    

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


get_nickname_or_alia(#user_info{nick_name = NickName, friends = Friends}, #user_info{uid = Uid}) ->
    case lists:keysearch(Uid, 1, Friends) of
        {value, {Uid, Alia}} ->
            Alia;
        _ ->
            NickName
    end.


get_group_name(Gid, #jid{server = Domain}) ->
    case mnesia:dirty_read(group_info_tab, Gid) of
        [] ->
            HTTPServer =  ejabberd_config:get_local_option({http_server, Domain}),
            HTTPService = ejabberd_config:get_local_option({http_server_service_client, Domain}),
            Url = string:concat(HTTPServer, HTTPService),
            ParamObj = {obj, [{"method", list_to_binary("getGroupInfo")},
                              {"groupId", list_to_binary(Gid)}
                             ]},
            Form = "body=" ++ rfc4627:encode(ParamObj),
            try
                case httpc:request(post, {Url ,[], "application/x-www-form-urlencoded" , Form}, [], []) of
                    {ok, {_, _, Body}} ->
                        DBody = rfc4627:decode(Body),
                        case DBody of
                            {ok, Obj, _Re} ->
                                case rfc4627:get_field(Obj, "errorCode") of
                                    {ok, <<"0">>} ->
                                        {ok, ValueObj} = rfc4627:get_field(Obj, "value"),
                                        Name = case rfc4627:get_field(ValueObj, "name") of
                                                   {ok, BinName} ->
                                                       binary_to_list(BinName);
                                                   _ ->
                                                       ""
                                               end,
                                        UserInfo = #moyou_group_info{gid = Gid, name = Name},
                                        mnesia:dirty_write(group_info_tab, UserInfo),
                                        UserInfo#moyou_group_info.name;
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
        [GrooupInfo] ->
            GrooupInfo#moyou_group_info.name
    end.

get_user_info(#jid{server = Domain} = User) ->
    Uid = get_uid(User),
    case mnesia:dirty_read(user_info_tab, Uid) of
        [] ->
            HTTPServer =  ejabberd_config:get_local_option({http_server, Domain}),
            HTTPService = ejabberd_config:get_local_option({http_server_service_client, Domain}),
            Url = string:concat(HTTPServer, HTTPService),
            ParamObj = {obj, [{"method", list_to_binary("getUserInfo")},
                              {"userid", list_to_binary(Uid)}
                             ]},
            Form = "body=" ++ rfc4627:encode(ParamObj),
            try
                case httpc:request(post, {Url ,[], "application/x-www-form-urlencoded" , Form}, [], []) of
                    {ok, {_, _, Body}} ->
                        DBody = rfc4627:decode(Body),
                        case DBody of
                            {ok, Obj, _Re} ->
                                case rfc4627:get_field(Obj, "errorCode") of
                                    {ok, <<"0">>} ->
                                        {ok, ValueObj} = rfc4627:get_field(Obj, "value"),
                                        Friends = case rfc4627:get_field(ValueObj, "friends") of
                                                      {ok, FriendsList} ->
                                                          [{binary_to_list(FriendId), binary_to_list(Alia)} || [FriendId, Alia] <- FriendsList];
                                                      _ ->
                                                          []
                                                  end,
                                        NickName = case rfc4627:get_field(ValueObj, "nickname") of
                                                       {ok, NickNameBin} ->
                                                           binary_to_list(NickNameBin);
                                                       _ ->
                                                           ""
                                                   end,
                                        Blacks = case rfc4627:get_field(ValueObj, "blackList") of
                                                     {ok, BlackList} ->
                                                         [binary_to_list(Bin) || Bin <- BlackList];
                                                     _ ->
                                                         []
                                                 end,
                                        DeviceToken = case rfc4627:get_field(ValueObj, "deviceToken") of
                                                          {ok, null} ->
                                                              null;
                                                          {ok, DeviceTokenBin} ->
                                                              binary_to_list(DeviceTokenBin);
                                                          _ ->
                                                              null
                                                      end,
                                        Imei = case rfc4627:get_field(ValueObj, "imei") of
                                                   {ok, null} ->
                                                       null;
                                                   {ok, ImeiBin} ->
                                                       binary_to_list(ImeiBin);
                                                   _ ->
                                                       null
                                               end,
                                        UserInfo = #user_info{uid = Uid, friends = Friends, blacks = Blacks, imei = Imei, device_token = DeviceToken},
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


format_user_data(Jid) ->
    Jid#jid{resource = [], lresource = []}.

get_uid(#jid{user = User}) when is_binary(User) ->
    binary_to_list(User);
get_uid(#jid{user = User}) ->
    User.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

init([]) ->
    create_or_copy_table(user_info_tab, [{record_name, user_info},
                                         {attributes, record_info(fields, user_info)},
                                         {ram_copies, [node()]}], ram_copies),
    create_or_copy_table(group_info_tab, [{record_name, moyou_group_info},
                                          {attributes, record_info(fields, moyou_group_info)},
                                          {ram_copies, [node()]}], ram_copies),
    [Domain | _] = ?MYHOSTS,
    CertFile = ejabberd_config:get_local_option({ios_push_certfile, Domain}),
    KeyFile = ejabberd_config:get_local_option({ios_push_keyfile, Domain}),
    Host = ejabberd_config:get_local_option({ios_push_host, Domain}),
    IOSPushPid = [begin
                      {ok, Pid} = my_ios_provider:start(CertFile, KeyFile, Host),
                      Pid
                  end || _ <- lists:duplicate(?USER_MSD_PID_COUNT, 1)],
    {ok, #state{ios_push_certfile = CertFile,
                ios_push_keyfile = KeyFile,
                ios_push_host = Host,
                ios_push_handlers = IOSPushPid}}.


handle_call({list}, _From, State) ->
    {reply, {ok, State}, State};

handle_call({random_ios_push_pid}, _From, #state{ios_push_certfile = CertFile,
                                                 ios_push_keyfile = KeyFile,
                                                 ios_push_host = Host,
                                                 ios_push_handlers = Handler} = State) ->
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
