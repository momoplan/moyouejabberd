-module(aa_http).

-behaviour(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").

%% API
-export([start_link/0]).

-define(Port,5380).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         stop/0]).

-record(state, {}).
-record(success,{success=true,entity}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    exit(erlang:whereis(?MODULE), kill),
    spawn(fun() ->
                  timer:sleep(3000),
                  aa_http:init([])
          end).

handle_http(Req) ->
    try
        Method = Req:get(method),
        Args = case Method of
                   'GET' ->
                       Req:parse_qs();
                   'POST' ->
                       Req:parse_post();
                   'HEAD' ->
                       []
               end,
        if Args == [] ->
                http_response({#success{success=false,entity=list_to_binary("empty argrment")},Req});
            true ->
                [{"body",Body}] = Args,
                ?DEBUG("###### handle_http :::> Body=~p",[Body]),
                {ok,Obj,_Re} = rfc4627:decode(Body),
                %%{ok,T} = rfc4627:get_field(Obj, "token"),
                {ok,M} = rfc4627:get_field(Obj, "method"),
			   
                case binary_to_list(M) of
                    "message_count" ->
                        GoodNodes = lists:filter(fun(Node) ->
                                                         P = rpc:call(Node,erlang,whereis,[aa_msg_statistic]),
                                                         is_pid(P)
                                                 end, [node()|nodes()]),
                        Datas =	[begin
                                     {ok, Info} = rpc:call(Node,aa_msg_statistic,info,[]),
                                     {Node, Info}
                                 end || Node <- GoodNodes],
                        {ok, {Total, TotalDel, TotalToday, TotalTodayDel}} = aa_msg_statistic:info(),
                        Json = lists:map(fun({Node, {Total, TotalDel, TotalToday, TotalTodayDel}}) ->
                                                 {obj, [{node, Node},
                                                        {total, Total},
                                                        {total_delete, TotalDel},
                                                        {today_total, TotalToday},
                                                        {today_total_delete, TotalTodayDel}]}
                                         end, Datas),
                        http_response({#success{success=true,entity=Json},Req});
                    "process_counter" ->
                        Counter = aa_process_counter:process_counter(),
                        http_response({#success{success=true,entity=Counter},Req});
                    "get_user_list" ->
                        UserList = aa_session:get_user_list(Body),
                        http_response({#success{success=true,entity=UserList},Req});
                    "remove_group" ->
                        Params = get_group_info(Body),
                        ?DEBUG("remove_group params=~p",[Params]),
                        {ok,Gid1} = rfc4627:get_field(Params,"gid"),
                        Gid = binary_to_list(Gid1),
                        Result = aa_group_chat:remove_group(Gid),
                        http_response({#success{success=true,entity=Result},Req});
                    "remove_user" ->
                        Params = get_group_info(Body),
                        {ok,Gid1} = rfc4627:get_field(Params,"gid"),
                        {ok,Uid1} = rfc4627:get_field(Params,"uid"),
                        Gid = binary_to_list(Gid1),
                        Uid = binary_to_list(Uid1),
                        Result = aa_group_chat:remove_user(Gid,Uid),
                        http_response({#success{success=true,entity=Result},Req});
                    "append_user" ->
                        Params = get_group_info(Body),
                        {ok,Gid1} = rfc4627:get_field(Params,"gid"),
                        {ok,Uid1} = rfc4627:get_field(Params,"uid"),
                        Gid = binary_to_list(Gid1),
                        Uid = binary_to_list(Uid1),
                        Result = aa_group_chat:append_user(Gid,Uid),
                        http_response({#success{success=true,entity=Result},Req});
                    "ack" ->
                        case rfc4627:get_field(Obj, "service") of
                            {ok,<<"emsg_bridge">>} ->
                                Result = aa_bridge:ack(Body),
                                http_response({#success{success=true,entity=Result},Req});
                            _ ->
                                http_response({#success{success=false,entity=list_to_binary("method undifine")},Req})
                        end;
                    "route" ->
                        case rfc4627:get_field(Obj, "service") of
                            {ok, <<"emsg_bridge">>} ->
                                Result = aa_bridge:route(Body),
                                http_response({#success{success=true,entity=Result},Req});
                            _ ->
                                http_response({#success{success=false,entity=list_to_binary("method undifine")},Req})
                        end;
                    "update_group_info" ->
                        case rfc4627:get_field(Obj, "gid") of
                            {ok, Gid} ->
                                {ok, GName} = rfc4627:get_field(Obj, "gname"),
                                NotPush = case rfc4627:get_field(Obj, "notPushUserids") of
                                              {ok, NotPushList} ->
                                                  [binary_to_list(Bin) || Bin <- NotPushList];
                                              _ ->
                                                  []
                                          end,
                                case aa_hookhandler:get_offline_data_node() of
                                    none ->
                                        http_response({#success{success = false, entity = list_to_binary("unknow data node")}, Req});
                                    Node ->
                                        rpc:cast(Node, my_offline_msg_center, update_group_info, [binary_to_list(Gid), binary_to_list(GName), NotPush]),
                                        http_response({#success{success = true, entity = <<"ok">>}, Req})
                                end;
                            _ ->
                                http_response({#success{success = true, entity = <<"unknow gid">>},Req})
                        end;
                    "update_user_info" ->
                        case rfc4627:get_field(Obj, "uid") of
                            {ok, Uid} ->
                                {ok, NickName} = rfc4627:get_field(Obj, "nickname"),
                                {ok, DeviceToken} = rfc4627:get_field(Obj, "deviceToken"),
                                {ok, SilenceConfig} = rfc4627:get_field(Obj, "silenceConfig"),
                                {ok, Imei} = rfc4627:get_field(Obj, "imei"),
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
                                case aa_hookhandler:get_offline_data_node() of
                                    none ->
                                        http_response({#success{success = false, entity = list_to_binary("unknow data node")}, Req});
                                    Node ->
                                        rpc:cast(Node, my_offline_msg_center, update_user_info, [binary_to_list(Uid), Friends, binary_to_list(NickName),
                                                                                                  re:replace(binary_to_list(DeviceToken), " ", "", [global, {return, list}]),
                                                                                                  binary_to_list(Imei), Blacks, SilenceConfig, MessageConfig]),
                                        http_response({#success{success = true, entity = <<"ok">>}, Req})
                                end;
                            _ ->
                                http_response({#success{success = true, entity = <<"unknow uid">>},Req})
                        end;
                    "query_group_id" ->
                        case rfc4627:get_field(Obj, "gid") of
                            {ok, Gid} ->
                                case aa_hookhandler:get_group_data_node() of
                                    none ->
                                        http_response({#success{success = false, entity = list_to_binary("unknow data node")}, Req});
                                    Node ->
                                        case rpc:call(Node, my_group_msg_center, query_group_id, [binary_to_list(Gid)]) of
                                            {badrpc, Reason1} ->
                                                ?ERROR_MSG("query_group_id failed, gid : ~p, Reason : ~p~n",[Node, Gid, Reason1]),
                                                http_response({#success{success = false, entity = list_to_binary("badrpc error")}, Req});
                                            {ok, Seq} ->
                                                Entity = {obj, [{seq, Seq}]},
                                                http_response({#success{success = true, entity = Entity}, Req})
                                        end
                                end;
                            _ ->
                                http_response({#success{success = true, entity = <<"unknow gid">>},Req})
                        end;
                    "query_group_msg" ->
                        case rfc4627:get_field(Obj, "msg_id") of
                            {ok, Mid} ->
                                case rfc4627:get_field(Obj, "uid") of
                                    {ok, Uid} ->
                                        Size1 = case rfc4627:get_field(Obj, "size") of
                                                    {ok, Size} ->
                                                        list_to_integer(binary_to_list(Size));
                                                    _ ->
                                                        20
                                                end,
                                        case aa_hookhandler:get_group_data_node() of
                                            none ->
                                                http_response({#success{success = false, entity = list_to_binary("unknow data node")}, Req});
                                            Node ->
                                                [_, Gid, Seq] = re:split(binary_to_list(Mid), "_", [{return, list}]),
                                                case rpc:call(Node, my_group_msg_center, query_group_msg, [Gid, binary_to_list(Uid), list_to_integer(Seq), Size1]) of
                                                    {badrpc, Reason1} ->
                                                        ?ERROR_MSG("query_group_msg failed, Mid : ~p, Size : ~p, Reason : ~p~n",[Node, Mid, Size1, Reason1]),
                                                        http_response({#success{success = false, entity = list_to_binary("badrpc error")}, Req});
                                                    {ok, Data} ->
                                                        Entity = [encode_msg_to_json(Message) ||Message <- Data],
                                                        http_response({#success{success = true, entity = Entity}, Req})
                                                end
                                        end;
                                    _ ->
                                        http_response({#success{success = true, entity = <<"unknow uid">>},Req})
                                end;
                            _ ->
                                http_response({#success{success=false,entity=list_to_binary("method undifine")},Req})
                        end;
                    _ ->
                        http_response({#success{success=false,entity=list_to_binary("method undifine")},Req})
                end
        end
    catch
        C:Reason ->
            ?ERROR_MSG("aa_http c=~p ; reason=~p",[C,Reason]),
            http_response({#success{success=false,entity=list_to_binary("bad argrment")},Req})
    end.
%% 	gen_server:call(?MODULE,{handle_http,Req}).

http_response({S,Req}) ->
    Res = {obj,[{success,S#success.success},{entity,S#success.entity}]},
    ?DEBUG("##### http_response ::::> S=~p",[Res]),
    J = rfc4627:encode(Res),
    ?DEBUG("##### http_response ::::> J=~p",[J]),
    Req:ok([{"Content-Type", "text/json"}], "~s", [J]).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    misultin:start_link([{port, ?Port}, {loop, fun(Req) -> handle_http(Req) end}]),
    {ok, #state{}}.

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

%%%===================================================================
%%% Internal functions
%%%===================================================================


get_group_info(Body) ->
    {ok,Obj,_Re} = rfc4627:decode(Body),
    {ok,Params} = rfc4627:get_field(Obj,"params"),
    Params.



encode_msg_to_json(Message)->
    {xmlelement, Name, Attrs, Els} = element(5,Message),
    From = element(3,Message),
    To = element(4,Message),
    Attrs1 = jlib:replace_from_to_attrs(jlib:jid_to_string(From),
                                        jlib:jid_to_string(To),
                                        Attrs),
    FixedPacket = {xmlelement, Name, Attrs1, Els},
    Text = xml:element_to_binary(FixedPacket),
    {obj,[{item, Text}]}.