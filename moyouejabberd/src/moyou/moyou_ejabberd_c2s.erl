%%%----------------------------------------------------------------------
%%% File    : moyou_ejabberd_c2s.erl
%%% Author  : chen.kangmin
%%% Purpose : Moyou Serve C2S connection
%%%----------------------------------------------------------------------

-module(moyou_ejabberd_c2s).
-author('chen.kangmin').


-behaviour(gen_server).

%% External exports
-export([start/2,
	 socket_type/0]).

-export([init/1,
         start_link/2,
	 code_change/3,
	 handle_info/2,
         handle_call/3,
         handle_cast/2,
	 terminate/2
        ]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("mod_privacy.hrl").
-include("moyou_pb.hrl").
%% pres_a contains all the presence available send (either through roster mechanism or directed).
%% Directed presence unavailable remove user from pres_a.
-record(state, {socket,
		sockmod,
		streamid,
                loop_pid,
		jid,
		user = "",
                server = ?MYNAME,
                resource = "",
		sid,
		ip,
                access,
                authenticated = false,
                data = <<>>,
                size = 0,
                type = 0,
                bound}).


%% This is the timeout to apply between event when starting a new
%% session:
-define(C2S_OPEN_TIMEOUT, 60000).
-define(C2S_HIBERNATE_TIMEOUT, 90000).
-define(INTERVAL, 300).

-define(STREAM_HEADER,
	"<?xml version='1.0'?>"
            "<stream:stream xmlns='jabber:client' "
            "xmlns:stream='http://etherx.jabber.org/streams' "
            "id='~s' from='~s'~s~s>"
       ).

-define(STREAM_TRAILER, "</stream:stream>").
-define(HIBERNATE_TIMEOUT, 90000).
-define(INVALID_NS_ERR, ?SERR_INVALID_NAMESPACE).
-define(INVALID_XML_ERR, ?SERR_XML_NOT_WELL_FORMED).
-define(HOST_UNKNOWN_ERR, ?SERR_HOST_UNKNOWN).
-define(POLICY_VIOLATION_ERR(Lang, Text),
	?SERRT_POLICY_VIOLATION(Lang, Text)).
-define(INVALID_FROM, ?SERR_INVALID_FROM).


%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
start(SockData, Opts) ->
    supervisor:start_child(moyou_ejabberd_c2s_sup,
                           [SockData, Opts]).


start_link(SockData, Opts) ->
    gen_server:start_link(?MODULE, [SockData, Opts], []).


socket_type() ->
    raw.

%%%----------------------------------------------------------------------
%%% Callback functions from gen_fsm
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, StateData}          |
%%          {ok, StateName, StateData, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}
%%----------------------------------------------------------------------
init([{SockMod, Socket}, Opts]) ->
    Access = case lists:keysearch(access, 1, Opts) of
		 {value, {_, A}} -> A;
		 _ -> all
             end,
    IP = peerip(SockMod, Socket),
    %% Check if IP is blacklisted:
    case is_ip_blacklisted(IP) of
        true ->
            ?INFO_MSG("Connection attempt from blacklisted IP: ~s (~w)",
                      [jlib:ip_to_list(IP), IP]),
            {stop, normal};
        false ->
            Parent = self(),
            Pid = spawn(fun() -> loop(Parent) end),
            State = #state{socket         = Socket,
                           sockmod        = SockMod,
                           loop_pid   = Pid,
                           streamid       = new_id(),
                           access         = Access,
                           ip             = IP},
            activate_socket(State),
	    {ok, State, ?C2S_OPEN_TIMEOUT}
    end.
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(closed, State) ->
    {stop, normal, State};
handle_cast({update, State1}, State) ->
    State2 = State#state{authenticated = State1#state.authenticated,
                         user = State1#state.user,
                         resource = State1#state.resource,
                         server = State1#state.server,
                         jid = State1#state.jid,
                         sid = State1#state.sid},
    {noreply, State2, ?HIBERNATE_TIMEOUT};
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({route, From, To, Packet}, State)->
    {xmlelement, _Name, Attrs, Els} = Packet,
    Attrs2 = jlib:replace_from_to_attrs(jlib:jid_to_string(From),
                                        jlib:jid_to_string(To),
                                        Attrs),
    MsgRecord = #msg{msg_id   = xml:get_attr_s("id", Attrs2),
                     server_id  = xml:get_attr_s("server_id", Attrs2),
                     from = xml:get_attr_s("from", Attrs2),
                     to_user = xml:get_attr_s("to", Attrs2),
                     msg_type = xml:get_attr_s("msgtype", Attrs2),
                     group_id = xml:get_attr_s("groupid", Attrs2),
                     msg_time = xml:get_attr_s("msgTime", Attrs2),
                     serial_num = xml:get_attr_s("serialNumber", Attrs2),
                     number = xml:get_attr_s("number", Attrs2),
                     body = get_msg_body({xmlelement, "message", [], Els}),
                     payload = get_msg_payload({xmlelement, "message", [], Els})
                    },
    send_response(moyou_pb:encode_msg(MsgRecord), 3, State),
    {noreply, State, ?HIBERNATE_TIMEOUT};
 

handle_info({tcp, _TCPSocket, Data}, State) ->
    OldBuff = State#state.data,
    OldSize = State#state.size,
    State1 = State#state{data = <<OldBuff/binary, Data/binary>>, size = OldSize + size(Data)},
    try
        State2 = parse_data(State1),
        activate_socket(State2),
        {noreply, State2, ?HIBERNATE_TIMEOUT}
    catch
        Type:Err ->
            ?ERROR_MSG("parse_data error type : ~p, error : ~p, state : ~p~n", [Type, Err, State1]),
            {stop, normal, State}
    end;
handle_info({tcp_closed, _TCPSocket}, State) ->
    {stop, normal, State};
handle_info({tcp_error, _TCPSocket, Reason}, State) ->
    case Reason of
	timeout ->
	    {noreply, State, ?HIBERNATE_TIMEOUT};
	_ ->
	    {stop, normal, State}
    end;
handle_info({loop_error, _Reason}, State) ->
    {stop, normal, State};
handle_info(timeout, State) ->
    proc_lib:hibernate(gen_server, enter_loop, [?MODULE, [], State]),
    {noreply, State, ?HIBERNATE_TIMEOUT};
handle_info(_Info, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    ejabberd_sm:close_session(State#state.sid,
                              State#state.user,
                              State#state.server,
                              State#state.resource),
                              catch (State#state.sockmod):close(State#state.socket),
    case is_process_alive(State#state.loop_pid) of
        true ->
            State#state.loop_pid ! stop;
        _ ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


new_id() ->
    randoms:get_string().


activate_socket(#state{socket = Socket, sockmod = SockMod} ) ->
    PeerName =  case SockMod of
                    gen_tcp ->
                        inet:setopts(Socket, [{active, once}]),
                        inet:peername(Socket);
                    _ ->
                        SockMod:setopts(Socket, [{active, once}]),
                        SockMod:peername(Socket)
                end,
    case PeerName of
        {error, _Reason} ->
            self() ! {tcp_closed, Socket};
        {ok, _} ->
            ok
    end.


peerip(SockMod, Socket) ->
    IP = case SockMod of
	     gen_tcp -> inet:peername(Socket);
	     _ -> SockMod:peername(Socket)
	 end,
    case IP of
	{ok, IPOK} -> IPOK;
	_ -> undefined
    end.


is_ip_blacklisted(undefined) ->
    false;
is_ip_blacklisted({IP,_Port}) ->
    ejabberd_hooks:run_fold(check_bl_c2s, false, [IP]).


parse_data(State) when State#state.type =:= 0 andalso State#state.size >= 10 ->
    Data = State#state.data,
    Size = State#state.size - 10,
    <<BoundBin:10/binary, Buff:Size/binary>> = Data,
    Bound = moyou_pb:decode_bound(BoundBin),
    if
        Size >= Bound#bound.size ->
            Size1 = Bound#bound.size,
            Size2 = Size - Size1,
            <<BusinessBin:Size1/binary, Buff1:Size2/binary>> = Buff,
            Record = decode_proto(BusinessBin, Bound#bound.type),
            State#state.loop_pid ! {business, Record, State},
            State1 = State#state{data = Buff1, size = Size2, bound = undefined, type = 0},
            parse_data(State1);
        true ->
            State#state{data = Buff, size = Size, bound = Bound, type = 1}
    end;
parse_data(State) when State#state.type =:= 0 ->
    State;
parse_data(State) ->
    Bound = State#state.bound,
    if
        State#state.size >= Bound#bound.size ->
            Data = State#state.data,
            Size1 = Bound#bound.size,
            Size = State#state.size - Size1,
            <<ProtoBin:Size1/binary, Buff:Size/binary>> = Data,
            Record = decode_proto(ProtoBin, Bound#bound.type),
            State#state.loop_pid ! {business, Record, State},
            State2 = State#state{data = Buff, size = Size, bound = undefined, type = 0},
            parse_data(State2);
        true ->
            State
    end.


decode_proto(ProtoBin, 1) ->
    moyou_pb:decode_login(ProtoBin);
decode_proto(ProtoBin, 3) ->
    moyou_pb:decode_msg(ProtoBin);
decode_proto(ProtoBin, 4) ->
    moyou_pb:decode_heart(ProtoBin);

decode_proto(_ProtoBin, _) ->
    throw("unknow proto type").

business(Record, State, Parent) when is_record(Record, login) ->
    login(Record, State, Parent);
business(Record, State, _Parent) when is_record(Record, msg) ->
    message(Record, State);
business(Record, State, _Parent) when is_record(Record, heart) ->
    heart(Record, State);

business(_Record, State, _Parent) ->
    State.

heart(Record, State) ->
    send_response(moyou_pb:encode_heart(Record), 4, State),
    State.


message(Record, State) when State#state.authenticated ->
    User = State#state.user,
    Server = State#state.server,
    FromJID = State#state.jid,
    To = Record#msg.to_user,
    ToJID = case To of
                "" ->
                    jlib:make_jid(User, Server, "");
                _ ->
                    jlib:string_to_jid(To)
            end,
    Attrs = [{"id", Record#msg.msg_id},
             {"server_id", Record#msg.server_id},
             {"from", Record#msg.from},
             {"to", Record#msg.to_user},
             {"msgtype", Record#msg.msg_type},
             {"groupid", Record#msg.group_id},
             {"msgTime", Record#msg.msg_time},
             {"serialNumber", Record#msg.serial_num},
             {"number", Record#msg.number}],
    Body = Record#msg.body,
    Payload = Record#msg.payload,
    Els = case Payload of
              undefined->
                  [{xmlelement, "body", [], [{xmlcdata, list_to_binary(Body)}]}];
              "" ->
                  [{xmlelement, "body", [], [{xmlcdata, list_to_binary(Body)}]}];
              _->
                  [{xmlelement, "body", [], [{xmlcdata, list_to_binary(Body)}]},
                   {xmlelement, "payload", [], [{xmlcdata, list_to_binary(Payload)}]}]
          end,
    Packet ={xmlelement, "message", Attrs, Els},
    ejabberd_hooks:run(user_send_packet,
                       Server,
                       [FromJID, ToJID, Packet]),
    State;
message(_Record, _State) ->
    throw("no login").
            

login(Record, State, Parent) ->
    {LoginRecord, State1} = case ejabberd_auth_external:check_password_extauth(Record#login.user,
                                                                               Record#login.server,
                                                                               Record#login.password) of
                                true ->
                                    R = case jlib:resourceprep(Record#login.resource) of
                                            error -> error;
                                            "" ->
                                                lists:concat(
                                                    [randoms:get_string() | tuple_to_list(now())]);
                                            Res -> Res
                                        end,
                                    case R of
                                        error ->
                                            {pack_login_response(1, [], ""), State};
                                        _ ->
                                            JID = jlib:make_jid(Record#login.user, Record#login.server, R),
                                            case acl:match_rule(Record#login.server, State#state.access, JID) of
                                                allow ->
                                                    ?INFO_MSG("(~w) Opened session for ~s", [State#state.socket, jlib:jid_to_string(JID)]),

                                                    SID = {now(), Parent},
                                                    Info = [{ip, State#state.ip}],
                                                    ejabberd_sm:open_session(
                                                        SID, Record#login.user, Record#login.server, R, Info),
                                                    NewStateData =
                                                        State#state{
                                                        authenticated = true,
                                                        user = Record#login.user,
                                                        resource = R,
                                                        server = Record#login.server,
                                                        jid = JID,
                                                        sid = SID},
                                                    {pack_login_response(0, get_offline_message(JID,NewStateData), jlib:jid_to_string(JID)), NewStateData};
                                                _ ->
                                                    ?INFO_MSG("(~w) Forbidden session for ~s", [State#state.socket, jlib:jid_to_string(JID)]),
                                                    {pack_login_response(1, [], ""), State}
                                            end
                                    end;
                                _ ->
                                    ?INFO_MSG("(~w) Failed authentication for ~s@~s from IP ~s (~w)", [State#state.socket, Record#login.user, Record#login.server, jlib:ip_to_list(State#state.ip), State#state.ip]),
                                    {pack_login_response(1, [], ""), State}
                            end,
    send_response(moyou_pb:encode_offlinemsg(LoginRecord), 2, State1),
    State1.

get_offline_message(Jid, State) when State#state.server =:= "gamepro.com" ->
    {Messages, _OldMessages} = moyou_rpc_util:get_offline_msg(Jid#jid.user),
    pack_get_offline_response(Messages);
get_offline_message(_Record, _State) ->
    [].

pack_get_offline_response([]) ->
    [];
pack_get_offline_response(Messages) ->
    [begin
         {xmlelement, _Name, Attrs, Els} =  element(6, Message),
         #msg{msg_id   = xml:get_attr_s("id", Attrs),
              server_id   = xml:get_attr_s("server_id", Attrs),
              from = xml:get_attr_s("from", Attrs),
              to_user = xml:get_attr_s("to", Attrs),
              msg_type = xml:get_attr_s("msgtype", Attrs),
              group_id = xml:get_attr_s("groupid", Attrs),
              msg_time = xml:get_attr_s("msgTime", Attrs),
              serial_num = xml:get_attr_s("serialNumber", Attrs),
              number = xml:get_attr_s("number", Attrs),
              body =get_msg_body({xmlelement, "message", [], Els}),
              payload = get_msg_payload({xmlelement, "message", [], Els})
             }
     end|| Message <- Messages].

    
get_msg_body({xmlelement, "message", _, Els}) ->
    fetch_cdata(Els, "body").

get_msg_payload({xmlelement, "message", _, Els}) ->
    fetch_cdata(Els, "payload").

fetch_cdata([Element | Message], Name) ->
    case Element of
        {xmlelement, Name, _, CDATAList} ->
            parse_cdata(CDATAList);
        _ ->
            fetch_cdata(Message, Name)
    end;
fetch_cdata([], _) ->
    "".

parse_cdata([]) ->
    "";
parse_cdata(CDATAList) ->
    Data = lists:foldl(fun({xmlcdata, Bin}, Acc) ->
                               <<Acc/binary, Bin/binary>>
                       end, <<>>, CDATAList),
    binary_to_list(Data).

send_response(Bin, Type, State) ->
    BoundBin = moyou_pb:encode_bound(#bound{type = Type, size = size(Bin)}),
    (State#state.sockmod):send(State#state.socket, <<BoundBin/binary, Bin/binary>>).


pack_login_response(Status, Msg, Jid) ->
    #offlinemsg{status = Status, msg = Msg, jid = Jid}.


loop(Parent) ->
    try
        receive
            {business, Record, State} ->
                State1 = business(Record, State, Parent),
                gen_server:cast(Parent, {update, State1}),
                loop(Parent);
            stop ->
                ok;
            _ ->
                loop(Parent)
        end
    catch
        Type:Err ->
            ?ERROR_MSG("loop error type : ~p, error : ~p~n", [Type, Err]),
            gen_server:cast(Parent, close)
    end.
