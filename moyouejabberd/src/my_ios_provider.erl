-module(my_ios_provider).

-behaviour(gen_server).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-export([start/3,
         push/3]).


-export([start/0,
         push_test/1]).


-include("ejabberd.hrl").


-record(payload, {alert,
                  badge,
                  sound}).


-record(state, {host,
                certfile,
                keyfile,
                socket}).

%% hosts and ports of ios push service.
-define(IOS_PUSH_PRODUCTION, "gateway.push.apple.com").

-define(IOS_PUSH_DEVELOPMENT, "gateway.sandbox.push.apple.com").

-define(IOS_PUSH_PORT, 2195).

-define(TIMEOUT, 5*1000).



start(CertFile, KeyFile, Host) ->
    gen_server:start(?MODULE, [CertFile, KeyFile, Host], []).


push(Pid, Tokens, Payload) ->
    gen_server:cast(Pid, {push, Tokens, Payload}).


%% ----------------------------------------------------
%% gen_server callbacks.
%% ----------------------------------------------------
init([CertFile, KeyFile, Host]) ->
    process_flag(trap_exit, true),
    case create_socket(CertFile, KeyFile, Host) of
        {ok, Socket} ->
            {ok, #state{host = Host,
                        certfile = CertFile,
                        keyfile = KeyFile,
                        socket = Socket}};
        Crap ->
            ?ERROR_MSG("failed to create socket to apns for reason :~p~n", [Crap]),
            {ok, #state{host = Host,
                        certfile = CertFile,
                        keyfile = KeyFile,
                        socket = undefined}}
    end.

create_socket(CertFile, KeyFile, Host)  ->
    Options =  [{certfile, CertFile},
                {keyfile, KeyFile},
                {mode, binary}],
    case ssl:connect(Host, ?IOS_PUSH_PORT, Options, infinity) of
        {ok, _Socket}  = Tuple ->
            Tuple;
        {error, Reason} ->
            {error, Reason}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({push, Token, Payload}, #state{socket = undefined,
                                           host = Host,
                                           certfile = CertFile,
                                           keyfile = KeyFile} = State) ->
    case create_socket(CertFile, KeyFile, Host) of
        {error, Reason} ->
            ?ERROR_MSG("lost push message : ~p, create socket error reason : ~p~n", [Payload, Reason]),
            {noreply, State};
        {ok, Socket}  ->
            case send(Socket, Token, Payload) of
                ok ->
                    {noreply, State#state{socket = Socket}};
                {error, Reason} ->
                    ssl:close(Socket),
                    ?ERROR_MSG("lost push message : ~p, send error reason : ~p~n", [Payload, Reason]),
                    {noreply, State#state{socket = undefined}};
                Error ->
                    ssl:close(Socket),
                    ?ERROR_MSG("lost push message : ~p, send unknow error reason : ~p~n", [Payload, Error]),
                    {noreply, State#state{socket = undefined}}
            end
    end;

handle_cast({push, Token, Payload}, #state{socket  = Socket} = State) ->
    case send(Socket, Token, Payload) of
        ok ->
            {noreply, State};
        {error, Reason} ->
            ssl:close(Socket),
            ?ERROR_MSG("lost push message : ~p, send error reason : ~p~n", [Payload, Reason]),
            {noreply, State#state{socket = undefined}};
        Error ->
            ssl:close(Socket),
            ?ERROR_MSG("lost push message : ~p, send unknow error reason : ~p~n", [Payload, Error]),
            {noreply, State#state{socket = undefined}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, _}, #state{socket = undefined} = State) ->
    {noreply, State};
handle_info({tcp_closed, _}, #state{socket = Socket} = State) ->
    ssl:close(Socket),
    {noreply, State#state{socket = undefined}};
handle_info({ssl_closed, _}, #state{socket = undefined} = State) ->
    {noreply, State};
handle_info({ssl_closed, _}, #state{socket = Socket} = State) ->
    ssl:close(Socket),
    {noreply, State#state{socket = undefined}};

handle_info(stop, State) ->
    ?ERROR_MSG("ios provider#~p received `stop` message.~n", [self()]),
    {stop, provider_stop, State};

handle_info({'EXIT', _From, Reason}, State) ->
    ?ERROR_MSG("ios provider#~p received `exit` message. Reason : ~p~n", [self(), Reason]),
    {stop, provider_exit, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason,  _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%% ----------------------------------------------------
%% Internal. functions.
%% -----------------------------------------------------
send(Socket, Token, Payload) ->
    do_send(Socket, Token, Payload).

do_send(_Socket, Token, _Payload) when is_list(Token), length(Token)=/=64 ->
    ?ERROR_MSG("Invalid token : ~p~n", [Token]);
do_send(Socket, Token, Payload) ->
    Packet = pack(Token, Payload),
    ssl:send(Socket, Packet).
    

pack(Token, Payload) ->
    TokenBits = if is_list(Token) ->
                        hex_str_to_binary(Token);
                    true ->
                        Token
                end,
    Json = payload_to_json(Payload),
    PayloadLen = size(Json),
    <<0:8, 32:16/big, TokenBits/bits, PayloadLen:16/big, Json/bits>>.


hex_str_to_binary(HexStr) ->
    do_hex_str_to_bin(HexStr, []).

do_hex_str_to_bin([], L) ->
    list_to_binary(lists:reverse(L));
do_hex_str_to_bin([C1,C2|Rest], L) ->
    Int = list_to_integer([C1,C2], 16),
    do_hex_str_to_bin(Rest, [Int|L]).

payload_to_json(Payload) ->
    ApsList = [{"alert", Payload#payload.alert},
               {"badge", Payload#payload.badge},
               {"sound", Payload#payload.sound}],
    FoldFun = fun({_, undefined}, Acc) ->
                      Acc;
                  ({Key, List}, Acc) when is_list(List) ->
                      [{Key, list_to_binary(List)} | Acc];
                  (Other, Acc) ->
                      [Other | Acc]
              end,
    PayloadJson = {obj, [{"aps", {obj, lists:foldl(FoldFun, [], ApsList)}}]},
    list_to_binary(rfc4627:encode(PayloadJson)).


%% tests.

push_test(Pid) ->
    Token = "602682db21c3f72a0a4af415eac20a1ca602189b0f840dbb4170a1d9b04dd62d",
    Payload = #payload{alert = "123456:[发怒]",
                       badge = 1,
                       sound = "default"},
    push(Pid, Token, Payload).


start() ->
    CertFile = "/etc/ejabberd/key/moyou_dev_cert.pem",
    KeyFile = "/etc/ejabberd/key/moyou_dev_key.pem",
    Host = "gateway.sandbox.push.apple.com",
    start(CertFile, KeyFile, Host).