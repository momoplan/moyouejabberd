-module(moyou_http_service).

-behaviour(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").

%% API
-export([start_link/0]).

-define(Port, 5380).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         stop/0]).


-record(state, {}).


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
        Args = http_args(Req),
        if
            Args == [] ->
                http_response(false, list_to_binary("empty argrment"), Req);
            true ->
                [{"body", Body}] = Args,
                {ok, Obj, _Re} = rfc4627:decode(Body),
                {ok, M} = rfc4627:get_field(Obj, "method"),
                {Success, Entity} = moyou_http_business:business(binary_to_list(M), Obj),
                http_response(Success, Entity, Req)
        end
    catch
        C:Reason ->
            ?ERROR_MSG("moyou_http_service c=~p~n ; reason=~p~n, Args : ~p~n",[C, Reason, http_args(Req)]),
            http_response(false, list_to_binary("bad argrment"), Req)
    end.

http_args(Req) ->
    Method = Req:get(method),
    case Method of
        'GET' ->
            Req:parse_qs();
        'POST' ->
            Req:parse_post();
        'HEAD' ->
            []
    end.


http_response(Success, Entity, Req) ->
    Res = {obj,[{success, Success}, {entity, Entity}]},
    J = rfc4627:encode(Res),
    Req:ok([{"Content-Type", "text/json"}], "~s", [J]).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
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