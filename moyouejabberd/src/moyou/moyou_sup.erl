-module(moyou_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    ssl:start(),
    inets:start(),
    timer:start(),
    ServiceList = case lists:member(node(), moyou_util:get_config(moyou_thrift_service)) of
                      true ->
                          [{aa_inf_server,
                            {aa_inf_server, start, []},
                            permanent,
                            5000,
                            worker,
                            [aa_inf_server]}];
                      _ ->
                          []
                  end,
    ServiceList1 = case lists:member(node(), moyou_util:get_config(moyou_push_service)) of
                       true ->
                           [{moyou_push_service,
                             {moyou_push_service, start_link, []},
                             permanent,
                             5000,
                             worker,
                             [moyou_push_service]} | ServiceList];
                       _ ->
                           ServiceList
                   end,
    ServiceList2 = case lists:member(node(), moyou_util:get_config(moyou_http_service)) of
                       true ->
                           [{moyou_http_service,
                             {moyou_http_service, start_link, []},
                             permanent,
                             5000,
                             worker,
                             [moyou_http_service]} | ServiceList1];
                       _ ->
                           ServiceList1
                   end,
    ServiceList3 = case lists:member(node(), moyou_util:get_config(moyou_session_service)) of
                       true ->
                           [{moyou_session_sup,
                             {moyou_session_sup, start_link, []},
                             permanent,
                             infinity,
                             supervisor,
                             [moyou_session_sup]},
                            {moyou_user_sup,
                             {moyou_user_sup, start_link, []},
                             permanent,
                             infinity,
                             supervisor,
                             [moyou_user_sup]},
                            {moyou_dump_service,
                             {moyou_dump_service, start_link, []},
                             permanent,
                             5000,
                             worker,
                             [moyou_dump_service]}| ServiceList2];
                       _ ->
                           ServiceList2
                   end,
    {ok, {{one_for_one, 5, 10},
          ServiceList3}}.
