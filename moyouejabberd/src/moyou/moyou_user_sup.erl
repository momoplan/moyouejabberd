-module(moyou_user_sup).

-behaviour(supervisor).

-export([start_link/0,
         init/1]).


-record(moyou_user, {id, session_list, resv1, resv2, resv3}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    ChildSpec = {undefined,
                 {moyou_user, start_link, []},
                 transient,
                 5000,
                 worker,
                 [moyou_user]},
    ets:new(ets_moyou_user, [named_table, public, set]),
    moyou_util:create_or_copy_table(moyou_user_tab, [{record_name, moyou_user},
                                                     {attributes, record_info(fields, moyou_user)},
                                                     {ram_copies, [node()]}], ram_copies),
    {ok, {{simple_one_for_one, 10, 3600}, [ChildSpec]}}.
