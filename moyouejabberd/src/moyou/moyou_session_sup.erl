-module(moyou_session_sup).

-behaviour(supervisor).

-export([start_link/0,
         init/1]).


-record(moyou_message, {id, session_id, from, mt, packet, time, resv1, resv2, resv3}).

-record(moyou_session_seq, {session_id, seq = 0, resv1, resv2, resv3}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    ChildSpec = {undefined,
                 {moyou_session, start_link, []},
                 transient,
                 10000,
                 worker,
                 [moyou_session]},
    ets:new(ets_moyou_session, [named_table, public, set]),
    ets:new(ets_cid_and_sid, [named_table, public, set]),
    moyou_util:create_or_copy_table(moyou_message_tab, [{record_name, moyou_message},
                                                        {attributes, record_info(fields, moyou_message)},
                                                        {ram_copies, [node()]}], ram_copies),
    moyou_util:create_or_copy_table(moyou_session_seq_tab, [{record_name, moyou_session_seq},
                                                            {attributes, record_info(fields, moyou_session_seq)},
                                                            {ram_copies, [node()]}], ram_copies),
    {ok, {{simple_one_for_one, 10, 3600}, [ChildSpec]}}.
