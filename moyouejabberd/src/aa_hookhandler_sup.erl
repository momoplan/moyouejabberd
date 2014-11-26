-module(aa_hookhandler_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_user_msg_handler/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_user_msg_handler() ->
	supervisor:start_child(?MODULE, {aa_usermsg_handler, {aa_usermsg_handler, start_link, []}, permanent, 3000, worker, [aa_usermsg_handler]}).

init([]) ->
	AAHookhandler ={ aa_hookhandler,{aa_hookhandler, start_link, []}, permanent, brutal_kill, worker, [aa_hookhandler] },
	AAOfflineMod ={ aa_offline_mod,{aa_offline_mod, start_link, []}, permanent, brutal_kill, worker, [aa_offline_mod] },
	AAGroupChatSup ={ aa_group_chat_sup,{aa_group_chat_sup, start_link, []}, temporary, brutal_kill, supervisor, [aa_group_chat_sup] },
	AAUserMsgPisSup ={my_usermsg_pid_sup, {my_usermsg_pid_sup, start_link, []},  permanent,infinity, supervisor, [my_usermsg_pid_sup]},
	AAMsgStastic ={aa_msg_statistic, {aa_msg_statistic, start_link, []}, permanent, 3000, worker, [aa_msg_statistic]},
	AAMsgCleaner ={my_msg_cleaner, {my_msg_cleaner, start_link, []}, permanent, 3000, worker, [my_msg_cleaner]},
	{ok, {{one_for_one, 5, 10}, [AAHookhandler,AAOfflineMod,AAGroupChatSup, AAUserMsgPisSup, AAMsgStastic, AAMsgCleaner]}}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
