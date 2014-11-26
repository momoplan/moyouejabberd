%% @author songzhiming
%% @doc @todo Add description to my_usermsg_pid_sup.


-module(my_usermsg_pid_sup).
-behaviour(supervisor).
-export([init/1]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%% ====================================================================
%% Behavioural functions 
%% ====================================================================

init([]) ->
    {ok,{{one_for_all,0,1}, []}}.

%% ====================================================================
%% Internal functions
%% ====================================================================


