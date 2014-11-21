%% @author songzhiming
%% @doc @todo Add description to aa_msg_statistic.


-module(aa_msg_statistic).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0,
		 add/0,
		 del/0,
		 info/0]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add() ->
	gen_server:cast(?MODULE, add).

del() ->
	gen_server:cast(?MODULE, del).


info() ->
	gen_server:call(?MODULE, info).



%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {total = 0, total_del = 0, total_today = 0, total_today_del = 0}).


init([]) ->
	{M, S, _} = erlang:now(),
    Stamp = M * 1000000 + S,
	SecondsToNexHour = 3600 - Stamp rem 3600,
	erlang:send_after((SecondsToNexHour + 1) * 1000, self(), check_new_day),
    {ok, #state{}}.


handle_call(info, _From, State) ->
	{reply, {ok, {State#state.total,
				  State#state.total_del,
				  State#state.total_today,
				  State#state.total_today_del}}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast(add, #state{total = Total, total_today = TotalToday} = State) ->
	{noreply, State#state{total = Total + 1, total_today = TotalToday + 1}};
handle_cast(del, #state{total_del = Total, total_today_del = TotalToday} = State) ->
	{noreply, State#state{total_del = Total + 1, total_today_del = TotalToday + 1}};
handle_cast(_Msg, State) ->
    {noreply, State}.



handle_info(check_new_day, State) ->
	erlang:send_after(3600000, self(), check_new_day),
	{Hour, _, _} = erlang:time(),
	if Hour == 0 ->
		   {noreply, State#state{total_today = 0,
								 total_today_del = 0}};
	   true ->
		   {noreply, State}
	end;
handle_info(_Info, State) ->
    {noreply, State}.



terminate(_Reason, _State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================


