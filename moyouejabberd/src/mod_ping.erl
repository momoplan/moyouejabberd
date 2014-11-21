%%%----------------------------------------------------------------------
%%% File    : mod_ping.erl
%%% Author  : Brian Cully <bjc@kublai.com>
%%% Purpose : Support XEP-0199 XMPP Ping and periodic keepalives
%%% Created : 11 Jul 2009 by Brian Cully <bjc@kublai.com>
%%%
%%%
%%% ejabberd, Copyright (C) 2002-2013   ProcessOne
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License
%%% along with this program; if not, write to the Free Software
%%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
%%% 02111-1307 USA
%%%
%%%----------------------------------------------------------------------

-module(mod_ping).
-author('bjc@kublai.com').

-behavior(gen_mod).
-behavior(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").

-define(SUPERVISOR, ejabberd_sup).
-define(NS_PING, "urn:xmpp:ping").
-define(DEFAULT_SEND_PINGS, false). % bool()
-define(DEFAULT_PING_INTERVAL, 60). % seconds

-define(DICT, dict).

%% API
-export([start_link/2, start_ping/2, stop_ping/2]).

%% gen_mod callbacks
-export([start/2, stop/1]).

%% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2,
         handle_info/2, code_change/3]).

%% Hook callbacks
-export([iq_ping/3, user_online/3, user_offline/3, user_send/3]).

-record(state, {host = "",
                send_pings = ?DEFAULT_SEND_PINGS,
                ping_interval = ?DEFAULT_PING_INTERVAL,
				timeout_action = none,
                timers = ?DICT:new()}).

%%====================================================================
%% API
%%====================================================================
start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

%% 当用户上线和发消息时，会调用这个方法，发出 ping 请求
start_ping(Host, JID) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:cast(Proc, {start_ping, JID}).

stop_ping(Host, JID) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:cast(Proc, {stop_ping, JID}).

%%====================================================================
%% gen_mod callbacks
%%====================================================================
start(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    PingSpec = {Proc, {?MODULE, start_link, [Host, Opts]},transient, 2000, worker, [?MODULE]},
    supervisor:start_child(?SUPERVISOR, PingSpec).

stop(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:call(Proc, stop),
    supervisor:delete_child(?SUPERVISOR, Proc).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Host, Opts]) ->
	
	%% 如果这个选项设为 true, 服务器在一个给定的间隔时间 ping_interval发送 pings 给已连接的不活跃的客户端. 
	%% 这对于保持活着的客户端连接或检查可用性是有用的. 缺省这个选项是禁止的.
    SendPings = gen_mod:get_opt(send_pings, Opts, ?DEFAULT_SEND_PINGS),
    
	%% 多久发送一次 ping 给已连接的用户, 如果前一个选项被激活. 如果一个客户端连接在这个间隔内没有发送或接收任何节, 
	%% 一个 ping 请求被发送给客户端. 缺省值为60秒.
	PingInterval = gen_mod:get_opt(ping_interval, Opts, ?DEFAULT_PING_INTERVAL),
    
	%% 当一个客户端在32秒内不回答一个服务器的ping请求时，服务器做什么. 缺省是什么也不做.
	%% {timeout_action, none|kill}
	TimeoutAction = gen_mod:get_opt(timeout_action, Opts, none),
    
	IQDisc = gen_mod:get_opt(iqdisc, Opts, no_queue),
    mod_disco:register_feature(Host, ?NS_PING),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_PING, ?MODULE, iq_ping, IQDisc),
    gen_iq_handler:add_iq_handler(ejabberd_local, Host, ?NS_PING, ?MODULE, iq_ping, IQDisc),
	
    case SendPings of
    	true ->
	    	%% Ping requests are sent to all entities, whether they
	    	%% announce 'urn:xmpp:ping' in their caps or not
            ejabberd_hooks:add(sm_register_connection_hook, Host, ?MODULE, user_online, 100),
            ejabberd_hooks:add(sm_remove_connection_hook, Host, ?MODULE, user_offline, 100),
	    	ejabberd_hooks:add(user_send_packet, Host, ?MODULE, user_send, 100);
        _ ->
            ok
    end,
    {ok, #state{ host = Host, 
				 send_pings = SendPings, 
				 ping_interval = PingInterval,
				 timeout_action = TimeoutAction,
                 timers = ?DICT:new() }
	}.

terminate(_Reason, #state{host = Host}) ->
    ejabberd_hooks:delete(sm_remove_connection_hook, Host,?MODULE, user_offline, 100),
    ejabberd_hooks:delete(sm_register_connection_hook, Host,?MODULE, user_online, 100),
    ejabberd_hooks:delete(user_send_packet, Host,?MODULE, user_send, 100),
    gen_iq_handler:remove_iq_handler(ejabberd_local, Host, ?NS_PING),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_PING),
    mod_disco:unregister_feature(Host, ?NS_PING).

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badarg}, State}.

handle_cast({start_ping, JID}, State) ->
    Timers = add_timer(JID, State#state.ping_interval, State#state.timers),
    {noreply, State#state{timers = Timers}};
handle_cast({stop_ping, JID}, State) ->
    Timers = del_timer(JID, State#state.timers),
    {noreply, State#state{timers = Timers}};
handle_cast({iq_pong, JID, timeout}, State) ->
	%% 这个逻辑看来，貌似是得不到响应才会出发回调呢 ????
	%% XXX : IQ消息默认是32秒的超时时间，区间内得不到响应，就会调用超时的函数
	%% ?DEBUG("[iq_pong]====>  JID=~p ; State=~p",[JID,State]),
    Timers = del_timer(JID, State#state.timers),
    ejabberd_hooks:run(user_ping_timeout, State#state.host, [JID]),
    case State#state.timeout_action of
		kill ->
		    #jid{user = User, server = Server, resource = Resource} = JID,
		    case ejabberd_sm:get_session_pid(User, Server, Resource) of
				Pid when is_pid(Pid) ->
				    ejabberd_c2s:stop(Pid);
				_ ->
				    ok
		    end;
		_ ->
		    ok
    end,
    {noreply, State#state{timers = Timers}};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% 这个方法，被 add_timer 激活，因为里面有 erlang:start_timer 的调用
%% start_timer 自动的在超时后, 要发送消息前, 在消息里面添加了{timeout, TimerRef, Msg}, 达到识别的目的.
handle_info({timeout, _TRef, {ping, JID}}, State) ->
	%% 构造了一个 IQ 的 ping 消息
    IQ = #iq{type = get, sub_el = [{xmlelement, "ping", [{"xmlns", ?NS_PING}], []}]},
    Pid = self(),
	%% TODO : 这里有一个疑问，到底是得到响应出发回调，还是得不到响应才触发回调？？？
    F = fun(Response) ->
		gen_server:cast(Pid, {iq_pong, JID, Response})
	end,
	%% 把一个 IQ 消息路由出去
    From = jlib:make_jid("", State#state.host, ""),
    ejabberd_local:route_iq(From, JID, IQ, F),
	%% 这构成了一个循环，本次 ping 操作结束了，在这里开启下一次 ping 的计时器
	Timers = add_timer(JID, State#state.ping_interval, State#state.timers),
    {noreply, State#state{timers = Timers}};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Hook callbacks
%%====================================================================
iq_ping(_From, _To, #iq{type = Type, sub_el = SubEl} = IQ) ->
    case {Type, SubEl} of
        {get, {xmlelement, "ping", _, _}} ->
            IQ#iq{type = result, sub_el = []};
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_FEATURE_NOT_IMPLEMENTED]}
    end.

user_online(_SID, JID, _Info) ->
	%% ?DEBUG("[ ping user_online ]====>  _SID=~p ; JID=~p ; _Info=~p",[_SID, JID, _Info]),
    start_ping(JID#jid.lserver, JID).

user_offline(_SID, JID, _Info) ->
    stop_ping(JID#jid.lserver, JID).

user_send(JID, _From, _Packet) ->
	%% ?DEBUG("[ ping user_send ]====>  JID=~p ; _From=~p ; _Packet=~p",[JID, _From, _Packet]),
    start_ping(JID#jid.lserver, JID).

%%====================================================================
%% Internal functions
%%====================================================================
add_timer(JID, Interval, Timers) ->
    LJID = jlib:jid_tolower(JID),
    NewTimers = case ?DICT:find(LJID, Timers) of
	    {ok, OldTRef} ->
			cancel_timer(OldTRef),
			?DICT:erase(LJID, Timers);
	    _ ->
			Timers
	end,
	%% 以秒为单位，向自己发 ping 消息 ，此消息由 handle_info 处理
    TRef = erlang:start_timer(Interval * 1000, self(), {ping, JID}),
    ?DICT:store(LJID, TRef, NewTimers).

del_timer(JID, Timers) ->
    LJID = jlib:jid_tolower(JID),
    case ?DICT:find(LJID, Timers) of
        {ok, TRef} ->
	    cancel_timer(TRef),
	    ?DICT:erase(LJID, Timers);
        _ ->
	    Timers
    end.

cancel_timer(TRef) ->
    case erlang:cancel_timer(TRef) of
	false ->
	    receive
                {timeout, TRef, _} ->
                    ok
            after 0 ->
                    ok
            end;
        _ ->
            ok
    end.
