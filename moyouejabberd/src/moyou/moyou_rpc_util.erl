%%%----------------------------------------------------------------------
%%% File    : moyou_rpc_util.erl
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

-module(moyou_rpc_util).
-author('chenkangmin').

-include("ejabberd.hrl").

-export([
    store_message/3,
    query_group_id/1,
    update_session_all_seq/3,
    update_session_read_seq/3,
    send_offline_message/3,
    update_group_info/3,
    update_user_info/9,
    clear_user_session_info/2,
    get_session_msg/4,
    get_offline_msg/1
        ]).


store_message(SessionID, From, Packet) ->
    case moyou_util:get_config(moyou_session_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_call(NodeList, moyou_session_manage, store_message, [SessionID, From, Packet])
    end.


query_group_id(SessionID) ->
    case moyou_util:get_config(moyou_session_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_call(NodeList, moyou_session_manage, query_session_seq, [SessionID])
    end.


get_session_msg(Uid, SessionID, Seq, Size) ->
    case moyou_util:get_config(moyou_session_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_call(NodeList, moyou_user_manage, get_session_msg, [Uid, SessionID, Seq, Size])
    end.


get_offline_msg(Uid) ->
    case moyou_util:get_config(moyou_session_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_call(NodeList, moyou_user_manage, get_offline_msg, [Uid])
    end.


update_session_all_seq([], _SessionID, _Seq) ->
    skip;
update_session_all_seq(UserList, SessionID, Seq) ->
    case moyou_util:get_config(moyou_session_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_cast(NodeList, moyou_user_manage, update_session_all_seq, [UserList, SessionID, Seq])
    end.


send_offline_message(From, To, Packet) ->
    case moyou_util:get_config(moyou_push_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_cast(NodeList, moyou_push_service, send_offline_message, [From, To, Packet])
    end.


update_group_info(Gid, Gname, NotPush) ->
    case moyou_util:get_config(moyou_push_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_cast(NodeList, moyou_push_service, update_group_info, [Gid, Gname, NotPush])
    end.


update_user_info(Uid, Friends, NickName, DeviceToken, Imei, Blacks, SilenceConfig, MessageConfig,
                 Env) ->
    case moyou_util:get_config(moyou_push_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_cast(NodeList, moyou_push_service, update_user_info, [Uid, Friends, NickName, DeviceToken, Imei, Blacks,
                                                                      SilenceConfig, MessageConfig, Env])
    end.


update_session_read_seq(Uid, SessionID, Seq) ->
    case moyou_util:get_config(moyou_session_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_cast(NodeList, moyou_user_manage, update_session_read_seq, [Uid, SessionID, Seq])
    end.

clear_user_session_info(UserList, SessionID) ->
    case moyou_util:get_config(moyou_session_service) of
        undefined ->
            skip;
        NodeList ->
            rpc_cast(NodeList, moyou_user_manage, clear_user_session_info, [UserList, SessionID])
    end.


rpc_call([], _Module, _Function, _Args) ->
    skip;
rpc_call([Node | Rest], Module, Function, Args) ->
    case rpc:call(Node, Module, Function, Args) of
        {badrpc, Reason} ->
            ?ERROR_MSG("rpc_call ~p:~p(~p) failed~n, Reason : ~p~n", [Module, Function, Args, Reason]),
            rpc_call(Rest, Module, Function, Args);
        {ok, Data} ->
            Data
    end.


rpc_cast([], _Module, _Function, _Args) ->
    skip;
rpc_cast([Node | Rest], Module, Function, Args) ->
    case lists:member(Node, [node() | nodes()]) of
        true ->
            rpc:cast(Node, Module, Function, Args);
        _ ->
            rpc_cast(Rest, Module, Function, Args)
    end.
