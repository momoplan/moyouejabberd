%%%----------------------------------------------------------------------
%%% File    : moyou_util.erl
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

-module(moyou_util).
-author('chenkangmin').

-include("ejabberd.hrl").
-include("jlib.hrl").

-export([
    unixtime/0,
    now_str/0,
    get_id/0,
    get_config/1,
    get_config/2,
    create_or_copy_table/3,
    random/1,
    http_request/2,
    get_msg_content/1,
    msg_type_2_push_name/1,
    msg_type_2_push_type/1,
    get_json_field/3,
    get_session_id/3,
    get_session_id/1,
    is_group_session/1,
    is_system_session/1,
    get_group_id_from_session/1,
    format_jid/1,
    term_to_bitstring/1,
    bitstring_to_term/1
        ]).



unixtime() ->
    {M, S, _} = erlang:now(),
    M * 1000000 + S.


now_str() ->
    {M, S, SS} = os:timestamp(),
    lists:sublist(erlang:integer_to_list(M*1000000000000 + S*1000000 + SS), 1, 13).


get_id() ->
    {M,S,SS} = now(),
    atom_to_list(node())++"_"++integer_to_list(M)++integer_to_list(S)++integer_to_list(SS).


get_config(Key) ->
    [Domain | _] = ejabberd_config:get_global_option(hosts),
    ejabberd_config:get_local_option({Key, Domain}).

get_config(Key, Default) ->
    case get_config(Key) of
        undefined ->
            Default;
        Value ->
            Value
    end.

create_or_copy_table(TableName, Opts, Copy) ->
    case mnesia:create_table(TableName, Opts) of
        {aborted, {already_exists, _}} ->
            mnesia:add_table_copy(TableName, node(), Copy);
        _ ->
            skip
    end.
    

random(Num) ->
    {A, B, C} = os:timestamp(),
    random:seed(A, B, C),
    random:uniform(Num).


http_request(Url, Params) ->
    case httpc:request(post, {Url, [], "application/x-www-form-urlencoded" , Params}, [{timeout, 60000}], []) of
        {ok, {_, _, ResBody}} ->
            ResBody;
        {error, Reason} ->
            ?ERROR_MSG("http_request error, Url : ~p~n, Params : ~p~n, Reason : ~p~n", [Url, Params, Reason]),
            []
    end.


get_msg_content({xmlelement, "message", _, Els}) ->
    feach_message(Els, []);
get_msg_content(_Packet) ->
    [].

feach_message([Element | Message], List) ->
    case Element of
        {xmlelement, "body", _, _} ->
            feach_message(Message, [get_text_message_form_packet_result(Element) | List]);
        _ ->
            feach_message(Message, List)
    end;
feach_message([], List) ->
    List.

get_text_message_form_packet_result({xmlelement, "body", _, List})->
    Res = lists:map(fun({_, V})-> binary_to_list(V) end, List),
    binary_to_list(list_to_binary(Res)).


msg_type_2_push_name("system") ->
    "系统消息";
msg_type_2_push_name("joinGroupApplication") ->
    "入群申请";
msg_type_2_push_name("groupBillboard") ->
    "群公告";
msg_type_2_push_name("startTeamPreparedConfirm") ->
    "就位确认";
msg_type_2_push_name("teamInvite") ->
    "组队消息";
msg_type_2_push_name("requestJoinTeam") ->
    "组队申请";
msg_type_2_push_name("teamHelper") ->
    "组队助手";
msg_type_2_push_name(Mt) ->
    Mt.


msg_type_2_push_type("system") ->
    "normalchat";
msg_type_2_push_type("joinGroupApplication") ->
    "normalchat";
msg_type_2_push_type("groupBillboard") ->
    "normalchat";
msg_type_2_push_type("startTeamPreparedConfirm") ->
    "normalchat";
msg_type_2_push_type("teamInvite") ->
    "normalchat";
msg_type_2_push_type("requestJoinTeam") ->
    "normalchat";
msg_type_2_push_type("teamHelper") ->
    "normalchat";
msg_type_2_push_type(Mt) ->
    Mt.


get_json_field(Obj, Key, Default) ->
    case rfc4627:get_field(Obj, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.


get_session_id(Gid) ->
    lists:concat(["group_", Gid]).

get_session_id(Mt, _From, To) when Mt =:= "groupchat" ->
    lists:concat(["group_", To#jid.user]);
get_session_id(_Mt, From, To) ->
    case From#jid.user of
        [$s, $y, $s | _] ->  %%系统消息
            lists:concat(["system_", To#jid.user]);
        "10000" ->           %%系统消息
            lists:concat(["system1_", To#jid.user]);
        _ ->
            lists:concat(["friend_" | lists:sort([From#jid.user, To#jid.user])])
    end.

is_group_session([$g, $r, $o, $u, $p | _]) ->
    true;
is_group_session(_) ->
    false.


%%system和10000号的消息都是系统消息
is_system_session([$s, $y, $s, $t, $e, $m | _]) ->
    true;
is_system_session(_) ->
    false.

get_group_id_from_session(SessionID) ->
    [_Prefix, GroupID] = re:split(SessionID, "_", [{return, list}]),
    GroupID.


format_jid(Jid) ->
    Jid#jid{resource = [], lresource = []}.


term_to_bitstring(Term) ->
    erlang:list_to_bitstring(io_lib:format("~p", [Term])).

bitstring_to_term(undefined) -> undefined;
bitstring_to_term(BitString) ->
    string_to_term(binary_to_list(BitString)).


string_to_term(String) ->
    case erl_scan:string(String++".") of
        {ok, Tokens, _} ->
            case catch erl_parse:parse_term(Tokens) of
                {ok, Term} -> Term;
                _Err -> undefined
            end;
        _Error ->
            undefined
    end.
