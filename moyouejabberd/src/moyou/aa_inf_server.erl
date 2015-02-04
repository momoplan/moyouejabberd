-module(aa_inf_server).

-include("aa_inf_thrift.hrl").
-include("aa_inf_types.hrl").
-include("ejabberd.hrl").
-include("jlib.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-export([start/0, handle_function/2, process/1, stop/1]).

build_packet(<<"xml">>,Content)->
    xml_stream:parse_element(binary_to_list(Content));
build_packet(<<"term">>,Content)->
    {_,_,_,Packet}=binary_to_term(Content),
    Packet.

process({#aaRequest{sn=SN}=Args})->
    try
        case build_packet(Args#aaRequest.type,Args#aaRequest.content) of
            {error,Reason1} ->
                ?ERROR_MSG("build_packet error Reason : ~p, Args : ~p~n",[Reason1, Args]);
            Packet ->
                run(Packet)
        end,
        "OK"
    catch
        Type:Reason->
            ?ERROR_MSG("moyou_thrift_service_process exception Type : ~p, Reason :~p, Args : ~p",[Type, Reason, Args])
    end.



run(Packet) ->
    try
        From = jlib:string_to_jid(xml:get_tag_attr_s("from", Packet)),
        To = jlib:string_to_jid(xml:get_tag_attr_s("to", Packet)),
        {xmlelement, "message", _Attrs, _Kids} = Packet,
        mod_customize:user_send_packet(From, To, Packet)
    catch
        _:Clazz ->
            ?ERROR_MSG("exception :::> Packet=~p",[Packet]),
            ?ERROR_MSG("exception :::> clazz=~p ; err=~p",[Clazz,erlang:get_stacktrace()])
    end.

start()->
    start(5281).

start(Port)->
    Handler = ?MODULE,
    ?INFO_MSG("aa_inf_server start on ~p port, Handler=~p",[Port,Handler]),
    thrift_socket_server:start([{handler, Handler},
                                {service, aa_inf_thrift},
                                {port, Port},
                                %%									{name, aa_inf_server}]).
                                {name, aa_inf_server},
                                {socket_opts, [{recv_timeout, 60*60*1000}]}]).


stop(Server)->
    thrift_socket_server:stop(Server).

handle_function(Function, Args) ->
    case Function of
        process ->
            {reply, process(Args)};
        _ ->
            error
    end.
