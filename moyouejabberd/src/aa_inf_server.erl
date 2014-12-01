-module(aa_inf_server).

-include("aa_inf_thrift.hrl").
-include("aa_inf_types.hrl").
-include("ejabberd.hrl").
-include("jlib.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-export([start/0, handle_function/2, process/1, stop/1]).
-record(session, {sid, usr, us, priority, info}).


build_packet(<<"xml">>,Content)->
        xml_stream:parse_element(binary_to_list(Content));
build_packet(<<"term">>,Content)->
        {_,_,_,Packet}=binary_to_term(Content),
        Packet.

process({#aaRequest{sn=SN}=Args})->
	    case build_packet(Args#aaRequest.type,Args#aaRequest.content) of
			{error,Reason} -> 
				?ERROR_MSG("aa_info_server_process xmpp exception :::> SN=~p ;Content = ~p; Err=~p",[SN,Args#aaRequest.content,Reason]);
			Packet -> 
				run(Packet)
		end.

run(Packet) ->
        try
                ?DEBUG("aa_info_server ::: Packet ====> ~p",[Packet]),
                From = jlib:string_to_jid(xml:get_tag_attr_s("from", Packet)),
                To = jlib:string_to_jid(xml:get_tag_attr_s("to", Packet)),
				aa_hookhandler:user_send_packet_handler(From, To, Packet),
                ejabberd_router:route(From, To, Packet)
        catch
                _:Clazz ->
                        ?ERROR_MSG("exception :::> Packet=~p",[Packet]),
                        ?ERROR_MSG("exception :::> clazz=~p ; err=~p",[Clazz,erlang:get_stacktrace()])
        end.

loop()->
        receive
                {retome_push,SN,Type,Content} ->
                        try
                                ?INFO_MSG("routem_push ==> SN=~p",[SN]),
                                ?DEBUG("routem_push ==> SN=~p~n ; Content=~p~n ; Decode=~p~n",[SN,Content,binary_to_list(Content)]),
                                Packet = build_packet(Type,Content),
                                run(Packet)
                        catch
                                E0:E1 ->
                                        ?ERROR_MSG("retome_push_error : ~p",[{E0,E1,erlang:get_stacktrace()}])
                        end,
                        loop();
                {push,Packet} ->
                        run(Packet),
                        loop();
                Other ->
                        ?INFO_MSG("aa_inf_server_run_Other=~p",[Other]),
                        loop()
        end.

start()->
        start(5281).

start(Port)->
        try
                [Domain|_] = ?MYHOSTS,
                AA_INF_SERVER_NODE =  ejabberd_config:get_local_option({aa_inf_server,Domain}),
                AA_INF_SERVER_PING = net_adm:ping(AA_INF_SERVER_NODE),
				?INFO_MSG("aa_inf_server start AA_INFO_SERVER_PING=~p",[AA_INF_SERVER_PING]),
				case erlang:whereis(aa_inf_server_run) of
					undefined ->
						LoopPid = erlang:spawn(fun()-> loop() end),
						RegRtn = erlang:register(aa_inf_server_run,LoopPid),
						?INFO_MSG("aa_inf_server start looppid=~p ; reg=~p",[LoopPid,RegRtn]);
					_Pid ->
						?INFO_MSG("aa info server start loopexisted", [])
				end						
        catch
                _:_->
                        ?ERROR_MSG("aa_inf_server start reg_err :::> ~p",[erlang:get_stacktrace()])
		end,
		Handler = ?MODULE,
		?INFO_MSG("aa_inf_server start on ~p port, Handler=~p",[Port,Handler]),
		thrift_socket_server:start([{handler, Handler},
									{service, aa_inf_thrift},
									{port, Port},
									{name, aa_inf_server}]).
%% 									{name, aa_inf_server},
%% 									{socket_opts, [{recv_timeout, 60*60*1000}]}]).


stop(Server)->
        thrift_socket_server:stop(Server).

handle_function(Function, Args) ->
        case Function of
                process ->
                        {reply, process(Args)};
                _ ->
                        error
        end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% 在 ejabberd_sm 模块移植的方法
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_user_present_resources(LUser, LServer) ->
    US = {LUser, LServer},
    case catch mnesia:dirty_index_read(session, US, #session.us) of
                {'EXIT', _Reason} ->
                    [];
                Ss ->
                    [{S#session.priority, element(3, S#session.usr)} || S <- clean_session_list(Ss), is_integer(S#session.priority)]
    end.

clean_session_list(Ss) ->
    clean_session_list(lists:keysort(#session.usr, Ss), []).

clean_session_list([], Res) ->
    Res;
clean_session_list([S], Res) ->
    [S | Res];
clean_session_list([S1, S2 | Rest], Res) ->
    if
        S1#session.usr == S2#session.usr ->
            if
                S1#session.sid > S2#session.sid ->
                    clean_session_list([S1 | Rest], Res);
                true ->
                    clean_session_list([S2 | Rest], Res)
            end;
        true ->
            clean_session_list([S2 | Rest], [S1 | Res])
    end.