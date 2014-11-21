-module(aa_bridge).

-define(HTTP_HEAD,"application/x-www-form-urlencoded").
-define(DEF_EMSG_BRIDGE_URL,"http://192.168.1.11:4280").

-include("ejabberd.hrl").
-include("jlib.hrl").
%% API
-export([route/2,ack/2, route/1,ack/1 ,decode_json/1,encode_json/1]).

%%%===================================================================
%%% 发送到桥的 API
%%%===================================================================
route(Domain,Packet)->
	case ejabberd_config:get_local_option({emsg_bridge_enable,Domain}) of
		true ->
			ID = xml:get_tag_attr_s("id", Packet),
			case is_on_bridge(list_to_binary(ID),list_to_binary(Domain)) of
				false ->
					route(do,Domain,Packet);
				true ->
					?DEBUG("emsg_bridge_route_is_on_bridge_true skip",[]),
					skip
			end;
		_->
			?DEBUG("emsg_bridge_enable_false skip",[]),
			skip
	end.
route(do,Domain,Packet)->
	try
		{ok,JsonPacket} = encode_json(Packet), 
		{ok,Envelope} = dict:find(<<"envelope">>,dict:from_list(JsonPacket)),
		{ok,ID} = dict:find(<<"id">>,dict:from_list(Envelope)),
		?DEBUG("emsg_bridge_route ===> id=~p ; domain=~p~nPacket=~p~nJsonPacket=~p~n",[ID,Domain,Packet,JsonPacket]),
		HTTPTarget = case ejabberd_config:get_local_option({emsg_bridge_url,Domain}) of
			undefined ->
				?DEF_EMSG_BRIDGE_URL;
			URL1->
				URL1
		end,
		{Service,Method,SN} = {
				list_to_binary("emsg_bridge"),
				list_to_binary("route"),
				list_to_binary(index_score())
		},
		ParamObj=[{sn,SN},{service,Service},{method,Method},{params,[{packet,JsonPacket},{domain,list_to_binary(Domain)}]}],
		Json = jsx:encode(ParamObj),
		?DEBUG("emsg_bridge_route ===> Json=~p ; HTTPTarget=~p",[Json,HTTPTarget]),
		Form = "body="++binary_to_list(Json),
		case httpc:request(post,{ HTTPTarget ,[], ?HTTP_HEAD , Form },[],[] ) of   
	       	{ok, {_,_,Body}} ->
				DBody = rfc4627:decode(Body),
	 			case DBody of
	 				{ok,Obj,_Re} -> 
						case rfc4627:get_field(Obj,"success") of
							{ok,true} ->
								{ok,Entity} = rfc4627:get_field(Obj,"entity"),
								?DEBUG("emsg_bridge_route_success=~p ; entity=~p",[true,Entity]),
								put_on_bridge(ID,list_to_binary(Domain)),
								{ok,Entity};
							_ ->
								{ok,Entity} = rfc4627:get_field(Obj,"entity"),
								?DEBUG("emsg_bridge_route_success=~p ; entity=~p",[false,Entity]),
								{fail,Entity}
						end;
	 				Error -> 
	 					?ERROR_MSG("[ ERROR ] cause ~p~n",[Error]),
						{error,Error}
	 			end ;
	       	{error, Reason} ->
	 			?ERROR_MSG("[ ERROR ] cause ~p~n",[Reason]),
				{error,Reason}
		end
	catch
		_:_ ->
			Err = erlang:get_stacktrace(),
			?ERROR_MSG("error ~p",[Err]),
			{error,Err}
	end.

ack(Domain,IDF)->
	[ID|_] = string:tokens(IDF,"@"),
	case ejabberd_config:get_local_option({emsg_bridge_enable,Domain}) of
		true ->
			case is_on_bridge(list_to_binary(ID),list_to_binary(Domain)) of
				true ->
					ack(do,Domain,ID);
				Other ->
					?INFO_MSG("emsg_bridge is_on_bridge=~p",[Other]) 
			end;
		_->
			?DEBUG("emsg_bridge_enable_false",[]),
			skip
	end.
ack(do,Domain,ID)->
	try
		?DEBUG("emsg_bridge_ack ===> ID=~p",[ID]),
		HTTPTarget = case ejabberd_config:get_local_option({emsg_bridge,Domain}) of
			undefined ->
				?DEF_EMSG_BRIDGE_URL;
			URL1->
				URL1
		end,
		{Service,Method,SN} = {
				list_to_binary("emsg_bridge"),
				list_to_binary("ack"),
				list_to_binary(index_score())
		},
		ParamObj=[{sn,SN},{service,Service},{method,Method},{params,[{id,list_to_binary(ID)},{domain,list_to_binary(Domain)}]}],
		Json = jsx:encode(ParamObj),
		?DEBUG("emsg_bridge_route ===> Json=~p ; HTTPTarget=~p",[Json,HTTPTarget]),
		Form = "body="++binary_to_list(Json),
		case httpc:request(post,{ HTTPTarget ,[], ?HTTP_HEAD , Form },[],[] ) of   
	       	{ok, {_,_,Body}} ->
				DBody = rfc4627:decode(Body),
	 			case DBody of
	 				{ok,Obj,_Re} -> 
						case rfc4627:get_field(Obj,"success") of
							{ok,true} ->
	 							?INFO_MSG("send_ack_to_emsg_success=true id=~p",[ID]),
								del_on_bridge(list_to_binary(ID),list_to_binary(Domain)),
								{ok,Entity} = rfc4627:get_field(Obj,"entity"),
								{ok,Entity};
							_ ->
								{ok,Entity} = rfc4627:get_field(Obj,"entity"),
	 							?ERROR_MSG("[ ERROR ] send_ack_to_emsg_fail id=~p ; entity=~p",[ID,Entity]),
								{fail,Entity}
						end;
	 				Error -> 
	 					?ERROR_MSG("[ ERROR ] send_ack_to_emsg_error_cause id=~p ; err=~p",[ID,Error]),
						{error,Error}
	 			end ;
	       	{error, Reason} ->
	 			?ERROR_MSG("[ ERROR ] send_ack_to_emsg_error_reason id=~p ; reason=~p",[ID,Reason]),
				{error,Reason}
		end
	catch
		_:_ ->
			Err = erlang:get_stacktrace(),
			?ERROR_MSG("send_ack_to_emsg_exception id=~p ; err=~p",[ID,Err]),
			{error,Err}
	end.



%%%===================================================================
%%% 接收自桥的 API
%%%===================================================================
route(Body) ->
	DBody = rfc4627:decode(Body),
	case DBody of
		{ok,Obj,_Re} -> 
			case rfc4627:get_field(Obj,"params") of
				{ok,Params} ->
					{ok,Packet} = rfc4627:get_field(Params,"packet"),
					{ok,Domain} = rfc4627:get_field(Params,"domain"),
					?DEBUG("emsg_bridge_route_recv domain=~p ; packet=~p",[Domain,Packet]),
					SPacket = rfc4627:encode(Packet),
					{ok,XmlPacket} = decode_json(SPacket),
					?DEBUG("emsg_bridge_route_recv domain=~p ; xml_packet=~p",[Domain,XmlPacket]),
				    ID = xml:get_tag_attr_s("id", XmlPacket),
				    From = jlib:string_to_jid(xml:get_tag_attr_s("from", XmlPacket)),
				    To = jlib:string_to_jid(xml:get_tag_attr_s("to", XmlPacket)),
					?DEBUG("emsg_bridge_route_recv id=~p ; from=~p ; to=~p",[ID,From,To]),
					case ejabberd_router:route(From, To, XmlPacket) of
					    ok ->
							put_on_bridge(list_to_binary(ID),Domain),
							aa_hookhandler:user_send_packet_handler(From,To,XmlPacket);
					    RouteExcept -> 
							?ERROR_MSG("[ERROR] emsg_bridge_route_recv_send_error ~p",[RouteExcept]) 
					end;
				Other ->
					?DEBUG("emsg_bridge_route_recv=~p",[Other]),
					fail 
			end;
		Error -> 
			?ERROR_MSG("[ERROR] cause ~p~n",[Error]),
			error
	end.

ack(Body) ->
	DBody = rfc4627:decode(Body),
	case DBody of
		{ok,Obj,_Re} -> 
			case rfc4627:get_field(Obj,"params") of
				{ok,Params} ->
					{ok,ID} = rfc4627:get_field(Params,"id"),
					{ok,Domain} = rfc4627:get_field(Params,"domain"),
					del_on_bridge(ID,Domain),
					Key = binary_to_list(ID)++"@"++binary_to_list(Domain),
					?DEBUG("emsg_bridge_ack_recv id=~p ; domain=~p ; key=~p",[ID,Domain,Key]),
					gen_server:call(aa_hookhandler,{ecache_cmd,["DEL",Key]}),
					ok;
				Other ->
					?DEBUG("emsg_bridge_ack_recv=~p",[Other]),
					fail 
			end;
		Error -> 
			?ERROR_MSG("[ ERROR ] cause ~p~n",[Error]),
			error
	end.



%%%===================================================================
%%% 辅助 API
%%%===================================================================


%% 从桥上发出去的，和接收到的，都要使用此API记录到存储中，以便作出正确的ACK同步
%% ID/binary() , Domain/binary()
put_on_bridge(ID,Domain)-> 
	K = binary_to_list(ID)++"@bridge."++binary_to_list(Domain), 
	Cmd = ["PSETEX",K,integer_to_list(1000*60*60*24*7),"1"],
	Res = gen_server:call(aa_hookhandler,{ecache_cmd,Cmd}), 
	?DEBUG("put_on_bridge id=~p ; res=~p",[ID,Res]), 
	void.
    
%% 判断一个ID，是否是从桥上过来的,如果是，那么需要同步 ACK 的哦
%% ID/binary() , Domain/binary()
del_on_bridge(ID,Domain)->
    Bridge_key = binary_to_list(ID)++"@bridge."++binary_to_list(Domain),
	Cmd = ["DEL",Bridge_key],
    ?DEBUG("del_on_bridge cmd=~p",[Cmd]),
	Res = gen_server:call(aa_hookhandler,{ecache_cmd,Cmd}),
    ?DEBUG("del_on_bridge id=~p ; res=~p",[ID,Res]),
	void.
    
%% 判断一个ID是否经过了桥来处理
%% ID/binary() , Domain/binary()
is_on_bridge(ID,Domain)->
    Bridge_key = binary_to_list(ID)++"@bridge."++binary_to_list(Domain),
    ?DEBUG("is_on_bridge key=~p",[Bridge_key]),
	case gen_server:call(aa_hookhandler,{ecache_cmd,["GET",Bridge_key]}) of
        undefined ->
    		?DEBUG("is_on_bridge id=~p ; res=~p",[ID,undefined]),
            false;
        [V] ->
            ?DEBUG("is_on_bridge id=~p ; res=~p",[ID,V]),
            true;
        Obj ->
            ?DEBUG("is_on_bridge id=~p ; res=~p",[ID,Obj]),
            false 
	end.



%% json to xml_packet
%% "{\"envelope\":{\"id\":\"xx\",\"type\":4,\"ack\":1,\"from\":\"sys@gamepro.com\",\"to\":\"97@gamepro.com\" },\"payload\":{\"attrs\":{\"type\":\"chat\",\"msgtype\":\"frienddynamicmsg\",\"msgTime\":\"14\"},\"content\":{\"body\":258961,\"payload\":{\"img\":\"1512610,2830864,\"}}},\"vsn\":\"0.0.1\"}"
%% 
%% {
%%    "envelope": {
%% 	     "id": "7A5BC003DDC8427782768019624BB5F4",
%% 	     "type": 4,
%% 	     "ack": 1,
%% 	     "from": "sys00000008@gamepro.com",
%% 	     "to": "10030397@gamepro.com"
%%    },
%%    "payload": {
%%    		"attrs": {
%% 				"type": "chat",
%% 		    	"msgtype": "frienddynamicmsg",
%% 		    	"msgTime": "1403007055898"
%% 			},
%% 			"content": {
%% 		    	"body": 258961,
%% 		    	"payload": {
%% 		         	"img": "1512610,2830864,"
%% 		    	}
%% 			}
%%    },
%%   "vsn": "0.0.1"
%% }
%% =============================================================
%%	{
%%	 	xmlelement,"message", 
%%			[
%%				{"from","cc@test.com"}, 
%%				{"to","liangc@test.com"}, 
%%				{"id","bf4acd3c-ba26-45e4-989a-9d6375d3bfe1"}, 
%%				{"msgtype","normalchat"}, 
%%				{"type","chat"}
%%			], 
%%			[ 	
%%				{xmlelement,"body",[], [{xmlcdata,<<" fuck you ,ok ?">>}]},
%%				{xmlelement,"payload",[],[{xmlcdata,<<"hello world">>}]} 
%%			]
%%	} 
%%
%[
% 	{<<"envelope">>, [
%		{<<"id">>,<<"xx">>}, {<<"type">>,4}, {<<"ack">>,1}, {<<"from">>,<<"sys@gamepro.com">>}, {<<"to">>,<<"97@gamepro.com">>}
%	]}, 
% 	{<<"payload">>, [
%		{<<"attrs">>, [{<<"type">>,<<"chat">>}, {<<"msgtype">>,<<"frienddynamicmsg">>}, {<<"msgTime">>,<<"14">>}]}, 
%		{<<"content">>, [{<<"body">>,258961}, {<<"payload">>,[{<<"img">>,<<"1512610,2830864,">>}]}]}
%	]}, 
%	{<<"vsn">>,<<"0.0.1">>}
%]
decode_json(Json) when is_list(Json) ->
	decode_json(list_to_binary(Json));
decode_json(Json) when is_binary(Json) ->
	case jsx:is_json(Json) of
		true ->
			{E,M} = {xmlelement,"message"},

			List = jsx:decode(Json),
			Dict = dict:from_list(List),	
			{ok,Envelope0} = dict:find(<<"envelope">>,Dict),
			{ok,Payload0} = dict:find(<<"payload">>,Dict),
			{ok,Attrs0} = dict:find(<<"attrs">>,dict:from_list(Payload0)),
			{ok,Contents0} = dict:find(<<"content">>,dict:from_list(Payload0)),

			%% 找出 XML PACKET 的信封属性
			Envelope0_dict = dict:from_list(Envelope0),	
			{ok,ID} = dict:find(<<"id">>,Envelope0_dict),
			{ok,From} = dict:find(<<"from">>,Envelope0_dict),
			{ok,To} = dict:find(<<"to">>,Envelope0_dict),
			%% 非 JSON 协议信封属性,但是是 XML 协议的信封属性
			%% 这里可能会有很多自定义的属性，所以必须遍历才能得到结果
			Attrs1 = lists:map(fun({K,V})-> 
				{binary_to_list(K),binary_to_list(V)}				   
			end,Attrs0),
			%% Attr 就是 message 的 属性列表
			Attr = [{"id",binary_to_list(ID)},{"from",binary_to_list(From)},{"to",binary_to_list(To)}]++Attrs1,
			
			%[{xmlelement,"body",[], [{xmlcdata,<<" fuck you ,ok ?">>}]},..]
			Body = lists:map(fun({K,V})->
				V0 = try 
					dict:from_list(V),
					jsx:encode(V)
				catch _:_ -> 
					V 
				end,	
				{xmlelement,binary_to_list(K),[], [{xmlcdata,V0}]}
		 	end,Contents0),
			{ok,{E,M,Attr,Body}};
		_ ->
			format_error
	end.

%% xml_packet to json_packet
%% xml_stream:parse_element(M)
%%	{
%%	 	xmlelement,"message", 
%%			[{"from","cc@test.com"}, {"to","liangc@test.com"}, {"id","bf4acd3c-ba26-45e4-989a-9d6375d3bfe1"}, {"msgtype","normalchat"}, {"type","chat"}], 
%%			[ {xmlelement,"body",[], [{xmlcdata,<<" fuck you ,ok ?">>}]},{xmlelement,"payload",[],[{xmlcdata,<<"hello world">>}]} ]
%%	} 
%%	=====================================================
%% {
%%    "envelope": {
%% 	     "id": "7A5BC003DDC8427782768019624BB5F4",
%% 	     "type": 4,
%% 	     "ack": 1,
%% 	     "from": "sys00000008@gamepro.com",
%% 	     "to": "10030397@gamepro.com"
%%    },
%%    "payload": {
%%    		"attrs": {
%% 				"type": "chat",
%% 		    	"msgtype": "frienddynamicmsg",
%% 		    	"msgTime": "1403007055898"
%% 			},
%% 			"content": {
%% 		    	"body": 258961,
%% 		    	"payload": {
%% 		         	"img": "1512610,2830864,"
%% 		    	}
%% 			}
%%    },
%%   "vsn": "0.0.1"
%% }
encode_json(Packet)->
	case Packet of
		{xmlelement,"message",Attrs,Contents} ->
			Attrs0 = lists:map(fun({K,V})-> 
				K0 = erlang:list_to_binary(K), 
				V0 = erlang:list_to_binary(V), 
				{K0,V0} 
			end,Attrs),
			Contents0 = lists:map(fun(Obj)->
				case Obj of 
					{xmlelement,K,_,V} -> 	
						K0 = erlang:list_to_binary(K),
						V0 = get_text_message_form_packet_result(V),
						io:format("src=~p ; res=~p~n",[V0,jsx:is_json(V0)]),
						V1 = case jsx:is_json(V0) of 
							true ->
								jsx:decode(V0);
							_ ->
								V0
						end,
						{K0,V1};
					_ ->
						skip
				end
			end,Contents),					
			Contents1 = [X||X<-Contents0,X=/=skip],

			DAttrs = dict:from_list(Attrs0),
			%% XXX 发到这里的消息，都是不需要判断状态的，直接包装好了就行,逻辑判断都在各个模块中完成
			{ok,MsgType} = dict:find(<<"msgtype">>,DAttrs),
			Ack = 1,	
			Type = case MsgType of
				<<"normalchat">> ->
					1;
				<<"groupchat">> ->
					2;
				<<"msgStatus">> ->
					3;
				_ ->
					4
			end,
			{ok,ID} = dict:find(<<"id">>,DAttrs),
			{ok,From} = dict:find(<<"from">>,DAttrs),
			{ok,To} = dict:find(<<"to">>,DAttrs),	

			Envelope0 = [{<<"id">>,ID},{<<"type">>,Type},{<<"ack">>,Ack},{<<"from">>,From},{<<"to">>,To}],
			Envelope1 = case Type of
				2 -> %% groupchat	
					{ok,Gid} = dict:find(<<"gid">>,DAttrs),	
					lists:append(Envelope0,[{<<"gid">>,Gid}]);
				_ ->
					Envelope0
			end,

			%% 把 xml 中 message 的未加入到信封中的属性过滤出来，并加入到 payload 中
			%% id,from,to,xmlns,都可以过滤掉
			Attrs1 = lists:map(fun({K,V})->
				case K of 
					<<"id">> ->
						skip;
					<<"from">> ->
						skip;
					<<"to">> ->
						skip;
					<<"xmlns">> ->
						skip;
					_ ->
						{K,V}
				end
			end,Attrs0),
			Attrs2 = [X||X<-Attrs1,X=/=skip],
			Payload = [{<<"attrs">>,Attrs2},{<<"content">>,Contents1}],
			JP = [{<<"envelope">>,Envelope1},{<<"payload">>,Payload},{<<"vsn">>,<<"0.0.1">>}],
			%% J = jsx:encode(JP),
			{ok,JP};
		_ ->
			skip	
	end.


get_text_message_form_packet_result( List )->
	Res = lists:map(fun({_,V})-> binary_to_list(V) end,List),                                       
	ResultMessage = binary_to_list(list_to_binary(Res)), 
	list_to_binary(ResultMessage).	

index_score()-> {M,S,T} = now(), erlang:integer_to_list(M*1000000000000+S*1000000+T).
