-module(aa_inf_client).
-include("aa_inf_thrift.hrl").
-export([test/1]).

p(X)->
	io:format("in the p() ~w~n", [X]),
	ok.

test(IP)->
	Port = 5281,
	AARequest = #aaRequest{content="hello world.",sn="fuck"},
	{ok, Client0} = thrift_client_util:new(IP, Port, aa_inf_thrift, []),
	io:format("~n Client0 : ~p~n", [Client0]),
	{Client1, Res} =  thrift_client:call(Client0, process, [AARequest]),
	p(Res),
	thrift_client:close(Client1),
	ok.

