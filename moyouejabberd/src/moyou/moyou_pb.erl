-module(moyou_pb).

-export([encode_heart/1, decode_heart/1, encode_msg/1,
	 decode_msg/1, encode_offlinemsg/1, decode_offlinemsg/1,
	 encode_login/1, decode_login/1, encode_bound/1,
	 decode_bound/1]).

-record(heart, {time}).

-record(msg,
	{msg_id, server_id, from, to_user, msg_type, group_id,
	 msg_time, serial_num, number, body, payload, resv1,
	 resv2, resv3}).

-record(offlinemsg,
	{status, jid, msg, resv1, resv2, resv3}).

-record(login,
	{server, user, password, resource, resv1, resv2,
	 resv3}).

-record(bound, {type, size}).

encode_heart(Record) when is_record(Record, heart) ->
    encode(heart, Record).

encode_msg(Record) when is_record(Record, msg) ->
    encode(msg, Record).

encode_offlinemsg(Record)
    when is_record(Record, offlinemsg) ->
    encode(offlinemsg, Record).

encode_login(Record) when is_record(Record, login) ->
    encode(login, Record).

encode_bound(Record) when is_record(Record, bound) ->
    encode(bound, Record).

encode(bound, Record) ->
    iolist_to_binary([pack(1, required,
			   with_default(Record#bound.type, none), sfixed32, []),
		      pack(2, required, with_default(Record#bound.size, none),
			   sfixed32, [])]);
encode(login, Record) ->
    iolist_to_binary([pack(1, required,
			   with_default(Record#login.server, none), string, []),
		      pack(2, required, with_default(Record#login.user, none),
			   string, []),
		      pack(3, required,
			   with_default(Record#login.password, none), string,
			   []),
		      pack(4, required,
			   with_default(Record#login.resource, none), string,
			   []),
		      pack(5, optional,
			   with_default(Record#login.resv1, none), string, []),
		      pack(6, optional,
			   with_default(Record#login.resv2, none), string, []),
		      pack(7, optional,
			   with_default(Record#login.resv3, none), string,
			   [])]);
encode(offlinemsg, Record) ->
    iolist_to_binary([pack(1, required,
			   with_default(Record#offlinemsg.status, none), int32,
			   []),
		      pack(2, optional,
			   with_default(Record#offlinemsg.jid, none), string,
			   []),
		      pack(3, repeated,
			   with_default(Record#offlinemsg.msg, none), msg, []),
		      pack(4, optional,
			   with_default(Record#offlinemsg.resv1, none), string,
			   []),
		      pack(5, optional,
			   with_default(Record#offlinemsg.resv2, none), string,
			   []),
		      pack(6, optional,
			   with_default(Record#offlinemsg.resv3, none), string,
			   [])]);
encode(msg, Record) ->
    iolist_to_binary([pack(1, required,
			   with_default(Record#msg.msg_id, none), string, []),
		      pack(2, optional,
			   with_default(Record#msg.server_id, none), string,
			   []),
		      pack(3, required, with_default(Record#msg.from, none),
			   string, []),
		      pack(4, required,
			   with_default(Record#msg.to_user, none), string, []),
		      pack(5, required,
			   with_default(Record#msg.msg_type, none), string, []),
		      pack(6, optional,
			   with_default(Record#msg.group_id, none), string, []),
		      pack(7, required,
			   with_default(Record#msg.msg_time, none), string, []),
		      pack(8, optional,
			   with_default(Record#msg.serial_num, none), string,
			   []),
		      pack(9, optional, with_default(Record#msg.number, none),
			   string, []),
		      pack(10, required, with_default(Record#msg.body, none),
			   string, []),
		      pack(11, optional,
			   with_default(Record#msg.payload, none), string, []),
		      pack(12, optional, with_default(Record#msg.resv1, none),
			   string, []),
		      pack(13, optional, with_default(Record#msg.resv2, none),
			   string, []),
		      pack(14, optional, with_default(Record#msg.resv3, none),
			   string, [])]);
encode(heart, Record) ->
    iolist_to_binary([pack(1, required,
			   with_default(Record#heart.time, none), string, [])]).

with_default(undefined, none) -> undefined;
with_default(undefined, Default) -> Default;
with_default(Val, _) -> Val.

pack(_, optional, undefined, _, _) -> [];
pack(_, repeated, undefined, _, _) -> [];
pack(FNum, required, undefined, Type, _) ->
    exit({error,
	  {required_field_is_undefined, FNum, Type}});
pack(_, repeated, [], _, Acc) -> lists:reverse(Acc);
pack(FNum, repeated, [Head | Tail], Type, Acc) ->
    pack(FNum, repeated, Tail, Type,
	 [pack(FNum, optional, Head, Type, []) | Acc]);
pack(FNum, _, Data, _, _) when is_tuple(Data) ->
    [RecName | _] = tuple_to_list(Data),
    protobuffs:encode(FNum, encode(RecName, Data), bytes);
pack(FNum, _, Data, Type, _) ->
    protobuffs:encode(FNum, Data, Type).

decode_heart(Bytes) when is_binary(Bytes) ->
    decode(heart, Bytes).

decode_msg(Bytes) when is_binary(Bytes) ->
    decode(msg, Bytes).

decode_offlinemsg(Bytes) when is_binary(Bytes) ->
    decode(offlinemsg, Bytes).

decode_login(Bytes) when is_binary(Bytes) ->
    decode(login, Bytes).

decode_bound(Bytes) when is_binary(Bytes) ->
    decode(bound, Bytes).

decode(bound, Bytes) when is_binary(Bytes) ->
    Types = [{2, size, sfixed32, []},
	     {1, type, sfixed32, []}],
    Decoded = decode(Bytes, Types, []),
    to_record(bound, Decoded);
decode(login, Bytes) when is_binary(Bytes) ->
    Types = [{7, resv3, string, []}, {6, resv2, string, []},
	     {5, resv1, string, []}, {4, resource, string, []},
	     {3, password, string, []}, {2, user, string, []},
	     {1, server, string, []}],
    Decoded = decode(Bytes, Types, []),
    to_record(login, Decoded);
decode(offlinemsg, Bytes) when is_binary(Bytes) ->
    Types = [{6, resv3, string, []}, {5, resv2, string, []},
	     {4, resv1, string, []},
	     {3, msg, 'Msg', [is_record, repeated]},
	     {2, jid, string, []}, {1, status, int32, []}],
    Decoded = decode(Bytes, Types, []),
    to_record(offlinemsg, Decoded);
decode(msg, Bytes) when is_binary(Bytes) ->
    Types = [{14, resv3, string, []},
	     {13, resv2, string, []}, {12, resv1, string, []},
	     {11, payload, string, []}, {10, body, string, []},
	     {9, number, string, []}, {8, serial_num, string, []},
	     {7, msg_time, string, []}, {6, group_id, string, []},
	     {5, msg_type, string, []}, {4, to_user, string, []},
	     {3, from, string, []}, {2, server_id, string, []},
	     {1, msg_id, string, []}],
    Decoded = decode(Bytes, Types, []),
    to_record(msg, Decoded);
decode(heart, Bytes) when is_binary(Bytes) ->
    Types = [{1, time, string, []}],
    Decoded = decode(Bytes, Types, []),
    to_record(heart, Decoded).

decode(<<>>, _, Acc) -> Acc;
decode(Bytes, Types, Acc) ->
    {{FNum, WireType}, Rest} =
	protobuffs:read_field_num_and_wire_type(Bytes),
    case lists:keysearch(FNum, 1, Types) of
      {value, {FNum, Name, Type, Opts}} ->
	  {Value1, Rest1} = case lists:member(is_record, Opts) of
			      true ->
				  {V, R} = protobuffs:decode_value(Rest,
								   WireType,
								   bytes),
				  RecVal =
				      decode(list_to_atom(string:to_lower(atom_to_list(Type))),
					     V),
				  {RecVal, R};
			      false ->
				  {V, R} = protobuffs:decode_value(Rest,
								   WireType,
								   Type),
				  {unpack_value(V, Type), R}
			    end,
	  case lists:member(repeated, Opts) of
	    true ->
		case lists:keytake(FNum, 1, Acc) of
		  {value, {FNum, Name, List}, Acc1} ->
		      decode(Rest1, Types,
			     [{FNum, Name,
			       lists:reverse([Value1 | lists:reverse(List)])}
			      | Acc1]);
		  false ->
		      decode(Rest1, Types, [{FNum, Name, [Value1]} | Acc])
		end;
	    false ->
		decode(Rest1, Types, [{FNum, Name, Value1} | Acc])
	  end;
      false -> exit({error, {unexpected_field_index, FNum}})
    end.

unpack_value(Binary, string) when is_binary(Binary) ->
    binary_to_list(Binary);
unpack_value(Value, _) -> Value.

to_record(bound, DecodedTuples) ->
    lists:foldl(fun ({_FNum, Name, Val}, Record) ->
			set_record_field(record_info(fields, bound), Record,
					 Name, Val)
		end,
		#bound{}, DecodedTuples);
to_record(login, DecodedTuples) ->
    lists:foldl(fun ({_FNum, Name, Val}, Record) ->
			set_record_field(record_info(fields, login), Record,
					 Name, Val)
		end,
		#login{}, DecodedTuples);
to_record(offlinemsg, DecodedTuples) ->
    lists:foldl(fun ({_FNum, Name, Val}, Record) ->
			set_record_field(record_info(fields, offlinemsg),
					 Record, Name, Val)
		end,
		#offlinemsg{}, DecodedTuples);
to_record(msg, DecodedTuples) ->
    lists:foldl(fun ({_FNum, Name, Val}, Record) ->
			set_record_field(record_info(fields, msg), Record, Name,
					 Val)
		end,
		#msg{}, DecodedTuples);
to_record(heart, DecodedTuples) ->
    lists:foldl(fun ({_FNum, Name, Val}, Record) ->
			set_record_field(record_info(fields, heart), Record,
					 Name, Val)
		end,
		#heart{}, DecodedTuples).

set_record_field(Fields, Record, Field, Value) ->
    Index = list_index(Field, Fields),
    erlang:setelement(Index + 1, Record, Value).

list_index(Target, List) -> list_index(Target, List, 1).

list_index(Target, [Target | _], Index) -> Index;
list_index(Target, [_ | Tail], Index) ->
    list_index(Target, Tail, Index + 1);
list_index(_, [], _) -> 0.

