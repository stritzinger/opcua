-module(opcua_binary).

-export([encode/2, decode/2]).

decode(boolean, <<0, T/binary>>) -> {false, T};
decode(boolean, <<_Bin:1/binary, T/binary>>) -> {true, T};
decode(byte, <<Byte:8, T/binary>>) -> {Byte, T};
decode(sbyte, <<SByte:8/signed-integer, T/binary>>) -> {SByte, T};
decode(uint16, <<UInt16:2/little-unsigned-integer-unit:8, T/binary>>) -> {UInt16, T};
decode(uint32, <<UInt32:4/little-unsigned-integer-unit:8, T/binary>>) -> {UInt32, T};
decode(uint64, <<UInt64:8/little-unsigned-integer-unit:8, T/binary>>) -> {UInt64, T};
decode(int16, <<Int16:2/little-signed-integer-unit:8, T/binary>>) -> {Int16, T};
decode(int32, <<Int32:4/little-signed-integer-unit:8, T/binary>>) -> {Int32, T};
decode(int64, <<Int64:8/little-signed-integer-unit:8, T/binary>>) -> {Int64, T};
decode(float, <<Float:4/little-signed-float-unit:8, T/binary>>) -> {Float, T};
decode(double, <<Double:8/little-unsigned-float-unit:8, T/binary>>) -> {Double, T};
decode(string, <<Int32:4/little-signed-integer-unit:8, T/binary>>) when Int32 == -1 -> {undefined, T};
decode(string, <<Int32:4/little-signed-integer-unit:8, String:Int32/binary, T/binary>>) -> {String, T};
decode(date_time, Bin) -> decode(int64, Bin);
decode(guid, Bin) -> decode_guid(Bin);
decode(xml, Bin) -> decode(string, Bin);
decode(status_code, Bin) -> decode(uint32, Bin);
decode(byte_string, Bin) -> decode(string, Bin);
decode(node_id, <<Mask:8, Bin/binary>>) -> decode_node_id(Mask, Bin);
decode(expanded_node_id, <<Mask:8, Bin/binary>>) -> decode_expanded_node_id(Mask, Bin);
decode(diagnostic_info, <<Mask:1/binary, Bin/binary>>) -> decode_diagnostic_info(Mask, Bin);
decode(qualified_name, Bin) -> decode_qualified_name(Bin);
decode(localized_text, <<Mask:1/binary, Bin/binary>>) -> decode_localized_text(Mask, Bin);
decode(extension_object, Bin) -> decode_extension_object(Bin);
decode(variant, <<0:6, Bin/binary>>) -> {undefined, Bin};
decode(variant, <<Type_Id:6, Dim_Flag:1/bits, Array_Flag:1/bits, Bin/binary>>) ->
	decode_variant(Type_Id, Dim_Flag, Array_Flag, Bin);
decode(data_value, <<0:2, Mask:6/bits, Bin/binary>>) -> decode_data_value(Mask, Bin);
decode(_Type, _Bin) -> error(badarg).

encode(boolean, false) -> <<0:8>>;
encode(boolean, true) -> <<1:8>>;
encode(byte, Byte) -> <<Byte:8>>;
encode(sbyte, SByte) -> <<SByte:8/signed-integer>>;
encode(uint16, UInt16) -> <<UInt16:2/little-unsigned-integer-unit:8>>;
encode(uint32, UInt32) -> <<UInt32:4/little-unsigned-integer-unit:8>>;
encode(uint64, UInt64) -> <<UInt64:8/little-unsigned-integer-unit:8>>;
encode(int16, Int16) -> <<Int16:2/little-signed-integer-unit:8>>;
encode(int32, Int32) -> <<Int32:4/little-signed-integer-unit:8>>;
encode(int64, Int64) -> <<Int64:8/little-signed-integer-unit:8>>;
encode(float, Float) -> <<Float:4/little-signed-float-unit:8>>;
encode(double, Double) -> <<Double:8/little-signed-float-unit:8>>;
encode(string, String) when String == undefined -> <<-1:4/little-signed-integer-unit:8>>;
encode(string, String) -> <<(byte_size(String)):4/little-signed-integer-unit:8, String/binary>>;
encode(date_time, Bin) -> encode(int64, Bin);
encode(guid, Bin) -> encode_guid(Bin);
encode(xml, Bin) -> encode(string, Bin);
encode(status_code, Bin) -> encode(uint32, Bin);
encode(byte_string, Bin) -> encode(string, Bin);
encode(node_id, Node_Id) -> encode_node_id(Node_Id).


%% internal

decode_guid(<<D1:4/little-integer-unit:8, D2:2/little-integer-unit:8,
	      D3:2/little-integer-unit:8, D4:8/binary, T/binary>>) ->
	{<<D1:4/big-integer-unit:8, D2:2/big-integer-unit:8,
	   D3:2/big-integer-unit:8, D4:8/binary>>, T}.

decode_node_id(16#00, <<Id:1/little-unsigned-integer-unit:8, T/binary>>) ->
	{#{namespace => default, identifier_type => numeric, value => Id}, T};
decode_node_id(16#01, <<Ns:1/little-unsigned-integer-unit:8, Id:2/little-unsigned-integer-unit:8, T/binary>>) ->
	{#{namespace => Ns, identifier_type => numeric, value => Id}, T};
decode_node_id(16#02, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	{Id, T} = decode(uint32, Bin),
	{#{namespace => Ns, identifier_type => numeric, value => Id}, T};
decode_node_id(16#03, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	{Id, T} = decode(string, Bin),
	{#{namespace => Ns, identifier_type => string, value => Id}, T};
decode_node_id(16#04, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	{Id, T} = decode(guid, Bin),
	{#{namespace => Ns, identifier_type => guid, value => Id}, T};
decode_node_id(16#05, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	{Id, T} = decode(string, Bin),
	{#{namespace => Ns, identifier_type => opaque, value => Id}, T}.

decode_expanded_node_id(Mask, Bin) ->
	{Node_Id, T} = decode_node_id(Mask rem 16#40, Bin),
	Namespace_Uri_Flag = (Mask div 16#80 == 1),
	Server_Index_Flag = (Mask div 16#40 == 1),
	Types = [{namespace_uri, string, undefined},
		 {server_index, uint32, undefined}],
	Boolean_Mask = [Namespace_Uri_Flag, Server_Index_Flag],
	{Map, T1} = decode_masked(Boolean_Mask, Types, T),
	{maps:put(node_id, Node_Id, Map), T1}.

decode_diagnostic_info(<<0:1, Mask:7/bits>>, Bin) ->
	Types = [{symbolic_id, int32, undefined},
		 {namespace_uri, int32, undefined},
		 {locale, int32, undefined},
		 {localized_text, int32, undefined},
		 {additional_info, string, undefined},
		 {inner_status_code, status_code, undefined},
		 {inner_diagnostic_info, diagnostic_info, undefined}],
	decode_masked(Mask, Types, Bin).

decode_qualified_name(Bin) ->
	{[Namespace_Index, Name], T}  = decode_multi([uint16, string], Bin),
	{#{namespace_index => Namespace_Index, name => Name}, T}.

decode_localized_text(<<0:6, Mask:2/bits>>, Bin) ->
	Types = [{locale, string, undefined},
		 {text, string, undefined}],
	decode_masked(Mask, Types, Bin).

decode_extension_object(Bin) ->
	{Type_Id, <<Mask:8, T/binary>>} = decode(node_id, Bin),
	decode_extension_object1(Mask, Type_Id, T).

decode_extension_object1(16#00, Type_Id, T) ->
	{#{type_id => Type_Id, body => undefined}, T};
decode_extension_object1(16#01, Type_Id, T) ->
	{Body, T1} = decode(byte_string, T),
	{#{type_id => Type_Id, body => Body}, T1};
decode_extension_object1(16#02, Type_Id, T) ->
	{Body, T1} = decode(xml, T),
	{#{type_id => Type_Id, body => Body}, T1}.

decode_variant(_Type_Id, _Dim_Flag, <<0:1>>, Bin) ->
	{[], Bin};
decode_variant(Type_Id, <<0:1>>, <<1:1>>, Bin) ->
	decode_array(get_built_in_type(Type_Id), Bin);
decode_variant(Type_Id, <<1:1>>, <<1:1>>, Bin) ->
	decode_multi_array(get_built_in_type(Type_Id), Bin).

decode_data_value(Mask, Bin) ->
	Types = [{value, variant, undefined},
		 {status, status_code, good},
		 {source_timestamp, date_time, 0},
		 {source_pico_seconds, uint16, 0},
		 {server_timestamp, date_time, 0},
		 {server_pico_seconds, uint16, 0}],
	decode_masked(Mask, Types, Bin).

decode_masked(Mask, Types, Bin) ->
	Boolean_Mask = boolean_mask(Mask),
	{Apply, Defaults} = lists:splitwith(fun({_, Cond}) -> Cond end, lists:zip(Types, Boolean_Mask)),
	Apply1 = element(1, lists:unzip(Apply)),
	Defaults1 = element(1, lists:unzip(Defaults)),
	Final_Defaults = lists:map(fun({Name, _, Default}) -> {Name, Default} end, Defaults1),
	{List, T} = decode_multi(lists:map(fun({_,Type,_}) -> Type end, Apply1), Bin),
	Final_Apply = lists:zip(lists:map(fun({Name,_,_}) -> Name end, Apply1), List),
	{maps:from_list(Final_Apply ++ Final_Defaults), T}.

boolean_mask(Mask) when is_binary(Mask) ->
	[X==1 || <<X:1>> <= Mask];
boolean_mask(Mask) ->
	Mask.

decode_multi(Type_List, Bin) ->
	decode_multi(Type_List, Bin, []).

decode_multi([], T, Acc) ->
	{lists:reverse(Acc), T};
decode_multi([Type|Type_List], Bin, Acc) ->
	{Elem, T} = decode(Type, Bin),
	decode_multi(Type_List, T, [Elem|Acc]).

decode_multi_array(Type, Bin) ->
	{Object_Array, T} = decode_array(Type, Bin),
	{Dim_Array, T1} = decode_array(int32, T),
	{honor_dimensions(Object_Array, Dim_Array), T1}.

decode_array(Type, Bin) ->
	{Length, T} = decode(int32, Bin),
	decode_array(Type, T, Length).

decode_array(_Type, _Array, -1) ->
	undefined;
decode_array(Type, Array, N) ->
	decode_array(Type, Array, N, []).

decode_array(_Type, T, 0, Acc) ->
	{lists:reverse(Acc), T};
decode_array(Type, Array, N, Acc) ->
	{Elem, T} = decode(Type, Array),
	decode_array(Type, T, N-1, [Elem|Acc]).

honor_dimensions(Object_Array, Dim_Array) ->
	lists:reverse(
	  lists:foldl(fun(X, Acc) -> honor_dimensions1(X, Acc) end,
		      {[], Object_Array}, Dim_Array)).

honor_dimensions1(Dim, {Array, Array_List}) ->
	{El, New_Array} = lists:split(Dim, Array),
	{New_Array, [El|Array_List]}.

encode_guid(<<D1:4/big-integer-unit:8, D2:2/big-integer-unit:8,
	      D3:2/big-integer-unit:8, D4:8/binary>>) ->
	<<D1:4/little-integer-unit:8, D2:2/little-integer-unit:8,
	  D3:2/little-integer-unit:8, D4:8/binary>>.

encode_node_id(#{namespace := default, identifier_type := numeric, value := Id}) ->
	<<16#00:8, Id:1/little-unsigned-integer-unit:8>>;
encode_node_id(#{namespace := Ns, identifier_type := numeric, value := Id}) when Id < 65536 ->
  	<<16#01:8, Ns:1/little-unsigned-integer-unit:8, Id:2/little-unsigned-integer-unit:8>>;
encode_node_id(#{namespace := Ns, identifier_type := numeric, value := Id}) ->
	Bin_Id = encode(uint32, Id),
	<<16#02:8, Ns:2/little-unsigned-integer-unit:8, Bin_Id/binary>>;
encode_node_id(#{namespace := Ns, identifier_type := string, value := Id}) ->
	Bin_Id = encode(string, Id),
  	<<16#03:8, Ns:2/little-unsigned-integer-unit:8, Bin_Id/binary>>;
encode_node_id(#{namespace := Ns, identifier_type := guid, value := Id}) ->
	Bin_Id = encode(guid, Id),
  	<<16#04:8, Ns:2/little-unsigned-integer-unit:8, Bin_Id/binary>>;
encode_node_id(#{namespace := Ns, identifier_type := opaque, value := Id}) ->
	Bin_Id = encode(string, Id),
	<<16#05:8, Ns:2/little-unsigned-integer-unit:8, Bin_Id/binary>>.

get_built_in_type(Id) ->
	maps:get(Id, #{
		1 => boolean,
		2 => sbyte,
		3 => byte,
		4 => int16,
		5 => uint16,
		6 => int32,
		7 => uint32,
		8 => int64,
		9 => uint64,
		10 => float,
		11 => double,
		12 => string,
		13 => date_time,
		14 => guid,
		15 => byte_string,
		16 => xml,
		17 => node_id,
		18 => expanded_node_id,
		19 => status_code,
		20 => qualified_name,
		21 => localized_text,
		22 => extension_object,
		23 => data_value,
		24 => variant,
		25 => diagnostic_info,
		26 => byte_string,
		27 => byte_string,
		28 => byte_string,
		29 => byte_string,
		30 => byte_string,
		31 => byte_string}).
