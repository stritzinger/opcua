-module(opcua_binary).

-export([encode/2, decode/2]).

decode(boolean, <<0, T/binary>>) -> {ok, false, T};
decode(boolean, <<_Bin:1/binary, T/binary>>) -> {ok, true, T};
decode(byte, <<Byte:8, T/binary>>) -> {ok, Byte, T};
decode(sbyte, <<SByte:8/signed-integer, T/binary>>) -> {ok, SByte, T};
decode(uint16, <<UInt16:2/little-signed-integer-unit:8, T/binary>>) -> {ok, UInt16, T};
decode(uint32, <<UInt32:4/little-signed-integer-unit:8, T/binary>>) -> {ok, UInt32, T};
decode(uint64, <<UInt64:8/little-signed-integer-unit:8, T/binary>>) -> {ok, UInt64, T};
decode(int16, <<Int16:2/little-unsigned-integer-unit:8, T/binary>>) -> {ok, Int16, T};
decode(int32, <<Int32:4/little-unsigned-integer-unit:8, T/binary>>) -> {ok, Int32, T};
decode(int64, <<Int64:8/little-unsigned-integer-unit:8, T/binary>>) -> {ok, Int64, T};
decode(float, <<Float:4/little-signed-float-unit:8, T/binary>>) -> {ok, Float, T};
decode(double, <<Double:8/little-unsigned-float-unit:8, T/binary>>) -> {ok, Double, T};
decode(string, <<Int32:4/little-signed-integer-unit:8, T/binary>>) when Int32 == -1 -> {ok, undefined, T};
decode(string, <<Int32:4/little-signed-integer-unit:8, String:Int32/binary, T/binary>>) -> {ok, String, T};
decode(date_time, Bin) -> decode(int64, Bin);
decode(guid, Bin) -> decode_guid(Bin);
decode(xml, Bin) -> decode_xml(Bin);
decode(status_code, Bin) -> decode(uint32, Bin);
decode(byte_string, Bin) -> decode(string, Bin);
decode(node_id, <<Mask:8, Bin/binary>>) -> decode_node_id(Mask, Bin);
decode(expanded_node_id, <<Mask:8, Bin/binary>>) -> decode_expanded_node_id(Mask, Bin);
decode(diagnostic_info, <<Mask:1/binary, Bin/binary>>) -> decode_diagnostic_info(Mask, Bin);
decode(qualified_name, Bin) -> decode_qualified_name(Bin);
decode(localized_text, <<Mask:1/binary, Bin/binary>>) -> decode_localized_text(Mask, Bin);
decode(extension_object, Bin) -> decode_extension_object(Bin);
decode(variant, <<0:6, Bin/binary>>) -> {ok, undefined, Bin};
decode(variant, <<TypeId:6, DimFlag:1/bits, ArrayFlag:1/bits, Bin/binary>>) ->
	decode_variant(TypeId, DimFlag, ArrayFlag, Bin);
decode(data_value, <<0:2, Mask:6/bits, Bin/binary>>) -> decode_data_value(Mask, Bin);
decode(Type, T) -> {error, {no_match, Type, T}, T}.

encode(_Type, _Data) -> ok.


%% internal

decode_guid(<<D1:4/little-integer-unit:8, D2:2/little-integer-unit:8,
	      D3:2/little-integer-unit:8, D4:8/binary, T/binary>>) ->
	{ok, <<D1:4/big-integer-unit:8, D2:2/big-integer-unit:8,
	       D3:2/big-integer-unit:8, D4:8/binary>>, T}.

decode_xml(Bin) ->
	case decode(string, Bin) of
		{ok, String, T} ->
			{ok, element(1, xmerl_scan:string(String)), T};
		Error ->
			Error
	end.

decode_node_id(16#00, <<Id:1/little-unsigned-integer-unit:8, T/binary>>) ->
	{ok, #{namespace => default, identifier_type => numeric, value => Id}, T};
decode_node_id(16#01, <<Ns:1/little-unsigned-integer-unit:8, Id:2/little-unsigned-integer-unit:8, T/binary>>) ->
	{ok, #{namespace => Ns, identifier_type => numeric, value => Id}, T};
decode_node_id(16#02, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	{ok, Id, T} = decode(uint32, Bin),
	{ok, #{namespace => Ns, identifier_type => numeric, value => Id}, T};
decode_node_id(16#03, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	case decode(string, Bin) of
		{ok, Id, T} ->
			{ok, #{namespace => Ns, identifier_type => string, value => Id}, T};
		Error ->
			Error
	end;
decode_node_id(16#04, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	case decode(guid, Bin) of
		{ok, Id, T} ->
			{ok, #{namespace => Ns, identifier_type => guid, value => Id}, T};
		Error ->
			Error
	end;
decode_node_id(16#05, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	case decode(string, Bin) of
		{ok, Id, T} ->
			{ok, #{namespace => Ns, identifier_type => opaque, value => Id}, T};
		Error ->
			Error
	end.

decode_expanded_node_id(Mask, Bin) ->
	case decode_node_id(Mask rem 16#40, Bin) of
		{ok, NodeId, T} ->
			NamespaceUriFlag = (Mask div 16#80 == 1),
			ServerIndexFlag = (Mask div 16#40 == 1),
			Types = [{namespace_uri, string, undefined},
				 {server_index, uint32, undefined}],
			BooleanMask = [NamespaceUriFlag, ServerIndexFlag],
			case decode_masked(BooleanMask, Types, T) of
				{ok, Map, T1} ->
					{ok, maps:put(node_id, NodeId, Map), T1};
				Error ->
					Error
			end
	end.

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
	case decode_multi([uint16, string], Bin) of
		{ok, [NsIndex, Name], T} ->
			{ok, #{namespace_index => NsIndex, name => Name}, T};
		Error ->
			Error
	end.

decode_localized_text(<<0:6, Mask:2/bits>>, Bin) ->
	Types = [{locale, string, undefined},
		 {text, string, undefined}],
	decode_masked(Mask, Types, Bin).

decode_extension_object(Bin) ->
	case decode(node_id, Bin) of
		{ok, TypeId, <<Mask:8, T/binary>>} ->
			decode_extension_object1(Mask, TypeId, T);
		Error ->
			Error
	end.

decode_extension_object1(16#00, TypeId, T) ->
	{ok, #{type_id => TypeId, body => undefined}, T};
decode_extension_object1(16#01, TypeId, T) ->
	case decode(byte_string, T) of
		{ok, Body, T1} 	->
			{ok, #{type_id => TypeId, body => Body}, T1};
		Error ->
			Error
	end;
decode_extension_object1(16#02, TypeId, T) ->
	case decode(xml, T) of
		{ok, Body, T1} ->
			{ok, #{type_id => TypeId, body => Body}, T1};
		Error ->
			Error
	end.

decode_variant(_TypeId, _DimFlag, <<0:1>>, Bin) ->
	{ok, [], Bin};
decode_variant(TypeId, <<0:1>>, <<1:1>>, Bin) ->
	decode_array(get_built_in_type(TypeId), Bin);
decode_variant(TypeId, <<1:1>>, <<1:1>>, Bin) ->
	decode_multi_array(get_built_in_type(TypeId), Bin).

decode_data_value(Mask, Bin) ->
	Types = [{value, variant, undefined},
		 {status, status_code, good},
		 {source_timestamp, date_time, 0},
		 {source_pico_seconds, uint16, 0},
		 {server_timestamp, date_time, 0},
		 {server_pico_seconds, uint16, 0}],
	decode_masked(Mask, Types, Bin).

decode_masked(Mask, Types, Bin) ->
	BooleanMask = boolean_mask(Mask),
	{Apply, Defaults} = lists:splitwith(fun({_, Cond}) -> Cond end, lists:zip(Types, BooleanMask)),
	Apply1 = element(1, lists:unzip(Apply)),
	Defaults1 = element(1, lists:unzip(Defaults)),
	FinalDefaults = lists:map(fun({Name, _, Default}) -> {Name, Default} end, Defaults1),
	case decode_multi(lists:map(fun({_,Type,_}) -> Type end, Apply1), Bin) of
		{ok, List, T} ->
			FinalApply = lists:zip(lists:map(fun({Name,_,_}) -> Name end, Apply1), List),
			{ok, maps:from_list(FinalApply ++ FinalDefaults), T};
		Error ->
			Error
	end.

boolean_mask(Mask) when is_binary(Mask) ->
	[X==1 || <<X:1>> <= Mask];
boolean_mask(Mask) ->
	Mask.

decode_multi(TypeList, Bin) ->
	decode_multi(TypeList, Bin, []).

decode_multi([], T, Acc) ->
	{ok, lists:reverse(Acc), T};
decode_multi([Type|TypeList], Bin, Acc) ->
	case decode(Type, Bin) of
		{ok, Elem, T} ->
			decode_multi(TypeList, T, [Elem|Acc]);
		{error, Reason, T} ->
			{error, {Reason, lists:reverse(Acc)}, T}
	end.

decode_multi_array(Type, Bin) ->
	case decode_array(Type, Bin) of
		{ok, ObjectArray, T} ->
			case decode_array(int32, T) of
				{ok, DimArray, T1} ->
					{ok, honor_dimensions(ObjectArray, DimArray), T1};
				Error ->
					Error
			end;
		Error ->
			Error
	end.

decode_array(Type, Bin) ->
	{ok, Length, T} = decode(int32, Bin),
	decode_array(Type, T, Length).

decode_array(_Type, _Array, -1) ->
	undefined;
decode_array(Type, Array, N) ->
	decode_array(Type, Array, N, []).

decode_array(_Type, T, 0, Acc) ->
	{ok, lists:reverse(Acc), T};
decode_array(Type, Array, N, Acc) ->
	case decode(Type, Array) of
		{ok, Elem, T} ->
			decode_array(Type, T, N-1, [Elem|Acc]);
		{error, Reason, T} ->
			{error, {Reason, lists:reverse(Acc)}, T}
	end.

honor_dimensions(ObjectArray, DimArray) ->
	lists:reverse(
	  lists:foldl(fun(X, Acc) -> honor_dimensions1(X, Acc) end,
		      {[], ObjectArray}, DimArray)).

honor_dimensions1(Dim, {Array, ArrayList}) ->
	{El, NewArray} = lists:split(Dim, Array),
	{NewArray, [El|ArrayList]}.

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
