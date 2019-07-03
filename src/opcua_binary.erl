-module(opcua_binary).

-export([encode/1, decode/2]).

decode(Type, Bin) when is_atom(Type) ->
	decode1(Type, Bin);
decode([{_,_}|_] = TypeProplist, Bin) ->
	decode_multi_as_map(TypeProplist, Bin);
decode(TypeList, Bin) when is_list(TypeList) ->
	decode_multi(TypeList, Bin).

encode(_Bin) -> ok.

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

decode_multi_as_map(TypeProplist, Bin) ->
	Names = lists:map(fun({Name, _}) -> Name end, TypeProplist),
	Types = lists:map(fun({_, Type}) -> Type end, TypeProplist),
	case decode_multi(Types, Bin) of
		{ok, Objects, T} ->
			ObjectMap = maps:from_list(lists:zip(Names, Objects)),
			{ok, ObjectMap, T};
		Error ->
			Error
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

decode_array(Type, <<Int32:4/binary, Array/binary>>) ->
	decode_array(Type, Array, decode(int32, Int32)).

decode_array(_Type, _Array, -1) ->
	undefined;
decode_array(Type, Array, N) ->
	decode_array(Type, Array, N, []).

decode_array(_Type, T, 0, Acc) -> {ok, lists:reverse(Acc), T};
decode_array(Type, Array, N, Acc) ->
	case decode(Type, Array) of
		{ok, Elem, T} ->
			decode_array(Type, T, N-1, [Elem|Acc]);
		{error, Reason, T} ->
			{error, {Reason, lists:reverse(Acc)}, T}
	end.

decode1(boolean, <<0, T/binary>>) -> {ok, false, T};
decode1(boolean, <<_Bin:1/binary, T/binary>>) -> {ok, true, T};
decode1(byte, <<Byte:8, T/binary>>) -> {ok, Byte, T};
decode1(sbyte, <<SByte:8/signed-integer, T/binary>>) -> {ok, SByte, T};
decode1(uint16, <<UInt16:2/little-signed-integer-unit:8, T/binary>>) -> {ok, UInt16, T};
decode1(uint32, <<UInt32:4/little-signed-integer-unit:8, T/binary>>) -> {ok, UInt32, T};
decode1(uint64, <<UInt64:8/little-signed-integer-unit:8, T/binary>>) -> {ok, UInt64, T};
decode1(int16, <<Int16:2/little-unsigned-integer-unit:8, T/binary>>) -> {ok, Int16, T};
decode1(int32, <<Int32:4/little-unsigned-integer-unit:8, T/binary>>) -> {ok, Int32, T};
decode1(int64, <<Int64:8/little-unsigned-integer-unit:8, T/binary>>) -> {ok, Int64, T};
decode1(float, <<Float:4/little-signed-float-unit:8, T/binary>>) -> {ok, Float, T};
decode1(double, <<Double:8/little-unsigned-float-unit:8, T/binary>>) -> {ok, Double, T};
decode1(string, <<Int32:4/little-signed-integer-unit:8, T/binary>>)
  when Int32 == -1 -> {ok, undefined, T};
decode1(string, <<Int32:4/little-signed-integer-unit:8, String:Int32/binary, T/binary>>) ->
	{ok, String, T};
decode1(date_time, Bin) -> decode1(int64, Bin);
decode1(guid, <<D1:4/little-integer-unit:8, D2:2/little-integer-unit:8,
	      	D3:2/little-integer-unit:8, D4:8/binary, T/binary>>) ->
	{ok, <<D1:4/big-integer-unit:8, D2:2/big-integer-unit:8,
	       D3:2/big-integer-unit:8, D4:8/binary>>, T};
decode1(xml, Bin) ->
	case decode(string, Bin) of
		{ok, String, T} ->
			{ok, element(1, xmerl_scan:string(String)), T};
		Error ->
			Error
	end;
decode1(status_code, Bin) -> decode1(uint32, Bin);
decode1(byte_string, Bin) -> decode1(string, Bin);
decode1(enumeration, Bin) -> decode1(int32, Bin);
decode1(node_id, <<Mask:8, Bin/binary>>) ->
	decode_node_id(Mask, Bin);
decode1(expanded_node_id, Bin) ->
	decode_expanded_node_id(Bin);
decode1(diagnostic_info, <<Mask:1/binary, Bin/binary>>) ->
	decode_diagnostic_info(Mask, Bin);
decode1(qualified_name, Bin) ->
	decode_multi([uint16, string], Bin);
decode1(localized_text, <<Mask:1/binary, Bin/binary>>) ->
	decode_localized_text(Mask, Bin);
decode1(extension_object, Bin) ->
	decode_extension_object(Bin);
decode1(variant, <<0:6, Bin/binary>>) ->
	{ok, undefined, Bin};
decode1(variant, <<TypeId:6, DimFlag:1/bits, ArrayFlag:1/bits, Bin/binary>>) ->
	decode_variant(TypeId, DimFlag, ArrayFlag, Bin);
decode1(data_value, Bin) ->
	decode_data_value(Bin);
decode1(data_decimal, Bin) ->
	decode_decimal(Bin);
decode1(Type, T) ->
	{error, {no_match, Type, T}, T}.

decode_node_id(16#00, <<Id:1/little-unsigned-integer-unit:8, T/binary>>) ->
	 NodeId = #{namespace => default,
		    identifier_type => numeric,
		    value => Id},
	{ok, NodeId, T};
decode_node_id(16#01, <<Ns:1/little-unsigned-integer-unit:8, Id:2/little-unsigned-integer-unit:8, T/binary>>) ->
	 NodeId = #{namespace => Ns,
		    identifier_type => numeric,
		    value => Id},
	{ok, NodeId, T};
decode_node_id(16#03, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	case decode(string, Bin) of
		{ok, Id, T} ->
	 		NodeId = #{namespace => Ns,
		    		   identifier_type => string,
		    		   value => Id},
			{ok, NodeId, T};
		Error ->
			Error
	end;
decode_node_id(16#04, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	case decode(guid, Bin) of
		{ok, Id, T} ->
	 		NodeId = #{namespace => Ns,
		    		   identifier_type => guid,
		    		   value => Id},
			{ok, NodeId, T};
		Error ->
			Error
	end;
decode_node_id(16#05, <<Ns:2/little-unsigned-integer-unit:8, Bin/binary>>) ->
	case decode(string, Bin) of
		{ok, Id, T} ->
	 		NodeId = #{namespace => Ns,
		    		   identifier_type => opaque,
		    		   value => Id},
			{ok, NodeId, T};
		Error ->
			Error
	end.

decode_expanded_node_id(Bin) ->
	case decode_multi([node_id, string, uint32], Bin) of
		{ok, [NodeId, NamespaceUri, ServerIndex], T} ->
			NamespaceUri1 = case NamespaceUri of
						<<>> -> undefined;
						_ -> NamespaceUri
					end,
			ServerIndex1 = case ServerIndex of
						0 -> undefined;
					       _Else -> ServerIndex
				       end,
			ExpandedNodeId = #{node_id => NodeId,
					   namespace_uri => NamespaceUri1,
					   server_index => ServerIndex1},
			{ok, ExpandedNodeId, T};
		Error ->
			Error
	end.

decode_diagnostic_info(<<0:1, Mask:7>>, Bin) ->
	Types = [{symbolic_id, int32, undefined},
		 {namespace_uri, int32, undefined},
		 {locale, int32, undefined},
		 {localized_text, int32, undefined},
		 {additional_info, string, undefined},
		 {inner_status_code, status_code, undefined},
		 {inner_diagnostic_info, diagnostic_info, undefined}],
	decode_masked(Mask, Types, Bin).

decode_localized_text(<<0:6, Mask:2>>, Bin) ->
	Types = [{locale, string, undefined},
		 {text, string, undefined}],
	decode_masked(Mask, Types, Bin).

decode_extension_object(Bin) ->
	case decode1(node_id, Bin) of
		{ok, TypeId, <<Mask:8, T/binary>>} ->
			decode_extension_object1(Mask, TypeId, T);
		Error ->
			Error
	end.

decode_extension_object1(16#00, TypeId, T) ->
	{ok, #{type_id => TypeId, body => undefined}, T};
decode_extension_object1(16#01, TypeId, T) ->
	case decode1(byte_string, T) of
		{ok, Body, T1} ->
			{ok, #{type_id => TypeId, body => Body}, T1};
		Error ->
			Error
	end;
decode_extension_object1(16#02, TypeId, T) ->
	case decode1(xml, T) of
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

decode_data_value(<<0:2, Mask:6/bits, Bin/binary>>) ->
	Types = [{value, variant, undefined},
		 {status, status_code, good},
		 {source_timestamp, date_time, 0},
		 {source_pico_seconds, uint16, 0},
		 {server_timestamp, date_time, 0},
		 {server_pico_seconds, uint16, 0}],
	decode_masked(Mask, Types, Bin).

decode_decimal(Bin) ->
	case decode_multi([node_id, byte, int32, int16], Bin) of
		{ok, [NodeId, _, Length, Scale], T} when Length=<0 ->
			Decimal = #{type_id => NodeId,
				    scale => Scale,
				    value => 0},
			{ok, Decimal, T};
		{ok, [NodeId, _, Length, Scale], T} ->
			<<Value:Length/little-signed-integer-unit:8, T1/binary>> = T,
			Decimal = #{type_id => NodeId,
				    scale => Scale,
				    value => Value},
			{ok, Decimal, T1};
		Error ->
			Error
	end.

decode_masked(Mask, Types, Bin) ->
	BooleanMask = boolean_mask(Mask),
	{Apply, Defaults} = lists:split_with(fun({_, Cond}) -> Cond end, lists:zip(Types, BooleanMask)),
	FinalDefaults = lists:map(fun({Name, _, Default}) -> {Name, Default} end, Defaults),
	case decode_multi(lists:map(fun({_,Type,_}) -> Type end, Apply), Bin) of
		{ok, List, T} ->
			FinalApply = lists:zip(lists:map(fun({Name,_,_}) -> Name end, Apply), List),
			{ok, maps:from_list(FinalApply ++ FinalDefaults), T};
		Error ->
			Error
	end.

boolean_mask(Mask) ->
	lists:flatten(
	  [[X==<<1:1>> || X <-  [X1,X2,X3,X4,X5,X6,X7,X8]]
	   || <<X1:1,X2:1,X3:1,X4:1,X5:1,X6:1,X7:1,X8:1>> <= Mask]).

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

