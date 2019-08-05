-module(opcua_codec_binary_builtin).

-include("opcua_codec.hrl").

-export([encode/2, decode/2]).

decode(boolean, <<0, T/binary>>) -> {false, T};
decode(boolean, <<_Bin:1/binary, T/binary>>) -> {true, T};
decode(byte, <<Byte:8, T/binary>>) -> {Byte, T};
decode(sbyte, <<SByte:8/signed-integer, T/binary>>) -> {SByte, T};
decode(uint16, <<UInt16:16/little-unsigned-integer, T/binary>>) -> {UInt16, T};
decode(uint32, <<UInt32:32/little-unsigned-integer, T/binary>>) -> {UInt32, T};
decode(uint64, <<UInt64:64/little-unsigned-integer, T/binary>>) -> {UInt64, T};
decode(int16, <<Int16:16/little-signed-integer, T/binary>>) -> {Int16, T};
decode(int32, <<Int32:32/little-signed-integer, T/binary>>) -> {Int32, T};
decode(int64, <<Int64:64/little-signed-integer, T/binary>>) -> {Int64, T};
decode(float, <<Float:32/little-signed-float, T/binary>>) -> {Float, T};
decode(double, <<Double:64/little-unsigned-float, T/binary>>) -> {Double, T};
decode(string, <<Int32:32/little-signed-integer, T/binary>>) when Int32 == -1 -> {undefined, T};
decode(string, <<Int32:32/little-signed-integer, String:Int32/binary, T/binary>>) -> {String, T};
decode(date_time, Bin) -> decode(int64, Bin);
decode(guid, Bin) -> decode_guid(Bin);
decode(xml, Bin) -> decode(string, Bin);
decode(status_code, Bin) ->
    {Code, Rest} = decode(uint32, Bin),
    {opcua_database_status_codes:name(Code, Code), Rest};
decode(byte_string, Bin) -> decode(string, Bin);
decode(node_id, <<Mask:8, Bin/binary>>) -> decode_node_id(Mask, Bin);
decode(expanded_node_id, <<Mask:8, Bin/binary>>) -> decode_expanded_node_id(Mask, Bin);
decode(diagnostic_info, <<Mask:1/binary, Bin/binary>>) -> decode_diagnostic_info(Mask, Bin);
decode(qualified_name, Bin) -> decode_qualified_name(Bin);
decode(localized_text, <<Mask:1/binary, Bin/binary>>) -> decode_localized_text(Mask, Bin);
decode(extension_object, Bin) -> decode_extension_object(Bin);
decode(variant, <<0:6, Bin/binary>>) -> {undefined, Bin};
decode(variant, <<DimFlag:1/bits, ArrayFlag:1/bits, TypeId:6/little-unsigned-integer, Bin/binary>>) ->
    decode_variant(TypeId, DimFlag, ArrayFlag, Bin);
decode(data_value, <<0:2, Mask:6/bits, Bin/binary>>) -> decode_data_value(Mask, Bin);
decode(_Type, _Bin) -> throw(bad_decoding_error).

encode(boolean, false) -> <<0:8>>;
encode(boolean, true) -> <<1:8>>;
encode(byte, Byte) -> <<Byte:8>>;
encode(sbyte, SByte) -> <<SByte:8/signed-integer>>;
encode(uint16, UInt16) -> <<UInt16:16/little-unsigned-integer>>;
encode(uint32, UInt32) -> <<UInt32:32/little-unsigned-integer>>;
encode(uint64, UInt64) -> <<UInt64:64/little-unsigned-integer>>;
encode(int16, Int16) -> <<Int16:16/little-signed-integer>>;
encode(int32, Int32) -> <<Int32:32/little-signed-integer>>;
encode(int64, Int64) -> <<Int64:64/little-signed-integer>>;
encode(float, Float) -> <<Float:32/little-signed-float>>;
encode(double, Double) -> <<Double:64/little-signed-float>>;
encode(string, String) when String == undefined -> <<-1:32/little-signed-integer>>;
encode(string, String) -> <<(byte_size(String)):32/little-signed-integer, String/binary>>;
encode(date_time, Bin) -> encode(int64, Bin);
encode(guid, Guid) -> encode_guid(Guid);
encode(xml, Bin) -> encode(string, Bin);
encode(status_code, Atom) when is_atom(Atom) ->
    encode(uint32, opcua_database_status_codes:code(Atom));
encode(status_code, UInt32) when is_integer(UInt32), UInt32 >= 0 ->
    encode(uint32, UInt32);
encode(byte_string, Bin) -> encode(string, Bin);
encode(node_id, NodeId) -> encode_node_id(NodeId);
encode(expanded_node_id, ExpandedNodeId) -> encode_expanded_node_id(ExpandedNodeId);
encode(diagnostic_info, DiagnosticInfo) -> encode_diagnostic_info(DiagnosticInfo);
encode(qualified_name, QualifiedName) -> encode_qualified_name(QualifiedName);
encode(localized_text, LocalizedText) -> encode_localized_text(LocalizedText);
encode(extension_object, ExtensionObject) -> encode_extension_object(ExtensionObject);
encode(variant, Variant) -> encode_variant(Variant);
encode(data_value, DataValue) -> encode_data_value(DataValue);
encode(_Type, _Value) -> throw(bad_encoding_error).


%% internal

decode_guid(<<D1:32/little-integer, D2:16/little-integer, D3:16/little-integer, D4:8/binary, T/binary>>) ->
    {<<D1:32/big-integer, D2:16/big-integer, D3:16/big-integer, D4:8/binary>>, T}.

decode_node_id(16#00, <<Id:8/little-unsigned-integer, T/binary>>) ->
    {#node_id{ns = 0, type = numeric, value = Id}, T};
decode_node_id(16#01, <<Ns:8/little-unsigned-integer,
            Id:16/little-unsigned-integer, T/binary>>) ->
    {#node_id{ns = Ns, type = numeric, value = Id}, T};
decode_node_id(16#02, <<Ns:16/little-unsigned-integer, Bin/binary>>) ->
    {Id, T} = decode(uint32, Bin),
    {#node_id{ns = Ns, type = numeric, value = Id}, T};
decode_node_id(16#03, <<Ns:16/little-unsigned-integer, Bin/binary>>) ->
    {Id, T} = decode(string, Bin),
    {#node_id{ns = Ns, type = string, value = Id}, T};
decode_node_id(16#04, <<Ns:16/little-unsigned-integer, Bin/binary>>) ->
    {Id, T} = decode(guid, Bin),
    {#node_id{ns = Ns, type = guid, value = Id}, T};
decode_node_id(16#05, <<Ns:16/little-unsigned-integer, Bin/binary>>) ->
    {Id, T} = decode(string, Bin),
    {#node_id{ns = Ns, type = opaque, value = Id}, T}.

decode_expanded_node_id(Mask, Bin) ->
    {NodeId, T} = decode_node_id(Mask rem 16#40, Bin),
    NamespaceUriFlag = (Mask div 16#80 == 1),
    ServerIndexFlag = ((Mask rem 16#80) div 16#40 == 1),
    Types = [{namespace_uri, string, undefined},
             {server_index, uint32, undefined}],
    BooleanMask = [NamespaceUriFlag, ServerIndexFlag],
    RecordInfo = {expanded_node_id, record_info(fields, expanded_node_id)},
    {ExpandedNodeId, T1} = decode_masked(RecordInfo, BooleanMask, Types, T),
    {ExpandedNodeId#expanded_node_id{node_id = NodeId}, T1}.

decode_diagnostic_info(<<0:1, Mask:7/bits>>, Bin) ->
    Types = [{symbolic_id, int32, undefined},
             {namespace_uri, int32, undefined},
             {locale, int32, undefined},
             {localized_text, int32, undefined},
             {additional_info, string, undefined},
             {inner_status_code, status_code, undefined},
             {inner_diagnostic_info, diagnostic_info, undefined}],
    RecordInfo = {diagnostic_info, record_info(fields, diagnostic_info)},
    decode_masked(RecordInfo, Mask, Types, Bin).

decode_qualified_name(Bin) ->
    {[NamespaceIndex, Name], T}  = decode_multi([uint16, string], Bin),
    {#qualified_name{ns = NamespaceIndex, name = Name}, T}.

decode_localized_text(<<0:6, Mask:2/bits>>, Bin) ->
    Types = [{locale, string, undefined},
             {text, string, undefined}],
    RecordInfo = {localized_text, record_info(fields, localized_text)},
    decode_masked(RecordInfo, Mask, Types, Bin).

decode_extension_object(Bin) ->
    {TypeId, <<Mask:8, T/binary>>} = decode(node_id, Bin),
    decode_extension_object1(Mask, TypeId, T).

decode_extension_object1(16#00, TypeId, T) ->
    {#extension_object{type_id = TypeId, encoding = undefined, body = undefined}, T};
decode_extension_object1(16#01, TypeId, T) ->
    {Body, T1} = decode(byte_string, T),
    {#extension_object{type_id = TypeId, encoding = byte_string, body = Body}, T1};
decode_extension_object1(16#02, TypeId, T) ->
    {Body, T1} = decode(xml, T),
    {#extension_object{type_id = TypeId, encoding = xml, body = Body}, T1}.

decode_variant(TypeId, <<0:1>>, <<0:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {Value, Bin1} = decode(TypeName, Bin),
    {#variant{type = TypeName, value = Value}, Bin1};
decode_variant(TypeId, <<0:1>>, <<1:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {Array, Bin1} = decode_array(TypeName, Bin),
    {#variant{type = TypeName, value = Array}, Bin1};
decode_variant(TypeId, <<1:1>>, <<1:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {MultiArray, Bin1} = decode_multi_array(TypeName, Bin),
    {#variant{type = TypeName, value = MultiArray}, Bin1}.

decode_data_value(Mask, Bin) ->
    Types = [{value, variant, undefined},
             {status, status_code, 0},
             {source_timestamp, date_time, 0},
             {source_pico_seconds, uint16, 0},
             {server_timestamp, date_time, 0},
             {server_pico_seconds, uint16, 0}],
    RecordInfo = {data_value, record_info(fields, data_value)},
    decode_masked(RecordInfo, Mask, Types, Bin).

decode_masked(RecordInfo, Mask, Types, Bin) ->
    BooleanMask = boolean_mask(Mask),
    Apply = lists:filter(fun({_, Cond}) -> Cond end, lists:zip(Types, BooleanMask)),
    Apply1 = element(1, lists:unzip(Apply)),
    Defaults = lists:map(fun({Name, _, Default}) -> {Name, Default} end, Types -- Apply1),
    {List, T} = decode_multi(lists:map(fun({_,Type,_}) -> Type end, Apply1), Bin),
    FinalApply = lists:zip(lists:map(fun({Name,_,_}) -> Name end, Apply1), List),
    {to_record(RecordInfo, FinalApply ++ Defaults), T}.

to_record({RecordName, RecordFields}, Proplist) ->
    Values = [proplists:get_value(Key, Proplist) || Key <- RecordFields],
    list_to_tuple([RecordName | Values]).

boolean_mask(Mask) when is_bitstring(Mask) ->
    lists:reverse([X==1 || <<X:1>> <= Mask]);
boolean_mask(Mask) ->
    Mask.

decode_multi(TypeList, Bin) ->
    decode_multi(TypeList, Bin, []).

decode_multi([], T, Acc) ->
    {lists:reverse(Acc), T};
decode_multi([Type|TypeList], Bin, Acc) ->
    {Elem, T} = decode(Type, Bin),
    decode_multi(TypeList, T, [Elem|Acc]).

decode_multi_array(Type, Bin) ->
    {ObjectArray, T} = decode_array(Type, Bin),
    {DimArray, T1} = decode_array(int32, T),
    {honor_dimensions(ObjectArray, DimArray), T1}.

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

honor_dimensions(ObjectArray, DimArray) ->
    lists:reverse(
      lists:foldl(fun(X, Acc) -> honor_dimensions1(X, Acc) end,
                  {[], ObjectArray}, DimArray)).

honor_dimensions1(Dim, {Array, ArrayList}) ->
    {El, NewArray} = lists:split(Dim, Array),
    {NewArray, [El|ArrayList]}.

encode_guid(<<D1:32/big-integer, D2:16/big-integer, D3:16/big-integer, D4:8/binary>>) ->
    <<D1:32/little-integer, D2:16/little-integer, D3:16/little-integer, D4:8/binary>>.

encode_node_id(#node_id{ns = 0, type = numeric, value = Id}) when Id < 256 ->
    <<16#00:8, Id:8/little-unsigned-integer>>;
encode_node_id(#node_id{ns = Ns, type = numeric, value = Id}) when Id < 65536 ->
    <<16#01:8, Ns:8/little-unsigned-integer, Id:16/little-unsigned-integer>>;
encode_node_id(#node_id{ns = Ns, type = numeric, value = Id}) when Id < 4294967296 ->
    BinId = encode(uint32, Id),
    <<16#02:8, Ns:16/little-unsigned-integer, BinId/binary>>;
encode_node_id(#node_id{ns = Ns, type = string, value = Id}) ->
    BinId = encode(string, Id),
    <<16#03:8, Ns:16/little-unsigned-integer, BinId/binary>>;
encode_node_id(#node_id{ns = Ns, type = guid, value = Id}) ->
    BinId = encode(guid, Id),
    <<16#04:8, Ns:16/little-unsigned-integer, BinId/binary>>;
encode_node_id(#node_id{ns = Ns, type = opaque, value = Id}) ->
    BinId = encode(string, Id),
    <<16#05:8, Ns:16/little-unsigned-integer, BinId/binary>>;
encode_node_id(NodeSpec) ->
    encode_node_id(opcua_codec:node_id(NodeSpec)).

encode_expanded_node_id(#expanded_node_id{node_id = NodeId,
                                          namespace_uri = NamespaceUri,
                                          server_index = ServerIndex}) ->
    <<Mask:8, Rest/binary>> = encode_node_id(NodeId),
    {NamespaceUriFlag, BinNamespaceUri}
        = case NamespaceUri of
              undefined -> {0, <<>>};
              _ -> {16#80, encode(string, NamespaceUri)}
          end,
    {ServerIndexFlag, BinServerIndex}
        = case ServerIndex of
              undefined -> {0, <<>>};
              _ -> {16#40, encode(uint32, ServerIndex)}
          end,
    <<(Mask + NamespaceUriFlag + ServerIndexFlag):8, Rest/binary,
      BinNamespaceUri/binary, BinServerIndex/binary>>.

encode_diagnostic_info(DI = #diagnostic_info{}) ->
    Types = [{int32, DI#diagnostic_info.symbolic_id},
             {int32, DI#diagnostic_info.namespace_uri},
             {int32, DI#diagnostic_info.locale},
             {int32, DI#diagnostic_info.localized_text},
             {string, DI#diagnostic_info.additional_info},
             {status_code, DI#diagnostic_info.inner_status_code},
             {diagnostic_info, DI#diagnostic_info.inner_diagnostic_info}],
    encode_masked(Types).

encode_qualified_name(#qualified_name{ns = NamespaceIndex, name = Name}) ->
    encode_multi([{uint16, NamespaceIndex}, {string, Name}]).

encode_localized_text(LocalizedText) when is_binary(LocalizedText) ->
    encode_localized_text(#localized_text{locale = undefined, text = LocalizedText});
encode_localized_text(#localized_text{locale = Locale, text = Text}) ->
    Types = [{string, Locale}, {string, Text}],
    encode_masked(Types).

encode_extension_object(#extension_object{type_id = TypeId, encoding = undefined, body = undefined}) ->
    NodeId = encode(node_id, TypeId),
    <<NodeId/binary, 16#00:8>>;
encode_extension_object(#extension_object{type_id = TypeId, encoding = Encoding, body = Body}) ->
    NodeId = encode(node_id, TypeId),
    %% NOTE: encoding byte strings and xml also
    %% encodes the 'Length' of those as prefix
    BinBody = encode(Encoding, Body),
    EncodingFlag = case Encoding of
                        byte_string -> 16#01;
                        xml -> 16#02
                    end,
    <<NodeId/binary, EncodingFlag:8, BinBody/binary>>.

encode_variant(#variant{type = Type, value = MultiArray})
  when is_list(MultiArray) ->
    TypeId = opcua_codec:builtin_type_id(Type),
    {Length, Value, Dims} = build_variant_value(Type, MultiArray),
    case Length of
        0 ->
            <<0:2, TypeId:6>>;
        Length when Length > 0 ->
            BinLength = encode(int32, Length),
            BinDimLength = encode(int32, length(Dims)),
            BinDims = list_to_binary([encode(int32, Dim) || Dim <- Dims]),
            <<3:2, TypeId:6, BinLength/binary, Value/binary,
              BinDimLength/binary, BinDims/binary>>
    end;
encode_variant(#variant{type = Type, value = Value}) ->
    TypeId = opcua_codec:builtin_type_id(Type),
    BinValue = encode(Type, Value),
    <<0:2, TypeId:6, BinValue/binary>>.

build_variant_value(Type, MultiArray) ->
    build_variant_value(Type, MultiArray, 0, <<>>, []).

build_variant_value(_Type, [], Length, Value, Dims) ->
    {Length, Value, Dims};
build_variant_value(Type, [Elem|T], Length, Value, Dims) ->
    {ArrayLength, ArrayBin} =
        case Elem of
            Elem when is_list(Elem) ->
                {BinElemList, _} = lists:unzip([encode(Type, El) || El <- Elem]),
                {length(Elem), BinElemList};
            _ ->
                {BinElem, _} = encode(Type, Elem),
                {1, BinElem}
        end,
    build_variant_value(Type, T, Length + ArrayLength,
                        <<Value/binary, ArrayBin/binary>>,
                        [ArrayLength|Dims]).

encode_data_value(DataValue) ->
    Types = [{variant,      DataValue#data_value.value},
             {status_code,  maybe_undefined(DataValue#data_value.status, 0)},
             {date_time,    maybe_undefined(DataValue#data_value.source_timestamp, 0)},
             {uint16,       maybe_undefined(DataValue#data_value.source_pico_seconds, 0)},
             {date_time,    maybe_undefined(DataValue#data_value.server_timestamp, 0)},
             {uint16,       maybe_undefined(DataValue#data_value.server_pico_seconds, 0)}],
    encode_masked(Types).

maybe_undefined(Value, Value) -> undefined;
maybe_undefined(Value, _Cond) -> Value.

encode_multi(TypeList) ->
    encode_multi(TypeList, <<>>).

encode_multi([], Bin) ->
    Bin;
encode_multi([{Type, Value}|TypeList], Bin) ->
    NewBin = encode(Type, Value),
    encode_multi(TypeList, <<Bin/binary, NewBin/binary>>).

encode_masked(TypeList) ->
    encode_masked(TypeList, <<>>, <<>>).

encode_masked([], Mask, Bin) ->
    <<0:(8-bit_size(Mask)), Mask/bits, Bin/binary>>;
encode_masked([{_Type, undefined}|TypeList], Mask, Bin) ->
    encode_masked(TypeList, <<0:1, Mask/bits>>, Bin);
encode_masked([{Type, Value}|TypeList], Mask, Bin) ->
    NewBin = encode(Type, Value),
    encode_masked(TypeList, <<1:1, Mask/bits>>, <<Bin/binary, NewBin/binary>>).
