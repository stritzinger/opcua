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
decode(_Type, _Bin) -> error('Bad_DecodingError').

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
encode(status_code, UInt32) -> encode(uint32, UInt32);
encode(byte_string, Bin) -> encode(string, Bin);
encode(node_id, Node_Id) -> encode_node_id(Node_Id);
encode(expanded_node_id, Expanded_Node_Id) -> encode_expanded_node_id(Expanded_Node_Id);
encode(diagnostic_info, Diagnostic_Info) -> encode_diagnostic_info(Diagnostic_Info);
encode(qualified_name, Qualified_Name) -> encode_qualified_name(Qualified_Name);
encode(localized_text, Localized_Text) -> encode_localized_text(Localized_Text);
encode(extension_object, Extension_Object) -> encode_extension_object(Extension_Object);
encode(variant, Variant) -> encode_variant(Variant);
encode(data_value, Data_Value) -> encode_data_value(Data_Value);
encode(_Type, _Value) -> error('Bad_EncodingError').


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
    {Node_Id, T} = decode_node_id(Mask rem 16#40, Bin),
    Namespace_Uri_Flag = (Mask div 16#80 == 1),
    Server_Index_Flag = ((Mask rem 16#80) div 16#40 == 1),
    Types = [{namespace_uri, string, undefined},
             {server_index, uint32, undefined}],
    Boolean_Mask = [Namespace_Uri_Flag, Server_Index_Flag],
    RecordInfo = {expanded_node_id, record_info(fields, expanded_node_id)},
    {ExpandedNodeId, T1} = decode_masked(RecordInfo, Boolean_Mask, Types, T),
    {ExpandedNodeId#expanded_node_id{node_id = Node_Id}, T1}.

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
    {[Namespace_Index, Name], T}  = decode_multi([uint16, string], Bin),
    {#qualified_name{namespace_index = Namespace_Index, name = Name}, T}.

decode_localized_text(<<0:6, Mask:2/bits>>, Bin) ->
    Types = [{locale, string, undefined},
             {text, string, undefined}],
    RecordInfo = {localized_text, record_info(fields, localized_text)},
    decode_masked(RecordInfo, Mask, Types, Bin).

decode_extension_object(Bin) ->
    {Type_Id, <<Mask:8, T/binary>>} = decode(node_id, Bin),
    decode_extension_object1(Mask, Type_Id, T).

decode_extension_object1(16#00, Type_Id, T) ->
    {#extension_object{type_id = Type_Id, encoding = undefined, body = undefined}, T};
decode_extension_object1(16#01, Type_Id, T) ->
    {Body, T1} = decode(byte_string, T),
    {#extension_object{type_id = Type_Id, encoding = byte_string, body = Body}, T1};
decode_extension_object1(16#02, Type_Id, T) ->
    {Body, T1} = decode(xml, T),
    {#extension_object{type_id = Type_Id, encoding = xml, body = Body}, T1}.

decode_variant(Type_Id, _Dim_Flag, <<0:1>>, Bin) ->
    {#variant{type = opcua_codec:builtin_type_name(Type_Id), value = []}, Bin};
decode_variant(Type_Id, <<0:1>>, <<1:1>>, Bin) ->
    {Array, Bin1} = decode_array(opcua_codec:builtin_type_name(Type_Id), Bin),
    {#variant{type = opcua_codec:builtin_type_name(Type_Id), value = Array}, Bin1};
decode_variant(Type_Id, <<1:1>>, <<1:1>>, Bin) ->
    {Multi_Array, Bin1} = decode_multi_array(opcua_codec:builtin_type_name(Type_Id), Bin),
    {#variant{type = opcua_codec:builtin_type_name(Type_Id), value = Multi_Array}, Bin1}.

decode_data_value(Mask, Bin) ->
    Types = [{value, variant, undefined},
             {status, status_code, good},
             {source_timestamp, date_time, 0},
             {source_pico_seconds, uint16, 0},
             {server_timestamp, date_time, 0},
             {server_pico_seconds, uint16, 0}],
    RecordInfo = {data_value, record_info(fields, data_value)},
    decode_masked(RecordInfo, Mask, Types, Bin).

decode_masked(RecordInfo, Mask, Types, Bin) ->
    Boolean_Mask = boolean_mask(Mask),
    {Apply, Defaults} = lists:splitwith(fun({_, Cond}) -> Cond end,
                        lists:zip(Types, Boolean_Mask)),
    Apply1 = element(1, lists:unzip(Apply)),
    Defaults1 = element(1, lists:unzip(Defaults)),
    Final_Defaults = lists:map(fun({Name, _, Default}) -> {Name, Default} end, Defaults1),
    {List, T} = decode_multi(lists:map(fun({_,Type,_}) -> Type end, Apply1), Bin),
    Final_Apply = lists:zip(lists:map(fun({Name,_,_}) -> Name end, Apply1), List),
    {to_record(RecordInfo, Final_Apply ++ Final_Defaults), T}.

to_record({RecordName, RecordFields}, Proplist) ->
    Values = [proplists:get_value(Key, Proplist) || Key <- RecordFields],
    list_to_tuple([RecordName | Values]).

boolean_mask(Mask) when is_bitstring(Mask) ->
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

encode_guid(<<D1:32/big-integer, D2:16/big-integer, D3:16/big-integer, D4:8/binary>>) ->
    <<D1:32/little-integer, D2:16/little-integer, D3:16/little-integer, D4:8/binary>>.

encode_node_id(#node_id{ns = 0, type = numeric, value = Id}) when Id < 256 ->
    <<16#00:8, Id:8/little-unsigned-integer>>;
encode_node_id(#node_id{ns = Ns, type = numeric, value = Id}) when Id < 65536 ->
    <<16#01:8, Ns:8/little-unsigned-integer, Id:16/little-unsigned-integer>>;
encode_node_id(#node_id{ns = Ns, type = numeric, value = Id}) when Id < 4294967296 ->
    Bin_Id = encode(uint32, Id),
    <<16#02:8, Ns:16/little-unsigned-integer, Bin_Id/binary>>;
encode_node_id(#node_id{ns = Ns, type = string, value = Id}) ->
    Bin_Id = encode(string, Id),
    <<16#03:8, Ns:16/little-unsigned-integer, Bin_Id/binary>>;
encode_node_id(#node_id{ns = Ns, type = guid, value = Id}) ->
    Bin_Id = encode(guid, Id),
    <<16#04:8, Ns:16/little-unsigned-integer, Bin_Id/binary>>;
encode_node_id(#node_id{ns = Ns, type = opaque, value = Id}) ->
    Bin_Id = encode(string, Id),
    <<16#05:8, Ns:16/little-unsigned-integer, Bin_Id/binary>>;
encode_node_id(NodeSpec) ->
    encode_node_id(opcua_codec:node_id(NodeSpec)).

encode_expanded_node_id(#expanded_node_id{node_id = Node_Id,
                                          namespace_uri = Namespace_Uri,
                                          server_index = Server_Index}) ->
    <<Mask:8, Rest/binary>> = encode_node_id(Node_Id),
    {Namespace_Uri_Flag, Bin_Namespace_Uri}
        = case Namespace_Uri of
              undefined -> {0, <<>>};
              _ -> {16#80, encode(string, Namespace_Uri)}
          end,
    {Server_Index_Flag, Bin_Server_Index}
        = case Server_Index of
              undefined -> {0, <<>>};
              _ -> {16#40, encode(uint32, Server_Index)}
          end,
    <<(Mask + Namespace_Uri_Flag + Server_Index_Flag):8, Rest/binary,
       Bin_Namespace_Uri/binary, Bin_Server_Index/binary>>.

encode_diagnostic_info(DI = #diagnostic_info{}) ->
    Types = [{int32, DI#diagnostic_info.symbolic_id},
             {int32, DI#diagnostic_info.namespace_uri},
             {int32, DI#diagnostic_info.locale},
             {int32, DI#diagnostic_info.localized_text},
             {string, DI#diagnostic_info.additional_info},
             {status_code, DI#diagnostic_info.inner_status_code},
             {diagnostic_info, DI#diagnostic_info.inner_diagnostic_info}],
    encode_masked(Types).

encode_qualified_name(#qualified_name{namespace_index = Namespace_Index, name = Name}) ->
    encode_multi([{uint16, Namespace_Index}, {string, Name}]).

encode_localized_text(Localized_Text) when is_binary(Localized_Text) ->
    encode_localized_text(#localized_text{locale = undefined, text = Localized_Text});
encode_localized_text(#localized_text{locale = Locale, text = Text}) ->
    Types = [{string, Locale}, {string, Text}],
    encode_masked(Types).

encode_extension_object(#extension_object{type_id = Type_Id, encoding = undefined, body = undefined}) ->
    Node_Id = encode(node_id, Type_Id),
    <<Node_Id/binary, 16#00:8>>;
encode_extension_object(#extension_object{type_id = Type_Id, encoding = Encoding, body = Body}) ->
    Node_Id = encode(node_id, Type_Id),
    %% NOTE: encoding byte strings and xml also
    %% encodes the 'Length' of those as prefix
    Bin_Body = encode(Encoding, Body),
    Encoding_Flag = case Encoding of
                        byte_string -> 16#01;
                        xml -> 16#02
                    end,
    <<Node_Id/binary, Encoding_Flag:8, Bin_Body/binary>>.

encode_variant(#variant{type = Type, value = Multi_Array}) ->
    Type_Id = opcua_codec:builtin_type_id(Type),
    {Length, Value, Dims} = build_variant_value(Multi_Array),
    case Length of
        0 ->
            <<0:2, Type_Id:6>>;
        Length when Length > 0 ->
            Bin_Length = encode(int32, Length),
            Bin_Dim_Length = encode(int32, length(Dims)),
            Bin_Dims = list_to_binary([encode(int32, Dim) || Dim <- Dims]),
            <<3:2, Type_Id:6, Bin_Length/binary, Value/binary,
              Bin_Dim_Length/binary, Bin_Dims/binary>>
    end.

build_variant_value(Multi_Array) ->
    build_variant_value(Multi_Array, 0, <<>>, []).

build_variant_value([], Length, Value, Dims) ->
    {Length, Value, Dims};
build_variant_value([Elem|T], Length, Value, Dims) ->
    {Array_Length, Array_Bin} =
        case Elem of
            Elem when is_list(Elem) ->
                {length(Elem), list_to_binary(Elem)};
            _ ->
                {1, Elem}
        end,
    build_variant_value(T, Length + Array_Length,
                <<Value/binary, Array_Bin/binary>>,
                [Array_Length|Dims]).

encode_data_value(Data_Value) ->
    Types = [{variant, maps:get(value, Data_Value, undefined)},
             {status_code, maybe_undefined(status, Data_Value, good)},
             {date_time, maybe_undefined(source_timestamp, Data_Value, 0)},
             {uint16, maybe_undefined(source_pico_seconds, Data_Value, 0)},
             {date_time, maybe_undefined(server_timestamp, Data_Value, 0)},
             {uint16, maybe_undefined(server_pico_seconds, Data_Value, 0)}],
    encode_masked(Types).

maybe_undefined(Key, Map, Cond) ->
    case maps:get(Key, Map, undefined) of
        Cond ->
            undefined;
        Value ->
            Value
    end.

encode_multi(Type_List) ->
    encode_multi(Type_List, <<>>).

encode_multi([], Bin) ->
    Bin;
encode_multi([{Type, Value}|Type_List], Bin) ->
    NewBin = encode(Type, Value),
    encode_multi(Type_List, <<Bin/binary, NewBin/binary>>).

encode_masked(Type_List) ->
    encode_masked(Type_List, <<>>, <<>>).

encode_masked([], Mask, Bin) ->
    <<0:(8-bit_size(Mask)), Mask/bits, Bin/binary>>;
encode_masked([{_Type, undefined}|Type_List], Mask, Bin) ->
    encode_masked(Type_List, <<0:1, Mask/bits>>, Bin);
encode_masked([{Type, Value}|Type_List], Mask, Bin) ->
    NewBin = encode(Type, Value),
    encode_masked(Type_List, <<1:1, Mask/bits>>, <<Bin/binary, NewBin/binary>>).
