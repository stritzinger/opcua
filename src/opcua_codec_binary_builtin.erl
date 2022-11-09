-module(opcua_codec_binary_builtin).

-include("opcua.hrl").
-include("opcua_internal.hrl").

-export([encode/2, decode/2]).

decode(boolean, <<0, T/binary>>) ->
    {false, T};
decode(boolean, <<_Bin:1/binary, T/binary>>) ->
    {true, T};
decode(byte, <<Byte:8, T/binary>>) ->
    {Byte, T};
decode(sbyte, <<SByte:8/signed-integer, T/binary>>) ->
    {SByte, T};
decode(uint16, <<UInt16:16/little-unsigned-integer, T/binary>>) ->
    {UInt16, T};
decode(uint32, <<UInt32:32/little-unsigned-integer, T/binary>>) ->
    {UInt32, T};
decode(uint64, <<UInt64:64/little-unsigned-integer, T/binary>>) ->
    {UInt64, T};
decode(int16, <<Int16:16/little-signed-integer, T/binary>>) ->
    {Int16, T};
decode(int32, <<Int32:32/little-signed-integer, T/binary>>) ->
    {Int32, T};
decode(int64, <<Int64:64/little-signed-integer, T/binary>>) ->
    {Int64, T};
decode(float, <<Float:32/little-signed-float, T/binary>>) ->
    {Float, T};
decode(double, <<Double:64/little-unsigned-float, T/binary>>) ->
    {Double, T};
decode(string, <<Int32:32/little-signed-integer, T/binary>>)
  when Int32 == -1 ->
    {undefined, T};
decode(string, <<Int32:32/little-signed-integer,
                 String:Int32/binary, T/binary>>) ->
    {String, T};
decode(date_time, Bin) ->
    decode(int64, Bin);
decode(guid, Bin) ->
    decode_guid(Bin);
decode(xml, Bin) ->
    decode(string, Bin);
decode(status_code, Bin) ->
    {Code, Rest} = decode(uint32, Bin),
    {opcua_database_status_codes:name(Code, Code), Rest};
decode(byte_string, Bin) ->
    decode(string, Bin);
decode(node_id, <<Mask:8, Bin/binary>>) ->
    decode_node_id(Mask, Bin);
decode(expanded_node_id, <<Mask:8, Bin/binary>>) ->
    decode_expanded_node_id(Mask, Bin);
decode(diagnostic_info, <<Mask:1/binary, Bin/binary>>) ->
    decode_diagnostic_info(Mask, Bin);
decode(qualified_name, Bin) ->
    decode_qualified_name(Bin);
decode(localized_text, <<Mask:1/binary, Bin/binary>>) ->
    decode_localized_text(Mask, Bin);
decode(_Type, _Bin) ->
    throw(bad_decoding_error).

encode(boolean, false) ->
    <<0:8>>;
encode(boolean, true) ->
    <<1:8>>;
encode(byte, Byte) when is_integer(Byte) ->
    <<Byte:8>>;
encode(sbyte, SByte) when is_integer(SByte)->
    <<SByte:8/signed-integer>>;
encode(uint16, UInt16) when is_integer(UInt16)->
    <<UInt16:16/little-unsigned-integer>>;
encode(uint32, UInt32) when is_integer(UInt32)->
    <<UInt32:32/little-unsigned-integer>>;
encode(uint64, UInt64) when is_integer(UInt64)->
    <<UInt64:64/little-unsigned-integer>>;
encode(int16, Int16) when is_integer(Int16)->
    <<Int16:16/little-signed-integer>>;
encode(int32, Int32) when is_integer(Int32)->
    <<Int32:32/little-signed-integer>>;
encode(int64, Int64) when is_integer(Int64)->
    <<Int64:64/little-signed-integer>>;
encode(float, Float) when is_float(Float)->
    <<Float:32/little-signed-float>>;
encode(double, Double) when is_float(Double) ->
    <<Double:64/little-signed-float>>;
encode(double, Int) when is_integer(Int) ->
    <<Int:64/little-signed-float>>;
encode(string, String) when String == undefined ->
    <<-1:32/little-signed-integer>>;
encode(string, Atom) when is_atom(Atom) ->
    encode(string, atom_to_binary(Atom));
encode(string, String) when is_binary(String) ->
    <<(byte_size(String)):32/little-signed-integer, String/binary>>;
encode(date_time, DateTime) when is_integer(DateTime) ->
    encode(int64, DateTime);
encode(guid, Guid) when is_binary(Guid) ->
    encode_guid(Guid);
encode(xml, Bin) when is_binary(Bin) ->
    encode(string, Bin);
encode(status_code, Atom) when is_atom(Atom) ->
    encode(uint32, opcua_database_status_codes:code(Atom));
encode(status_code, UInt32) when is_integer(UInt32), UInt32 >= 0 ->
    encode(uint32, UInt32);
encode(byte_string, Bin) ->
    encode(string, Bin);
encode(node_id, NodeId) ->
    encode_node_id(NodeId);
encode(expanded_node_id, ExpandedNodeId) ->
    encode_expanded_node_id(ExpandedNodeId);
encode(diagnostic_info, DiagnosticInfo) ->
    encode_diagnostic_info(DiagnosticInfo);
encode(qualified_name, QualifiedName) ->
    encode_qualified_name(QualifiedName);
encode(localized_text, LocalizedText) ->
    encode_localized_text(LocalizedText);
encode(Type, Value) ->
    throw({bad_encoding_error, Type, Value}).


%% internal

decode_guid(<<D1:32/little-integer, D2:16/little-integer, D3:16/little-integer, D4:8/binary, T/binary>>) ->
    {<<D1:32/big-integer, D2:16/big-integer, D3:16/big-integer, D4:8/binary>>, T}.

decode_node_id(16#00, <<Id:8/little-unsigned-integer, T/binary>>) ->
    {#opcua_node_id{ns = 0, type = numeric, value = Id}, T};
decode_node_id(16#01, <<Ns:8/little-unsigned-integer,
            Id:16/little-unsigned-integer, T/binary>>) ->
    {#opcua_node_id{ns = Ns, type = numeric, value = Id}, T};
decode_node_id(16#02, <<Ns:16/little-unsigned-integer, Bin/binary>>) ->
    {Id, T} = decode(uint32, Bin),
    {#opcua_node_id{ns = Ns, type = numeric, value = Id}, T};
decode_node_id(16#03, <<Ns:16/little-unsigned-integer, Bin/binary>>) ->
    {Id, T} = decode(string, Bin),
    {#opcua_node_id{ns = Ns, type = string, value = Id}, T};
decode_node_id(16#04, <<Ns:16/little-unsigned-integer, Bin/binary>>) ->
    {Id, T} = decode(guid, Bin),
    {#opcua_node_id{ns = Ns, type = guid, value = Id}, T};
decode_node_id(16#05, <<Ns:16/little-unsigned-integer, Bin/binary>>) ->
    {Id, T} = decode(string, Bin),
    {#opcua_node_id{ns = Ns, type = opaque, value = Id}, T}.

decode_expanded_node_id(Mask, Bin) ->
    {NodeId, T} = decode_node_id(Mask rem 16#40, Bin),
    NamespaceUriFlag = (Mask div 16#80 == 1),
    ServerIndexFlag = ((Mask rem 16#80) div 16#40 == 1),
    Types = [{namespace_uri, string, undefined},
             {server_index, uint32, undefined}],
    BooleanMask = [NamespaceUriFlag, ServerIndexFlag],
    RecordInfo = {opcua_expanded_node_id, record_info(fields, opcua_expanded_node_id)},
    {ExpandedNodeId, T1} = decode_masked(RecordInfo, BooleanMask, Types, T),
    {ExpandedNodeId#opcua_expanded_node_id{node_id = NodeId}, T1}.

decode_diagnostic_info(<<0:1, Mask:7/bits>>, Bin) ->
    Types = [{symbolic_id, int32, undefined},
             {namespace_uri, int32, undefined},
             {locale, int32, undefined},
             {localized_text, int32, undefined},
             {additional_info, string, undefined},
             {inner_status_code, status_code, undefined},
             {inner_diagnostic_info, diagnostic_info, undefined}],
    RecordInfo = {opcua_diagnostic_info, record_info(fields, opcua_diagnostic_info)},
    decode_masked(RecordInfo, Mask, Types, Bin).

decode_qualified_name(Bin) ->
    {[NamespaceIndex, Name], T}  = decode_multi([uint16, string], Bin),
    {#opcua_qualified_name{ns = NamespaceIndex, name = Name}, T}.

decode_localized_text(<<0:6, Mask:2/bits>>, Bin) ->
    Types = [{locale, string, undefined},
             {text, string, undefined}],
    RecordInfo = {opcua_localized_text, record_info(fields, opcua_localized_text)},
    decode_masked(RecordInfo, Mask, Types, Bin).

decode_multi(TypeList, Bin) ->
    decode_multi(TypeList, Bin, []).

decode_multi([], T, Acc) ->
    {lists:reverse(Acc), T};
decode_multi([Type|TypeList], Bin, Acc) ->
    {Elem, T} = decode(Type, Bin),
    decode_multi(TypeList, T, [Elem|Acc]).

decode_masked(RecordInfo, Mask, Types, Bin) ->
    BooleanMask = boolean_mask(Mask),
    Apply = lists:filter(fun({_, Cond}) -> Cond end, lists:zip(Types, BooleanMask)),
    Apply1 = element(1, lists:unzip(Apply)),
    Defaults = lists:map(fun({Name, _, Default}) -> {Name, Default} end, Types -- Apply1),
    {List, T} = decode_multi(lists:map(fun({_,Type,_}) -> Type end, Apply1), Bin),
    FinalApply = lists:zip(lists:map(fun({Name,_,_}) -> Name end, Apply1), List),
    {to_record(RecordInfo, FinalApply ++ Defaults), T}.

boolean_mask(Mask) when is_bitstring(Mask) ->
    lists:reverse([X==1 || <<X:1>> <= Mask]);
boolean_mask(Mask) ->
    Mask.

to_record({RecordName, RecordFields}, Proplist) ->
    Values = [proplists:get_value(Key, Proplist) || Key <- RecordFields],
    list_to_tuple([RecordName | Values]).

encode_guid(<<D1:32/big-integer, D2:16/big-integer, D3:16/big-integer, D4:8/binary>>) ->
    <<D1:32/little-integer, D2:16/little-integer, D3:16/little-integer, D4:8/binary>>.

encode_node_id(#opcua_node_id{ns = 0, type = numeric, value = Id}) when Id < 256 ->
    <<16#00:8, Id:8/little-unsigned-integer>>;
encode_node_id(#opcua_node_id{ns = Ns, type = numeric, value = Id}) when Id < 65536 ->
    <<16#01:8, Ns:8/little-unsigned-integer, Id:16/little-unsigned-integer>>;
encode_node_id(#opcua_node_id{ns = Ns, type = numeric, value = Id}) when Id < 4294967296 ->
    BinId = encode(uint32, Id),
    [<<16#02:8, Ns:16/little-unsigned-integer>>, BinId];
encode_node_id(#opcua_node_id{ns = Ns, type = string, value = Id}) ->
    BinId = encode(string, Id),
    [<<16#03:8, Ns:16/little-unsigned-integer>>, BinId];
encode_node_id(#opcua_node_id{ns = Ns, type = guid, value = Id}) ->
    BinId = encode(guid, Id),
    [<<16#04:8, Ns:16/little-unsigned-integer>>, BinId];
encode_node_id(#opcua_node_id{ns = Ns, type = opaque, value = Id}) ->
    BinId = encode(string, Id),
    [<<16#05:8, Ns:16/little-unsigned-integer>>, BinId];
encode_node_id(NodeSpec) ->
    encode_node_id(opcua_codec:node_id(NodeSpec)).

encode_expanded_node_id(#opcua_node_id{} = NodeId) ->
    encode_expanded_node_id(#opcua_expanded_node_id{node_id = NodeId});
encode_expanded_node_id(#opcua_expanded_node_id{node_id = #opcua_node_id{ns = NS} = NodeId,
                                          namespace_uri = NamespaceUri,
                                          server_index = ServerIndex}) ->
    {NamespaceUriFlag, BinNamespaceUri, NS2}
        = case NamespaceUri of
              undefined -> {0, <<>>, NS};
              _ -> {16#80, encode(string, NamespaceUri), 0}
          end,
    {ServerIndexFlag, BinServerIndex}
        = case ServerIndex of
              undefined -> {0, <<>>};
              _ -> {16#40, encode(uint32, ServerIndex)}
          end,
    EncNodeId = encode_node_id(NodeId#opcua_node_id{ns = NS2}),
    <<Mask:8, Rest/binary>> = iolist_to_binary(EncNodeId),
    [<<(Mask + NamespaceUriFlag + ServerIndexFlag):8>>, Rest,
     BinNamespaceUri, BinServerIndex];
encode_expanded_node_id(#opcua_expanded_node_id{node_id = NodeSpec} = ExtNodeId) ->
    NodeId = opcua_codec:node_id(NodeSpec),
    encode_expanded_node_id(ExtNodeId#opcua_expanded_node_id{node_id = NodeId});
encode_expanded_node_id(NodeSpec) ->
    encode_expanded_node_id(opcua_codec:node_id(NodeSpec)).

encode_diagnostic_info(DI = #opcua_diagnostic_info{}) ->
    Types = [{int32, DI#opcua_diagnostic_info.symbolic_id},
             {int32, DI#opcua_diagnostic_info.namespace_uri},
             {int32, DI#opcua_diagnostic_info.locale},
             {int32, DI#opcua_diagnostic_info.localized_text},
             {string, DI#opcua_diagnostic_info.additional_info},
             {status_code, DI#opcua_diagnostic_info.inner_status_code},
             {diagnostic_info, DI#opcua_diagnostic_info.inner_diagnostic_info}],
    encode_masked(Types).

encode_qualified_name(#opcua_qualified_name{ns = NamespaceIndex, name = Name}) ->
    encode_multi([{uint16, NamespaceIndex}, {string, Name}]).

encode_localized_text(LocalizedText) when is_binary(LocalizedText) ->
    encode_localized_text(#opcua_localized_text{locale = undefined, text = LocalizedText});
encode_localized_text(#opcua_localized_text{locale = Locale, text = Text}) ->
    Types = [{string, Locale}, {string, Text}],
    encode_masked(Types).

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
