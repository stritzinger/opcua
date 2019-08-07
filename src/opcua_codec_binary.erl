-module(opcua_codec_binary).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua_codec.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([decode/2]).
-export([encode/2]).

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decode(opcua_spec(), binary()) -> {term(), binary()}.
decode(Spec, Data) -> decode_type(Spec, iolist_to_binary(Data)).

-spec encode(opcua_spec(), term()) -> {iolist(), term()}.
encode(Spec, Data) -> encode_type(Spec, Data).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% decoding

decode_type([{Key, _} | _] = Spec, Data) when is_atom(Key) ->
    decode_map(Spec, Data);
decode_type(Spec, Data) when is_list(Spec) ->
    decode_list(Spec, Data);
decode_type(#node_id{type = string, value = Name}, Data)
  when ?IS_BUILTIN_TYPE_NAME(Name) ->
    decode_builtin(Name, Data);
decode_type(#node_id{type = numeric, value = Num}, Data)
  when ?IS_BUILTIN_TYPE_ID(Num) ->
    decode_builtin(opcua_codec:builtin_type_name(Num), Data);
decode_type(#node_id{} = NodeId, Data) ->
    decode_schema(opcua_database:lookup_schema(NodeId), Data);
decode_type(NodeSpec, Data) ->
    decode_type(opcua_codec:node_id(NodeSpec), Data).

decode_map(Spec, Data) ->
    decode_map(Spec, Data, #{}).

decode_map([], Data, Acc) ->
    {Acc, Data};
decode_map([{Key, Spec} | Rest], Data, Acc) when is_atom(Key) ->
    {Value, Data2}  = decode_type(Spec, Data),
    decode_map(Rest, Data2, Acc#{Key => Value}).

decode_list(Specs, Data) ->
    decode_list(Specs, Data, []).

decode_list([], Data, Acc) ->
    {lists:reverse(Acc), Data};
decode_list([Spec | Rest], Data, Acc) ->
    {Value, Data2}  = decode_type(Spec, Data),
    decode_list(Rest, Data2, [Value | Acc]).

decode_builtin(extension_object, Data) -> decode_extension_object(Data);
decode_builtin(variant, Data) -> decode_variant(Data);
decode_builtin(data_value, Data) -> decode_data_value(Data);
decode_builtin(Type, Data) -> opcua_codec_binary_builtin:decode(Type, Data).

decode_schema(#structure{with_options = false, fields = Fields}, Data) ->
    decode_fields(Fields, Data);
decode_schema(#structure{with_options = true, fields = Fields}, Data) ->
    {Mask, Data1} = decode_builtin(uint32, Data),
    decode_masked_fields(Mask, Fields, Data1);
decode_schema(#union{fields = Fields}, Data) ->
    {SwitchValue, Data1} = decode_builtin(uint32, Data),
    resolve_union_value(SwitchValue, Fields, Data1);
decode_schema(#enum{fields = Fields}, Data) ->
    {Value, Data1} = decode_builtin(int32, Data),
    {resolve_enum_value(Value, Fields), Data1};
decode_schema(#builtin{builtin_node_id = BuiltinNodeId}, Data) ->
    decode_type(BuiltinNodeId, Data).

decode_fields(Fields, Data) ->
    decode_fields(Fields, Data, #{}).

decode_fields([], Data, Acc) ->
    {Acc, Data};
decode_fields([Field | Fields], Data, Acc) ->
    {Value, Data1} = decode_field(Field, Data),
    Acc1 = maps:put(Field#field.name, Value, Acc),
    decode_fields(Fields, Data1, Acc1).

decode_field(#field{node_id = NodeId, value_rank = -1}, Data) ->
    decode_type(NodeId, Data);
decode_field(#field{node_id = NodeId, value_rank = N}, Data) when N >= 1 ->
    {Dims, Data1} = lists:mapfoldl(fun(_, D) ->
        decode_builtin(int32, D)
    end, Data, lists:seq(1, N)),
    decode_field_array(NodeId, Dims, Data1, []).

%% TODO: check the order, specs are somewhat unclear
decode_field_array(NodeId, [Dim], Data, _Acc) ->
    decode_list([NodeId || _ <- lists:seq(1, Dim)], Data);
decode_field_array(NodeId, [1|Dims], Data, Acc) ->
    {Row, Data1} = decode_field_array(NodeId, Dims, Data, []),
    {[Row|Acc], Data1};
decode_field_array(NodeId, [Dim|Dims], Data, Acc) ->
    {Row, Data1} = decode_field_array(NodeId, Dims, Data, []),
    decode_field_array(NodeId, [Dim-1|Dims], Data1, [Row|Acc]).

decode_masked_fields(Mask, Fields, Data) ->
    BooleanMask = [X==1 || <<X:1>> <= <<Mask:32>>],
    {BooleanMask1, _} = lists:split(length(Fields), lists:reverse(BooleanMask)),
    BooleanMask2 = lists:filtermap(fun({_Idx, Boolean}) -> Boolean end,
                                   lists:zip(
                                     lists:seq(1, length(BooleanMask1)), BooleanMask1)),
    {IntMask, _} = lists:unzip(BooleanMask2),
    MaskedFields = [Field || Field = #field{value=Value, is_optional=Optional} <- Fields,
                             not Optional or lists:member(Value, IntMask)],
    decode_fields(MaskedFields, Data).

resolve_union_value(SwitchValue, Fields, Data) ->
    [Field] = [F || F = #field{value=Value, is_optional=Optional} <- Fields,
                    Optional and (Value == SwitchValue)],
    decode_fields([Field], Data).

resolve_enum_value(EnumValue, Fields) ->
    [Field] = [F || F = #field{value=Value} <- Fields, Value == EnumValue],
    Field#field.name.

decode_extension_object(Data) ->
    {TypeId, <<Mask:8, T/binary>>} = decode_builtin(node_id, Data),
    case decode_extension_object(Mask, TypeId, T) of
        {?UNDEF_EXT_OBJ, _Data2} = Result -> Result;
        {#extension_object{type_id = NodeSpec, encoding = byte_string, body = Body} = ExtObj, Data2} ->
            case opcua_database:resolve_encoding(NodeSpec) of
                {NodeId, Enc} when Enc =:= binary; Enc =:= undefined ->
                    {DecodedBody, _} = decode_type(NodeId, Body),
                    ExtObj2 = ExtObj#extension_object{type_id = NodeId,
                                                      body = DecodedBody},
                    {ExtObj2, Data2}
            end;
        {#extension_object{} = ExtObj2, _Data2} ->
            throw(bad_data_encoding_unsupported)
    end.

decode_extension_object(16#00, TypeId, T) ->
    {#extension_object{type_id = TypeId, encoding = undefined, body = undefined}, T};
decode_extension_object(16#01, TypeId, T) ->
    {Body, T1} = decode_builtin(byte_string, T),
    {#extension_object{type_id = TypeId, encoding = byte_string, body = Body}, T1};
decode_extension_object(16#02, TypeId, T) ->
    {Body, T1} = decode_builtin(xml, T),
    {#extension_object{type_id = TypeId, encoding = xml, body = Body}, T1};
decode_extension_object(_, _, _) ->
    throw(bad_decoding_error).

decode_variant(<<0:6, Bin/binary>>) ->
    {undefined, Bin};
decode_variant(<<DimFlag:1/bits, ArrayFlag:1/bits, TypeId:6/little-unsigned-integer, Bin/binary>>) ->
    decode_variant(TypeId, DimFlag, ArrayFlag, Bin);
decode_variant(_) ->
    throw(bad_decoding_error).

decode_variant(TypeId, <<0:1>>, <<0:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {Value, Bin2} = decode_builtin(TypeName, Bin),
    {#variant{type = TypeName, value = Value}, Bin2};
decode_variant(TypeId, <<0:1>>, <<1:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {Array, Bin1} = decode_array(TypeName, Bin),
    {#variant{type = TypeName, value = Array}, Bin1};
decode_variant(TypeId, <<1:1>>, <<1:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {MultiArray, Bin1} = decode_multi_array(TypeName, Bin),
    {#variant{type = TypeName, value = MultiArray}, Bin1}.

decode_multi_array(Type, Bin) ->
    {ObjectArray, T} = decode_array(Type, Bin),
    {DimArray, T1} = decode_array(int32, T),
    {honor_dimensions(ObjectArray, DimArray), T1}.

decode_array(Type, Bin) ->
    {Length, T} = decode_builtin(int32, Bin),
    decode_array(Type, T, Length).

decode_array(_Type, _Array, -1) ->
    undefined;
decode_array(Type, Array, N) ->
    decode_array(Type, Array, N, []).

decode_array(_Type, T, 0, Acc) ->
    {lists:reverse(Acc), T};
decode_array(Type, Array, N, Acc) ->
    {Elem, T} = decode_type(Type, Array),
    decode_array(Type, T, N-1, [Elem|Acc]).

honor_dimensions(ObjectArray, DimArray) ->
    lists:reverse(lists:foldl(fun(Dim, {Array, ArrayList}) ->
        {El, NewArray} = lists:split(Dim, Array),
        {NewArray, [El|ArrayList]}
    end, {[], ObjectArray}, DimArray)).

decode_data_value(<<0:2, Mask:6/bits, Bin/binary>>) ->
    Types = [
        {value, variant, undefined},
        {status, status_code, 0},
        {source_timestamp, date_time, 0},
        {source_pico_seconds, uint16, 0},
        {server_timestamp, date_time, 0},
        {server_pico_seconds, uint16, 0}
    ],
    RecordInfo = {data_value, record_info(fields, data_value)},
    decode_masked(RecordInfo, Mask, Types, Bin);
decode_data_value(_) ->
    throw(bad_decoding_error).

decode_masked(RecordInfo, Mask, Fields, Bin) ->
    BooleanMask = lists:reverse([X == 1 || <<X:1>> <= Mask]),
    {DecFields, Bin2} = lists:foldl(fun
        ({{N, _, D}, false}, {Acc, B}) -> {[{N, D} | Acc], B};
        ({{N, T, _}, true}, {Acc, B}) ->
            {V, B2} = decode_type(T, B),
            {[{N, V} | Acc], B2}
    end, {[], Bin}, lists:zip(Fields, BooleanMask)),
    {RecordName, RecordFields} = RecordInfo,
    Values = [proplists:get_value(Key, DecFields) || Key <- RecordFields],
    {list_to_tuple([RecordName | Values]), Bin2}.


%%% encoding

encode_type([{Key, _} | _] = Spec, Data) when is_atom(Key) ->
    encode_map(Spec, Data);
encode_type(Spec, Data) when is_list(Spec) ->
    encode_list(Spec, Data);
encode_type(#node_id{type = string, value = Name}, Data)
  when ?IS_BUILTIN_TYPE_NAME(Name) ->
    {encode_builtin(Name, Data), undefined};
encode_type(#node_id{type = numeric, value = Num}, Data)
  when ?IS_BUILTIN_TYPE_ID(Num) ->
    {encode_builtin(opcua_codec:builtin_type_name(Num), Data), undefined};
encode_type(#node_id{} = NodeId, Data) ->
    encode_schema(opcua_database:lookup_schema(NodeId), Data);
encode_type(NodeSpec, Data) ->
    encode_type(opcua_codec:node_id(NodeSpec), Data).

encode_map(Spec, Data) ->
    encode_map(Spec, Data, []).

encode_map([], Data, Acc) ->
    {lists:reverse(Acc), Data};
encode_map([{Key, Spec} | Rest], Data, Acc) ->
    case maps:take(Key, Data) of
        error -> throw(bad_encoding_error);
        {Value, Data2} ->
            {Result, _} = encode_type(Spec, Value),
            encode_map(Rest, Data2, [Result | Acc])
    end.

encode_list(Spec, Data) ->
    encode_list(Spec, Data, []).

encode_list([], Data, Acc) ->
    {lists:reverse(Acc), Data};
encode_list([Spec | Rest], [Value | Data], Acc) ->
    {Result, _} = encode_type(Spec, Value),
    encode_list(Rest, Data, [Result | Acc]).

encode_builtin(extension_object, ExtensionObject) ->
    encode_extension_object(ExtensionObject);
encode_builtin(variant, Variant) ->
    encode_variant(Variant);
encode_builtin(data_value, DataValue) ->
    encode_data_value(DataValue);
encode_builtin(Type, Data) ->
    opcua_codec_binary_builtin:encode(Type, Data).

encode_schema(#structure{with_options = false, fields = Fields}, Data) ->
    encode_fields(Fields, Data);
encode_schema(#structure{with_options = true, fields = Fields}, Data) ->
    encode_masked_fields(Fields, Data);
encode_schema(#union{fields = Fields}, UnionMap) ->
    [Name] = maps:keys(UnionMap),
    [Field] = [Field || Field = #field{name=FieldName} <- Fields, FieldName==Name],
    SwitchValue = encode_builtin(uint32, Field#field.value),
    {EncodedValue, _} = encode_field(Field, maps:get(Name, UnionMap)),
    {[SwitchValue, EncodedValue], undefined};
encode_schema(#enum{fields = Fields}, Name) ->
    [Field] = [Field || Field = #field{name=FieldName} <- Fields, FieldName==Name],
    {encode_builtin(int32, Field#field.value), undefined};
encode_schema(#builtin{builtin_node_id = BuiltinNodeId}, Data) ->
    encode_type(BuiltinNodeId, Data).

encode_masked_fields(Fields, Data) ->
    encode_masked_fields(Fields, Data, [], []).

encode_masked_fields([], Data, Mask, Acc) ->
    BinFields1 = lists:reverse(Acc),
    Mask1 = lists:foldl(fun(Bit, M) -> M bxor Bit end, 0, [1 bsl (N-1) || N <- Mask]),
    BinMask1 = encode_builtin(uint32, Mask1),
    {iolist_to_binary([BinMask1, BinFields1]), Data};
encode_masked_fields([Field = #field{is_optional = false, name = Name} | Fields], Data, Mask, Acc) ->
    {Value, Data2} = maps:take(Name, Data),
    {EncodedField, _} = encode_field(Field, Value),
    encode_masked_fields(Fields, Data2, Mask, [EncodedField|Acc]);
encode_masked_fields([Field = #field{is_optional = true, name = Name} | Fields], Data, Mask, Acc) ->
    case maps:take(Name, Data) of
        {Value, Data2} ->
            {EncodedField, _} = encode_field(Field, Value),
            encode_masked_fields(Fields, Data2, [Field#field.value|Mask], [EncodedField|Acc]);
        error ->
            encode_masked_fields(Fields, Data, Mask, Acc)
    end.

encode_fields(Fields, Data) ->
    encode_fields(Fields, Data, []).

encode_fields([], _Data, Acc) ->
    {iolist_to_binary(lists:reverse(Acc)), undefined};
encode_fields([Field | Fields], Data, Acc) ->
    {_, {Value, Data2}} = {Field#field.name, maps:take(Field#field.name, Data)},
    {FieldValue, _} = encode_field(Field, Value),
    encode_fields(Fields, Data2, [FieldValue|Acc]).

encode_field(#field{node_id = NodeId, value_rank = -1}, Data) ->
    encode_type(NodeId, Data);
encode_field(#field{node_id = NodeId, value_rank = N}, Array)
  when N>=1, is_list(Array) ->
    Dims = resolve_dims(Array, []),
    Array2 = encode_array(NodeId, Array, []),
    {[Dims, Array2], undefined}.

resolve_dims(List, Acc) when is_list(List) ->
    {EncDim, _} = encode_type(int32, length(List)),
    case List of
        [El|_] ->
            resolve_dims(El, [EncDim|Acc]);
        [] ->
            lists:reverse([EncDim|Acc])
    end;
resolve_dims(_El, Acc) ->
    lists:reverse(Acc).

%% TODO: check order here as with decoding
encode_array(_NodeId, [], Acc) ->
    lists:reverse(Acc);
encode_array(NodeId, [Array|Arrays], Acc) when is_list(Array) ->
    BinArray = encode_array(NodeId, Array, []),
    encode_array(NodeId, Arrays, [BinArray|Acc]);
encode_array(NodeId, Array, Acc) ->
    {BinArray, []} = encode_list(lists:duplicate(length(Array), NodeId), Array),
    [BinArray|Acc].

encode_extension_object(?UNDEF_EXT_OBJ) ->
    [encode_builtin(node_id, ?UNDEF_NODE_ID), <<16#00>>];
encode_extension_object(#extension_object{type_id = NodeSpec,
                                          encoding = byte_string,
                                          body = Body} = ExtObj) ->
    {EncNodeId, _} = opcua_database:lookup_encoding(NodeSpec, binary),
    {EncodedBody, _} = encode_type(NodeSpec, Body),
    NodeIdData = encode_builtin(node_id, EncNodeId),
    %% NOTE: encoding byte strings also encodes the 'Length' of those as prefix
    BodyData = encode_builtin(byte_string, EncodedBody),
    FlagData = <<16#01>>,
    [NodeIdData, FlagData, BodyData];
encode_extension_object(#extension_object{}) ->
    throw(bad_data_encoding_unsupported).

encode_variant(#variant{type = Type, value = MultiArray})
  when is_list(MultiArray) ->
    TypeId = opcua_codec:builtin_type_id(Type),
    {Length, Value, Dims} = build_variant_value(Type, MultiArray),
    case Length of
        0 -> <<0:2, TypeId:6>>;
        Length when Length > 0 ->
            LengthData = encode_builtin(int32, Length),
            DimLengthData = encode_builtin(int32, length(Dims)),
            DimsData = [encode_builtin(int32, Dim) || Dim <- Dims],
            [<<3:2, TypeId:6>>, LengthData, Value, DimLengthData, DimsData]
    end;
encode_variant(#variant{type = Type, value = Value}) ->
    TypeId = opcua_codec:builtin_type_id(Type),
    BinValue = encode_builtin(Type, Value),
    [<<0:2, TypeId:6>>, BinValue].

build_variant_value(Type, MultiArray) ->
    build_variant_value(Type, MultiArray, 0, [], []).

build_variant_value(_Type, [], Length, ValueAcc, DimsAcc) ->
    {Length, lists:reverse(ValueAcc), lists:reverse(DimsAcc)};
build_variant_value(Type, [Elem | Rest], Length, ValueAcc, DimsAcc) ->
    {ArrayLength, ArrayData} = case Elem of
        Elem when is_list(Elem) ->
            ElemData = [V || {V, _} <- [encode_type(Type, E) || E <- Elem]],
            {length(Elem), ElemData};
        _ ->
            {ElemData, _} = encode_type(Type, Elem),
            {1, ElemData}
    end,
    build_variant_value(Type, Rest, Length + ArrayLength,
                        [ArrayData | ValueAcc], [ArrayLength | DimsAcc]).

encode_data_value(DataValue) ->
    #data_value{
        value = Value,
        status = Status,
        source_timestamp = SourceTimestamp,
        source_pico_seconds = SourcePicoSeconds,
        server_timestamp = ServerTimestamp,
        server_pico_seconds = ServerPicoSeconds
    } = DataValue,
    Types = [
        {variant,      Value},
        {status_code,  maybe_undefined(Status, 0)},
        {date_time,    maybe_undefined(SourceTimestamp, 0)},
        {uint16,       maybe_undefined(SourcePicoSeconds, 0)},
        {date_time,    maybe_undefined(ServerTimestamp, 0)},
        {uint16,       maybe_undefined(ServerPicoSeconds, 0)}
    ],
    encode_masked(Types).

maybe_undefined(Value, Value) -> undefined;
maybe_undefined(Value, _Cond) -> Value.

encode_masked(TypeList) ->
    encode_masked(TypeList, <<>>, []).

encode_masked([], Mask, Acc) ->
    [<<0:(8-bit_size(Mask)), Mask/bits>>, lists:reverse(Acc)];
encode_masked([{_Type, undefined} | Rest], Mask, Acc) ->
    encode_masked(Rest, <<0:1, Mask/bits>>, Acc);
encode_masked([{Type, Value} | Rest], Mask, Acc) ->
    {Data, _} = encode_type(Type, Value),
    encode_masked(Rest, <<1:1, Mask/bits>>, [Data | Acc]).
