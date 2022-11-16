-module(opcua_codec_binary).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").
-include("opcua_codec_context.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([decode/2]).
-export([encode/2]).
-export([safe_decode/2]).
-export([safe_encode/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decode(opcua:codec_spec(), binary()) -> {term(), binary()}.
decode(Spec, Data) ->
    Ctx = opcua_codec_context:new(decoding, false),
    try decode_type(Ctx, Spec, iolist_to_binary(Data)) of
        {Result, Rest, #ctx{issues = []}} -> {Result, Rest};
        {_Result, _Rest, Ctx2} ->
            opcua_codec_context:throw_first_issue(Ctx2)
    catch
        T:R:S ->
            opcua_codec_context:catch_and_throw(T, R, S)
    end.

-spec encode(opcua:codec_spec(), term()) -> {iolist(), term()}.
encode(Spec, Data) ->
    Ctx = opcua_codec_context:new(encoding, false),
    try encode_type(Ctx, Spec, Data) of
        {Result, Rest, #ctx{issues = []}} -> {Result, Rest};
        {_Result, _Rest, Ctx2} ->
            opcua_codec_context:throw_first_issue(Ctx2)
    catch
        T:R:S ->
            opcua_codec_context:catch_and_throw(T, R, S)
    end.


-spec safe_decode(opcua:codec_spec(), binary()) ->
    {term(), binary(), opcua_codec_conrtext:issues()}.
safe_decode(Spec, Data) ->
    Ctx = opcua_codec_context:new(decoding, true),
    try decode_type(Ctx, Spec, iolist_to_binary(Data)) of
        {Result, Rest, Ctx2} ->
            {Result, Rest, opcua_codec_context:export_issues(Ctx2)}
    catch
        T:R:S ->
            opcua_codec_context:catch_and_throw(T, R, S)
    end.

-spec safe_encode(opcua:codec_spec(), term()) ->
    {iolist(), term(), opcua_codec_conrtext:issues()}.
safe_encode(Spec, Data) ->
    Ctx = opcua_codec_context:new(encoding, true),
    try encode_type(Ctx, Spec, Data) of
        {Result, Rest, Ctx2} ->
            {Result, Rest, opcua_codec_context:export_issues(Ctx2)}
    catch
        T:R:S ->
            opcua_codec_context:catch_and_throw(T, R, S)
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% decoding

decode_type(Ctx, [{Key, _} | _] = Spec, Data) when is_atom(Key) ->
    decode_map(Ctx, Spec, Data);
decode_type(Ctx, Spec, Data) when is_list(Spec) ->
    decode_list(Ctx, Spec, Data);
decode_type(Ctx, #opcua_node_id{type = string, value = Name}, Data)
  when ?IS_BUILTIN_TYPE_NAME(Name) ->
    decode_builtin(Ctx, Name, Data);
decode_type(Ctx, #opcua_node_id{type = numeric, value = Num}, Data)
  when ?IS_BUILTIN_TYPE_ID(Num) ->
    decode_builtin(Ctx, opcua_codec:builtin_type_name(Num), Data);
decode_type(Ctx, #opcua_node_id{} = NodeId, Data) ->
    case opcua_database:lookup_schema(NodeId) of
        undefined -> opcua_codec_context:issue_schema_not_found(Ctx, NodeId);
        Schema -> decode_schema(Ctx, Schema, Data)
    end;
decode_type(Ctx, NodeSpec, Data) ->
    decode_type(Ctx, opcua_codec:node_id(NodeSpec), Data).

decode_map(Ctx, Spec, Data) ->
    decode_map(Ctx, Spec, Data, #{}).

decode_map(Ctx, [], Data, Acc) ->
    {Acc, Data, Ctx};
decode_map(Ctx, [{Key, Spec} | Rest], Data, Acc) when is_atom(Key) ->
    {Value, Data2, Ctx2} = decode_type(?PUSHK(Ctx, Key), Spec, Data),
    decode_map(?POPK(Ctx2, Key), Rest, Data2, Acc#{Key => Value}).

decode_list(Ctx, Specs, Data) ->
    decode_list(Ctx, Specs, Data, 0, []).

decode_list(Ctx, [], Data, _Idx, Acc) ->
    {lists:reverse(Acc), Data, Ctx};
decode_list(Ctx, [Spec | Rest], Data, Idx, Acc) ->
    {Value, Data2, Ctx2} = decode_type(?PUSHK(Ctx, Idx), Spec, Data),
    decode_list(?POPK(Ctx2, Idx), Rest, Data2, Idx + 1, [Value | Acc]).

decode_builtin(Ctx, extension_object, Data) ->
    decode_extension_object(Ctx, Data);
decode_builtin(Ctx, variant, Data) ->
    decode_variant(Ctx, Data);
decode_builtin(Ctx, data_value, Data) ->
    decode_data_value(Ctx, Data);
decode_builtin(Ctx, Type, Data) ->
    {V, R} = opcua_codec_binary_builtin:decode(Type, Data),
    {V, R, Ctx}.

decode_schema(Ctx, #opcua_structure{with_options = false, fields = Fields}, Data) ->
    decode_fields(Ctx, Fields, Data);
decode_schema(Ctx, #opcua_structure{with_options = true, fields = Fields}, Data) ->
    {Mask, Data2, Ctx2} = decode_builtin(?PUSHF(Ctx, mask), uint32, Data),
    decode_masked_fields(?POPF(Ctx2, mask), Mask, Fields, Data2);
decode_schema(Ctx, #opcua_union{fields = Fields}, Data) ->
    {SwitchValue, Data2, Ctx2} = decode_builtin(?PUSHF(Ctx, switch), uint32, Data),
    resolve_union_value(?POPF(Ctx2, switch), SwitchValue, Fields, Data2);
decode_schema(Ctx, #opcua_enum{fields = Fields}, Data) ->
    {Value, Data2, Ctx2} = decode_builtin(?PUSHF(Ctx, value), int32, Data),
    {resolve_enum_value(Value, Fields), Data2, ?POPF(Ctx2, value)};
decode_schema(Ctx, #opcua_option_set{mask_type = MaskNodeId, fields = Fields}, Data) ->
    {Value, Data2, Ctx2} = decode_type(?PUSHF(Ctx, value), MaskNodeId, Data),
    {resolve_option_set_value(Value, Fields), Data2, ?POPF(Ctx2, value)};
decode_schema(Ctx, #opcua_builtin{builtin_node_id = BuiltinNodeId}, Data) ->
    decode_type(Ctx, BuiltinNodeId, Data).

decode_fields(Ctx, Fields, Data) ->
    decode_fields(Ctx, Fields, Data, #{}).

decode_fields(Ctx, [], Data, Acc) ->
    {Acc, Data, Ctx};
decode_fields(Ctx, [Field | Fields], Data, Acc) ->
    Name = Field#opcua_field.name,
    {Value, Data2, Ctx2} = decode_field(?PUSHF(Ctx, Name), Field, Data),
    Acc2 = maps:put(Name, Value, Acc),
    decode_fields(?POPF(Ctx2, Name), Fields, Data2, Acc2).

decode_field(Ctx, #opcua_field{node_id = NodeId, value_rank = -1}, Data) ->
    decode_type(Ctx, NodeId, Data);
decode_field(Ctx, #opcua_field{node_id = NodeId, value_rank = N}, Data) when N >= 1 ->
    {Dims, {Data2, Ctx2}} = lists:mapfoldl(fun(_, {D, C}) ->
        {R, D2, C2} = decode_builtin(C, int32, D),
        {R, {D2, C2}}
    end, {Data, Ctx}, lists:seq(1, N)),
    decode_field_array(Ctx2, NodeId, Dims, Data2, 0, []).

%% TODO: check the order, specs are somewhat unclear
decode_field_array(Ctx, _NodeId, [-1], Data, _Idx, _Acc) -> {[], Data, Ctx};
decode_field_array(Ctx, NodeId, [Dim], Data, _Idx, _Acc) ->
    decode_list(Ctx, [NodeId || _ <- lists:seq(1, Dim)], Data);
decode_field_array(Ctx, NodeId, [1 | Dims], Data, Idx, Acc) ->
    {Row, Data2, Ctx2} = decode_field_array(?PUSHK(Ctx, Idx), NodeId, Dims, Data, 0, []),
    {[Row | Acc], Data2, ?POPK(Ctx2, Idx)};
decode_field_array(Ctx, NodeId, [Dim | Dims], Data, Idx, Acc) ->
    {Row, Data2, Ctx2} = decode_field_array(?PUSHK(Ctx, Idx), NodeId, Dims, Data, 0, []),
    decode_field_array(?POPK(Ctx2, Idx), NodeId, [Dim - 1 | Dims], Data2, Idx + 1, [Row|Acc]).

decode_masked_fields(Ctx, Mask, Fields, Data) ->
    BooleanMask = [X==1 || <<X:1>> <= <<Mask:32>>],
    {BooleanMask1, _} = lists:split(length(Fields), lists:reverse(BooleanMask)),
    BooleanMask2 = lists:filtermap(fun({_Idx, Boolean}) -> Boolean end,
                                   lists:zip(
                                     lists:seq(1, length(BooleanMask1)), BooleanMask1)),
    {IntMask, _} = lists:unzip(BooleanMask2),
    MaskedFields = [Field || Field = #opcua_field{value=Value, is_optional=Optional} <- Fields,
                             not Optional or lists:member(Value, IntMask)],
    decode_fields(Ctx, MaskedFields, Data).

resolve_union_value(Ctx, SwitchValue, Fields, Data) ->
    [Field] = [F || F = #opcua_field{value=Value, is_optional=Optional} <- Fields,
                    Optional and (Value == SwitchValue)],
    decode_fields(Ctx, [Field], Data).

resolve_enum_value(EnumValue, Fields) ->
    [Field] = [F || F = #opcua_field{value=Value} <- Fields, Value == EnumValue],
    Field#opcua_field.name.

resolve_option_set_value(OptionSetValue, Fields) ->
    FieldNames = lists:foldl(fun(X, Acc) ->
                                case (OptionSetValue bsr X#opcua_field.value) rem 2 of
                                    0  -> Acc;
                                    1  -> [X#opcua_field.name|Acc]
                                end
                             end, [], Fields),
    lists:reverse(FieldNames).

decode_extension_object(Ctx, Data) ->
    {TypeId, <<Mask:8, T/binary>>, Ctx2} = decode_builtin(?PUSHF(Ctx, node_id), node_id, Data),
    case decode_extension_object(?POPF(Ctx2, node_id), Mask, TypeId, T) of
        {?UNDEF_EXT_OBJ, _Data2, _Ctx3} = Result -> Result;
        {#opcua_extension_object{type_id = NodeSpec, encoding = byte_string, body = Body} = ExtObj, Data2, Ctx3} ->
            %TODO: Figure out if we shouldn't fail when we can't resolve the encoding ?
            case opcua_database:resolve_encoding(NodeSpec) of
                {NodeId, Enc} when Enc =:= binary; Enc =:= undefined ->
                    try decode_type(?PUSHF(Ctx3, object), NodeId, Body) of
                        {DecodedBody, _Rest, Ctx4} ->
                            ExtObj2 = ExtObj#opcua_extension_object{type_id = NodeId,
                                                                    body = DecodedBody},
                            {ExtObj2, Data2, ?POPF(Ctx4, object)}
                    catch
                        ErrorType:ErrorReason:Stack ->
                            opcua_codec_context:catch_and_continue(ErrorType,
                                                    ErrorReason, Stack,
                                                    Ctx, ?UNDEF_EXT_OBJ, Data2)
                    end
            end;
        {#opcua_extension_object{encoding = EncInfo}, _Data2, Ctx3} ->
            opcua_codec_context:issue_encoding_not_supported(Ctx3, EncInfo)
    end.

decode_extension_object(Ctx, 16#00, TypeId, T) ->
    {#opcua_extension_object{type_id = TypeId, encoding = undefined, body = undefined}, T, Ctx};
decode_extension_object(Ctx, 16#01, TypeId, T) ->
    {Body, T1, Ctx2} = decode_builtin(Ctx, byte_string, T),
    {#opcua_extension_object{type_id = TypeId, encoding = byte_string, body = Body}, T1, Ctx2};
decode_extension_object(Ctx, 16#02, TypeId, T) ->
    {Body, T1, Ctx2} = decode_builtin(Ctx, xml, T),
    {#opcua_extension_object{type_id = TypeId, encoding = xml, body = Body}, T1, Ctx2};
decode_extension_object(Ctx, _, _, _) ->
    opcua_codec_context:issue(Ctx, bad_extension_object).

decode_variant(Ctx, <<0:6, Bin/binary>>) ->
    {undefined, Bin, Ctx};
decode_variant(Ctx, <<ArrayFlag:1/bits, DimFlag:1/bits, TypeId:6/little-unsigned-integer, Bin/binary>>) ->
    decode_variant(Ctx, TypeId, DimFlag, ArrayFlag, Bin);
decode_variant(Ctx, _) ->
    opcua_codec_context:issue(Ctx, bad_variant).

decode_variant(Ctx, TypeId, <<0:1>>, <<0:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {Value, Bin2, Ctx2} = decode_builtin(?PUSHF(Ctx, value), TypeName, Bin),
    {#opcua_variant{type = TypeName, value = Value}, Bin2, ?POPF(Ctx2, value)};
decode_variant(Ctx, TypeId, <<0:1>>, <<1:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {_, Array, Bin2, Ctx2} = decode_array(Ctx, TypeName, Bin),
    {#opcua_variant{type = TypeName, value = Array}, Bin2, Ctx2};
decode_variant(Ctx, TypeId, <<1:1>>, <<1:1>>, Bin) ->
    TypeName = opcua_codec:builtin_type_name(TypeId),
    {MultiArray, Bin2, Ctx2} = decode_multi_array(Ctx, TypeName, Bin),
    {#opcua_variant{type = TypeName, value = MultiArray}, Bin2, Ctx2}.

decode_multi_array(Ctx, Type, Bin) ->
    {L, ObjectArray, Bin2, Ctx2} = decode_array(Ctx, Type, Bin),
    {_, DimArray, Bin3, Ctx3} = decode_array(Ctx2, int32, Bin2),
    ExpSize = lists:foldl(fun(V, A) -> A * V end, 1, DimArray),
    case L =:= ExpSize of
        false -> opcua_codec_context:issue(Ctx3, bad_array);
        true -> {honor_dimensions(L, ObjectArray, DimArray), Bin3, Ctx3}
    end.

decode_array(Ctx, Type, Bin) ->
    {Length, Bin2, Ctx2} = decode_builtin(?PUSHF(Ctx, length), int32, Bin),
    decode_array(?POPF(Ctx2, length), Type, Bin2, Length).

decode_array(Ctx, _Type, Bin, -1) ->
    {undefined, undefined, Bin, Ctx};
decode_array(Ctx, Type, Bin, N) ->
    decode_array(Ctx, Type, Bin, N, N, []).

decode_array(Ctx, _Type, Bin, L, 0, Acc) ->
    {L, lists:reverse(Acc), Bin, Ctx};
decode_array(Ctx, Type, Bin, L, N, Acc) ->
    {Elem, Bin2, Ctx2} = decode_type(Ctx, Type, Bin),
    decode_array(Ctx2, Type, Bin2, L, N - 1, [Elem | Acc]).

honor_dimensions(L, ObjectArray, [L]) -> ObjectArray;
honor_dimensions(ArrayLen, ObjectArray, [Dim | Rest]) ->
    DimSize = ArrayLen div Dim,
    [honor_dimensions(DimSize, E, Rest)
     || E <- lists_nsplit(ObjectArray, DimSize, Dim, [])].

lists_nsplit([], _, 0, Acc) ->
    lists:reverse(Acc);
lists_nsplit(L, S, C, Acc) ->
    {E, L2} = lists:split(S, L),
    lists_nsplit(L2, S, C - 1, [E | Acc]).

decode_data_value(Ctx, <<0:2, Mask:6/bits, Bin/binary>>) ->
    Types = [
        {value, variant, undefined},
        {status, status_code, good},
        {source_timestamp, date_time, 0},
        {server_timestamp, date_time, 0},
        {source_pico_seconds, uint16, 0},
        {server_pico_seconds, uint16, 0}
    ],
    RecordInfo = {opcua_data_value, record_info(fields, opcua_data_value)},
    decode_masked(Ctx, RecordInfo, Mask, Types, Bin);
decode_data_value(Ctx, _) ->
    opcua_codec_context:issue(Ctx, bad_data_value).

decode_masked(Ctx, RecordInfo, Mask, Fields, Bin) ->
    BooleanMask = lists:reverse([X == 1 || <<X:1>> <= Mask]),
    {DecFields, Bin2, Ctx2} = lists:foldl(fun
        ({{N, _, D}, false}, {Acc, B, C}) -> {[{N, D} | Acc], B, C};
        ({{N, T, _}, true}, {Acc, B, C}) ->
            {V, B2, C2} = decode_type(?PUSHF(C, N), T, B),
            {[{N, V} | Acc], B2, ?POPF(C2, N)}
    end, {[], Bin, Ctx}, lists:zip(Fields, BooleanMask)),
    {RecordName, RecordFields} = RecordInfo,
    Values = [proplists:get_value(Key, DecFields) || Key <- RecordFields],
    {list_to_tuple([RecordName | Values]), Bin2, Ctx2}.


%%% encoding

encode_type(Ctx, [{Key, _} | _] = Spec, Data) when is_atom(Key) ->
    encode_map(Ctx, Spec, Data);
encode_type(Ctx, Spec, Data) when is_list(Spec) ->
    encode_list(Ctx, Spec, Data);
encode_type(Ctx, #opcua_node_id{type = string, value = Name}, Data)
  when ?IS_BUILTIN_TYPE_NAME(Name) ->
    encode_builtin(Ctx, Name, Data);
encode_type(Ctx, #opcua_node_id{type = numeric, value = Num}, Data)
  when ?IS_BUILTIN_TYPE_ID(Num) ->
    encode_builtin(Ctx, opcua_codec:builtin_type_name(Num), Data);
encode_type(Ctx, #opcua_node_id{} = NodeId, Data) ->
    case opcua_database:lookup_schema(NodeId) of
        undefined -> opcua_codec_context:issue_schema_not_found(Ctx, NodeId);
        Schema -> encode_schema(Ctx, Schema, Data)
    end;
encode_type(Ctx, NodeSpec, Data) ->
    encode_type(Ctx, opcua_codec:node_id(NodeSpec), Data).

encode_map(Ctx, Spec, Data) ->
    encode_map(Ctx, Spec, Data, []).

encode_map(Ctx, [], Data, Acc) ->
    {lists:reverse(Acc), Data, Ctx};
encode_map(Ctx, [{Key, Spec} | Rest], Data, Acc) ->
    case maps:take(Key, Data) of
        error -> opcua_codec_context:issue(Ctx, missing_key);
        {Value, Data2} ->
            {Result, _, Ctx2} = encode_type(?PUSHK(Ctx, Key), Spec, Value),
            encode_map(?POPK(Ctx2, Key), Rest, Data2, [Result | Acc])
    end.

encode_list(Ctx, Spec, Data) ->
    encode_list(Ctx, Spec, Data, 0, []).

encode_list(Ctx, [], Data, _Idx, Acc) ->
    {lists:reverse(Acc), Data, Ctx};
encode_list(Ctx, [Spec | Rest], [Value | Data], Idx, Acc) ->
    {Result, _, Ctx2} = encode_type(?PUSHK(Ctx, Idx), Spec, Value),
    encode_list(?POPK(Ctx2, Idx), Rest, Data, Idx + 1, [Result | Acc]).

encode_builtin(Ctx, extension_object, ExtensionObject) ->
    encode_extension_object(Ctx, ExtensionObject);
encode_builtin(Ctx, variant, Variant) ->
    encode_variant(Ctx, Variant);
encode_builtin(Ctx, data_value, DataValue) ->
    encode_data_value(Ctx, DataValue);
encode_builtin(Ctx, Type, Data) ->
    {opcua_codec_binary_builtin:encode(Type, Data), undefined, Ctx}.

encode_schema(Ctx, #opcua_structure{with_options = false, fields = Fields}, Data) ->
    encode_fields(Ctx, Fields, Data);
encode_schema(Ctx, #opcua_structure{with_options = true, fields = Fields}, Data) ->
    encode_masked_fields(Ctx, Fields, Data);
encode_schema(Ctx, #opcua_union{fields = Fields}, UnionMap) ->
    [Name] = maps:keys(UnionMap),
    [Field] = [Field || Field = #opcua_field{name = FieldName} <- Fields,
                        FieldName == Name],
    {SwitchValue, _, Ctx2} = encode_builtin(?PUSHF(Ctx, switch), uint32,
                                            Field#opcua_field.value),
    {EncodedValue, _, Ctx3} = encode_field(?POPF(Ctx2, switch), Field,
                                           maps:get(Name, UnionMap)),
    {[SwitchValue, EncodedValue], undefined, Ctx3};
encode_schema(Ctx, #opcua_enum{fields = Fields}, Name) ->
    [Field] = [Field || Field = #opcua_field{name=FieldName} <- Fields,
                        FieldName==Name],
    {Value, Data2, Ctx2} = encode_builtin(?PUSHF(Ctx, value), int32,
                                          Field#opcua_field.value),
    {Value, Data2, ?POPF(Ctx2, value)};
encode_schema(Ctx, #opcua_option_set{mask_type = MaskNodeId,
                                     fields = Fields}, OptionSet) ->
    ChosenFields = [Field || Field = #opcua_field{name = Name} <- Fields,
                             lists:member(Name, OptionSet)],
    Int = lists:foldl(fun(X, Acc) ->
        Acc bxor (1 bsl X#opcua_field.value)
    end, 0, ChosenFields),
    encode_type(Ctx, MaskNodeId, Int);
encode_schema(Ctx, #opcua_builtin{builtin_node_id = BuiltinNodeId}, Data) ->
    encode_type(Ctx, BuiltinNodeId, Data).

encode_masked_fields(Ctx, Fields, Data) ->
    encode_masked_fields(Ctx, Fields, Data, [], []).

encode_masked_fields(Ctx, [], Data, Mask, Acc) ->
    BinFields1 = lists:reverse(Acc),
    Mask1 = lists:foldl(fun(Bit, M) -> M bxor Bit end, 0,
                        [1 bsl (N-1) || N <- Mask]),
    {BinMask1, _, Ctx2} = encode_builtin(?PUSHF(Ctx, mask), uint32, Mask1),
    {iolist_to_binary([BinMask1, BinFields1]), Data, ?POPF(Ctx2, mask)};
encode_masked_fields(Ctx, [Field = #opcua_field{is_optional = false, name = Name}
                           | Fields], Data, Mask, Acc) ->
    case maps:take(Name, Data) of
        error ->
            opcua_condec_context:issue(?PUSHF(Ctx, Name), missing_required_field);
        {Value, Data2} ->
            {EncodedField, _, Ctx2} = encode_field(Ctx, Field, Value),
            encode_masked_fields(Ctx2, Fields, Data2, Mask,
                                 [EncodedField | Acc])
    end;
encode_masked_fields(Ctx, [Field = #opcua_field{is_optional = true, name = Name}
                           | Fields], Data, Mask, Acc) ->
    case maps:take(Name, Data) of
        {Value, Data2} ->
            {EncodedField, _, Ctx2} = encode_field(Ctx, Field, Value),
            encode_masked_fields(Ctx2, Fields, Data2,
                                 [Field#opcua_field.value|Mask],
                                 [EncodedField | Acc]);
        error ->
            encode_masked_fields(Ctx, Fields, Data, Mask, Acc)
    end.

encode_fields(Ctx, Fields, Data) ->
    encode_fields(Ctx, Fields, Data, []).

encode_fields(Ctx, [], _Data, Acc) ->
    {iolist_to_binary(lists:reverse(Acc)), undefined, Ctx};
encode_fields(Ctx, [#opcua_field{name = Name} = Field | Fields], Data, Acc) ->
    case maps:take(Name, Data) of
        error ->
            opcua_condec_context:issue(?PUSHF(Ctx, Name), missing_required_field);
        {Value, Data2} ->
            {FieldValue, _, Ctx2} = encode_field(Ctx, Field, Value),
            encode_fields(Ctx2, Fields, Data2, [FieldValue|Acc])
    end.

encode_field(Ctx, #opcua_field{name = Name, node_id = NodeId, value_rank = -1}, Data) ->
    {Result, Data2, Ctx2} = encode_type(?PUSHF(Ctx, Name), NodeId, Data),
    {Result, Data2, ?POPF(Ctx2, Name)};
encode_field(Ctx, #opcua_field{name = Name, node_id = NodeId, value_rank = N}, Array)
  when N >= 1, is_list(Array) ->
    {Dims, Ctx2} = resolve_dims(?PUSHF(Ctx, Name), Array, []),
    {Array2, Ctx3} = encode_array(Ctx2, NodeId, Array, []),
    {[Dims, Array2], undefined, ?POPF(Ctx3, Name)}.

resolve_dims(Ctx, List, Acc) when is_list(List) ->
    {EncDim, _, Ctx2} = encode_type(Ctx, int32, length(List)),
    case List of
        [El | _] ->
            resolve_dims(Ctx2, El, [EncDim|Acc]);
        [] ->
            {lists:reverse([EncDim | Acc]), Ctx2}
    end;
resolve_dims(Ctx, _El, Acc) ->
    {lists:reverse(Acc), Ctx}.

%% TODO: check order here as with decoding
encode_array(Ctx, _NodeId, [], Acc) ->
    {lists:reverse(Acc), Ctx};
encode_array(Ctx, NodeId, [Array | Arrays], Acc) when is_list(Array) ->
    {BinArray, Ctx2} = encode_array(Ctx, NodeId, Array, []),
    encode_array(Ctx2, NodeId, Arrays, [BinArray | Acc]);
encode_array(Ctx, NodeId, Array, Acc) ->
    {BinArray, [], Ctx2} = encode_list(Ctx, lists:duplicate(length(Array), NodeId), Array),
    {[BinArray | Acc], Ctx2}.

encode_extension_object(Ctx, ?UNDEF_EXT_OBJ) ->
    {Result, _, Ctx2} = encode_builtin(Ctx, node_id, ?UNDEF_NODE_ID),
    {[Result, <<16#00>>], undefined, Ctx2};
encode_extension_object(Ctx, #opcua_extension_object{type_id = NodeSpec,
                                                     encoding = byte_string,
                                                     body = Body}) ->
    %TODO: Figure out what to do when lookup fail ?
    case opcua_database:lookup_encoding(NodeSpec, binary) of
        {EncNodeId, binary} ->
            {EncodedBody, _, Ctx2} = encode_type(Ctx, NodeSpec, Body),
            {NodeIdData, _, Ctx3} = encode_builtin(Ctx2, node_id, EncNodeId),
            %% NOTE: encoding byte strings also encodes the 'Length' of those as prefix
            {BodyData, _, Ctx4} = encode_builtin(Ctx3, byte_string, EncodedBody),
            FlagData = <<16#01>>,
            {[NodeIdData, FlagData, BodyData], undefined, Ctx4}
    end;
encode_extension_object(Ctx, #opcua_extension_object{encoding = EncInfo}) ->
    opcua_codec_context:issue_encoding_not_supported(Ctx, EncInfo).


encode_variant(Ctx, #opcua_variant{type = Type, value = MultiArray})
  when is_list(MultiArray) ->
    TypeId = opcua_codec:builtin_type_id(Type),
    case pack_array(Ctx, Type, MultiArray) of
        {0, _, _, Ctx2} ->
            {<<0:2, TypeId:6>>, undefined, Ctx2};
        {Length, Value, [Length], Ctx2} ->
            {LengthData, _, Ctx3}  = encode_builtin(Ctx2, int32, Length),
            {[<<1:1, 0:1, TypeId:6>>, LengthData, Value], undefined, Ctx3};
        {Length, Value, Dims, Ctx2} ->
            {LengthData, _, Ctx3} = encode_builtin(Ctx2, int32, Length),
            {DimLengthData, _, Ctx4} = encode_builtin(Ctx3, int32, length(Dims)),
            {DimsData, Ctx5} = lists:foldl(fun(Dim, {Acc, C}) ->
                {Res, _, C2} = encode_builtin(C, int32, Dim),
                {[Res | Acc], C2}
            end, {[], Ctx4}, Dims),
            {[<<3:2, TypeId:6>>, LengthData, Value, DimLengthData, DimsData],
             undefined, Ctx5}
    end;
encode_variant(Ctx, #opcua_variant{type = Type, value = Value}) ->
    TypeId = opcua_codec:builtin_type_id(Type),
    {BinValue, _, Ctx2} = encode_builtin(?PUSHF(Ctx, value), Type, Value),
    {[<<0:2, TypeId:6>>, BinValue], undefined, ?POPF(Ctx2, value)}.

pack_array(Ctx, Type, Values) ->
    {L, Acc, Dims, Ctx2} = pack_array(Ctx, Type, Values, 0, []),
    {L, lists:reverse(Acc), Dims, Ctx2}.

pack_array(Ctx, Type, [Row | Rest] = Values, Idx, Acc)
  when is_list(Row) ->
    Dim = length(Values),
    {DimLen, Acc2, SubDims, Ctx2} = pack_array(Ctx, Type, Row, Idx + 1, Acc),
    {Len, Acc3, Ctx3} = pack_array_rows(?PUSHK(Ctx2, Idx), Type, Rest, DimLen, Acc2, DimLen, SubDims),
    {Len, Acc3, [Dim | SubDims], ?POPK(Ctx3, Idx)};
pack_array(Ctx, Type, Values, _Idx, Acc) ->
    pack_array_values(Ctx, Type, Values, 0, Acc).

pack_array_rows(Ctx, _Type, [], Len, Acc, _ExpLen, _ExpDims) ->
    {Len, Acc, Ctx};
pack_array_rows(Ctx, Type, [Row | Rest], Len, Acc, ExpLen, ExpDims)
  when is_list(Row) ->
    case pack_array(Ctx, Type, Row, 0, Acc) of
        {ExpLen, Acc2, ExpDims, Ctx2} ->
            pack_array_rows(Ctx2, Type, Rest, Len + ExpLen, Acc2, ExpLen, ExpDims);
        {_, _, _, Ctx2} ->
            opcua_codec_context:issue(Ctx2, malformed_array)
    end;
pack_array_rows(Ctx, _Type, _Values, _Len, _Acc, _ExpLen, _ExpDims) ->
    opcua_codec_context:issue(Ctx, malformed_array).

pack_array_values(Ctx, _Type, [], Idx, Acc) ->
    {Idx, Acc, [Idx], Ctx};
pack_array_values(Ctx, Type, [Val | Rest], Idx, Acc) ->
    {Data, _, Ctx2} = encode_type(?PUSHK(Ctx, Idx), Type, Val),
    pack_array_values(?POPK(Ctx2, Idx), Type, Rest, Idx + 1, [Data | Acc]).

encode_data_value(Ctx, DataValue) ->
    #opcua_data_value{
        value = Value,
        status = Status,
        source_timestamp = SourceTimestamp,
        source_pico_seconds = SourcePicoSeconds,
        server_timestamp = ServerTimestamp,
        server_pico_seconds = ServerPicoSeconds
    } = DataValue,
    Types = [
        {value,               variant,     Value},
        {status,              status_code, maybe_undefined(Status, 0)},
        {source_timestamp,    date_time,   maybe_undefined(SourceTimestamp, 0)},
        {source_pico_seconds, uint16,      maybe_undefined(SourcePicoSeconds, 0)},
        {server_timestamp,    date_time,   maybe_undefined(ServerTimestamp, 0)},
        {server_pico_seconds, uint16,      maybe_undefined(ServerPicoSeconds, 0)}
    ],
    encode_masked(Ctx, Types).

maybe_undefined(Value, Value) -> undefined;
maybe_undefined(Value, _Cond) -> Value.

encode_masked(Ctx, TypeList) ->
    encode_masked(Ctx, TypeList, <<>>, []).

encode_masked(Ctx, [], Mask, Acc) ->
    {[<<0:(8-bit_size(Mask)), Mask/bits>>, lists:reverse(Acc)], undefined, Ctx};
encode_masked(Ctx, [{_Name, _Type, undefined} | Rest], Mask, Acc) ->
    encode_masked(Ctx, Rest, <<0:1, Mask/bits>>, Acc);
encode_masked(Ctx, [{Name, Type, Value} | Rest], Mask, Acc) ->
    {Data, _, Ctx2} = encode_type(?PUSHF(Ctx, Name), Type, Value),
    encode_masked(?POPF(Ctx2, Name), Rest, <<1:1, Mask/bits>>, [Data | Acc]).
