-module(opcua_codec_binary).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua_codec.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([decode/2]).
-export([encode/2]).

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decode(opcua_spec(), binary()) -> {term(), binary()}.
decode([{Key, _} | _] = Spec, Data) when is_atom(Key) -> decode_map(Spec, Data);
decode(Spec, Data) when is_list(Spec) -> decode_list(Spec, Data);
decode(Type, Data) -> decode_type(Type, Data).

-spec encode(opcua_spec(), term()) -> {iolist(), term()}.
encode([{Key, _} | _] = Spec, Data) when is_atom(Key) -> encode_map(Spec, Data);
encode(Spec, Data) when is_list(Spec) -> encode_list(Spec, Data);
encode(Type, Data) -> encode_type(Type, Data).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% decoding

decode_map(Spec, Data) ->
    decode_map(Spec, Data, #{}).

decode_map([], Data, Acc) ->
    {Acc, Data};
decode_map([{Key, Spec} | Rest], Data, Acc) when is_atom(Key) ->
    {Value, Data2}  = decode(Spec, Data),
    decode_map(Rest, Data2, Acc#{Key => Value}).

decode_list(Specs, Data) ->
    decode_list(Specs, Data, []).

decode_list([], Data, Acc) ->
    {lists:reverse(Acc), Data};
decode_list([Spec | Rest], Data, Acc) ->
    {Value, Data2}  = decode(Spec, Data),
    decode_list(Rest, Data2, [Value | Acc]).

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

decode_builtin(extension_object, Data) ->
    {#{type_id := NodeId, body := Body} = ExtObj, Data1} =
        opcua_codec_binary_builtin:decode(extension_object, iolist_to_binary(Data)),
    case NodeId of
        #node_id{value = 0} ->
            {ExtObj#{body := undefined}, Data1};
        _ ->
            {DecodedBody, _} = decode(NodeId, Body),
            {ExtObj#{body := DecodedBody}, Data1}
    end;
decode_builtin(Type, Data) ->
    opcua_codec_binary_builtin:decode(Type, iolist_to_binary(Data)).

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
    decode(BuiltinNodeId, Data).

decode_fields(Fields, Data) ->
    decode_fields(Fields, Data, #{}).

decode_fields([], Data, Acc) ->
    {Acc, Data};
decode_fields([Field | Fields], Data, Acc) ->
    {Value, Data1} = decode_field(Field, Data),
    Acc1 = maps:put(Field#field.name, Value, Acc),
    decode_fields(Fields, Data1, Acc1).

decode_field(#field{node_id = NodeId, value_rank = -1}, Data) ->
    decode(NodeId, Data);
decode_field(#field{node_id = NodeId, value_rank = N}, Data) when N >= 1 ->
    {Dims, Data1} = lists:mapfoldl(fun(_, D) ->
                                    decode_builtin(int32, D)
                                   end, Data, lists:seq(1, N)),
    decode_array(NodeId, Dims, Data1, []).

%% TODO: check the order, specs are somewhat unclear
decode_array(NodeId, [Dim], Data, _Acc) ->
    decode_list([NodeId || _ <- lists:seq(1, Dim)], Data);
decode_array(NodeId, [1|Dims], Data, Acc) ->
    {Row, Data1} = decode_array(NodeId, Dims, Data, []),
    {[Row|Acc], Data1};
decode_array(NodeId, [Dim|Dims], Data, Acc) ->
    {Row, Data1} = decode_array(NodeId, Dims, Data, []),
    decode_array(NodeId, [Dim-1|Dims], Data1, [Row|Acc]).

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
    #{name => Field#field.name}.


%%% encoding

encode_map(Spec, Data) ->
    encode_map(Spec, Data, []).

encode_map([], Data, Acc) ->
    {lists:reverse(Acc), Data};
encode_map([{Key, Spec} | Rest], Data, Acc) ->
    case maps:take(Key, Data) of
        error -> error('Bad_EncodingError');
        {Value, Data2} ->
            {Result, _} = encode(Spec, Value),
            encode_map(Rest, Data2, [Result | Acc])
    end.

encode_list(Spec, Data) ->
    encode_list(Spec, Data, []).

encode_list([], Data, Acc) ->
    {lists:reverse(Acc), Data};
encode_list([Spec | Rest], [Value | Data], Acc) ->
    {Result, _} = encode(Spec, Value),
    encode_list(Rest, Data, [Result | Acc]).

encode_type(#node_id{type = string, value = Name}, Data)
  when ?IS_BUILTIN_TYPE_NAME(Name) ->
    encode_builtin(Name, Data);
encode_type(#node_id{type = numeric, value = Num}, Data)
  when ?IS_BUILTIN_TYPE_ID(Num) ->
    encode_builtin(opcua_codec:builtin_type_name(Num), Data);
encode_type(#node_id{} = NodeId, Data) ->
    encode_schema(opcua_database:lookup_schema(NodeId), Data);
encode_type(NodeSpec, Data) ->
    encode_type(opcua_codec:node_id(NodeSpec), Data).

encode_builtin(extension_object, #{type_id := NodeSpec, body := Body} = ExtObj) ->
    ExtObj1 = case opcua_database:lookup_id(NodeSpec) of
                #node_id{value = 0} ->
                    ExtObj#{body := undefined};
                NodeId ->
                    {EncodedBody, _} = encode(NodeId, Body),
                    ExtObj#{body := EncodedBody}
              end,
    {opcua_codec_binary_builtin:encode(extension_object, ExtObj1), undefined};
encode_builtin(Type, Data) ->
    {opcua_codec_binary_builtin:encode(Type, Data), undefined}.

encode_schema(#structure{with_options = false, fields = Fields}, Data) ->
    encode_fields(Fields, Data);
encode_schema(#structure{with_options = true, fields = Fields}, Data) ->
    encode_masked_fields(Fields, Data);
encode_schema(#union{fields = Fields}, UnionMap) ->
    [Name] = maps:keys(UnionMap),
    [Field] = [Field || Field = #field{name=FieldName} <- Fields, FieldName==Name],
    {SwitchValue, _} = encode_builtin(uint32, Field#field.value),
    {EncodedValue, _} = encode_field(Field, maps:get(Name, UnionMap)),
    {[SwitchValue, EncodedValue], undefined};
encode_schema(#enum{fields = Fields}, #{name := Name}) ->
    [Field] = [Field || Field = #field{name=FieldName} <- Fields, FieldName==Name],
    encode_builtin(int32, Field#field.value);
encode_schema(#builtin{builtin_node_id = BuiltinNodeId}, Data) ->
    encode(BuiltinNodeId, Data).

encode_masked_fields(Fields, Data) ->
    encode_masked_fields(Fields, Data, [], []).

encode_masked_fields([], Data, Mask, Acc) ->
    BinFields1 = lists:reverse(Acc),
    Mask1 = lists:foldl(fun(Bit, M) -> M bxor Bit end, 0, [1 bsl (N-1) || N <- Mask]),
    {BinMask1, _} = encode_builtin(uint32, Mask1),
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
    encode(NodeId, Data);
encode_field(#field{node_id = NodeId, value_rank = N}, Array)
  when N>=1, is_list(Array) ->
    Dims = resolve_dims(Array, []),
    Array2 = encode_array(NodeId, Array, []),
    {[Dims, Array2], undefined}.

resolve_dims(List, Acc) when is_list(List) ->
    {EncDim, _} = encode(int32, length(List)),
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
