-module(opcua_codec_binary).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua_codec.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([decode/2]).
-export([encode/2]).

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decode(opcua_spec(), binary()) -> {term(), binary()}.
decode(NodeId = #node_id{}, Data) when ?IS_BUILTIN_TYPE(NodeId#node_id.value) ->
    decode_builtin(NodeId, Data);
decode(NodeIds, Data) when is_list(NodeIds) ->
    decode_list(NodeIds, Data);
decode(NodeId = #node_id{}, Data) ->
    decode_schema(opcua_schema:resolve(NodeId), Data);
decode(_NodeId, _Data) ->
    {error, decoding_error}.

-spec encode(opcua_spec(), term()) -> {iolist(), term()}.
encode(NodeId = #node_id{}, Data) when ?IS_BUILTIN_TYPE(NodeId#node_id.value) ->
    encode_builtin(NodeId, Data);
encode(NodeIds, Data) when is_list(NodeIds) ->
    encode_list(NodeIds, Data);
encode(NodeId = #node_id{}, Data) ->
    encode_schema(opcua_schema:resolve(NodeId), Data);
encode(_NodeId, _Data) ->
    {error, encoding_error}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% decoding

decode_builtin(#node_id{type = numeric, value = Value}, Data) ->
    decode_builtin(opcua_codec:builtin_type_name(Value), Data);
decode_builtin(#node_id{value = Value}, Data) ->
    decode_builtin(Value, Data);
decode_builtin(Type, Data) ->
    try opcua_codec_binary_builtin:decode(Type, Data) of
        {Result, Rest} -> {ok, Result, Rest}
    catch
        _:decoding_error -> {error, decoding_error}
    end.

decode_schema(#data_type{type = structure, with_options = false, fields = Fields}, Data) ->
    decode_fields(Fields, Data);
decode_schema(#data_type{type = structure, with_options = true, fields = Fields}, Data) ->
    {Mask, Data1} = decode_builtin(uint32, Data),
    decode_masked_fields(Mask, Fields, Data1);
decode_schema(#data_type{type = union, fields = Fields}, Data) ->
    {SwitchValue, Data1} = decode_builtin(uint32, Data),
    resolve_union_value(SwitchValue, Fields, Data1);
decode_schema(#data_type{type = enum, fields = Fields}, Data) ->
    {Value, Data1} = decode_builtin(int32, Data),
    resolve_enum_value(Value, Fields, Data1).

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

decode_array(_NodeId, _Dims, _Data, _Acc) -> ok.

decode_masked_fields(_Mask, _Fields, _Data) -> ok.

resolve_union_value(_SwitchValue, _Fields, _Data) -> ok.

resolve_enum_value(_Value, _Fields, _Data) -> ok.

decode_list(Types, Data) ->
    decode_list(Types, Data, []).

decode_list([], Data, Acc) ->
    {lists:reverse(Acc), Data};
decode_list([Type | Types], Data, Acc) ->
    {Value, Data1}  = decode(Type, Data),
    decode_list(Types, Data1, [Value | Acc]).

%%% encoding

encode_builtin(#node_id{type = numeric, value = Value}, Data) ->
    encode_builtin(opcua_codec:builtin_type_name(Value), Data);
encode_builtin(#node_id{value = Value}, Data) ->
    encode_builtin(Value, Data);
encode_builtin(Type, Data) ->
    try opcua_codec_binary_builtin:encode(Type, Data) of
        Result -> {ok, Result}
    catch
        _:encoding_error -> {error, encoding_error}
    end.

encode_schema(_Type, _Data) -> ok.

encode_list(Types, Data) ->
    encode_list(Types, Data, []).

encode_list([], Data, Acc) ->
    {lists:reverse(Acc), Data};
encode_list([Type | Types], [Value | Data], Acc) ->
    Result = encode(Type, Value),
    encode_list(Types, Data, [Result | Acc]).
