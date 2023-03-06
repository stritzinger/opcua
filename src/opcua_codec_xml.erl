-module(opcua_codec_xml).

% This module decodes XML from a pre-parsed term composed of tuples like:
% {<<"ROOT">>, #{<<"ATTR1">> => <<"VALUE">>}, [
%    {<<"SUB">>, #{}, [<<"DATA">>}]}
%
% It is only used to decode the values in the NodeSet XML files.
% It would probably not work for decoding OPCUA protocol.


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").
-include("opcua_codec_context.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([decode/2, decode/3]).
-export([decode_value/2, decode_value/3]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decode(opcua:codec_spec(), term()) -> term().
decode(Spec, Data) ->
    decode(Spec, Data, #{}).

-spec decode(opcua:codec_spec(), term(), opcua_codec_context:options()) -> term().
decode(Spec, Data, Opts) ->
    Ctx = opcua_codec_context:new(decoding, Opts),
    try decode_type(Ctx, Spec, Data) of
        {Result, Ctx2} ->
            opcua_codec_context:finalize(Ctx2, Result)
    catch
        T:R:S ->
            opcua_codec_context:resolve(T, R, S)
    end.

% @doc See decode_value/3
-spec decode_value(undefined | opcua:node_spec(), term()) -> term().
decode_value(ExpectedTypeSpec, Data) ->
    decode_value(ExpectedTypeSpec, Data, #{}).

% @doc Decodes a value from a pre-parsed list of XML nodes.
% The difference with decode/[2,3] is that the expected XML data contains
% type information. If a defined type is givem, it will be enforced and the
% decoding will fail if the data do not match it.
% As enumerations and option sets are encoded as the base integer types,
% they cannot be properly decoded if the expected type is not provided.
% Beside enumerations and option sets, GUID are decoded as string if no explict
% type is provided.
-spec decode_value(undefined | opcua:node_spec(), term(),
                   opcua_codec_context:options()) -> term().
decode_value(ExpectedTypeSpec, Data, Opts) ->
    Ctx = opcua_codec_context:new(decoding, Opts),
    try decode_root(Ctx, ExpectedTypeSpec, Data) of
        {Result, Ctx2} ->
            opcua_codec_context:finalize(Ctx2, Result)
    catch
        T:R:S ->
            opcua_codec_context:resolve(T, R, S)
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_root(Ctx, undefined, [{Tag, _, Data}]) ->
    {ValueRank, BasicTypeName} = tag2type(Tag),
    BasicTypeId = opcua_node:id(BasicTypeName),
    decode_root(Ctx, undefined, undefined, BasicTypeId, ValueRank, Data);
decode_root(#ctx{space = Space} = Ctx, TypeSpec, [{Tag, _, Data}]) ->
    ExpectedTypeId = opcua_node:id(TypeSpec),
    {ValueRank, BasicTypeName} = tag2type(Tag),
    BasicTypeId = opcua_node:id(BasicTypeName),
    Schema = opcua_space:schema(Space, ExpectedTypeId),
    decode_root(Ctx, ExpectedTypeId, Schema, BasicTypeId, ValueRank, Data).

%TODO: Add support for generic array dimensions
decode_root(Ctx, ExpectedTypeId, Schema, BuiltinType, 1, Data) ->
    SubTag = type2tag(BuiltinType),
    {RevResult, Ctx2} = lists:foldl(fun(D, {Acc, C}) ->
        {R, C2} = decode_root(C, ExpectedTypeId, Schema, BuiltinType, -1, D),
        {[R | Acc], C2}
    end, {[], Ctx}, [X || {T, _, X} <- Data, T =:= SubTag]),
    {lists:reverse(RevResult), Ctx2};
decode_root(#ctx{space = Space} = Ctx, ExpectedTypeId, _Schema, ?NNID(22), -1,
            [{<<"TypeId">>, _, XMLTypeDescId}, {<<"Body">>, _, Body}]) ->
        {TypeDescId, Ctx2} = decode_type(Ctx, node_id, XMLTypeDescId),
    case {ExpectedTypeId, opcua_space:data_type(Space, TypeDescId)} of
        {_, undefined} ->
            % Type identifier not found for the type descriptor
            throw(bad_decoding_error);
        {undefined, {DataTypeId, xml}} ->
            % Decoding unknown object without any expected type
            decode_type(Ctx2, DataTypeId, Body);
        {DataTypeId, {DataTypeId, xml}} ->
            % Decoding object of expected type
            decode_type(Ctx2, DataTypeId, Body);
        _ ->
            % The expected type do not match the object type
            throw(bad_decoding_error)
    end;
decode_root(Ctx, ?NNID(14), _Schema, ?NNID(12), -1, Data) ->
    % GUID are encoded as strings
    decode_type(Ctx, ?NNID(14), Data);
decode_root(Ctx, DataTypeId, #opcua_enum{}, ?NNID(6), -1, Data) -> % int32
    decode_type(Ctx, DataTypeId, Data);
decode_root(_Ctx, _DataTypeId, #opcua_union{}, _BuiltinType, -1, _Data) ->
    throw(bad_not_implemented);
decode_root(_Ctx, _DataTypeId, #opcua_option_set{}, _BuiltinType, -1, _Data) ->
    throw(bad_not_implemented);
decode_root(Ctx, _DataTypeId, #opcua_builtin{builtin_node_id = BuiltinType}, BuiltinType, -1, Data) ->
    decode_type(Ctx, BuiltinType, Data);
decode_root(Ctx, BuiltinType, _Schema, BuiltinType, -1, Data) ->
    decode_type(Ctx, BuiltinType, Data);
decode_root(Ctx, undefined, undefined, BuiltinType, -1, Data) ->
    decode_type(Ctx, BuiltinType, Data);
decode_root(_Ctx, _ExpectedTypeId, _Schema, _BuiltinType, _ValueRank, _Data) ->
    % Expected type do not match the given data
    throw(bad_decoding_error).

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
decode_type(#ctx{space = Space} = Ctx, #opcua_node_id{} = NodeId, Data) ->
    case opcua_space:schema(Space, NodeId) of
        undefined -> opcua_codec_context:issue_schema_not_found(Ctx, NodeId);
        Schema -> decode_schema(Ctx, Schema, Data)
    end;
decode_type(Ctx, NodeSpec, Data) ->
    decode_type(Ctx, opcua_node:id(NodeSpec), Data).

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

%TODO: Implemente support for extension objects, variant and data values
decode_builtin(_Ctx, extension_object, _Data) ->
    throw(bad_not_implemented);
decode_builtin(_Ctx, variant, _Data) ->
    throw(bad_not_implemented);
decode_builtin(_Ctx, data_value, _Data) ->
    throw(bad_not_implemented);
decode_builtin(Ctx, Type, Data) ->
    V = opcua_codec_xml_builtin:decode(Type, Data),
    {V, Ctx}.

decode_schema(Ctx, #opcua_structure{name = Name, fields = Fields}, Data) ->
    case Data of
        [{Name, _, StructData}] -> decode_fields(Ctx, Fields, StructData);
        _ -> throw(bad_decoding_error)
    end;
decode_schema(_Ctx, #opcua_union{fields = _Fields}, _Data) ->
    throw(bad_not_implemented);
decode_schema(Ctx, #opcua_enum{} = Schema, Data) ->
    {Value, Ctx2} = decode_builtin(?PUSHF(Ctx, value), int32, Data),
    {opcua_codec:unpack_enum(Schema, Value), ?POPF(Ctx2, value)};
decode_schema(_Ctx, #opcua_option_set{}, _Data) ->
    throw(bad_not_implemented);
decode_schema(Ctx, #opcua_builtin{builtin_node_id = BuiltinNodeId}, Data) ->
    decode_type(Ctx, BuiltinNodeId, Data);
decode_schema(_Ctx, _Schema, _Data) ->
    throw(bad_decoding_error).

decode_fields(Ctx, Fields, Data) ->
    decode_fields(Ctx, Fields, Data, #{}).

decode_fields(Ctx, [], [], Acc) ->
    {Acc, Ctx};
decode_fields(Ctx, [#opcua_field{name = Name} = Field | Fields], Data, Acc)
  when is_binary(Name) ->
    #opcua_field{tag = Tag, is_optional = IsOpt, name = Name} = Field,
    case {IsOpt, Data} of
        {_, [{Name, _, FieldData} | Data2]} ->
            {Value, Ctx2} = decode_field(?PUSHF(Ctx, Tag), Field, FieldData),
            Acc2 = maps:put(Tag, Value, Acc),
            decode_fields(?POPF(Ctx2, Tag), Fields, Data2, Acc2);
        {false, Data} ->
            % It seems even though the field is not optional it can be missing,
            % in this case we just set it to some relevent default value...
            Acc2 = maps:put(Tag, default_field_value(Field), Acc),
            decode_fields(Ctx, Fields, Data, Acc2);
        {true, Data} ->
            decode_fields(Ctx, Fields, Data, Acc)
    end;
decode_fields(_Ctx, _Fields, _Data, _Acc) ->
    % This schema do not support XML decoding (missing field name)
    throw(bad_decoding_error).

default_field_value(#opcua_field{node_id = ?NNID(21), value_rank = -1}) ->
    #opcua_localized_text{};
default_field_value(#opcua_field{}) ->
    throw(bad_decoding_error).

%TODO: Support generic arrays sizes
decode_field(Ctx, #opcua_field{node_id = TypeId, value_rank = -1}, Data) ->
    decode_type(Ctx, TypeId, Data);
decode_field(Ctx, #opcua_field{node_id = TypeId, value_rank = 1}, Data) ->
    Name = type2tag(TypeId),
    decode_array(Ctx, TypeId, Name, Data, []);
decode_field(_Ctx, _Field, _Data) ->
    throw(bad_not_implemented).

decode_array(Ctx, _TypeId, _Name, [], Acc) ->
    {lists:reverse(Acc), Ctx};
decode_array(Ctx, TypeId, Name, [{Name, _, Data} | Rest], Acc) ->
    {Value, Ctx2} = decode_type(Ctx, TypeId, Data),
    decode_array(Ctx2, TypeId, Name, Rest, [Value | Acc]);
decode_array(_Ctx, _TypeId, _Name, _Data, _Acc) ->
    throw(bad_decoding_error).

type2tag(#opcua_node_id{} = NodeId) ->
    type2tag(opcua_codec:builtin_type_name(NodeId));
type2tag(boolean) -> <<"Boolean">>;
type2tag(sbyte) -> <<"SByte">>;
type2tag(byte) -> <<"Byte">>;
type2tag(int16) -> <<"Int16">>;
type2tag(uint16) -> <<"UInt16">>;
type2tag(int32) -> <<"Int32">>;
type2tag(uint32) -> <<"UInt32">>;
type2tag(int64) -> <<"Int64">>;
type2tag(uint64) -> <<"UInt64">>;
type2tag(float) -> <<"Float">>;
type2tag(double) -> <<"Double">>;
type2tag(string) -> <<"String">>;
type2tag(date_time) -> <<"DateTime">>;
type2tag(guid) -> <<"String">>;
type2tag(byte_string) -> <<"ByteString">>;
type2tag(xml) -> throw(bad_not_implemented);
type2tag(node_id) -> throw(bad_not_implemented);
type2tag(expanded_node_id) -> throw(bad_not_implemented);
type2tag(status_code) -> <<"Code">>;
type2tag(qualified_name) -> throw(bad_not_implemented);
type2tag(localized_text) -> <<"LocalizedText">>;
type2tag(extension_object) -> <<"ExtensionObject">>;
type2tag(data_value) -> <<"DataValue">>;
type2tag(variant) -> throw(bad_not_implemented);
type2tag(diagnostic_info) -> throw(bad_not_implemented).

tag2type(<<"Boolean">>) -> {-1, boolean};
tag2type(<<"ListOfBoolean">>) -> {1, boolean};
tag2type(<<"SByte">>) -> {-1, sbyte};
tag2type(<<"ListOfSByte">>) -> {1, sbyte};
tag2type(<<"Byte">>) -> {-1, byte};
tag2type(<<"ListOfByte">>) -> {1, byte};
tag2type(<<"Int16">>) -> {-1, int16};
tag2type(<<"ListOfInt16">>) -> {1, int16};
tag2type(<<"UInt16">>) -> {-1, uint16};
tag2type(<<"ListOfUInt16">>) -> {1, uint16};
tag2type(<<"Int32">>) -> {-1, int32};
tag2type(<<"ListOfInt32">>) -> {1, int32};
tag2type(<<"UInt32">>) -> {-1, uint32};
tag2type(<<"ListOfUInt32">>) -> {1, uint32};
tag2type(<<"Int64">>) -> {-1, int64};
tag2type(<<"ListOfInt64">>) -> {1, int64};
tag2type(<<"UInt64">>) -> {-1, uint64};
tag2type(<<"ListOfUInt64">>) -> {1, uint64};
tag2type(<<"Float">>) -> {-1, float};
tag2type(<<"ListOfFloat">>) -> {1, float};
tag2type(<<"Double">>) -> {-1, double};
tag2type(<<"ListOfDouble">>) -> {1, double};
tag2type(<<"String">>) -> {-1, string};
tag2type(<<"ListOfString">>) -> {1, string};
tag2type(<<"DateTime">>) -> {-1, date_time};
tag2type(<<"ListOfDateTime">>) -> {1, date_time};
tag2type(<<"ByteString">>) -> {-1, byte_string};
tag2type(<<"ListOfByteString">>) -> {1, byte_string};
%TODO: Support xml
%TODO: Support node_id
%TODO: Support expanded_node_id
tag2type(<<"Code">>) -> {-1, status_code};
tag2type(<<"ListOfCode">>) -> {1, status_code};
tag2type(<<"QualifiedName">>) -> {-1, qualified_name};
tag2type(<<"ListOfQualifiedName">>) -> {1, qualified_name};
tag2type(<<"LocalizedText">>) -> {-1, localized_text};
tag2type(<<"ListOfLocalizedText">>) -> {1, localized_text};
tag2type(<<"ExtensionObject">>) -> {-1, extension_object};
tag2type(<<"ListOfExtensionObject">>) -> {1, extension_object};
tag2type(<<"DataValue">>) -> {-1, data_value};
tag2type(<<"ListOfDataValue">>) -> {1, data_value};
%TODO: Support variant
%TODO: Support diagnostic_info
tag2type(_Other) -> throw(bad_not_implemented).
