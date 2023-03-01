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

%% API Functions
-export([decode/2, decode/3]).


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


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
    decode_type(Ctx, BuiltinNodeId, Data).

decode_fields(Ctx, Fields, Data) ->
    decode_fields(Ctx, Fields, Data, #{}).

decode_fields(Ctx, [], [], Acc) ->
    {Acc, Ctx};
decode_fields(Ctx, [Field | Fields], Data, Acc) ->
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
    end.

default_field_value(#opcua_field{node_id = ?NNID(21), value_rank = -1}) ->
    #opcua_localized_text{};
default_field_value(#opcua_field{}) ->
    throw(bad_decoding_error).

%TODO: Support generic arrays sizes
decode_field(Ctx, #opcua_field{node_id = TypeId, value_rank = -1}, Data) ->
    decode_type(Ctx, TypeId, Data);
decode_field(Ctx, #opcua_field{node_id = TypeId, value_rank = 1}, Data) ->
    Name = opcua_codec_xml_builtin:tag_name(-1, TypeId),
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
