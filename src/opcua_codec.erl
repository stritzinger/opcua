-module(opcua_codec).

%% TODO %%
%%
%% - Add or validate support for variable sized option sets
%% - Add or validate the support of data types with subtype as fields;
%%   e.g. a structure data type with a field that could be specified as any
%%   subtype of sthe defined type. See https://reference.opcfoundation.org/Core/Part3/v105/docs/8.49

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([pack_variant/4]).
-export([pack_enum/2]).
-export([pack_option_set/2]).
-export([unpack_type/3]).
-export([unpack_enum/2]).
-export([unpack_option_set/2]).
-export([builtin_type_name/1]).
-export([builtin_type_id/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%TODO: Add suport for generic value ranks. Maybe we should get the demensions too.
-spec pack_variant(opcua_space:state(), opcua:node_spec(), opcua:value_rank(), term()) -> opcua:variant().
pack_variant(_Space, _Type, _ValueRank, undefined) ->
    #opcua_variant{};
pack_variant(_Space, #opcua_node_id{ns = 0, type = numeric, value = Num}, 1, Value)
  when ?IS_BUILTIN_TYPE_ID(Num), is_list(Value) ->
    #opcua_variant{type = builtin_type_name(Num), value = Value};
pack_variant(_Space, #opcua_node_id{ns = 0, type = numeric, value = Num}, -1, Value)
  when ?IS_BUILTIN_TYPE_ID(Num) ->
    #opcua_variant{type = builtin_type_name(Num), value = Value};
pack_variant(_Space, #opcua_node_id{ns = 0, type = string, value = Name}, 1, Value)
  when ?IS_BUILTIN_TYPE_NAME(Name), is_list(Value) ->
    #opcua_variant{type = Name, value = Value};
pack_variant(_Space, #opcua_node_id{ns = 0, type = string, value = Name}, -1, Value)
  when ?IS_BUILTIN_TYPE_NAME(Name) ->
    #opcua_variant{type = Name, value = Value};
pack_variant(Space, #opcua_node_id{} = NodeId, 1, Values)
  when is_list(Values) ->
    case opcua_space:schema(Space, NodeId) of
        undefined -> throw({bad_encoding_error, {schema_not_found, NodeId}});
        #opcua_enum{} = Schema ->
            Data = [pack_enum(Schema, V) || V <- Values],
            #opcua_variant{type = int32, value = Data};
        #opcua_option_set{mask_type = TypeId} = Schema ->
            Data = [pack_option_set(Schema, V) || V <- Values],
            #opcua_variant{type = TypeId, value = Data};
        _Other ->
            Data = [#opcua_extension_object{type_id = NodeId,
                                            encoding = byte_string, body = V}
                    || V <- Values],
            #opcua_variant{type = extension_object, value = Data}
    end;
pack_variant(Space, #opcua_node_id{} = NodeId, -1, Value) ->
    case opcua_space:schema(Space, NodeId) of
        undefined -> throw({bad_encoding_error, {schema_not_found, NodeId}});
        #opcua_enum{} = Schema ->
            #opcua_variant{type = int32, value = pack_enum(Schema, Value)};
        #opcua_option_set{mask_type = TypeId} = Schema ->
            Int = pack_option_set(Schema, Value),
            #opcua_variant{type = TypeId, value = Int};
        _Other ->
            ExtObj = #opcua_extension_object{type_id = NodeId, encoding = byte_string, body = Value},
            #opcua_variant{type = extension_object, value = ExtObj}
    end;
pack_variant(_Space, #opcua_node_id{}, _ValueRank, _Value) ->
    throw(bad_encoding_error);
pack_variant(Space, NodeSpec, ValueRank, Value) ->
    pack_variant(Space, opcua_node:id(NodeSpec), ValueRank, Value).

pack_enum(#opcua_enum{fields = Fields}, Value) ->
    [Result] = [I || #opcua_field{tag = T, value = I} <- Fields, Value =:= T],
    Result.

pack_option_set(#opcua_option_set{fields = Fields}, Value) ->
    Bits = [V || #opcua_field{tag = T, value = V} <- Fields,
                 lists:member(T, Value)],
    lists:foldl(fun(BitIdx, Acc) -> Acc bxor (1 bsl BitIdx) end, 0, Bits).

-spec unpack_type(opcua_space:state(), opcua:node_spec(), integer()) -> term().
unpack_type(Space, TypeId, #opcua_extension_object{} = Value) ->
    unpack_structure(Space, TypeId, Value);
unpack_type(Space, TypeId, Value) ->
    case opcua_space:schema(Space, TypeId) of
        undefined -> throw({schema_not_found, TypeId});
        #opcua_enum{} = Schema -> unpack_enum(Schema, Value);
        #opcua_option_set{} = Schema -> unpack_option_set(Schema, Value)
    end.

unpack_structure(Space, RootType, #opcua_extension_object{type_id = SubType, body = Data}) ->
    case opcua_space:is_subtype(Space, SubType, RootType) of
        false -> throw({invalid_subtype, SubType, RootType});
        true -> Data
    end.

unpack_enum(#opcua_enum{node_id = TypeId, fields = Fields}, Value) ->
    case [F || F = #opcua_field{value = V} <- Fields, Value =:= V] of
        [Field] -> Field#opcua_field.tag;
        [] -> throw({bad_enum_value, TypeId, Value})
    end.

unpack_option_set(#opcua_option_set{fields = Fields}, Value) ->
    FieldNames = lists:foldl(fun(X, Acc) ->
        case (Value bsr X#opcua_field.value) rem 2 of
            0  -> Acc;
            1  -> [X#opcua_field.tag | Acc]
        end
     end, [], Fields),
    lists:reverse(FieldNames).

builtin_type_name( 1) -> boolean;
builtin_type_name( 2) -> sbyte;
builtin_type_name( 3) -> byte;
builtin_type_name( 4) -> int16;
builtin_type_name( 5) -> uint16;
builtin_type_name( 6) -> int32;
builtin_type_name( 7) -> uint32;
builtin_type_name( 8) -> int64;
builtin_type_name( 9) -> uint64;
builtin_type_name(10) -> float;
builtin_type_name(11) -> double;
builtin_type_name(12) -> string;
builtin_type_name(13) -> date_time;
builtin_type_name(14) -> guid;
builtin_type_name(15) -> byte_string;
builtin_type_name(16) -> xml;
builtin_type_name(17) -> node_id;
builtin_type_name(18) -> expanded_node_id;
builtin_type_name(19) -> status_code;
builtin_type_name(20) -> qualified_name;
builtin_type_name(21) -> localized_text;
builtin_type_name(22) -> extension_object;
builtin_type_name(23) -> data_value;
builtin_type_name(24) -> variant;
builtin_type_name(25) -> diagnostic_info;
builtin_type_name(#opcua_node_id{type = numeric, value = TypeNum}) ->
    builtin_type_name(TypeNum).

builtin_type_id(boolean)            -> 1;
builtin_type_id(sbyte)              -> 2;
builtin_type_id(byte)               -> 3;
builtin_type_id(int16)              -> 4;
builtin_type_id(uint16)             -> 5;
builtin_type_id(int32)              -> 6;
builtin_type_id(uint32)             -> 7;
builtin_type_id(int64)              -> 8;
builtin_type_id(uint64)             -> 9;
builtin_type_id(float)              -> 10;
builtin_type_id(double)             -> 11;
builtin_type_id(string)             -> 12;
builtin_type_id(date_time)          -> 13;
builtin_type_id(guid)               -> 14;
builtin_type_id(byte_string)        -> 15;
builtin_type_id(xml)                -> 16;
builtin_type_id(node_id)            -> 17;
builtin_type_id(expanded_node_id)   -> 18;
builtin_type_id(status_code)        -> 19;
builtin_type_id(qualified_name)     -> 20;
builtin_type_id(localized_text)     -> 21;
builtin_type_id(extension_object)   -> 22;
builtin_type_id(data_value)         -> 23;
builtin_type_id(variant)            -> 24;
builtin_type_id(diagnostic_info)    -> 25.
