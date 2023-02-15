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
-export([pack_variant/3]).
-export([builtin_type_name/1]).
-export([builtin_type_id/1]).

%% Schema resolver
-export([resolve/3]).
-export([resolve_enum/2]).
-export([resolve_option_set/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec pack_variant(opcua_space:state(), opcua:node_spec(), term()) -> opcua:variant().
pack_variant(_Space, #opcua_node_id{ns = 0, type = numeric, value = Num}, Value)
  when ?IS_BUILTIN_TYPE_ID(Num) ->
    #opcua_variant{type = builtin_type_name(Num), value = Value};
pack_variant(_Space, #opcua_node_id{ns = 0, type = string, value = Name}, Value)
  when ?IS_BUILTIN_TYPE_NAME(Name) ->
    #opcua_variant{type = Name, value = Value};
pack_variant(Space, #opcua_node_id{} = NodeId, Value) ->
    case opcua_space:schema(Space, NodeId) of
        undefined -> throw({bad_encoding_error, {schema_not_found, NodeId}});
        #opcua_enum{fields = Fields} ->
            %TODO: Remove code duplication with opcua_codec_binary
            [Idx] = [I || #opcua_field{name = N, value = I} <- Fields, Value =:= N],
            #opcua_variant{type = int32, value = Idx};
        #opcua_option_set{fields = Fields} ->
            %TODO: Remove code duplication with opcua_codec_binary
            %TODO: Add support for variable option set size
            ChosenFields = [Field || Field = #opcua_field{name = Name} <- Fields,
                            lists:member(Name, Value)],
            Int = lists:foldl(fun(X, Acc) ->
                    Acc bxor (1 bsl X#opcua_field.value)
            end, 0, ChosenFields),
            #opcua_variant{type = uint32, value = Int};
        _Other ->
            ExtObj = #opcua_extension_object{type_id = NodeId, encoding = byte_string, body = Value},
            #opcua_variant{type = extension_object, value = ExtObj}
    end;
pack_variant(Space, NodeSpec, Value) ->
    pack_variant(Space, opcua_node:id(NodeSpec), Value).

-spec resolve(opcua_space:state(), opcua:node_spec(), integer()) -> term().
resolve(Space, TypeId, Value) ->
    case opcua_space:schema(Space, TypeId) of
        undefined -> throw({schema_not_found, TypeId});
        #opcua_enum{} = Schema -> resolve_enum(Schema, Value);
        #opcua_option_set{} = Schema -> resolve_option_set(Schema, Value);
        #opcua_structure{} -> resolve_structure(Space, TypeId, Value);
        _Schema -> throw(not_implemented)
    end.

resolve_structure(Space, RootType, #opcua_extension_object{type_id = SubType, body = Data}) ->
    case opcua_space:is_subtype(Space, SubType, RootType) of
        false -> throw({invalid_subtype, SubType, RootType});
        true -> Data
    end.

resolve_enum(#opcua_enum{node_id = TypeId, fields = Fields}, Value) ->
    case [F || F = #opcua_field{value = V} <- Fields, Value =:= V] of
        [Field] -> Field#opcua_field.name;
        [] -> throw({bad_enum_value, TypeId, Value})
    end.

resolve_option_set(#opcua_option_set{fields = Fields}, Value) ->
    FieldNames = lists:foldl(fun(X, Acc) ->
        case (Value bsr X#opcua_field.value) rem 2 of
            0  -> Acc;
            1  -> [X#opcua_field.name | Acc]
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
builtin_type_name(26) -> byte_string;
builtin_type_name(27) -> byte_string;
builtin_type_name(28) -> byte_string;
builtin_type_name(29) -> byte_string;
builtin_type_name(30) -> byte_string;
builtin_type_name(31) -> byte_string.

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
