-module(opcua_codec).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([pack_variant/2]).
-export([unpack_variant/2]).
-export([builtin_type_name/1]).
-export([builtin_type_id/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec pack_variant(opcua:node_spec(), term()) -> opcua:variant().
pack_variant(#opcua_node_id{ns = 0, type = numeric, value = Num}, Value)
  when ?IS_BUILTIN_TYPE_ID(Num) ->
    #opcua_variant{type = builtin_type_name(Num), value = Value};
pack_variant(#opcua_node_id{ns = 0, type = string, value = Name}, Value)
  when ?IS_BUILTIN_TYPE_NAME(Name) ->
    #opcua_variant{type = Name, value = Value};
pack_variant(#opcua_node_id{} = NodeId, Value) ->
    case opcua_nodeset:schema(NodeId) of
        undefined -> throw({bad_encoding_error, {schema_not_found, NodeId}});
        #opcua_enum{fields = Fields} ->
            [Idx] = [I || #opcua_field{name = N, value = I} <- Fields, Value =:= N],
            #opcua_variant{type = int32, value = Idx};
        _Other ->
            ExtObj = #opcua_extension_object{type_id = NodeId, encoding = byte_string, body = Value},
            #opcua_variant{type = extension_object, value = ExtObj}
    end;
pack_variant(NodeSpec, Value) ->
    pack_variant(opcua_node:id(NodeSpec), Value).

-spec unpack_variant(opcua:node_spec(), opcua:variant()) -> term().
unpack_variant(_Type, _Value) ->
    throw(bad_not_implemented).

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
