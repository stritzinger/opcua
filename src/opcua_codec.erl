-module(opcua_codec).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([builtin_type_name/1]).
-export([builtin_type_id/1]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type builtin_type() :: boolean | byte | sbyte | uint16 | uint32 | uint64
                      | int16 | int32 | int64 | float | double | string
                      | date_time | guid | xml | status_code | byte_string
                      | node_id | expanded_node_id | diagnostic_info
                      | qualified_name | localized_text | extension_object
                      | variant | data_value.

-type schema() :: term().

-type spec() :: builtin_type() | [builtin_type()].


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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