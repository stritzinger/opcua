-record(node_id, {
    ns = 0 :: non_neg_integer(),
    type = numeric :: numeric | string | guid | opaque,
    value :: non_neg_integer() | atom() | binary()
}).

-record(structure, {
    node_id = #node_id{} :: node_id(),
    with_options = false :: boolean(),
    fields = [] :: fields()
}).

-record(union, {
    node_id = #node_id{} :: node_id(),
    fields = [] :: fields()
}).

-record(enum, {
    node_id = #node_id{} :: node_id(),
    fields = [] :: fields()
}).

-record(field, {
    name :: atom(),
    node_id = #node_id{} :: node_id(),
    value_rank = -1 :: value_rank(),
    is_optional = false :: boolean(),
    value :: integer()
}).

-type node_id() :: #node_id{}.
-type node_spec() :: non_neg_integer() | atom() | binary() | node_id() | #node_id{}.
-type opcua_spec() :: node_spec() | [node_spec()] | [{atom(), node_spec()}].
-type opcua_encoding() :: binary.
-type opcua_schema() :: term().
-type field() :: #field{}.
-type fields() :: [field()].
-type value_rank() :: -1 | pos_integer().
-type builtin_type() :: boolean | byte | sbyte | uint16 | uint32 | uint64
                      | int16 | int32 | int64 | float | double | string
                      | date_time | guid | xml | status_code | byte_string
                      | node_id | expanded_node_id | diagnostic_info
                      | qualified_name | localized_text | extension_object
                      | variant | data_value.

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(IS_BUILTIN_TYPE_NAME(T),
    T =:= boolean;
    T =:= sbyte;
    T =:= byte;
    T =:= uint16;
    T =:= uint32;
    T =:= uint64;
    T =:= int16;
    T =:= int32;
    T =:= int64;
    T =:= float;
    T =:= double;
    T =:= string;
    T =:= date_time;
    T =:= guid;
    T =:= xml;
    T =:= status_code;
    T =:= byte_string;
    T =:= node_id;
    T =:= expanded_node_id;
    T =:= diagnostic_info;
    T =:= qualified_name;
    T =:= localized_text;
    T =:= extension_object;
    T =:= variant;
    T =:= data_value
).

-define(IS_BUILTIN_TYPE_ID(T), is_integer(T), T > 0, T =< 25).
