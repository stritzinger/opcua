
%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua_node.hrl").


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(expanded_node_id, {
    node_id = #opcua_node_id{} :: opcua_node:node_id(),
    namespace_uri :: undefined | binary(),
    server_index :: undefined | non_neg_integer()
}).

-record(qualified_name, {
    ns  = 0 :: non_neg_integer(),
    name :: undefined | binary()
}).

-record(localized_text, {
    locale :: undefined | binary(),
    text :: undefined | binary()
}).

-record(extension_object, {
    type_id = #opcua_node_id{} :: opcua_node:node_id(),
    encoding :: xml | byte_string | undefined,
    body :: term()
}).

-record(data_value, {
    value :: term(),
    status = good :: atom() | pos_integer(),
    source_timestamp = 0 :: non_neg_integer(),
    source_pico_seconds = 0 :: non_neg_integer(),
    server_timestamp = 0 :: non_neg_integer(),
    server_pico_seconds = 0 :: non_neg_integer()
}).

-record(variant, {
    type :: builtin_type(),
    value :: term()
}).

-record(diagnostic_info, {
    symbolic_id :: integer(),
    namespace_uri :: integer(),
    locale :: integer(),
    localized_text :: integer(),
    additional_info :: binary(),
    inner_status_code :: integer(),
    inner_diagnostic_info :: term()
}).

-record(structure, {
    node_id = #opcua_node_id{} :: opcua_node:node_id(),
    with_options = false :: boolean(),
    fields = [] :: fields()
}).

-record(union, {
    node_id = #opcua_node_id{} :: opcua_node:node_id(),
    fields = [] :: fields()
}).

-record(enum, {
    node_id = #opcua_node_id{} :: opcua_node:node_id(),
    fields = [] :: fields()
}).

-record(builtin, {
    node_id = #opcua_node_id{} :: opcua_node:node_id(),
    builtin_node_id = #opcua_node_id{} :: opcua_node:node_id()
}).

-record(field, {
    name :: atom(),
    node_id = #opcua_node_id{} :: opcua_node:node_id(),
    value_rank = -1 :: value_rank(),
    is_optional = false :: boolean(),
    value :: integer()
}).

-type expanded_node_id() :: #expanded_node_id{}.
-type qualified_name() :: #qualified_name{}.
-type localized_text() :: #localized_text{}.
-type extension_object() :: #extension_object{}.
-type variant() :: #variant{}.
-type data_value() :: #data_value{}.
-type diagnostic_info() :: #diagnostic_info{}.
-type node_spec() :: non_neg_integer() | atom() | binary() | opcua_node:node_id().
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

-define(UNDEF_NODE_ID, #opcua_node_id{ns = 0, type = numeric, value = 0}).
-define(UNDEF_EXT_NODE_ID,
    #expanded_node_id{node_id = ?UNDEF_NODE_ID,
                      namespace_uri = undefined,
                      server_index = undefined}).
-define(UNDEF_EXT_OBJ,
    #extension_object{type_id = ?UNDEF_NODE_ID,
                      body = undefined}).
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
