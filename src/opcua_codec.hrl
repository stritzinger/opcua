-record(node_id, {
    ns = default :: default | non_neg_integer(),
    type = numeric :: numeric | string | guid | opaque,
    value :: non_neg_integer() | binary()
}).

-record(data_type, {
    node_id = #node_id{} :: node_id(),
    type = structure :: type(),
    with_options = false :: boolean(),
    fields = [] :: fields()
}).

-record(field, {
    name :: atom(),
    node_id = #node_id{} :: node_id(),
    value_rank = -1 :: value_rank(),
    is_optional = false :: boolean(),
    value :: integer()
}).

-type node_id() :: non_neg_integer() | atom() | binary() | #node_id{}.
-type opcua_spec() :: node_id() | [node_id()].
-type type() :: {builtin, builtin_type()} | structure | union | enum.
-type field() :: #field{}.
-type fields() :: [field()].
-type data_type() :: #data_type{}.
-type value_rank() :: -1 | pos_integer().
-type builtin_type() :: boolean | byte | sbyte | uint16 | uint32 | uint64
                      | int16 | int32 | int64 | float | double | string
                      | date_time | guid | xml | status_code | byte_string
                      | node_id | expanded_node_id | diagnostic_info
                      | qualified_name | localized_text | extension_object
                      | variant | data_value.

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(IS_BUILTIN_TYPE(T),
    (T =:= 1 );(T =:= boolean);
    (T =:= 2 );(T =:= byte);
    (T =:= 3 );(T =:= byte);
    (T =:= 4 );(T =:= uint16);
    (T =:= 5 );(T =:= uint32);
    (T =:= 6 );(T =:= uint64);
    (T =:= 7 );(T =:= int16);
    (T =:= 8 );(T =:= int32);
    (T =:= 9 );(T =:= int64);
    (T =:= 10);(T =:= float);
    (T =:= 11);(T =:= double);
    (T =:= 12);(T =:= string);
    (T =:= 13);(T =:= date_time);
    (T =:= 14);(T =:= guid);
    (T =:= 15);(T =:= xml);
    (T =:= 16);(T =:= status_code);
    (T =:= 17);(T =:= byte_string);
    (T =:= 18);(T =:= node_id);
    (T =:= 19);(T =:= expanded_node_id);
    (T =:= 20);(T =:= diagnostic_info);
    (T =:= 21);(T =:= qualified_name);
    (T =:= 22);(T =:= localized_text);
    (T =:= 23);(T =:= extension_object);
    (T =:= 24);(T =:= variant);
    (T =:= 25);(T =:= data_value)
).
