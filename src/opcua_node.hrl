
-ifndef(OPCUA_NODE_INCLUDED).
-define(OPCUA_NODE_INCLUDED, true).

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(NID_NS(NS), #opcua_node_id{ns = NS}).
-define(NNID(Num), #opcua_node_id{ns = 0, type = numeric, value = Num}).
-define(XID_(NID), #expanded_node_id{
    node_id = NID, namespace_uri = undefined, server_index = undefined}).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(role_permission, {
    role_id                     :: opcua_node:node_id(),
    permissions                 :: opcua_node:permissions()
}).

%% TODO: What about optional fields? E.g. display_name should be binary() | undefined?

-record(opcua_node_id, {
    ns = 0                      :: non_neg_integer(),
    type = numeric              :: numeric | string | guid | opaque,
    value = 0                   :: non_neg_integer() | atom() | binary()
}).

-record(opcua_node, {
    node_id                     :: opcua_node:node_id(),
    node_class                  :: opcua_node:node_class_rec(),
    browse_name                 :: opcua:optional(binary()),
    display_name                :: binary(),
    description                 :: opcua:optional(binary()),
    write_mask                  :: opcua:optional(non_neg_integer()),
    user_write_mask             :: opcua:optional(non_neg_integer()),
    role_permissions            :: opcua:optional(opcua_node:role_permission()),
    user_role_permissions       :: opcua:optional(opcua_node:role_permissions()),
    access_restrictions         :: opcua:optional(non_neg_integer()),
    references                  :: opcua_node:refs()
}).

-record(opcua_object, {
    event_notifier              :: byte()
}).

-record(opcua_variable, {
    value                       :: variant(),
    data_type                   :: opcua_node:node_id(),
    value_rank                  :: non_neg_integer(),
    array_dimensions            :: opcua:optional([non_neg_integer()]),
    access_level                :: opcua:optional(byte()),
    user_access_level           :: opcua:optional(byte()),
    minimum_sampling_interval   :: float(),
    historizing                 :: boolean(),
    access_level_ex             :: opcua:uint32()
}).

-record(opcua_method, {
    executable                  :: boolean(),
    user_executable             :: boolean()
}).

-record(opcua_reference, {
    reference_type_id           :: opcua_node:node_id(),
    is_forward                  :: boolean(),
    target_id                   :: expanded_node_id()
}).

-record(opcua_object_type, {
    is_abstract                 :: boolean()
}).

-record(opcua_variable_type, {
    value                       :: variant(),
    data_type                   :: opcua:optional(opcua_node:node_id()),
    value_rank                  :: opcua:optional(integer()),
    array_dimensions            :: opcua:optional([non_neg_integer()]),
    is_abstract                 :: boolean()
}).

-record(opcua_data_type, {
    is_abstract                 :: boolean(),
    data_type_definition        :: term()
}).

-record(opcua_reference_type, {
    is_abstract                 :: boolean(),
    symmetric                   :: boolean(),
    inverse_name                :: localized_text()
}).

-record(opcua_view, {
    contains_no_loops           :: boolean(),
    event_notifier              :: byte()
}).

-endif.