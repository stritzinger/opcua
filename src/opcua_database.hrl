-record(role_permission, {
    role_id                     :: node_id(),
    permissions                 :: permissions()
}).

%% TODO: What about optional fields? E.g. display_name should be binary() | undefined?

-record(opcua_node_id, {
    ns = 0                      :: non_neg_integer(),
    type = numeric              :: numeric | string | guid | opaque,
    value = 0                   :: non_neg_integer() | atom() | binary()
}).

-record(opcua_node, {
    node_id                     :: #opcua_node_id{},
    node_class                  :: node_class(),
    browse_name                 :: optional(binary()),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references()
}).

-record(opcua_object, {
    event_notifier              :: byte()
}).

-record(opcua_variable, {
    value                       :: variant(),
    data_type                   :: node_id(),
    value_rank                  :: non_neg_integer(),
    array_dimensions            :: optional([non_neg_integer()]),
    access_level                :: optional(byte()),
    user_access_level           :: optional(byte()),
    minimum_sampling_interval   :: float(),
    historizing                 :: boolean(),
    access_level_ex             :: uint32()
}).

-record(opcua_method, {
    executable                  :: boolean(),
    user_executable             :: boolean()
}).

-record(opcua_reference, {
    reference_type_id           :: node_id(),
    is_forward                  :: boolean(),
    target_id                   :: expanded_node_id()
}).

-record(opcua_object_type, {
    is_abstract                 :: boolean()
}).

-record(opcua_variable_type, {
    value                       :: variant(),
    data_type                   :: optional(node_id()),
    value_rank                  :: optional(integer()),
    array_dimensions            :: optional([non_neg_integer()]),
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

-type reference_type() ::   references |
                            hierarchical_references |
                            non_hierarchical_references |
                            has_event_source |
                            has_child |
                            organizes |
                            has_notifier |
                            aggregates |
                            has_subtype |
                            has_property |
                            has_component |
                            has_ordered_component |
                            generates_event |
                            always_generates_event |
                            has_encoding |
                            has_modelling_rule |
                            has_type_definition.

-type permission() ::       browse |
                            read_role_permissions |
                            write_attribute |
                            write_role_permissions |
                            write_historizing |
                            read |
                            write |
                            read_history |
                            insert_history |
                            modify_history |
                            delete_history |
                            receive_events |
                            call |
                            add_reference |
                            remove_reference |
                            delete_node |
                            add_node.

-type node_class() ::       #opcua_object{} |
                            #opcua_variable{} |
                            #opcua_method{} |
                            #opcua_object_type{} |
                            #opcua_variable_type{} |
                            #opcua_reference_type{} |
                            #opcua_data_type{} |
                            #opcua_view{}.

-type permissions() :: [permission()].
-type role_permission() :: #role_permission{}.
-type role_permissions() :: [role_permission()].
-type references() :: [#opcua_reference{}].

-type uint32() :: 0..4294967295.

-type optional(Type) :: undefined | Type.
