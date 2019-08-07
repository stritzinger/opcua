-include("opcua_codec.hrl").

-record(role_permission, {
    role_id                     :: node_id(),
    permissions                 :: permissions()
}).

%% this is probably never used but still
%% left to sit here as an example for what
%% a bare node looks like
-record(node, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references()
}).

-record(object, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references(),
    event_notifier              :: byte()
}).

-record(variable, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references(),
    value                       :: variant(),
    data_type                   :: node_id(),
    value_rank                  :: non_neg_integer(),
    array_dimensions            :: [non_neg_integer()],
    access_level                :: byte(),
    user_access_level           :: byte(),
    minimum_sampling_interval   :: float(),
    historizing                 :: boolean(),
    access_level_ex             :: non_neg_integer()
}).

-record(method, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references(),
    executable                  :: boolean(),
    user_executable             :: boolean()
}).

-record(reference, {
    reference_type_id           :: node_id(),
    is_inverse                  :: boolean(),
    target_id                   :: expanded_node_id()
}).

-record(object_type, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references(),
    is_abstract                 :: boolean()
}).

-record(variable_type, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references(),
    value                       :: variant(),
    data_type                   :: node_id(),
    value_rank                  :: non_neg_integer(),
    array_dimensions            :: [non_neg_integer()],
    is_abstract                 :: boolean()
}).

-record(data_type, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references(),
    is_abstract                 :: boolean(),
    data_type_definition        :: term()
}).

-record(reference_type, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references(),
    is_abstract                 :: boolean(),
    symmetric                   :: boolean(),
    inverse_name                :: localized_text()
}).

-record(view, {
    node_id                     :: node_id(),
    node_class                  :: node_class(),
    browse_name                 :: binary(),
    display_name                :: binary(),
    description                 :: binary(),
    write_mask                  :: non_neg_integer(),
    user_write_mask             :: non_neg_integer(),
    role_permissions            :: role_permission(),
    user_role_permissions       :: role_permissions(),
    access_restrictions         :: non_neg_integer(),
    references                  :: references(),
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

-type node_class() ::       unspecified|
                            object |
                            variable |
                            method |
                            object_type |
                            variable_type |
                            reference_type |
                            data_type |
                            view.

-type permissions() :: [permission()].
-type role_permission() :: #role_permission{}.
-type role_permissions() :: [role_permission()].
-type references() :: [#reference{}].
