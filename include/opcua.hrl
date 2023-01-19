
%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-- Node Id Helper Macros ------------------------------------------------------

-define(NID_NS(NS), #opcua_node_id{ns = NS}).
-define(NNID(Num), #opcua_node_id{ns = 0, type = numeric, value = Num}).
-define(NNID(NS, Num), #opcua_node_id{ns = NS, type = numeric, value = Num}).
-define(XID(NID), #opcua_expanded_node_id{node_id = NID,
    namespace_uri = undefined, server_index = undefined}).
-define(XID(IDX, NID), #opcua_expanded_node_id{node_id = NID,
    namespace_uri = undefined, server_index = IDX}).

-define(UNDEF_NODE_ID, #opcua_node_id{ns = 0, type = numeric, value = 0}).
-define(UNDEF_QUALIFIED_NAME, #opcua_qualified_name{ns = 0, name = undefined}).
-define(UNDEF_LOCALIZED_TEXT, #opcua_localized_text{}).


%-- OPCUA Standard Node Id Numbers ---------------------------------------------

-define(REF_HAS_CHILD,              34).
-define(REF_ORGANIZES,              35).
-define(REF_HAS_ENCODING,           38).
-define(REF_HAS_DESCRIPTION,        39).
-define(REF_HAS_TYPE_DEFINITION,    40).
-define(REF_HAS_SUBTYPE,            45).
-define(REF_HAS_PROPERTY,           46).
-define(REF_HAS_COMPONENT,          47).
-define(REF_HAS_NOTIFIER,           48).

-define(TYPE_BASE_OBJECT,           58).
-define(TYPE_FOLDER,                61).
-define(TYPE_PROPERTY,              68).

-define(OBJ_ROOT_FOLDER,            84).
-define(OBJ_OBJECTS_FOLDER,         85).
-define(OBJ_SERVER,                 2253).
-define(OBJ_SERVER_STATUS,          2256).

-define(OBJ_SERVER_TYPE,            2004).
-define(OBJ_SERVER_STATUS_TYPE,     2138).

% Attribute Id
%  ------------
%  Every node in an OPC UA information model contains attributes depending on
%  the node type. Possible attributes are as follows:
-define(UA_ATTRIBUTEID_NODEID, 1).
-define(UA_ATTRIBUTEID_NODECLASS, 2).
-define(UA_ATTRIBUTEID_BROWSENAME, 3).
-define(UA_ATTRIBUTEID_DISPLAYNAME, 4).
-define(UA_ATTRIBUTEID_DESCRIPTION, 5).
-define(UA_ATTRIBUTEID_WRITEMASK, 6).
-define(UA_ATTRIBUTEID_USERWRITEMASK, 7).
-define(UA_ATTRIBUTEID_ISABSTRACT, 8).
-define(UA_ATTRIBUTEID_SYMMETRIC, 9).
-define(UA_ATTRIBUTEID_INVERSENAME, 10).
-define(UA_ATTRIBUTEID_CONTAINSNOLOOPS, 11).
-define(UA_ATTRIBUTEID_EVENTNOTIFIER, 12).
-define(UA_ATTRIBUTEID_VALUE, 13).
-define(UA_ATTRIBUTEID_DATATYPE, 14).
-define(UA_ATTRIBUTEID_VALUERANK, 15).
-define(UA_ATTRIBUTEID_ARRAYDIMENSIONS, 16).
-define(UA_ATTRIBUTEID_ACCESSLEVEL, 17).
-define(UA_ATTRIBUTEID_USERACCESSLEVEL, 18).
-define(UA_ATTRIBUTEID_MINIMUMSAMPLINGINTERVAL,19).
-define(UA_ATTRIBUTEID_HISTORIZING, 20).
-define(UA_ATTRIBUTEID_EXECUTABLE, 21).
-define(UA_ATTRIBUTEID_USEREXECUTABLE, 22).
-define(UA_ATTRIBUTEID_DATATYPEDEFINITION, 23).
-define(UA_ATTRIBUTEID_ROLEPERMISSIONS, 24).
-define(UA_ATTRIBUTEID_USERROLEPERMISSIONS, 25).
-define(UA_ATTRIBUTEID_ACCESSRESTRICTIONS, 26).
-define(UA_ATTRIBUTEID_ACCESSLEVELEX, 27).
%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-- OPCUA Types Records --------------------------------------------------------

-record(opcua_node_id, {
    ns = 0                      :: non_neg_integer(),
    type = numeric              :: opcua:node_id_type(),
    value = 0                   :: non_neg_integer() | atom() | binary()
}).

-record(opcua_expanded_node_id, {
    node_id = ?UNDEF_NODE_ID    :: opcua:node_id(),
    namespace_uri               :: opcua:optional(binary()),
    server_index                :: opcua:optional(non_neg_integer())
}).

-record(opcua_qualified_name, {
    ns  = 0                     :: non_neg_integer(),
    name                        :: opcua:optional(binary())
}).

-record(opcua_localized_text, {
    locale                      :: opcua:optional(binary()),
    text                        :: opcua:optional(binary())
}).

-record(opcua_extension_object, {
    type_id = ?UNDEF_NODE_ID    :: opcua:node_id(),
    encoding                    :: opcua:optional(opcua:extobj_encoding()),
    body                        :: term()
}).

-record(opcua_data_value, {
    value                       :: term(),
    status = good               :: opcua:status(),
    source_timestamp = 0        :: non_neg_integer(),
    server_timestamp = 0        :: non_neg_integer(),
    source_pico_seconds = 0     :: non_neg_integer(),
    server_pico_seconds = 0     :: non_neg_integer()
}).

-record(opcua_variant, {
    type                        :: undefined | opcua:builtin_type(),
    value                       :: term()
}).

-record(opcua_error, {
    status                      :: opcua:status(),
    % This is only added by the client during decoding to simplify
    % error handling, to give a hint of what node was involved in the error
    % without having the API user keep track of what it requested when the
    % the client actually knows this information.
    node_id                     :: undefined | opcua:node_id()
}).


%-- Node Model Records ---------------------------------------------------------

-record(opcua_role_permission, {
    role_id                     :: opcua:node_id(),
    permissions                 :: opcua:permissions()
}).

-record(opcua_node, {
    node_id                     :: undefind | opcua:node_id(),
    node_class                  :: opcua:node_class_rec(),
    origin                      :: opcua:node_origin(),
    browse_name                 :: opcua:optional(binary()),
    display_name                :: opcua:optional(binary()),
    description                 :: opcua:optional(binary()),
    %TODO: create a type for write mask option set
    write_mask = []             :: [atom()],
    user_write_mask = []        :: [atom()],
    %FIXME: Role permision typing is broken
    role_permissions            :: opcua:optional(opcua:role_permission()),
    user_role_permissions       :: opcua:optional(opcua:role_permissions()),
    %TODO: create a type for access restriction option set
    access_restrictions = []    :: [atom()]
}).

-record(opcua_object, {
    event_notifier              :: undefined | byte()
}).

-record(opcua_variable, {
    value                       :: term(),
    data_type                   :: opcua:builtin_type() | opcua:node_id(),
    value_rank = -1             :: opcua:value_rank(),
    array_dimensions = []       :: [non_neg_integer()],
    %TODO: create a type for access level option set
    access_level = []           :: [atom()],
    %TODO: create a type for access level option set
    user_access_level = []      :: [atom()],
    minimum_sampling_interval   :: undefined | float(),
    historizing = false         :: boolean(),
    %TODO: create a type for access level extended option set
    access_level_ex = []        :: [atom()]
}).

-record(opcua_method, {
    executable                  :: boolean(),
    user_executable             :: boolean()
}).

-record(opcua_reference, {
    type_id                     :: opcua:node_id(),
    source_id                   :: opcua:node_id(),
    target_id                   :: opcua:node_id()
}).

-record(opcua_object_type, {
    is_abstract = false         :: boolean()
}).

-record(opcua_variable_type, {
    value                       :: term(),
    data_type                   :: opcua:builtin_type() | opcua:node_id(),
    value_rank = -1             :: opcua:value_rank(),
    array_dimensions = []       :: [non_neg_integer()],
    is_abstract = false         :: boolean()
}).

-record(opcua_data_type, {
    is_abstract = false         :: boolean(),
    data_type_definition        :: term()
}).

-record(opcua_reference_type, {
    is_abstract = false         :: boolean(),
    symmetric = false           :: boolean(),
    inverse_name                :: opcua:optional(binary())
}).

-record(opcua_view, {
    contains_no_loops           :: boolean(),
    event_notifier              :: byte()
}).


%-- Node Command Records -------------------------------------------------------

-record(opcua_read_command, {
    attr                        :: atom(),
    range                       :: opcua:optional([opcua:range()]),
    opts  = #{}                 :: opcua:read_options()
}).

-record(opcua_browse_command, {
    type                        :: opcua:optional(opcua:node_id()),
    subtypes = true             :: boolean(),
    direction = forward         :: opcua:direction(),
    opts = #{}                  :: opcua:browse_options()
}).
