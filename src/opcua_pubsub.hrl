-define(UA_PUBLISHERIDTYPE_BYTE,0).
-define(UA_PUBLISHERIDTYPE_UINT16,1).
-define(UA_PUBLISHERIDTYPE_UINT32,2).
-define(UA_PUBLISHERIDTYPE_UINT64,3).
-define(UA_PUBLISHERIDTYPE_STRING,4).

-record(dataset_mirror,{}).

-record(target_variable,{
    data_set_field_id  = 0 :: non_neg_integer(),
    receiver_index_range,
    target_node_id, % node_id to write to
    attribute_id,   % attribute to write
    write_index_range,
    override_value_handling,
    override_value
}).

-record(data_set_field_metadata,{
    name            :: string(),
    description     :: string(),
    field_flags,    % This flag indicates if the field is promoted to the NetworkMessage header
    builtin_type    :: opcua:builtin_type(),
    data_type       :: opcua:node_id(),
    valueRank       :: integer(),
    array_dimensions,
    maxStringLength,
    data_set_field_id   = 0 :: non_neg_integer(),
    properties
}).

-record(data_set_metadata,{
    name,
    description,
    fields,
    data_set_class_id,
    configuration_version :: undefined | {non_neg_integer(),non_neg_integer()}
}).


-record(data_set_reader_config,{
    name            :: binary(),
    publisher_id,
    publisher_id_type,
    writer_group_id,
    data_set_writer_id,
    data_set_metadata  :: #data_set_metadata{}
}).
