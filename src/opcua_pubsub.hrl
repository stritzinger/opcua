-define(UA_PUBLISHERIDTYPE_BYTE,0).
-define(UA_PUBLISHERIDTYPE_UINT16,1).
-define(UA_PUBLISHERIDTYPE_UINT32,2).
-define(UA_PUBLISHERIDTYPE_UINT64,3).
-define(UA_PUBLISHERIDTYPE_STRING,4).

-type pubsub_state_machine() :: operational | error | enabled | paused.

-record(dataset_mirror,{}).

-record(target_variable,{
    dataset_field_id  = 0 :: non_neg_integer(),
    receiver_index_range,
    target_node_id, % node_id to write to
    attribute_id,   % attribute to write
    write_index_range,
    override_value_handling,
    override_value
}).

-record(dataset_field_metadata,{
    name            :: string(),
    description     :: string(),
    field_flags,    % This flag indicates if the field is promoted to the NetworkMessage header
    builtin_type    :: opcua:builtin_type(),
    data_type       :: opcua:node_id(),
    valueRank       :: integer(),
    array_dimensions,
    maxStringLength,
    dataset_field_id   = 0 :: non_neg_integer(),
    properties
}).

-record(dataset_metadata,{
    name,
    description,
    fields = []             :: list(#dataset_field_metadata{}),
    dataset_class_id,
    configuration_version   :: undefined | {non_neg_integer(),non_neg_integer()}
}).

-record(dataset_reader_config,{
    name                :: binary(),
    publisher_id,
    publisher_id_type,
    writer_group_id,
    dataset_writer_id,
    dataset_metadata   :: #dataset_metadata{}
}).

-record(published_variable,{
    published_variable,
    attribute_id,
    sampling_interval_hint = -1,
    deadband_type = 0           :: 0 | 1 | 2,
    deadband_value = 0.0        :: float(),
    index_rande,
    substitute_value,
    metadata_properties = []
}).

-record(published_events, {
    event_notifier      :: opcua:node_id(),
    selected_fields     :: list(),
    filter
}).

-type published_dataset_source()   :: list(#published_variable{}) |
                                        #published_events{}.

-record(published_dataset,{
    name,
    dataset_folder         :: list(),% path to the destination folder
    dataset_metadata       :: #dataset_metadata{},
    extension_fields,
    dataset_source = []    :: published_dataset_source()
}).

-record(writer_group_config,{
    enabled = true :: boolean(),
    name,
    writer_group_id,
    publishing_interval,
    keep_alive_time,
    priority,
    locale_ids,
    transport_settings,
    message_settings
}).

-record(dataset_writer_config,{
    name                        :: binary(),
    dataset_writer_id           :: non_neg_integer(),
    dataset_field_content_mask,
    keyframe_count = 1          :: non_neg_integer(),
    dataset_name                :: string(),
    transport_settings,
    message_settings
}).