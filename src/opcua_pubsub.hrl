-record(field_meta_data, {
    name = <<"system time">>,
    description,
    field_flags = [],
    built_in_type = 7,
    data_type,
    value_rank = -1,
    array_dimensions = [],
    max_string_length = 0,
    data_set_field_id = 1,
    properties = []
}).

-record(configuration_version, {
    major_version = 1,
    minor_version = 0
}).

-record(data_set_meta_data, {
    name = <<"">>,
    description,
    fields = [#field_meta_data{}],
    data_set_class_id,
    configuration_version = #configuration_version{}
}).

-record(published_variable, {
    published_variable,
    attribute_id,
    sampling_interval_hint = 100,
    deadband_type,
    deadband_value,
    index_range,
    substitute_value,
    meta_data_properties = []
}).

-record(published_data_items, {
    published_data = [#published_variable{}]
}).

-record(element_operand, {
    index         
}).

-record(literal_operand, {
    value
}).

-record(attribute_operand, {
    node_id,
    alias,
    browse_path,
    attribute_id,
    index_range
}).

-record(simple_attribute_operand, {
    type_definition_id,
    browse_path = [],
    attribute_id,
    index_range    
}).

-record(content_filter_element, {
    filter_operator,
    filter_operands = []    
}).

-record(content_filter, {
    elements = []         
}).

-record(published_events, {
    event_notifier,
    selected_fields = [],
    filter = #content_filter{}
}).

-record(published_data_set, {
    name = <<"demo published data set">>,
    data_set_folder,
    data_set_meta_data = #data_set_meta_data{},
    extension_fields = [],
    data_set_source = #published_data_items{}
}).

-record(broker_data_set_writer_transport, {
    queue_name,
    resource_uri,
    authentication_profile_uri,
    requested_delivery_guarantee,
    meta_data_queue_name,
    meta_data_update_time    
}).

-record(uadp_data_set_writer_message, {
    data_set_message_content_mask,
    configured_size,
    network_message_number,
    data_set_offset
}).

-record(json_data_set_writer_message, {
    data_set_message_content_mask
}).

-record(data_set_writer, {
    name = <<"demo data set writer">>,
    enabled = false,
    data_set_writer_id = 1,
    data_set_field_content_mask = [status_code, source_timestamp, server_timestamp,
                                   source_pico_seconds, server_picoseconds],
    key_frame_count = 10,
    data_set_name = <<"demo published data set">>,
    data_set_writer_properties,
    transport_settings = #broker_data_set_writer_transport{},
    message_settings = #uadp_data_set_writer_message{}
}).

-record(datagram_writer_group_transport, {
    message_repeat_count,
    message_repeat_delay 
}).

-record(broker_writer_group_transport, {
    queue_name,
    resource_uri,
    authentication_profile_uri,
    requested_delivery_guarantee
}).

-record(uadp_writer_group_message, {
    group_version,
    data_set_ordering,
    network_message_content_mask = [publisher_id, group_header, writer_group_id, payload_header],
    sampling_offset,
    publishing_offset = []
}).

-record(json_writer_group_message, {
    network_message_content_mask
}).

-record(writer_group, {
    name = <<"demo writer group">>,
    enabled = true,
    security_mode = none,
    security_group_id = <<"demo security group">>,
    security_key_services = [],
    max_network_message_size = -1,
    group_properties = [],
    writer_group_id = 1,
    publishing_interval = 100,
    keep_alive_time,
    priority,
    locale_ids = [],
    header_layout_uri,
    transport_settings = #datagram_writer_group_transport{},
    message_settings = #uadp_writer_group_message{},
    data_set_writers = [#data_set_writer{}]
}).

-record(network_address_url, {
    url = <<"opc.udp://224.0.0.22:4444/">>     
}).

-record(pub_sub_connection, {
    name = <<"demo PubSub connection">>,
    enabled = true,
    publisher_id = 1,
    transport_profile_uri = <<"http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp">>,
    address = #network_address_url{},
    connection_properties = [],
    transport_settings,
    writer_groups = [#writer_group{}],
    reader_groups = []
}).
