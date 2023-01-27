-module(opcua_pubsub_dataset_writer).

-export([new/2]).
-export([write_dataset_message/1]).

-include("opcua.hrl").
-include("opcua_pubsub.hrl").
-include_lib("kernel/include/logger.hrl").

-record(state, {
    state = operational :: pubsub_state_machine(),
    name,
    dataset_writer_id,
    dataset_field_content_mask,
    keyframe_count,
    dataset_name,
    transport_settings,
    message_settings,
    connected_published_dataset
}).

new(PDS_ID, #dataset_writer_config{
            name = N,
            dataset_writer_id = DS_WID,
            dataset_field_content_mask = CM,
            keyframe_count = KF_C,
            dataset_name = DN,
            transport_settings = TS,
            message_settings = MS
        }) ->
    {ok, #state{
        state = operational,
        name = N,
        dataset_writer_id = DS_WID,
        dataset_field_content_mask = CM,
        keyframe_count = KF_C,
        dataset_name = DN,
        transport_settings = TS,
        message_settings = MS,
        connected_published_dataset = PDS_ID
    }}.

write_dataset_message(#state{dataset_writer_id = DSW_ID,
                             connected_published_dataset = PDS_id,
                             dataset_field_content_mask = ContentMask} = S) ->
    PDS = opcua_pubsub:get_published_dataset(PDS_id),
    % We are going to produce a keyframe, always.
    % We do not support delta-frames so we ignore keyframe_count
    #published_dataset{
        dataset_metadata = #dataset_metadata{fields = FieldsMetadata} = MD,
        dataset_source = DatasetSource
    } = PDS,
    Values = read_sources(DatasetSource, []),
    Fields = encode_data_set_fields(FieldsMetadata, Values),
    FieldCount = <<(length(Fields)):16/unsigned-little>>,
    Header = encode_message_header(variant, data_key_frame, ContentMask, MD),
    DataSetMessage = iolist_to_binary([Header, FieldCount, Fields]),
    {DataSetMessage, DSW_ID, S}.

read_sources([], Values) -> lists:reverse(Values);
read_sources([#published_variable{
            published_variable = NodeID,
            attribute_id = ?UA_ATTRIBUTEID_VALUE
        } | Rest], Values) ->
    [DataValue] = opcua_server_registry:perform(NodeID,[#opcua_read_command{attr = value}]),
    #opcua_data_value{
        value = Value,
        status = good
    } = DataValue,
    % io:format("Read value: ~p~n",[Value]),
    read_sources(Rest, [Value | Values]).

encode_message_header(FieldEncoding, MsgType, ContentMask, Metadata) ->

    opcua_pubsub_uadp:encode_dataset_message_header(FieldEncoding, MsgType,
                                                    ContentMask, Metadata).


encode_data_set_fields(FieldsMetadata, Values) ->
    encode_data_set_fields(FieldsMetadata, Values, []).

encode_data_set_fields([], [], Results) -> lists:reverse(Results);
encode_data_set_fields([ FieldMeta | FMD], [Value | Values], Results) ->
    Binary = opcua_pubsub_uadp:encode_dataset_message_field(FieldMeta, Value),
    encode_data_set_fields(FMD, Values, [Binary | Results]).

