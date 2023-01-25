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


write_dataset_message(#state{connected_published_dataset = PDS_id} = S) ->
    PDS = opcua_pubsub:get_published_dataset(PDS_id),
    {<<>>, S}.

