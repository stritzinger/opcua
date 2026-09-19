-module(opcua_pubsub_writer_group).

-export([new/3]).
-export([add_dataset_writer/3]).
-export([init/2]).
-export([write_network_message/1]).

-include("opcua_pubsub.hrl").

-record(state, {
    state = operational :: pubsub_state_machine(),
    publisher_id,
    publisher_id_type,
    config              :: #writer_group_config{},
    dataset_writers = #{},
    timer
}).

new(PublisherId, PublisherIdType, #writer_group_config{} = Config) ->
    {ok, #state{
        publisher_id = PublisherId,
        publisher_id_type = PublisherIdType,
        config = Config,
        dataset_writers = #{}
    }}.

add_dataset_writer(PDS_id, DSW_cfg, #state{dataset_writers = DSWs} = S) ->
    DSW_id = uuid:get_v4(),
    {ok, DSW} = opcua_pubsub_dataset_writer:new(PDS_id, DSW_cfg),
    NewDSWs = maps:put(DSW_id, DSW, DSWs),
    {ok, DSW_id, S#state{dataset_writers = NewDSWs}}.

init(ID, #state{config = #writer_group_config{
                publishing_interval = PublishingInterval}} = S) ->
    {ok, Tref} = timer:send_interval(PublishingInterval, {publish, ID}),
    S#state{state = operational, timer = Tref}.


write_network_message(#state{publisher_id = PublisherId,
                             publisher_id_type = PublisherIdType,
                             config = Config,
                             dataset_writers = DatasetWriters} = S) ->
    Results = [
        begin
            {DSM, DSW_ID, NewState} = opcua_pubsub_dataset_writer:write_dataset_message(DSW),
            {DSM, DSW_ID, {ID, NewState}}
        end || {ID, DSW} <- maps:to_list(DatasetWriters)],
    {DataSetMessages, DSW_IDS, KV_pairs_DSWs} = lists:unzip3(Results),
    NewState = S#state{dataset_writers = maps:from_list(KV_pairs_DSWs)},
    % io:format("DSMs: ~p~n", [DataSetMessages]),
    Payload = opcua_pubsub_uadp:encode_payload(DataSetMessages),
    % Headers presence in the Network message should be regulated by the content mask
    Headers = opcua_pubsub_uadp:encode_network_message_headers(PublisherId,
                                                              PublisherIdType,
                                                              DSW_IDS,
                                                              Config),
    NetworkMessage = iolist_to_binary([Headers, Payload]),
    {NetworkMessage, NewState}.
