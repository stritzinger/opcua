-module(opcua_pubsub_writer_group).

-export([new/1]).
-export([add_dataset_writer/3]).
-export([init/2]).
-export([write_network_message/1]).

-include("opcua_pubsub.hrl").

-record(state, {
    state = operational :: pubsub_state_machine(),
    name,
    writer_group_id,
    publishing_interval,
    keep_alive_time,
    priority,
    locale_ids,
    transport_settings,
    message_settings,
    dataset_writers = #{},
    timer
}).

new(#writer_group_config{
            enabled = E,
            name = N,
            writer_group_id = WG_ID,
            publishing_interval = P_INTERVAL,
            keep_alive_time = KA_TIME,
            priority = P,
            locale_ids = Locales,
            transport_settings = TS,
            message_settings = MS}) ->
    {ok, #state{
        name = N,
        writer_group_id = WG_ID,
        publishing_interval = P_INTERVAL,
        keep_alive_time = KA_TIME,
        priority = P,
        locale_ids = Locales,
        transport_settings = TS,
        message_settings = MS,
        dataset_writers = #{}
    }}.

add_dataset_writer(PDS_id, DSW_cfg, #state{dataset_writers = DSWs} = S) ->
    DSW_id = uuid:get_v4(),
    {ok, DSW} = opcua_pubsub_dataset_writer:new(PDS_id, DSW_cfg),
    NewDSWs = maps:put(DSW_id, DSW, DSWs),
    {ok, DSW_id, S#state{dataset_writers = NewDSWs}}.

init(ID, #state{publishing_interval = PublishingInterval} = S) ->
    {ok, Tref} = timer:send_interval(PublishingInterval, {publish, ID}),
    S#state{timer = Tref}.


write_network_message(#state{dataset_writers = DatasetWriters} = S) ->
    Results = [
        begin
            {DSM, NewState} = opcua_pubsub_dataset_writer:write_dataset_message(DSW),
            {DSM, {ID, NewState}}
        end || {ID, DSW} <- maps:to_list(DatasetWriters)],
    {DataSetMessages, KV_pairs_DSWs} = lists:unzip(Results),
    io:format("DSMs: ~p~n", [DataSetMessages]),
    NetMsg = <<>>,
    {NetMsg, S#state{dataset_writers = maps:from_list(KV_pairs_DSWs)}}.