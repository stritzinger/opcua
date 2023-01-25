-module(opcua_pubsub_reader_group).

-export([new/1]).
-export([add_dataset_reader/2]).
-export([filter_readers/2]).
-export([dispatch_messages/3]).
-export([create_target_variables/3]).

-record(state, {
    name,
    dataset_readers = #{}
}).

new(#{name := RG_name}) ->
    {ok, #state{name = RG_name}}.

add_dataset_reader(DSR_cfg, #state{dataset_readers = DSRs} = S) ->
    DSR_id = uuid:get_v4(),
    {ok, DSR} = opcua_pubsub_dataset_reader:new(DSR_cfg),
    NewDSRs = maps:put(DSR_id, DSR, DSRs),
    {ok, DSR_id, S#state{dataset_readers = NewDSRs}}.

create_target_variables(DSR_id, Config,#state{dataset_readers = DSRs} = S) ->
    DSR = maps:get(DSR_id, DSRs),
    {ok, NewDSR} = opcua_pubsub_dataset_reader:create_target_variables(Config, DSR),
    NewDSRs = maps:put(DSR_id, NewDSR, DSRs),
    {ok, S#state{dataset_readers = NewDSRs}}.

filter_readers(Headers, #state{dataset_readers = DSRs}) ->
    [DSR_id || {DSR_id, DSR} <- maps:to_list(DSRs),
        opcua_pubsub_dataset_reader:is_interested(Headers, DSR)].

dispatch_messages(BundledMessages, DSR_ids, #state{dataset_readers = DSRs} = S) ->
    Updated = [
        begin
            DSR = maps:get(ID, DSRs),
            NewDSR = opcua_pubsub_dataset_reader:process_messages(BundledMessages, DSR),
            {ID, NewDSR}
        end || ID <- DSR_ids],
   NewDSRs = lists:foldl(fun
        ({ID,Value}, Map) ->
            maps:put(ID, Value, Map)
        end, DSRs, Updated),
   S#state{dataset_readers = NewDSRs}.


