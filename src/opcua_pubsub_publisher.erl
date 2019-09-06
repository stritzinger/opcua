-module(opcua_pubsub_publisher).

-behaviour(gen_server).

-include("opcua_pubsub.hrl").


-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(sample, {
    data_set_name = <<"demo published data set">>,
    field_samples = #{},
    sample_timer_refs = []
}).

-record(state, {
    writer_group = #writer_group{},
    data_sets = #{<<"demo published data set">> => #published_data_set{}},
    samples = #{},
    publish_timer_ref,
    transport_context = #{}
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    State = #state{},
    PublishedDataSets = maps:values(State#state.data_sets),
    InitializedSamples = init_samples(PublishedDataSets),
    PubTRef = init_publish_interval(State#state.writer_group#writer_group.publishing_interval),
    {ok, State#state{samples = InitializedSamples, publish_timer_ref = PubTRef}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({sample, PDSName, PVIdx}, State = #state{samples = Samples, data_sets = PDSs}) ->
    FieldSample = sample(maps:get(PDSName, PDSs), PVIdx),
    PDSSample = maps:get(PDSName, Samples),
    NewFieldSamples = maps:put(PVIdx, FieldSample, PDSSample#sample.field_samples),
    NewPDSSample = PDSSample#sample{field_samples = NewFieldSamples},
    {noreply, State#state{samples = maps:put(PDSName, NewPDSSample, Samples)}};
handle_info(publish, State) ->
    #state{writer_group = WriterGroup,
           data_sets = DataSets,
           samples = Samples,
           transport_context = Transport} = State,
    DataSetWriters = WriterGroup#writer_group.data_set_writers,
    DataSetMessages = build_data_set_messages(DataSetWriters, DataSets, Samples),
    NetworkMessage = build_network_message(WriterGroup, DataSetMessages),
    send_network_message(Transport, NetworkMessage), 
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

init_samples(PDSs) ->
    init_samples(PDSs, #{}).

init_samples([], Samples) ->
    Samples;
init_samples([PDS | PDSs], Samples) ->
    PublishedData = PDS#published_data_set.data_set_source
                       #published_data_items.published_data,
    PDSName = PDS#published_data_set.name,
    IndexedVariables = lists:zip(lists:seq(1, length(PublishedData)), PublishedData),
    TRefs = lists:map(fun({PVIdx, #published_variable{sampling_interval_hint = SIH}}) ->
                        timer:send_interval(SIH, self(), {sample, PDSName, PVIdx})
                      end, IndexedVariables),
    Sample = #sample{data_set_name = PDSName,
                     field_samples = #{},
                     sample_timer_refs = TRefs},
    init_samples(PDSs, maps:put(PDSName, Sample, Samples)).

sample(_PV, _PVIdx) ->
    %% for now just return a random integer
    rand:uniform(100).

init_publish_interval(PublishInterval) ->
    timer:send_interval(PublishInterval, self(), publish).

send_network_message(_Transport, NetworkMessage) ->
    io:format("SEND NETWORK MESSAGE: ~p~n", [NetworkMessage]).

build_data_set_messages(_DataSetWriters, _DataSets, _Samples) ->
    [].

build_network_message(_WriterGroup, _DataSetMessages) ->
    [].
