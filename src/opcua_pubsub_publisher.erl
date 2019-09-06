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
    timer_refs = []
}).

-record(state, {
    writer_group = #writer_group{},
    data_sets = #{<<"demo published data set">> => #published_data_set{}},
    samples = #{}
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    State = #state{},
    PublishedDataSets = maps:values(State#state.data_sets),
    {ok, State#state{samples = init_samples(PublishedDataSets)}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({sample, PDSName, PVIdx}, State = #state{samples = Samples, data_sets = PDSs}) ->
    FieldSample = sample(maps:get(PDSName, PDSs), PVIdx),
    PDSSample = maps:get(PDSName, Samples),
    NewFieldSamples = maps:put(PVIdx, FieldSample, PDSSample#sample.field_samples),
    NewPDSSample = PDSSample#sample{field_samples = NewFieldSamples},
    {noreply, State#state{samples = maps:put(PDSName, NewPDSSample, Samples)}}.

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
                     timer_refs = TRefs},
    init_samples(PDSs, maps:put(PDSName, Sample, Samples)).

sample(_PV, _PVIdx) ->
    %% for now just return a random integer
    rand:uniform(100).
