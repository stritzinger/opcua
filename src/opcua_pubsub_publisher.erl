-module(opcua_pubsub_publisher).

-behaviour(gen_server).

-include("opcua_pubsub.hrl").


-export([start_link/1]).

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
    writer_group,
    data_sets,
    sequence_number_counter,
    samples = #{},
    publish_timer_ref,
    transport_context = #{}
}).

start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

init([_Url, WriterGroup, PublishedDataSets]) ->
    InitializedSamples = init_samples(PublishedDataSets),
    IndexedPublishedDataSets = maps:from_list([{Name, PDS} || PDS = #published_data_set{name = Name} <- PublishedDataSets]),
    State = #state{data_sets = IndexedPublishedDataSets, writer_group = WriterGroup},
    PubTRef = init_publish_interval(State#state.writer_group#writer_group.publishing_interval),
    {ok, State#state{samples = InitializedSamples,
                     sequence_number_counter = counters:new(2, []),
                     publish_timer_ref = PubTRef}}.

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
           sequence_number_counter = CRef,
           data_sets = DataSets,
           samples = Samples,
           transport_context = Transport} = State,
    DataSetWriters = WriterGroup#writer_group.data_set_writers,
    DataSetMessages = build_data_set_messages(CRef, DataSetWriters, DataSets, Samples),
    NetworkMessage = build_network_message(CRef, WriterGroup, DataSetMessages),
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
    rand:uniform(16#FFFFFFFF).

init_publish_interval(PublishInterval) ->
    timer:send_interval(PublishInterval, self(), publish).

send_network_message(_Transport, NetworkMessage) ->
    io:format("SEND NETWORK MESSAGE: ~w~n", [iolist_to_binary(NetworkMessage)]).

encode(Spec, Data) ->
    element(1, opcua_codec_binary:encode(Spec, Data)).

build_data_set_messages(CRef, DataSetWriters, DataSets, Samples) ->
    [build_data_set_message(CRef, DSW, maps:get(PDSName, DataSets), maps:get(PDSName, Samples))
     || DSW = #data_set_writer{data_set_name = PDSName} <- DataSetWriters].

build_data_set_message(CRef, DSW, PDS, Sample) ->
    {_, FieldSamples} = lists:unzip(lists:keysort(1, maps:to_list(Sample#sample.field_samples))),
    DataSetFieldContentMask = DSW#data_set_writer.data_set_field_content_mask,
    FieldsMetaData = PDS#published_data_set.data_set_meta_data
                        #data_set_meta_data.fields,
    MessageData = encode_data_set_fields(DataSetFieldContentMask, FieldsMetaData, FieldSamples),
    Header = build_data_set_message_header(CRef, DSW),
    [Header, MessageData].

encode_data_set_fields(DataSetFieldContentMask, FieldsMetaData, FieldSamples) ->
    case lists:member(raw_data, DataSetFieldContentMask) of
        true ->
            [encode_data_set_field_raw_data(FMD, FS)
             || {FMD, FS} <- lists:zip(FieldsMetaData, FieldSamples)];
        _   ->
            FieldSamples
    end.

encode_data_set_field_raw_data(#field_meta_data{built_in_type = Type}, FieldSample) ->
    element(1, opcua_codec_binary:encode(Type, FieldSample)).

build_data_set_message_header(CRef, #data_set_writer{}) ->
    %% currently we dont process the
    %% DataSetFieldContentMask here,
    %% just set some static flags
    DataSetFlags1 = 2#00111111,
    DataSetFlags2 = 2#00001100,
    SequenceNumber = get_data_set_message_sequence_number(CRef),
    Timestamp = opcua_util:date_time(),
    PicoSeconds = 0,
    Status = 0,
    Spec = [byte, byte, uint16, date_time, uint16, uint16],
    Data = [DataSetFlags1, DataSetFlags2, SequenceNumber, Timestamp, PicoSeconds, Status],
    element(1, opcua_codec_binary:encode(Spec, Data)).

get_data_set_message_sequence_number(CRef) ->
    get_sequence_number(CRef, 1).

get_network_message_sequence_number(CRef) ->
    get_sequence_number(CRef, 2).

get_sequence_number(CRef, Idx) ->
    counters:add(CRef, Idx, 1),
    SequenceNumber = counters:get(CRef, Idx),
    SequenceNumber rem 65535.

build_network_message(CRef, WriterGroup = #writer_group{}, DataSetMessages) ->
    Header = build_network_message_header(WriterGroup),
    GroupHeader = build_network_message_group_header(CRef, WriterGroup),
    ExtendedHeader = build_network_message_extended_header(WriterGroup),
    SecurityHeader = build_network_message_security_header(WriterGroup),
    [Header, GroupHeader, ExtendedHeader, SecurityHeader, DataSetMessages].

build_network_message_header(#writer_group{}) ->
    UADPVersion = 2#0001,
    UADPFlags = 2#00001001,
    ExtendedFlags1 = 2#00000111,
    ExtendedFlags2 = 2#00000000,
    PublisherId = 1,
    Spec = [byte, byte, byte, byte],
    Data = [UADPVersion + UADPFlags, ExtendedFlags1, ExtendedFlags2, PublisherId],
    encode(Spec, Data).

build_network_message_group_header(CRef, #writer_group{writer_group_id = WriterGroupId}) ->
    GroupFlags = 2#10010000,
    SequenceNumber = get_network_message_sequence_number(CRef),
    Spec = [byte, uint16, uint16],
    Data = [GroupFlags, WriterGroupId, SequenceNumber],
    encode(Spec, Data).

build_network_message_extended_header(#writer_group{}) ->
    Timestamp = opcua_util:date_time(),
    PicoSeconds = 0,
    Spec = [date_time, uint16],
    Data = [Timestamp, PicoSeconds],
    encode(Spec, Data).

build_network_message_security_header(#writer_group{}) ->
    SecurityFlags = 2#00000000,
    SecurityTokenId = 0,
    NonceLength = 1,
    MessageNonce = 0,
    SecurityFooterSize = 0,
    Spec = [byte, uint32, byte, byte, uint16],
    Data = [SecurityFlags, SecurityTokenId, NonceLength, MessageNonce, SecurityFooterSize],
    encode(Spec, Data).
