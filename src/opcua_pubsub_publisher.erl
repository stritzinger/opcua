-module(opcua_pubsub_publisher).

-behaviour(gen_server).

-include("opcua_pubsub.hrl").
-include("opcua.hrl").
-include("opcua_internal.hrl").


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

-record(network_target, {
    sock,
    ip,
    port
}).

-record(state, {
    writer_group,
    data_sets,
    sequence_number_counter,
    samples = #{},
    publish_timer_ref,
    network_target
}).

start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

init([Url, WriterGroup, PublishedDataSets]) ->
    #{host := Host, port := Port} = uri_string:parse(Url),
    {ok, ParsedIP} = inet:parse_address(binary_to_list(Host)),
    {ok, Sock} = gen_udp:open(Port, [binary, {active, true},
                                     {add_membership, {ParsedIP, {0,0,0,0}}}]), %% for testing we receive our own messages
    InitializedSamples = init_samples(PublishedDataSets),
    IndexedPublishedDataSets = maps:from_list([{Name, PDS} || PDS = #published_data_set{name = Name} <- PublishedDataSets]),
    State = #state{data_sets = IndexedPublishedDataSets, writer_group = WriterGroup},
    PubTRef = init_publish_interval(State#state.writer_group#writer_group.publishing_interval),
    {ok, State#state{samples = InitializedSamples,
                     sequence_number_counter = counters:new(2, []),
                     publish_timer_ref = PubTRef,
                     network_target = #network_target{sock = Sock, ip = ParsedIP, port = Port}}}.

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
           network_target = NetworkTarget} = State,
    DataSetWriters = WriterGroup#writer_group.data_set_writers,
    DataSetMessages = build_data_set_messages(CRef, DataSetWriters, DataSets, Samples),
    NetworkMessage = build_network_message(CRef, WriterGroup, DataSetMessages),
    send_network_message(NetworkTarget, NetworkMessage), 
    {noreply, State};
handle_info({udp, _, _, _, NetworkMessage}, State) ->
    io:format("RECEIVED NETWORK MESSAGE: \t~s~n", [opcua_util:bin_to_hex(NetworkMessage)]),
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
    %% for now just return a random date
    opcua_util:date_time().

init_publish_interval(PublishInterval) ->
    timer:send_interval(PublishInterval, self(), publish).

send_network_message(#network_target{sock = Sock, ip = IP, port = Port}, NetworkMessage) ->
    gen_udp:send(Sock, IP, Port, NetworkMessage),
    io:format("SENT NETWORK MESSAGE: \t\t~s~n", [opcua_util:bin_to_hex(iolist_to_binary(NetworkMessage))]).

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
    N = encode(uint16, length(FieldsMetaData)),
    [Header, N, MessageData].

encode_data_set_fields(DataSetFieldContentMask, FieldsMetaData, FieldSamples) ->
    case lists:member(raw_data, DataSetFieldContentMask) of
        true ->
            [encode_data_set_field_raw_data(FMD, FS)
             || {FMD, FS} <- lists:zip(FieldsMetaData, FieldSamples)];
        _   ->
            [encode_data_set_field_variant(FMD, FS)
             || {FMD, FS} <- lists:zip(FieldsMetaData, FieldSamples)]
    end.

encode_data_set_field_raw_data(#field_meta_data{built_in_type = Type}, FieldSample) ->
    element(1, opcua_codec_binary:encode(Type, FieldSample)).

encode_data_set_field_variant(#field_meta_data{built_in_type = Type}, FS) ->
    Variant = opcua_codec:pack_variant(Type, FS),
    element(1, opcua_codec_binary:encode(variant, Variant)).

build_data_set_message_header(CRef, #data_set_writer{}) ->
    %% currently we dont process the
    %% DataSetFieldContentMask here,
    %% just set some static flags
    DataSetFlags1 = 2#11100001,
    DataSetFlags2 = 2#00010000,
    %SequenceNumber = get_data_set_message_sequence_number(CRef),
    Timestamp = opcua_util:date_time(),
    ConfigurationVersionMajorVersion = 0,
    ConfigurationVersionMinorVersion = 0,
    Spec = [byte, byte, date_time, uint32, uint32],
    Data = [DataSetFlags1, DataSetFlags2, Timestamp,
            ConfigurationVersionMajorVersion, ConfigurationVersionMinorVersion],
    element(1, opcua_codec_binary:encode(Spec, Data)).

get_data_set_message_sequence_number(CRef) ->
    get_sequence_number(CRef, 1).

%get_network_message_sequence_number(CRef) ->
%    get_sequence_number(CRef, 2).

get_sequence_number(CRef, Idx) ->
    counters:add(CRef, Idx, 1),
    SequenceNumber = counters:get(CRef, Idx),
    SequenceNumber rem 65535.

build_network_message(CRef, WriterGroup = #writer_group{}, DataSetMessages) ->
    Header = build_network_message_header(WriterGroup),
    GroupHeader = build_network_message_group_header(CRef, WriterGroup),
    PayloadHeader = build_network_message_payload_header(WriterGroup),
    %%ExtendedHeader = build_network_message_extended_header(WriterGroup),
    %%SecurityHeader = build_network_message_security_header(WriterGroup),
    [Header, GroupHeader, PayloadHeader, DataSetMessages].

build_network_message_header(#writer_group{}) ->
    UADPConfig = 2#11110001,
    ExtendedFlags1 = 2#00000001,
    PublisherId = 2234,
    Spec = [byte, byte, uint16],
    Data = [UADPConfig, ExtendedFlags1, PublisherId],
    encode(Spec, Data).

build_network_message_group_header(_CRef, #writer_group{writer_group_id = WriterGroupId}) ->
    GroupFlags = 2#00000001,
    %SequenceNumber = get_network_message_sequence_number(CRef),
    Spec = [byte, uint16],
    Data = [GroupFlags, WriterGroupId],
    encode(Spec, Data).

build_network_message_payload_header(#writer_group{}) ->
    MessagerCount = 1,
    DataSetWriterId = 62541,
    Spec = [byte, uint16],
    Data = [MessagerCount, DataSetWriterId],
    encode(Spec, Data).


%%build_network_message_extended_header(#writer_group{}) ->
%%    Timestamp = opcua_util:date_time(),
%%    PicoSeconds = 0,
%%    Spec = [date_time, uint16],
%%    Data = [Timestamp, PicoSeconds],
%%    encode(Spec, Data).

build_network_message_security_header(#writer_group{}) ->
    SecurityFlags = 2#00000000,
    SecurityTokenId = 0,
    NonceLength = 1,
    MessageNonce = 0,
    SecurityFooterSize = 0,
    Spec = [byte, uint32, byte, byte, uint16],
    Data = [SecurityFlags, SecurityTokenId, NonceLength, MessageNonce, SecurityFooterSize],
    encode(Spec, Data).
