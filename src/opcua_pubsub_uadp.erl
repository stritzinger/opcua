-module(opcua_pubsub_uadp).


-export([decode_network_message_headers/1]).
-export([decode_payload/2]).
-export([decode_dataset_message_field/3]).

-export([encode_dataset_message_field/2]).
-export([encode_dataset_message_header/4]).
-export([encode_payload/1]).
-export([encode_network_message_headers/4]).

-include("opcua.hrl").
-include("opcua_pubsub.hrl").

%%%%%% Encoding binary flags %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Used In UADPFlags
-define(UADP_VERSION, 1).
-define(PUBLISHER_ID_ENABLED, (1 bsl 4)).
-define(GROUP_HEADER_ENABLED, (1 bsl 5)).
-define(PAYLOAD_HEADER_ENABLED, (1 bsl 6)).
-define(EXT_FLAGS_1_ENABLED, (1 bsl 7)).
% Used In ExtendedFlags2
-define(EXT_FLAGS2_CHUNK_MESSAGE, 1).
-define(EXT_FLAGS2_PROMOTED_FIELD_ENABLED, (1 bsl 1)).
-define(EXT_FLAGS2_DATASET_MSG_TYPE, 0).
-define(EXT_FLAGS2_DISCOVERY_REQUEST_MSG_TYPE, (1 bsl 2)).
-define(EXT_FLAGS2_DISCOVERY_RESPONSE_MSG_TYPE, (1 bsl 3)).
% Used In ExtendedFlags1
-define(EXT_FLAGS1_DATA_SET_CLASS_ID_ENABLED, (1 bsl 3)).
% -define(SECURITY_ENABLED, (1 bsl 4)). % not implemented
-define(EXT_FLAGS1_TIMESTAMP_ENABLED, (1 bsl 6)).
-define(EXT_FLAGS1_PICOSECONDS_ENABLED, (1 bsl 6)).
-define(EXT_FLAGS1_EXT_FLAGS_2_ENABLED, (1 bsl 7)).
% Used In GroupFlags
-define(WRITER_GROUP_ENABLED, 1).
-define(GROUP_VERSION_ENABLED, (1 bsl 1)).
-define(NETWORK_MESSAGE_ENABLED, (1 bsl 2)).
-define(SEQUENCE_NUMBER_ENABLED, (1 bsl 3)).
% Used in DataSetMessageHeader DataSetFlags1
-define(DATASET_FLAGS1_VALID, 1).
-define(DATASET_FLAGS1_VARIANT, 0).
-define(DATASET_FLAGS1_RAWDATA, (1 bsl 1)).
-define(DATASET_FLAGS1_DATAVALUE, (1 bsl 2)).
-define(DATASET_FLAGS1_SEQ_NUM_ENABLED, (1 bsl 3)).
-define(DATASET_FLAGS1_STATUS_ENABLED, (1 bsl 4)).
-define(DATASET_FLAGS1_MAJOR_V_ENABLED, (1 bsl 5)).
-define(DATASET_FLAGS1_MINOR_V_ENABLED, (1 bsl 6)).
-define(DATASET_FLAGS1_FLAGS2_ENABLED, (1 bsl 7)).
% Used in DataSetMessageHeader DataSetFlags2
-define(DATASET_FLAGS2_KEY_FRAME, 0).
-define(DATASET_FLAGS2_DELTA_FRAME, 1).
-define(DATASET_FLAGS2_EVENT, 2).
-define(DATASET_FLAGS2_KEEP_ALIVE, 3).
-define(DATASET_FLAGS2_TIMESTAMP_ENABLED, (1 bsl 4)).
-define(DATASET_FLAGS2_PICOSECONDS_ENABLED, (1 bsl 5)).

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Extracts the clear data from the message Headers and the payload binary
decode_network_message_headers(<<VersionFlags:8/bitstring, Rest/binary>>) ->
    <<ExtendedFlags1:1,
      PayloadHeader:1,
      GroupHeader:1,
      PublisherId:1,
      1:4/unsigned-little>> = VersionFlags,
    {ExtendedFlags1Map, Rest2} = decode_extended_flags1(ExtendedFlags1, Rest),

    % Skipping many optional fields, enforcing a minimal setup for testing
    % TODO: add them once needed and remove this hard match
    #{dataset_class_id := 0, extended_flags2 := 0,
        picoseconds := 0, decode_publisher_id_type := uint16,
        security := 0, timestamp := 0} = ExtendedFlags1Map,
    ExtendedFlags2 = maps:get(extended_flags2, ExtendedFlags1Map, 0),
    {ExtendedFlags2Map, Rest3} = decode_extended_flags2(ExtendedFlags2, Rest2),
    {PublisherIDValue, Rest4} = decode_publisherID(PublisherId, ExtendedFlags1Map, Rest3),
    % {DataSetClassId, Rest5} = decode_dataset_class_id(PublisherID, ExtendedFlags1Record, Rest4),
    {GroupHeaderMap, Rest5} = decode_group_header(GroupHeader, Rest4),
    {PayloadHeaderMap, Rest6} = decode_payload_header(PayloadHeader, ExtendedFlags2Map, Rest5),
    % Network Message Extended Header
    % {TimeStamp, Rest7} = ,
    % {Picoseconds, Rest8} = ,
    % {PromotedFields, Rest7} = decode_promoted_fields(ExtendedFlags2Record, Rest6),
    % Security
    % {SecurityHeader, Rest8} = decode_security_header(ExtendedFlags1Record, Rest7),
    Headers = #{
        publisher_id => PublisherIDValue,
        extended_flags1 => ExtendedFlags1Map,
        extended_flags2 => ExtendedFlags2Map,
        group_header => GroupHeaderMap,
        payload_header => PayloadHeaderMap
    },
    {Headers, Rest6};
decode_network_message_headers(_) ->
    {error, unknown_message}.

%extracts Dataset Messages from the payload blob decoding the headers
decode_payload(#{payload_header := undefined}, Payload) ->
    {DSM_header, Binary} = decode_dataset_message_header(Payload),
    [{DSM_header, Binary}];
decode_payload(#{payload_header := #{count := 1}}, Payload) ->
    {DSM_header, Binary} = decode_dataset_message_header(Payload),
    [{DSM_header, Binary}];
decode_payload(#{payload_header := #{count := Count}}, Payload) ->
    <<SizesBinary:(Count*2)/binary, Rest/binary>> = Payload,
    Sizes = [Size || <<Size:16/unsigned-little>> <= SizesBinary],
    decode_multi_dataset_message(Rest, Sizes).

decode_dataset_message_field(variant, FieldMetadata, Binary) ->
    #dataset_field_metadata{
        builtin_type = BuiltinType,
        data_type = _NodeId,
        valueRank = _
    } = FieldMetadata,
    {Result, Rest} = opcua_codec_binary:decode(variant, Binary),
    case Result of
        #opcua_variant{type = BuiltinType} -> {Result, Rest};
        _ -> {error, unmatched_metadata}
    end;
decode_dataset_message_field(_, _, _) ->
    error(bad_encoding_not_implemented).

encode_dataset_message_field(#dataset_field_metadata{
                data_type = DataType}, #opcua_variant{} = V) ->
    %io:format("Original Variant: ~p~n",[V]),
    {_TypeID, Val} = opcua_codec:unpack_variant(V),
    V2 = opcua_codec:pack_variant(opcua_server_space, DataType, Val),
    %io:format("Variant to publish: ~p~n",[V2]),
    {Binary, _} = opcua_codec_binary:encode(variant, V2),
    %io:format("Encoded ~p~n", [Binary]),
    {Result, _Rest} = opcua_codec_binary:decode(variant, Binary),
    %io:format("Decoded: ~p~n",[Result]),
    Binary.

encode_dataset_message_header(FieldEncoding, MsgType, ContentMask,
                    #dataset_metadata{configuration_version = _MajorMinor}) ->
    Flags1 = ?DATASET_FLAGS1_VALID
                bor encode_field_encoding(FieldEncoding)
                bor ?DATASET_FLAGS1_FLAGS2_ENABLED,
    io:format("Flags1 ~p~n",[Flags1]),
    Flags2 = 0,
    % DataSetFlags1
    % F1 = #{
    %     dataset_msg_valid => 1,
    %     field_encoding => FieldEncoding,
    %     dataset_flags2 => 1},
    % {Status, F1_1} = case ContentMask band ?UADP_DATA_SET_FIELD_MASK_STATUS of
    %     1 -> error(bad_not_implemented);
    %     0 -> {<<>>, maps:put(status, 0, F1)}
    % end,
    % {MajorVersion, F1_2} = case ContentMask band ?UADP_DATA_SET_FIELD_MASK_MAJORVERSION of
    %     1 -> error(bad_not_implemented);
    %     0 -> {<<>>, maps:put(config_ver_major_ver, 0, F1_1)}
    % end,
    % {MinorVersion, F1_3} = case ContentMask band ?UADP_DATA_SET_FIELD_MASK_MINORVERSION of
    %     1 -> error(bad_not_implemented);
    %     0 -> {<<>>, maps:put(config_ver_minor_ver, 0, F1_2)}
    % end,
    % {SeqNumber, F1_4} = case ContentMask band ?UADP_DATA_SET_FIELD_MASK_SEQUENCENUMBER of
    %     1 -> error(bad_not_implemented);
    %     0 -> {<<>>, maps:put(dataset_msg_seq_num, 0, F1_3)}
    % end,
    {Flags2_1, Timestamp} = case ContentMask band ?UADP_DATA_SET_FIELD_MASK_TIMESTAMP of
        0 -> {Flags2, <<>>};
        _ -> T = opcua_codec_binary_builtin:encode(date_time, opcua_util:date_time()),
            {Flags2 bor ?DATASET_FLAGS2_TIMESTAMP_ENABLED, T}
    end,
    % {PicoSeconds, F2_2} = case ContentMask band ?UADP_DATA_SET_FIELD_MASK_PICOSECONDS of
    %     1 -> error(bad_not_implemented);
    %     0 -> {<<>>, maps:put(picoseconds, 0, F2_1)}
    iolist_to_binary([Flags1, Flags2_1, Timestamp]).

encode_payload([DataSetMessage]) -> DataSetMessage;
encode_payload(DataSetMessages) ->
    Sizes = [ <<(byte_size(DSM)):16/unsigned-little>> || DSM <- DataSetMessages],
    iolist_to_binary([Sizes, DataSetMessages]).



encode_network_message_headers(PublisherID, PublisherIdType, DSW_IDS,
        #writer_group_config{
            message_settings = #uadp_writer_group_message_data{
                networkMessageContentMask = Mask}
            } = WriterGroupCfg) ->
    UADPFlags = ?UADP_VERSION, % from the specification
    ExtFlags1 = 0,
    % ExtFlags2 = 0, % unused
    % UADP main flags
    {UADPFlags1, PubID} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_PUBLISHER_ID of
        0 -> {UADPFlags, <<>>};
        _ -> ID = opcua_codec_binary_builtin:encode(PublisherIdType, PublisherID),
            {UADPFlags bor ?PUBLISHER_ID_ENABLED, ID}
    end,
    {UADPFlags2, GH} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_GROUP_HEADER of
        0 -> {UADPFlags1, <<>>};
        _ -> H = encode_group_header(WriterGroupCfg, Mask),
            {UADPFlags1 bor ?GROUP_HEADER_ENABLED, H}
    end,
    {UADPFlags3, PH} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_GROUP_HEADER of
        0 -> {UADPFlags2, <<>>};
        _ -> H_ = encode_payload_header(WriterGroupCfg, DSW_IDS),
            {UADPFlags2 bor ?PAYLOAD_HEADER_ENABLED, H_}
    end,
    % ExtendedFlags1 always enabled
    UADPFlags4 = UADPFlags3 bor ?EXT_FLAGS_1_ENABLED,
    ExtFlags1_1 = case UADPFlags4 band ?PUBLISHER_ID_ENABLED of
        0 -> ExtFlags1;
        _ -> ExtFlags1 bor encode_publisher_id_type(PublisherIdType)
    end,
    {ExtFlags1_2, ClassID} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_DATASET_CLASSID of
        0 -> {ExtFlags1_1, <<>>};
        _ -> {ExtFlags1_1 bor ?EXT_FLAGS1_DATA_SET_CLASS_ID_ENABLED, error(not_implemented)}
    end,
    % Security disabled,
    % TODO: add check here when is implemented
    {ExtFlags1_3, SH} = {ExtFlags1_2, <<>>},
    % ...
    {ExtFlags1_4, Timestamp} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_TIMESTAMP of
        0 -> {ExtFlags1_3, <<>>};
        _ -> T = opcua_codec_binary_builtin:encode(date_time, opcua_util:date_time()),
            {ExtFlags1_3 bor ?EXT_FLAGS1_TIMESTAMP_ENABLED, T}
    end,
    {ExtFlags1_5, PicoSeconds}  = case Mask band ?UADP_NET_MSG_CONTENT_MASK_PICOSECONDS of
        0 ->  {ExtFlags1_4, <<>>};
        _ -> {ExtFlags1_4 bor ?EXT_FLAGS1_PICOSECONDS_ENABLED, error(not_implemented)}
    end,

    % ExtendedFlags2 disabled for simplicity
    % this disables promoted_fields, picosecods timestamp
    %  and defaults to dataset_message
    ExtFlags1_6 = ExtFlags1_5, % bor ?EXT_FLAGS_2_ENABLED,

    %ExtFlags2_1 = ExtFlags2 bor 0, % TODO: add check and support for ?CHUNK_MESSAGE
    % {ExtFlags2_2, PromotedFields} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_PROMOTED_FIELDS of
    %     0 -> {ExtFlags2_1, <<>>};
    %     _ -> {ExtFlags2_1 bor ?PROMOTED_FIELD_ENABLED, error(not_implemented)}
    % end,
    % TODO: support more message types
    % HardcodedMsgType = dataset_message,
    % ExtFlags2_3 = ExtFlags2_2 bor encode_network_msg_type(HardcodedMsgType),

    iolist_to_binary([
        UADPFlags4, ExtFlags1_6, %% Flags ExtFlags2 is unused for now
        PubID, ClassID, GH, PH, % Main elements
        % extended header elements (unused)
        %Timestamp, PicoSeconds, PromotedFields,
        SH % optional security info (unused)
    ]).


%%% INTERNALS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_extended_flags1(0, Bin) ->
    {#{
        extended_flags2 => 0,
        picoseconds => 0,
        timestamp => 0,
        security => 0,
        dataset_class_id => 0,
        decode_publisher_id_type => decode_publisher_id_type(0)
    }, Bin};
decode_extended_flags1(1, <<
        ExtendedFlags2:1,
        PicoSeconds:1,
        Timestamp:1,
        Security:1,
        DataSetClassId:1,
        PublisherIdType:3/little-unsigned, Rest/binary>>) ->
    {#{
        extended_flags2 => ExtendedFlags2,
        picoseconds => PicoSeconds,
        timestamp => Timestamp,
        security => Security,
        dataset_class_id => DataSetClassId,
        decode_publisher_id_type => decode_publisher_id_type(PublisherIdType)
    }, Rest}.

decode_publisher_id_type(?UA_PUBLISHERIDTYPE_BYTE) -> byte;
decode_publisher_id_type(?UA_PUBLISHERIDTYPE_UINT16) -> uint16;
decode_publisher_id_type(?UA_PUBLISHERIDTYPE_UINT32) -> uint32;
decode_publisher_id_type(?UA_PUBLISHERIDTYPE_UINT64) -> uint64;
decode_publisher_id_type(?UA_PUBLISHERIDTYPE_STRING) -> string;
decode_publisher_id_type(_) -> reserved.

decode_extended_flags2(0, Bin) ->
    {#{
        chunk => 0,
        promoted_fields => 0,
        network_message_type => decode_network_msg_type(<<0:1,0:1,0:1>>)
    }, Bin};
decode_extended_flags2(1, <<
            _Reserved:3,
            NetworkMsgType:3/bitstring,
            PromotedFields:1,
            Chunk:1,
            Bin/binary>>) ->
    {#{
        chunk => Chunk,
        promoted_fields => PromotedFields,
        network_message_type => decode_network_msg_type(NetworkMsgType)
    }, Bin}.


decode_network_msg_type(<< 0:1, 0:1, 0:1>>) -> dataset_message;
decode_network_msg_type(<< 0:1, 0:1, 1:1>>) -> discovery_request;
decode_network_msg_type(<< 0:1, 1:1, 0:1>>) -> discovery_responce;
decode_network_msg_type(<< _:1, _:1, _:1>>) -> reserved.

decode_publisherID(0, _, Binary) -> {undefined, Binary};
decode_publisherID(1, #{decode_publisher_id_type := uint16}, Binary) ->
    <<PublisherID:16/unsigned-little, Rest/binary>> = Binary,
    {PublisherID, Rest}.
% TODO: handle other PublisherID types

decode_group_header(0, Bin) -> { undefined, Bin};
decode_group_header(1, <<GroupFlags:8/bitstring, Bin/binary>>) ->
    <<_ReservedBits:4,
      SeqNum_flag:1,
      NetworkMessageNumber_flag:1,
      GroupVersion_flag:1,
      WrtiterGroupId_flag:1>> = GroupFlags,
    {WriterGroupId, Rest} = decode_writer_group_id(WrtiterGroupId_flag, Bin),
    {GroupVersion, Rest2} = decode_group_version_id(GroupVersion_flag, Rest),
    {NetworkMessageNumber, Rest3} = decode_network_message_number(NetworkMessageNumber_flag, Rest2),
    {SeqNum, Rest4} = decode_network_sequence_number(SeqNum_flag, Rest3),
    {#{
        writer_group_id => WriterGroupId,
        group_version => GroupVersion,
        network_message_number => NetworkMessageNumber,
        sequence_number => SeqNum
    }, Rest4}.

decode_writer_group_id(0, Bin) -> {undefined, Bin};
decode_writer_group_id(1, Bin) -> opcua_codec_binary_builtin:decode(uint16, Bin).

decode_group_version_id(0, Bin) -> {undefined, Bin};
decode_group_version_id(1, Bin) ->
    %UInt32 that represents the time in seconds since the year 2000
    opcua_codec_binary_builtin:decode(uint32, Bin).

decode_network_message_number(0, Bin) -> {undefined, Bin};
decode_network_message_number(1, Bin) -> opcua_codec_binary_builtin:decode(uint16, Bin).

decode_network_sequence_number(0, Bin) -> {undefined, Bin};
decode_network_sequence_number(1, Bin) -> opcua_codec_binary_builtin:decode(uint16, Bin).

decode_payload_header(0, _, Bin) -> {undefined, Bin};
decode_payload_header(1, #{chunk := 1}, Bin) ->
    {DataSetWriterID, Rest} = opcua_codec_binary_builtin:decode(uint16, Bin),
    throw({not_implemented, chunked_network_message});
decode_payload_header(1, #{network_message_type := dataset_message}, Bin) ->
    <<MsgCount:8/unsigned-little, Rest/binary>> = Bin,
    <<DataWriterIDs:(MsgCount*2)/binary, Rest2/binary>> = Rest,
    {#{
        count => MsgCount,
        dataset_writer_ids =>
            [ DataWriterID || <<DataWriterID:16/unsigned-little>> <= DataWriterIDs]
    }, Rest2};
decode_payload_header(1, #{network_message_type := discovery_request}, Bin) ->
    throw({not_implemented, discovery_request});
decode_payload_header(1, #{network_message_type := discovery_responce}, Bin) ->
    throw({not_implemented, discovery_responce}).

decode_multi_dataset_message(Bin, Sizes) ->
    decode_multi_dataset_message(Bin, Sizes, []).

decode_multi_dataset_message(<<>>, [], Result) -> lists:reverse(Result);
decode_multi_dataset_message(Bin, [S|TL], Result) ->
    <<DSM:S/binary, Rest/binary>> = Bin,
    {DSM_header, Binary1} = decode_dataset_message_header(DSM),
    decode_multi_dataset_message(Rest, [ {DSM_header, Binary1} | Result], TL).



decode_dataset_message_header(DataSetMessageBinary) ->
    {DataSetFlags1, Rest} = decode_dataset_flags1(DataSetMessageBinary),
    {DataSetFlags2, Rest1} = decode_dataset_flags2(DataSetFlags1, Rest),
    {DataSetSeqNum, Rest2} = decode_dataset_seq_num(DataSetFlags1, Rest1),
    {Timestamp, Rest3} = decode_dataset_timestamp(DataSetFlags2, Rest2),
    {Picoseconds, Rest4} = decode_dataset_picoseconds(DataSetFlags2, Rest3),
    {Status, Rest5} = decode_dataset_status(DataSetFlags1, Rest4),
    {ConfigVerMajorVer, Rest6} = decode_dataset_cfg_major_ver(DataSetFlags1, Rest5),
    {ConfigVerMinorVer, Rest7} = decode_dataset_cfg_minor_ver(DataSetFlags1, Rest6),
    {#{
        dataset_flags1 => DataSetFlags1,
        dataset_flags2 => DataSetFlags2,
        dataset_seq_num => DataSetSeqNum,
        timestamp => Timestamp,
        picoseconds => Picoseconds,
        status => Status,
        config_ver_major_ver => ConfigVerMajorVer,
        config_ver_minor_ver => ConfigVerMinorVer
    },
    Rest7}.

decode_dataset_flags1(<<
        DataSetFlags2:1,
        ConfigVerMinorVer:1,
        ConfigVerMajorVer:1,
        Status:1,
        DataSetMsgSeqNum:1,
        FieldEncoding:2/bitstring,
        DataSetMsgValid:1,
        Rest/binary>>) ->
    {#{
        dataset_msg_valid => DataSetMsgValid,
        field_encoding => decode_field_encoding(FieldEncoding),
        dataset_msg_seq_num => DataSetMsgSeqNum,
        status => Status,
        config_ver_minor_ver => ConfigVerMajorVer,
        config_ver_major_ver => ConfigVerMinorVer,
        dataset_flags2 => DataSetFlags2
    }, Rest}.

decode_field_encoding(<<0:1, 0:1>>) -> variant;
decode_field_encoding(<<0:1, 1:1>>) -> raw;
decode_field_encoding(<<1:1, 0:1>>) -> data_value;
decode_field_encoding(<<1:1, 1:1>>) -> reserved.

decode_dataset_flags2(#{dataset_flags2 := 0}, Bin) ->
    {#{
        msg_type => decode_dataset_message_type(<<0:4>>),
        timestamp => 0,
        picoseconds => 0}, Bin};
decode_dataset_flags2(#{dataset_flags2 := 1},
        <<_Reserved:2,
        PicoSeconds:1,
        Timestamp:1,
        DataMsgType:4/bitstring,
        Rest/binary>>) ->
    {#{
        msg_type => decode_dataset_message_type(DataMsgType),
        timestamp => Timestamp,
        picoseconds => PicoSeconds
    }, Rest}.

decode_dataset_message_type(<<0:4>>) -> data_key_frame;
decode_dataset_message_type(<<0:1, 0:1, 0:1, 1:1>>) -> data_delta_frame;
decode_dataset_message_type(<<0:1, 0:1, 1:1, 0:1>>) -> event;
decode_dataset_message_type(<<0:1, 0:1, 1:1, 1:1>>) -> keep_alive;
decode_dataset_message_type(<<_:4>>) -> reserved.

decode_dataset_seq_num(#{dataset_msg_seq_num := 0}, Bin) -> {undefined, Bin};
decode_dataset_seq_num(#{dataset_msg_seq_num := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint16, Bin).

decode_dataset_timestamp(#{timestamp := 0}, Bin) -> {undefined, Bin};
decode_dataset_timestamp(#{timestamp := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(date_time, Bin).

decode_dataset_picoseconds(#{picoseconds := 0}, Bin) -> {undefined, Bin};
decode_dataset_picoseconds(#{picoseconds := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint16, Bin).

decode_dataset_status(#{status := 0}, Bin) -> {undefined, Bin};
decode_dataset_status(#{status := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint16, Bin).

decode_dataset_cfg_major_ver(#{config_ver_major_ver := 0}, Bin) ->
    {undefined, Bin};
decode_dataset_cfg_major_ver(#{config_ver_major_ver := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint32, Bin).

decode_dataset_cfg_minor_ver(#{config_ver_minor_ver := 0}, Bin) ->
    {undefined, Bin};
decode_dataset_cfg_minor_ver(#{config_ver_minor_ver := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint32, Bin).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode_dataset_message_flags1(#{
            dataset_msg_valid := DataSetMsgValid,
            field_encoding := FieldEncoding,
            dataset_msg_seq_num := DataSetMsgSeqNum,
            status := Status,
            config_ver_minor_ver := ConfigVerMajorVer,
            config_ver_major_ver := ConfigVerMinorVer,
            dataset_flags2 := DataSetFlags2
        }) ->
    <<DataSetFlags2:1,
      ConfigVerMinorVer:1,
      ConfigVerMajorVer:1,
      Status:1,
      DataSetMsgSeqNum:1,
      (encode_field_encoding(FieldEncoding)):2/bitstring,
      DataSetMsgValid:1>>.

encode_field_encoding(variant) -> ?DATASET_FLAGS1_VARIANT;
encode_field_encoding(raw) -> ?DATASET_FLAGS1_RAWDATA;
encode_field_encoding(data_value) -> ?DATASET_FLAGS1_DATAVALUE.

encode_dataset_message_flags2(#{
            msg_type := DataMsgType,
            timestamp := Timestamp,
            picoseconds := PicoSeconds
        }) ->
    <<0:2,
     PicoSeconds:1,
     Timestamp:1,
     (encode_dataset_message_type(DataMsgType)):4/bitstring>>.


encode_dataset_message_type(data_key_frame) -> <<0:4>>;
encode_dataset_message_type(data_delta_fram) -> <<0:1, 0:1, 0:1, 1:1>>;
encode_dataset_message_type(event) -> <<0:1, 0:1, 1:1, 0:1>>;
encode_dataset_message_type(keep_alive) -> <<0:1, 0:1, 1:1, 1:1>>.

encode_publisher_id_type(byte)   -> ?UA_PUBLISHERIDTYPE_BYTE;
encode_publisher_id_type(uint16) -> ?UA_PUBLISHERIDTYPE_UINT16;
encode_publisher_id_type(uint32) -> ?UA_PUBLISHERIDTYPE_UINT32;
encode_publisher_id_type(uint64) -> ?UA_PUBLISHERIDTYPE_UINT64;
encode_publisher_id_type(string) -> ?UA_PUBLISHERIDTYPE_STRING.

encode_network_msg_type(dataset_message) ->     ?EXT_FLAGS2_DATASET_MSG_TYPE;
encode_network_msg_type(discovery_request) ->   ?EXT_FLAGS2_DISCOVERY_REQUEST_MSG_TYPE;
encode_network_msg_type(discovery_responce) ->  ?EXT_FLAGS2_DISCOVERY_RESPONSE_MSG_TYPE.



encode_payload_header(WriterGroupCfg, DSW_IDS) ->
    IDS = [<<ID:16/unsigned-little>> || ID <- DSW_IDS ],
    Count = <<(length(DSW_IDS)):8/unsigned-little>>,
    iolist_to_binary([Count | IDS]).

encode_group_header(#writer_group_config{
                                    writer_group_id = WriterGroupId}, Mask) ->
    Flags = 0,
    Elements = [],
    {Flags1, Elements1} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_SEQ_NUM of
        0 -> {Flags, Elements};
        _ -> {Flags bor ?SEQUENCE_NUMBER_ENABLED, error(not_implemented)}
    end,
    {Flags2, Elements2} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_NET_MSG_NUM of
        0 -> {Flags1, Elements1};
        _ -> {Flags1 bor ?NETWORK_MESSAGE_ENABLED, error(not_implemented)}
    end,
    {Flags3, Elements3} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_GROUP_VERSION of
        0 -> {Flags2, Elements2};
        _ -> {Flags2 bor ?GROUP_VERSION_ENABLED, error(not_implemented)}
    end,
    {Flags4, Elements4} = case Mask band ?UADP_NET_MSG_CONTENT_MASK_WRITER_GROUP_ID of
        0 -> {Flags3, Elements3};
        _ -> {Flags3 bor ?WRITER_GROUP_ENABLED,
              [opcua_codec_binary_builtin:encode(uint16, WriterGroupId) | Elements3]}
    end,
    iolist_to_binary([Flags4, Elements4]).
