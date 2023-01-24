-module(opcua_pubsub_uadp).


-export([decode_network_message_headers/1]).
-export([decode_payload/2]).
-export([decode_data_set_message_field/3]).

-include("opcua.hrl").
-include("opcua_pubsub.hrl").

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
        picoseconds := 0, publisher_id_type := uint16,
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
    {DSM_header, Binary} = decode_data_set_message_header(Payload),
    [{DSM_header, Binary}];
decode_payload(#{payload_header := #{count := 1}}, Payload) ->
    {DSM_header, Binary} = decode_data_set_message_header(Payload),
    [{DSM_header, Binary}];
decode_payload(#{payload_header := #{count := Count}}, Payload) ->
    <<SizesBinary:(Count*2)/binary, Rest/binary>> = Payload,
    Sizes = [Size || <<Size:16/unsigned-little>> <= SizesBinary],
    decode_multi_data_set_message(Rest, Sizes).

decode_data_set_message_field(variant, FieldMetadata, Binary) ->
    #data_set_field_metadata{
        builtin_type = BuiltinType,
        data_type = _NodeId,
        valueRank = _
    } = FieldMetadata,
    {Result, Rest} = opcua_codec_binary:decode(variant, Binary),
    case Result of
        #opcua_variant{type = BuiltinType} -> {Result, Rest};
        _ -> {error, unmatched_metadata}
    end;
decode_data_set_message_field(_, _, _) ->
    error(bad_encoding_not_implemented).

%%% INTERNALS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


decode_extended_flags1(0, Bin) ->
    {#{
        extended_flags2 => 0,
        picoseconds => 0,
        timestamp => 0,
        security => 0,
        dataset_class_id => 0,
        publisher_id_type => publisher_id_type(0)
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
        publisher_id_type => publisher_id_type(PublisherIdType)
    }, Rest}.

publisher_id_type(?UA_PUBLISHERIDTYPE_BYTE) -> byte;
publisher_id_type(?UA_PUBLISHERIDTYPE_UINT16) -> uint16;
publisher_id_type(?UA_PUBLISHERIDTYPE_UINT32) -> uint32;
publisher_id_type(?UA_PUBLISHERIDTYPE_UINT64) -> uint64;
publisher_id_type(?UA_PUBLISHERIDTYPE_STRING) -> string;
publisher_id_type(_) -> reserved.

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


decode_network_msg_type(<< 0:1, 0:1, 0:1>>) -> data_set_message;
decode_network_msg_type(<< 0:1, 0:1, 1:1>>) -> discovery_request;
decode_network_msg_type(<< 0:1, 1:1, 0:1>>) -> discovery_responce;
decode_network_msg_type(<< _:1, _:1, _:1>>) -> reserved.

decode_publisherID(0, _, Binary) -> {undefined, Binary};
decode_publisherID(1, #{publisher_id_type := uint16}, Binary) ->
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
decode_payload_header(1, #{network_message_type := data_set_message}, Bin) ->
    <<MsgCount:8/unsigned-little, Rest/binary>> = Bin,
    <<DataWriterIDs:(MsgCount*2)/binary, Rest2/binary>> = Rest,
    {#{
        count => MsgCount,
        data_set_writer_ids =>
            [ DataWriterID || <<DataWriterID:16/unsigned-little>> <= DataWriterIDs]
    }, Rest2};
decode_payload_header(1, #{network_message_type := discovery_request}, Bin) ->
    throw({not_implemented, discovery_request});
decode_payload_header(1, #{network_message_type := discovery_responce}, Bin) ->
    throw({not_implemented, discovery_responce}).



decode_multi_data_set_message(Bin, Sizes) ->
    decode_multi_data_set_message(Bin, Sizes, []).

decode_multi_data_set_message(<<>>, [], Result) -> lists:reverse(Result);
decode_multi_data_set_message(Bin, [S|TL], Result) ->
    <<DSM:S/binary, Rest/binary>> = Bin,
    {DSM_header, Binary1} = decode_data_set_message_header(DSM),
    decode_multi_data_set_message(Rest, [ {DSM_header, Binary1} | Result], TL).



decode_data_set_message_header(DataSetMessageBinary) ->
    {DataSetFlags1, Rest} = decode_data_set_flags1(DataSetMessageBinary),
    {DataSetFlags2, Rest1} = decode_data_set_flags2(DataSetFlags1, Rest),
    {DataSetSeqNum, Rest2} = decode_data_set_seq_num(DataSetFlags1, Rest1),
    {Timestamp, Rest3} = decode_data_set_timestamp(DataSetFlags2, Rest2),
    {Picoseconds, Rest4} = decode_data_set_picoseconds(DataSetFlags2, Rest3),
    {Status, Rest5} = decode_data_set_status(DataSetFlags1, Rest4),
    {ConfigVerMajorVer, Rest6} = decode_data_set_cfg_major_ver(DataSetFlags1, Rest5),
    {ConfigVerMinorVer, Rest7} = decode_data_set_cfg_minor_ver(DataSetFlags1, Rest6),
    {#{
        data_set_flags1 => DataSetFlags1,
        data_set_flags2 => DataSetFlags2,
        data_set_seq_num => DataSetSeqNum,
        timestamp => Timestamp,
        picoseconds => Picoseconds,
        status => Status,
        config_ver_major_ver => ConfigVerMajorVer,
        config_ver_minor_ver => ConfigVerMinorVer
    },
    Rest7}.

decode_data_set_flags1(<<
        DataSetFlags2:1,
        ConfigVerMinorVer:1,
        ConfigVerMajorVer:1,
        Status:1,
        DataSetMsgSeqNum:1,
        FieldEncoding:2/bitstring,
        DataSetMsgValid:1,
        Rest/binary>>) ->
    {#{
        data_set_msg_valid => DataSetMsgValid,
        field_encoding => decode_field_encoding(FieldEncoding),
        data_set_msg_seq_num => DataSetMsgSeqNum,
        status => Status,
        config_ver_minor_ver => ConfigVerMajorVer,
        config_ver_major_ver => ConfigVerMinorVer,
        data_set_flags2 => DataSetFlags2
    }, Rest}.

decode_field_encoding(<<0:1, 0:1>>) -> variant;
decode_field_encoding(<<0:1, 1:1>>) -> raw;
decode_field_encoding(<<1:1, 0:1>>) -> data_value;
decode_field_encoding(<<1:1, 1:1>>) -> reserved.

decode_data_set_flags2(#{data_set_flags2 := 0}, Bin) ->
    {#{
        msg_type => decode_data_set_message_type(<<0:4>>),
        timestamp => 0,
        picoseconds => 0}, Bin};
decode_data_set_flags2(#{data_set_flags2 := 1},
        <<_Reserved:2,
        PicoSeconds:1,
        Timestamp:1,
        DataMsgType:4/bitstring,
        Rest/binary>>) ->
    {#{
        msg_type => decode_data_set_message_type(DataMsgType),
        timestamp => Timestamp,
        picoseconds => PicoSeconds
    }, Rest}.

decode_data_set_message_type(<<0:4>>) -> data_key_frame;
decode_data_set_message_type(<<0:1, 0:1, 0:1, 1:1>>) -> data_delta_frame;
decode_data_set_message_type(<<0:1, 0:1, 1:1, 0:1>>) -> event;
decode_data_set_message_type(<<0:1, 0:1, 1:1, 1:1>>) -> keep_alive;
decode_data_set_message_type(<<_:4>>) -> reserved.

decode_data_set_seq_num(#{data_set_msg_seq_num := 0}, Bin) -> {undefined, Bin};
decode_data_set_seq_num(#{data_set_msg_seq_num := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint16, Bin).

decode_data_set_timestamp(#{timestamp := 0}, Bin) -> {undefined, Bin};
decode_data_set_timestamp(#{timestamp := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(date_time, Bin).

decode_data_set_picoseconds(#{picoseconds := 0}, Bin) -> {undefined, Bin};
decode_data_set_picoseconds(#{picoseconds := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint16, Bin).

decode_data_set_status(#{status := 0}, Bin) -> {undefined, Bin};
decode_data_set_status(#{status := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint16, Bin).

decode_data_set_cfg_major_ver(#{config_ver_major_ver := 0}, Bin) ->
    {undefined, Bin};
decode_data_set_cfg_major_ver(#{config_ver_major_ver := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint32, Bin).

decode_data_set_cfg_minor_ver(#{config_ver_minor_ver := 0}, Bin) ->
    {undefined, Bin};
decode_data_set_cfg_minor_ver(#{config_ver_minor_ver := 1}, Bin) ->
    opcua_codec_binary_builtin:decode(uint32, Bin).
