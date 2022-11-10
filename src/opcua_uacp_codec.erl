-module(opcua_uacp_codec).

%TODO: Support non-fatal decoding errors when decoding multiple chunks by
%      adding the buffer to the error so the decoding can continue after
%      the chunk that couldn't be decoded.

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Chunk encoding and decoding functions
-export([decode_chunks/1]).
-export([prepare_chunks/1]).
-export([freeze_chunks/1]).
-export([encode_chunks/1]).

%% Message payload encoding and decoding functions
-export([encode_payload/3]).
-export([decode_payload/2]).
-export([encode_hello/1]).
-export([decode_hello/1]).
-export([encode_acknowledge/1]).
-export([decode_acknowledge/1]).
-export([encode_error/1]).
-export([decode_error/1]).
-export([encode_object/2]).
-export([decode_object/1]).

%% Utility encoding/decoding functions
-export([decode_sequence_header/1]).
-export([encode_sequence_header/2]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(HEL_SPEC, [
    {ver,                   uint32},
    {max_res_chunk_size,    uint32},
    {max_req_chunk_size,    uint32},
    {max_msg_size,          uint32},
    {max_chunk_count,       uint32},
    {endpoint_url,          string}
]).

-define(ACK_SPEC, [
    {ver,                   uint32},
    {max_res_chunk_size,    uint32},
    {max_req_chunk_size,    uint32},
    {max_msg_size,          uint32},
    {max_chunk_count,       uint32}
]).

-define(ERR_SPEC, [
    {error,                 uint32},
    {reason,                string}
]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decode_chunks(iodata()) -> {[opcua:chunk()], iodata()}.
decode_chunks(Data) -> decode_chunks(iolist_to_binary(Data), []).

-spec prepare_chunks(opcua:chunk() | [opcua:chunk()])
    -> opcua:chunk() | [opcua:chunk()].
prepare_chunks(#uacp_chunk{} = Chunk) -> prepare_chunk(Chunk);
prepare_chunks(Chunks) -> [prepare_chunk(C) || C <- Chunks].

-spec freeze_chunks(opcua:chunk() | [opcua:chunk()])
    -> opcua:chunk() | [opcua:chunk()].
freeze_chunks(#uacp_chunk{} = Chunk) -> freeze_chunk(Chunk);
freeze_chunks(Chunks) -> [freeze_chunk(C) || C <- Chunks].

-spec encode_chunks(opcua:chunk() | [opcua:chunk()])
    -> opcua:chunk() | [opcua:chunk()].
encode_chunks(#uacp_chunk{} = Chunk) -> encode_chunk(Chunk);
encode_chunks(Chunks) -> [encode_chunk(C) || C <- Chunks].

encode_payload(hello, undefined, Payload) ->
    opcua_uacp_codec:encode_hello(Payload);
encode_payload(acknowledge, undefined, Payload) ->
    opcua_uacp_codec:encode_acknowledge(Payload);
encode_payload(error, undefined, Payload) ->
    opcua_uacp_codec:encode_error(Payload);
encode_payload(_MsgType, NodeId, Payload) ->
    opcua_uacp_codec:encode_object(NodeId, Payload).

decode_payload(hello, Data) -> {undefined, decode_hello(Data)};
decode_payload(acknowledge, Data) -> {undefined, decode_acknowledge(Data)};
decode_payload(error, Data) -> {undefined, decode_error(Data)};
decode_payload(_MsgType, Data) -> decode_object(Data).

-spec encode_hello(opcua:hello_payload()) -> iodata().
encode_hello(Data) ->
    case opcua_codec_binary:encode(?HEL_SPEC, Data) of
        {Result, Extra} when Extra =:= #{} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("HELLO message encoding error; extra data: ~p", [Extra]),
            throw(bad_encoding_error)
    end.

-spec decode_hello(iodata()) -> opcua:hello_payload().
decode_hello(Data) ->
    case opcua_codec_binary:decode(?HEL_SPEC, iolist_to_binary(Data)) of
        {Result, <<>>} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("HELLO message decoding error; extra data: ~p", [Extra]),
            throw(bad_decoding_error)
    end.

-spec encode_acknowledge(opcua:acknowledge_payload()) -> iodata().
encode_acknowledge(Map) ->
    case opcua_codec_binary:encode(?ACK_SPEC, Map) of
        {Result, Extra} when Extra =:= #{} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("ACKNOWLEDGE message encoding error; extra data: ~p", [Extra]),
            throw(bad_encoding_error)
    end.

-spec decode_acknowledge(iodata()) -> opcua:acknowledge_payload().
decode_acknowledge(Data) ->
    case opcua_codec_binary:decode(?ACK_SPEC, iolist_to_binary(Data)) of
        {Result, <<>>} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("ACKNOWLEDGE message decoding error; extra data: ~p", [Extra]),
            throw(bad_decoding_error)
    end.

-spec encode_error(opcua:error_payload()) -> iodata().
encode_error(#{error := C, reason := D} = Data) ->
    {C2, D2} = opcua_database_status_codes:encode(C, D),
    Data2 = Data#{error := C2, reason := D2},
    case opcua_codec_binary:encode(?ERR_SPEC, Data2) of
        {Result, Extra} when Extra =:= #{} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("ERROR message encoding error; extra data: ~p", [Extra]),
            throw(bad_encoding_error)
    end.

-spec decode_error(iodata()) -> opcua:error_payload().
decode_error(Data) ->
    case opcua_codec_binary:decode(?ERR_SPEC, iolist_to_binary(Data)) of
        {#{error := C, reason := D} = Result, <<>>} ->
            {C2, D2} = opcua_database_status_codes:decode(C, D),
            Result#{error := C2, reason := D2};
        {_Result, Extra} ->
            ?LOG_ERROR("ERROR message decoding error; extra data: ~p", [Extra]),
            throw(bad_decoding_error)
    end.

-spec encode_object(opcua:node_id(), opcua:node_object()) -> iodata().
encode_object(NodeId, Data) ->
    {EncNodeId, _} = opcua_database:lookup_encoding(NodeId, binary),
    {Header, _} = opcua_codec_binary:encode(node_id, EncNodeId),
    {Msg, _} = opcua_codec_binary:encode(NodeId, Data),
    [Header, Msg].

-spec decode_object(iodata()) -> {opcua:node_id(), opcua:node_object()}.
decode_object(Data) ->
    {NodeId, RemData} = opcua_codec_binary:decode(node_id, iolist_to_binary(Data)),
    case opcua_database:resolve_encoding(NodeId) of
        {ObjNodeId, binary} ->
            {Obj, _} = opcua_codec_binary:decode(ObjNodeId, RemData),
            {ObjNodeId, Obj};
        {_, _} -> throw(bad_data_encoding_unsupported)
    end.

-spec encode_sequence_header(SeqNum, ReqId) -> iodata()
  when SeqNum :: opcua_protocol:sequence_num(),
       ReqId :: opcua_protocol:request_id().
encode_sequence_header(SeqNum, ReqId) ->
    {Result, []} = opcua_codec_binary:encode([uint32, uint32], [SeqNum, ReqId]),
    Result.

-spec decode_sequence_header(iodata()) -> {{SeqNum, ReqId}, iodata()}
  when SeqNum :: opcua_protocol:sequence_num(),
       ReqId :: opcua_protocol:request_id().
decode_sequence_header(Data) ->
    {[SeqNum, ReqId], RemData} =
        opcua_codec_binary:decode([uint32, uint32], iolist_to_binary(Data)),
    {{SeqNum, ReqId}, RemData}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_chunks(<<MessageHeader:8/binary, Rest/binary>> = Data, Acc) ->
    <<EncMsgType:3/binary,
      EncChunkType:1/binary,
      PacketLen:32/unsigned-little-integer>> = MessageHeader,
    BodyLen = PacketLen - 8,
    case Rest of
        <<Packet:BodyLen/binary, Data2/binary>> ->
            MsgType = decode_message_type(EncMsgType),
            ChunkType = decode_chunk_type(EncChunkType),
            Chunk = #uacp_chunk{
                message_type = MsgType,
                chunk_type = ChunkType,
                header = MessageHeader
            },
            Chunk2 = decode_chunk(MsgType, ChunkType, Packet, Chunk),
            decode_chunks(Data2, [Chunk2 | Acc]);
        _ ->
            {lists:reverse(Acc), Data}
    end;
decode_chunks(Data, Acc) ->
    {lists:reverse(Acc), Data}.

decode_chunk(MsgType, ChunkType, Data, Chunk)
  when MsgType =:= hello, ChunkType =:= final;
       MsgType =:= acknowledge, ChunkType =:= final;
       MsgType =:= error, ChunkType =:= final ->
    Chunk#uacp_chunk{body = Data};
decode_chunk(channel_open, final, Data, Chunk) ->
    #uacp_chunk{header = MessageHeader} = Chunk,
    Spec = [uint32, string, byte_string, byte_string],
    {DecFields, LockedData} = opcua_codec_binary:decode(Spec, Data),
    FullDataSize = byte_size(Data),
    LockedDataSize = byte_size(LockedData),
    MessageHeaderSize = iolist_size(MessageHeader),
    ExtraHeaderSize = FullDataSize - LockedDataSize,
    <<ExtraHeader:ExtraHeaderSize/binary, _/binary>> = Data,
    [ChannelId, PolicyUri, SenderCert, ReceiverThumbprint] = DecFields,
    SecPol = #uacp_chunk_security{
        policy_uri = PolicyUri,
        sender_cert = SenderCert,
        receiver_thumbprint = ReceiverThumbprint
    },
    Chunk#uacp_chunk{
        state = locked,
        channel_id = ChannelId,
        security = SecPol,
        header_size = MessageHeaderSize + ExtraHeaderSize,
        locked_size = LockedDataSize,
        header = [MessageHeader, ExtraHeader],
        body = LockedData
    };
decode_chunk(MsgType, ChunkType, Data, Chunk)
  when MsgType =:= channel_close, ChunkType =:= final;
       MsgType =:= channel_message ->
    #uacp_chunk{header = MessageHeader} = Chunk,
    Spec = [uint32, uint32],
    {[ChannelId, TokenId], LockedData} = opcua_codec_binary:decode(Spec, Data),
    FullDataSize = byte_size(Data),
    LockedDataSize = byte_size(LockedData),
    MessageHeaderSize = iolist_size(MessageHeader),
    ExtraHeaderSize = FullDataSize - LockedDataSize,
    <<ExtraHeader:ExtraHeaderSize/binary, _/binary>> = Data,
    Chunk#uacp_chunk{
        state = locked,
        channel_id = ChannelId,
        security = TokenId,
        header_size = MessageHeaderSize + ExtraHeaderSize,
        locked_size = LockedDataSize,
        header = [MessageHeader, ExtraHeader],
        body = LockedData
    };
decode_chunk(MsgType, ChunkType, Data, _Chunk) ->
    ?LOG_ERROR("Failed to decode unexpected ~s ~s chunk: ~p",
               [ChunkType, MsgType, Data]),
    throw(bad_unexpected_error).

prepare_chunk(#uacp_chunk{state = State, message_type = MsgType, chunk_type = ChunkType} = Chunk)
  when State =:= unlocked, MsgType =:= channel_open, ChunkType =:= final ->
    #uacp_chunk{
        security = #uacp_chunk_security{
            policy_uri = PolicyUri,
            sender_cert = SenderCert,
            receiver_thumbprint = ReceiverThumbprint
        },
        body = UnlockedBody
    } = Chunk,
    HeaderSize = 8 + 4 + encoded_string_size([PolicyUri, SenderCert, ReceiverThumbprint]),
    UnlockedSize = iolist_size(UnlockedBody),
    Chunk#uacp_chunk{header_size = HeaderSize, unlocked_size = UnlockedSize};
prepare_chunk(#uacp_chunk{state = State, message_type = MsgType, chunk_type = ChunkType} = Chunk)
  when State =:= unlocked, MsgType =:= channel_close, ChunkType =:= final;
       State =:= unlocked, MsgType =:= channel_message ->
    #uacp_chunk{body = UnlockedBody} = Chunk,
    UnlockedSize = iolist_size(UnlockedBody),
    Chunk#uacp_chunk{header_size = 8 + 4 + 4, unlocked_size = UnlockedSize};
prepare_chunk(#uacp_chunk{state = State, message_type = MsgType, chunk_type = ChunkType} = Chunk) ->
    ?LOG_ERROR("Failed to prepare unexpected ~s ~s ~s chunk: ~p",
               [State, ChunkType, MsgType, Chunk]),
    throw(bad_unexpected_error).

freeze_chunk(#uacp_chunk{state = State, message_type = MsgType, chunk_type = ChunkType,
                         header_size = HeaderSize, locked_size = LockedSize} = Chunk)
  when State =:= unlocked, MsgType =:= channel_open, ChunkType =:= final, HeaderSize =/= undefined, LockedSize =/= undefined ->
    #uacp_chunk{
        channel_id = ChannelId,
        security = #uacp_chunk_security{
            policy_uri = PolicyUri,
            sender_cert = SenderCert,
            receiver_thumbprint = ReceiverThumbprint
        }
    } = Chunk,
    EncMsgType = encode_message_type(MsgType),
    EncChunkType = encode_chunk_type(ChunkType),
    Len = LockedSize + HeaderSize,
    Header1 = <<EncMsgType:3/binary, EncChunkType:1/binary, Len:32/little-unsigned-integer>>,
    Header2Spec = [uint32, string, byte_string, byte_string],
    Header2Data = [ChannelId, PolicyUri, SenderCert, ReceiverThumbprint],
    {Header2, []} = opcua_codec_binary:encode(Header2Spec, Header2Data),
    ?assertEqual(Chunk#uacp_chunk.header_size, iolist_size([Header1, Header2])),
    Chunk#uacp_chunk{header = [Header1, Header2]};
freeze_chunk(#uacp_chunk{state = State, message_type = MsgType, chunk_type = ChunkType,
                         header_size = HeaderSize, locked_size = LockedSize} = Chunk)
  when State =:= unlocked, MsgType =:= channel_close, ChunkType =:= final, HeaderSize =/= undefined, LockedSize =/= undefined;
       State =:= unlocked, MsgType =:= channel_message, HeaderSize =/= undefined, LockedSize =/= undefined ->
    #uacp_chunk{
        channel_id = ChannelId,
        security = TokenId,
        header_size = HeaderSize,
        locked_size = LockedSize
    } = Chunk,
    EncMsgType = encode_message_type(MsgType),
    EncChunkType = encode_chunk_type(ChunkType),
    Len = LockedSize + HeaderSize,
    Header1 = <<EncMsgType:3/binary, EncChunkType:1/binary, Len:32/little-unsigned-integer>>,
    Header2Spec = [uint32, uint32],
    Header2Data = [ChannelId, TokenId],
    {Header2, []} = opcua_codec_binary:encode(Header2Spec, Header2Data),
    ?assertEqual(Chunk#uacp_chunk.header_size, iolist_size([Header1, Header2])),
    Chunk#uacp_chunk{header = [Header1, Header2]};
freeze_chunk(#uacp_chunk{message_type = MsgType, chunk_type = ChunkType, state = State} = Chunk) ->
    ?LOG_ERROR("Failed to freeze unexpected ~s ~s ~s chunk: ~p",
               [State, ChunkType, MsgType, Chunk]),
    throw(bad_unexpected_error).

encode_chunk(#uacp_chunk{state = undefined, message_type = MsgType, chunk_type = ChunkType} = Chunk)
  when MsgType =:= hello, ChunkType =:= final;
       MsgType =:= acknowledge, ChunkType =:= final;
       MsgType =:= error, ChunkType =:= final ->
    #uacp_chunk{body = Body} = Chunk,
    EncMsgType = encode_message_type(MsgType),
    EncChunkType = encode_chunk_type(ChunkType),
    BodySize = iolist_size(Body) + 8,
    Header = <<EncMsgType:3/binary, EncChunkType:1/binary,
               BodySize:32/little-unsigned-integer>>,
    [Header, Body];
encode_chunk(#uacp_chunk{state = locked, header = Header, body = Body} = Chunk)
  when Header =/= undefined, Body =/= undefined ->
    ?assertEqual(Chunk#uacp_chunk.header_size + Chunk#uacp_chunk.locked_size,
                 iolist_size([Header, Body])),
    [Header, Body];
encode_chunk(#uacp_chunk{message_type = MsgType, chunk_type = ChunkType, state = State} = Chunk) ->
    ?LOG_ERROR("Failed to encode unexpected ~s ~s ~s chunk: ~p",
               [State, ChunkType, MsgType, Chunk]),
    throw(bad_unexpected_error).

-spec decode_message_type(binary()) -> opcua:message_type().
decode_message_type(<<"HEL">>) -> hello;
decode_message_type(<<"ACK">>) -> acknowledge;
decode_message_type(<<"ERR">>) -> error;
% Reverse HELLO not yet supported
% decode_message_type(<<"RHE">>) -> reverse_hello;
decode_message_type(<<"OPN">>) -> channel_open;
decode_message_type(<<"CLO">>) -> channel_close;
decode_message_type(<<"MSG">>) -> channel_message;
decode_message_type(_) -> throw(bad_tcp_message_type_invalid).

-spec encode_message_type(opcua:message_type()) -> binary().
encode_message_type(hello)              -> <<"HEL">>;
encode_message_type(acknowledge)        -> <<"ACK">>;
encode_message_type(error)              -> <<"ERR">>;
% Reverse HELLO not yet supported
% encode_message_type(reverse_hello)      -> <<"RHE">>;
encode_message_type(channel_open)       -> <<"OPN">>;
encode_message_type(channel_close)      -> <<"CLO">>;
encode_message_type(channel_message)    -> <<"MSG">>.

decode_chunk_type(<<"F">>) -> final;
decode_chunk_type(<<"C">>) -> intermediate;
decode_chunk_type(<<"A">>) -> aborted;
decode_chunk_type(_) -> throw(bad_tcp_message_type_invalid).

encode_chunk_type(final)        -> <<"F">>;
encode_chunk_type(intermediate) -> <<"C">>;
encode_chunk_type(aborted)      -> <<"A">>.

encoded_string_size(undefined) -> 4;
encoded_string_size(Str) when is_binary(Str) -> 4 + byte_size(Str);
encoded_string_size([]) -> 0;
encoded_string_size([Str | Rest]) ->
    encoded_string_size(Str) + encoded_string_size(Rest).
