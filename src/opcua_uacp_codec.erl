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
-export([decode_chunks/2]).
-export([prepare_chunks/2]).
-export([freeze_chunks/2]).
-export([encode_chunks/2]).

%% Message payload encoding and decoding functions
-export([encode_payload/4]).
-export([decode_payload/3]).
-export([encode_hello/2]).
-export([decode_hello/2]).
-export([encode_acknowledge/2]).
-export([decode_acknowledge/2]).
-export([encode_error/2]).
-export([decode_error/2]).
-export([encode_object/3]).
-export([decode_object/2]).

%% Utility encoding/decoding functions
-export([decode_sequence_header/2]).
-export([encode_sequence_header/3]).


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

-spec decode_chunks(opcua_space:state(), iodata()) -> {[opcua:chunk()], iodata()}.
decode_chunks(Space, Data) ->
    decode_chunks(Space, iolist_to_binary(Data), []).

-spec prepare_chunks(opcua_space:state(), opcua:chunk() | [opcua:chunk()])
    -> opcua:chunk() | [opcua:chunk()].
prepare_chunks(Space, #uacp_chunk{} = Chunk) -> prepare_chunk(Space, Chunk);
prepare_chunks(Space, Chunks) -> [prepare_chunk(Space, C) || C <- Chunks].

-spec freeze_chunks(opcua_space:state(), opcua:chunk() | [opcua:chunk()])
    -> opcua:chunk() | [opcua:chunk()].
freeze_chunks(Space, #uacp_chunk{} = Chunk) -> freeze_chunk(Space, Chunk);
freeze_chunks(Space, Chunks) -> [freeze_chunk(Space, C) || C <- Chunks].

-spec encode_chunks(opcua_space:state(), opcua:chunk() | [opcua:chunk()])
    -> opcua:chunk() | [opcua:chunk()].
encode_chunks(Space, #uacp_chunk{} = Chunk) -> encode_chunk(Space, Chunk);
encode_chunks(Space, Chunks) -> [encode_chunk(Space, C) || C <- Chunks].

encode_payload(Space, hello, undefined, Payload) ->
    encode_hello(Space, Payload);
encode_payload(Space, acknowledge, undefined, Payload) ->
    encode_acknowledge(Space, Payload);
encode_payload(Space, error, undefined, Payload) ->
    encode_error(Space, Payload);
encode_payload(Space, _MsgType, NodeId, Payload) ->
    encode_object(Space, NodeId, Payload).

-spec decode_payload(opcua_space:state(), PayloadType, Data) ->
    {ok, undefined | NodeId, Payload}
  | {schema_not_found, undefined | NodeId, undefined | Payload, [NodeId]}
  when PayloadType :: hello | acknowledge | error | NodeId,
       Data :: iodata(), NodeId :: opcua:node_id(),
       Payload :: opcua:hello_payload() | opcua:acknowledge_payload()
                | opcua:error_payload() | opcua:node_object().
decode_payload(Space, hello, Data) ->
    {ok, undefined, decode_hello(Space, Data)};
decode_payload(Space, acknowledge, Data) ->
    {ok, undefined, decode_acknowledge(Space, Data)};
decode_payload(Space, error, Data) ->
    {ok, undefined, decode_error(Space, Data)};
decode_payload(Space, _MsgType, Data) ->
    decode_object(Space, Data).

-spec encode_hello(opcua_space:state(), opcua:hello_payload()) -> iodata().
encode_hello(Space, Data) ->
    case encode(Space, ?HEL_SPEC, Data) of
        {Result, Extra} when Extra =:= #{} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("HELLO message encoding error; extra data: ~p", [Extra]),
            throw(bad_encoding_error)
    end.

-spec decode_hello(opcua_space:state(), iodata()) -> opcua:hello_payload().
decode_hello(Space, Data) ->
    case decode(Space, ?HEL_SPEC, iolist_to_binary(Data)) of
        {Result, <<>>} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("HELLO message decoding error; extra data: ~p", [Extra]),
            throw(bad_decoding_error)
    end.

-spec encode_acknowledge(opcua_space:state(), opcua:acknowledge_payload()) -> iodata().
encode_acknowledge(Space, Map) ->
    case encode(Space, ?ACK_SPEC, Map) of
        {Result, Extra} when Extra =:= #{} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("ACKNOWLEDGE message encoding error; extra data: ~p", [Extra]),
            throw(bad_encoding_error)
    end.

-spec decode_acknowledge(opcua_space:state(), iodata()) -> opcua:acknowledge_payload().
decode_acknowledge(Space, Data) ->
    case decode(Space, ?ACK_SPEC, iolist_to_binary(Data)) of
        {Result, <<>>} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("ACKNOWLEDGE message decoding error; extra data: ~p", [Extra]),
            throw(bad_decoding_error)
    end.

-spec encode_error(opcua_space:state(), opcua:error_payload()) -> iodata().
encode_error(Space, #{error := C, reason := D} = Data) ->
    {C2, D2} = encode_status(C, D),
    Data2 = Data#{error := C2, reason := D2},
    case encode(Space, ?ERR_SPEC, Data2) of
        {Result, Extra} when Extra =:= #{} -> Result;
        {_Result, Extra} ->
            ?LOG_ERROR("ERROR message encoding error; extra data: ~p", [Extra]),
            throw(bad_encoding_error)
    end.

-spec decode_error(opcua_space:state(), iodata()) -> opcua:error_payload().
decode_error(Space, Data) ->
    case decode(Space, ?ERR_SPEC, iolist_to_binary(Data)) of
        {#{error := C, reason := D} = Result, <<>>} ->
            {C2, D2} = decode_status(C, D),
            Result#{error := C2, reason := D2};
        {_Result, Extra} ->
            ?LOG_ERROR("ERROR message decoding error; extra data: ~p", [Extra]),
            throw(bad_decoding_error)
    end.

-spec encode_object(opcua_space:state(), opcua:node_id(), opcua:node_object()) -> iodata().
encode_object(Space, NodeId, Data) ->
    case opcua_space:type_descriptor(Space, NodeId, binary) of
        undefined -> throw({bad_data_encoding_unsupported, NodeId});
        TypeDescNodeId ->
            {Header, _} = encode(Space, node_id, TypeDescNodeId),
            {Msg, _} = encode(Space, NodeId, Data),
            [Header, Msg]
    end.

-spec decode_object(opcua_space:state(), iodata()) ->
     {ok, opcua:node_id(), opcua:node_object()}
   | {schema_not_found, undefined | opcua:node_id(),
                        undefined | opcua:node_object(),
                       [] | [opcua:node_id()]}.
decode_object(Space, Data) ->
    {TypeDescId, RemData} = decode(Space, node_id, iolist_to_binary(Data)),
    case opcua_space:data_type(Space, TypeDescId) of
        {ObjNodeId, binary} ->
            {Obj, _, Issues} = safe_decode(Space, ObjNodeId, RemData),
            filter_decode_issues(ObjNodeId, Obj, Issues);
        _ -> throw({bad_data_encoding_unsupported, TypeDescId})
    end.

-spec encode_sequence_header(opcua_space:state(), SeqNum, ReqId) -> iodata()
  when SeqNum :: opcua_protocol:sequence_num(),
       ReqId :: opcua_protocol:request_id().
encode_sequence_header(Space, SeqNum, ReqId) ->
    {Result, []} = encode(Space, [uint32, uint32], [SeqNum, ReqId]),
    Result.

-spec decode_sequence_header(opcua_space:state(), iodata()) -> {{SeqNum, ReqId}, iodata()}
  when SeqNum :: opcua_protocol:sequence_num(),
       ReqId :: opcua_protocol:request_id().
decode_sequence_header(Space, Data) ->
    {[SeqNum, ReqId], RemData} =
        decode(Space, [uint32, uint32], iolist_to_binary(Data)),
    {{SeqNum, ReqId}, RemData}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode(Space, Spec, Data) ->
    opcua_codec_binary:encode(Spec, Data, #{space => Space}).

decode(Space, Spec, Data) ->
    opcua_codec_binary:decode(Spec, Data, #{space => Space}).

safe_decode(Space, Spec, Data) ->
    opcua_codec_binary:decode(Spec, Data, #{space => Space, allow_partial => true}).

filter_decode_issues(Id, Obj, Issues) ->
    filter_decode_issues(Id, Obj, Issues, []).

filter_decode_issues(Id, Obj, [], []) ->
    {ok, Id, Obj};
filter_decode_issues(Id, Obj, [], Acc) ->
    {schema_not_found, Id, Obj, Acc};
filter_decode_issues(_Id, _Obj, [{generic_codec_error, Reason, Details} | _Rest], _Acc) ->
    throw({bad_decoding_error, {Reason, Details}});
filter_decode_issues(_Id, _Obj, [{encoding_not_supported, Reason, Details} | _Rest], _Acc) ->
    throw({bad_data_encoding_unsupported, {Reason, Details}});
filter_decode_issues(Id, Obj, [{schema_not_found, Schema, Details} | Rest], Acc) ->
    ?LOG_DEBUG("Schema ~s not found while decoding ~s",
               [opcua_node:format(Schema), Details]),
    filter_decode_issues(Id, Obj, Rest, [Schema | Acc]).

decode_chunks(Space, <<MessageHeader:8/binary, Rest/binary>> = Data, Acc) ->
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
            Chunk2 = decode_chunk(Space, MsgType, ChunkType, Packet, Chunk),
            decode_chunks(Space, Data2, [Chunk2 | Acc]);
        _ ->
            {lists:reverse(Acc), Data}
    end;
decode_chunks(_Space, Data, Acc) ->
    {lists:reverse(Acc), Data}.

decode_chunk(_Space, MsgType, ChunkType, Data, Chunk)
  when MsgType =:= hello, ChunkType =:= final;
       MsgType =:= acknowledge, ChunkType =:= final;
       MsgType =:= error, ChunkType =:= final ->
    Chunk#uacp_chunk{body = Data};
decode_chunk(Space, channel_open, final, Data, Chunk) ->
    #uacp_chunk{header = MessageHeader} = Chunk,
    Spec = [uint32, string, byte_string, byte_string],
    {DecFields, LockedData} = decode(Space, Spec, Data),
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
decode_chunk(Space, MsgType, ChunkType, Data, Chunk)
  when MsgType =:= channel_close, ChunkType =:= final;
       MsgType =:= channel_message ->
    #uacp_chunk{header = MessageHeader} = Chunk,
    Spec = [uint32, uint32],
    {[ChannelId, TokenId], LockedData} = decode(Space, Spec, Data),
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
decode_chunk(_Space, MsgType, ChunkType, Data, _Chunk) ->
    ?LOG_ERROR("Failed to decode unexpected ~s ~s chunk: ~p",
               [ChunkType, MsgType, Data]),
    throw(bad_unexpected_error).

prepare_chunk(_Space, #uacp_chunk{state = State, message_type = MsgType,
                                  chunk_type = ChunkType} = Chunk)
  when State =:= unlocked, MsgType =:= channel_open, ChunkType =:= final ->
    #uacp_chunk{
        security = #uacp_chunk_security{
            policy_uri = PolicyUri,
            sender_cert = SenderCert,
            receiver_thumbprint = ReceiverThumbprint
        },
        body = UnlockedBody
    } = Chunk,
    HeaderSize = 8 + 4 + encoded_string_size([PolicyUri, SenderCert,
                                              ReceiverThumbprint]),
    UnlockedSize = iolist_size(UnlockedBody),
    Chunk#uacp_chunk{header_size = HeaderSize, unlocked_size = UnlockedSize};
prepare_chunk(_Space, #uacp_chunk{state = State, message_type = MsgType,
                                  chunk_type = ChunkType} = Chunk)
  when State =:= unlocked, MsgType =:= channel_close, ChunkType =:= final;
       State =:= unlocked, MsgType =:= channel_message ->
    #uacp_chunk{body = UnlockedBody} = Chunk,
    UnlockedSize = iolist_size(UnlockedBody),
    Chunk#uacp_chunk{header_size = 8 + 4 + 4, unlocked_size = UnlockedSize};
prepare_chunk(_Space, #uacp_chunk{state = State, message_type = MsgType,
                                  chunk_type = ChunkType} = Chunk) ->
    ?LOG_ERROR("Failed to prepare unexpected ~s ~s ~s chunk: ~p",
               [State, ChunkType, MsgType, Chunk]),
    throw(bad_unexpected_error).

freeze_chunk(Space, #uacp_chunk{state = State, message_type = MsgType,
                                 chunk_type = ChunkType,
                                 header_size = HeaderSize,
                                 locked_size = LockedSize} = Chunk)
  when State =:= unlocked, MsgType =:= channel_open, ChunkType =:= final,
       HeaderSize =/= undefined, LockedSize =/= undefined ->
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
    Header1 = <<EncMsgType:3/binary, EncChunkType:1/binary,
                Len:32/little-unsigned-integer>>,
    Header2Spec = [uint32, string, byte_string, byte_string],
    Header2Data = [ChannelId, PolicyUri, SenderCert, ReceiverThumbprint],
    {Header2, []} = encode(Space, Header2Spec, Header2Data),
    ?assertEqual(Chunk#uacp_chunk.header_size, iolist_size([Header1, Header2])),
    Chunk#uacp_chunk{header = [Header1, Header2]};
freeze_chunk(Space, #uacp_chunk{state = State, message_type = MsgType,
                                 chunk_type = ChunkType,
                                 header_size = HeaderSize,
                                 locked_size = LockedSize} = Chunk)
  when State =:= unlocked, MsgType =:= channel_close, ChunkType =:= final,
       HeaderSize =/= undefined, LockedSize =/= undefined;
       State =:= unlocked, MsgType =:= channel_message,
       HeaderSize =/= undefined, LockedSize =/= undefined ->
    #uacp_chunk{
        channel_id = ChannelId,
        security = TokenId,
        header_size = HeaderSize,
        locked_size = LockedSize
    } = Chunk,
    EncMsgType = encode_message_type(MsgType),
    EncChunkType = encode_chunk_type(ChunkType),
    Len = LockedSize + HeaderSize,
    Header1 = <<EncMsgType:3/binary, EncChunkType:1/binary,
                Len:32/little-unsigned-integer>>,
    Header2Spec = [uint32, uint32],
    Header2Data = [ChannelId, TokenId],
    {Header2, []} = encode(Space, Header2Spec, Header2Data),
    ?assertEqual(Chunk#uacp_chunk.header_size, iolist_size([Header1, Header2])),
    Chunk#uacp_chunk{header = [Header1, Header2]};
freeze_chunk(_Space, #uacp_chunk{message_type = MsgType, chunk_type = ChunkType,
                                 state = State} = Chunk) ->
    ?LOG_ERROR("Failed to freeze unexpected ~s ~s ~s chunk: ~p",
               [State, ChunkType, MsgType, Chunk]),
    throw(bad_unexpected_error).

encode_chunk(_Space, #uacp_chunk{state = undefined, message_type = MsgType,
                                 chunk_type = ChunkType} = Chunk)
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
encode_chunk(_Space, #uacp_chunk{state = locked, header = Header,
                                 body = Body} = Chunk)
  when Header =/= undefined, Body =/= undefined ->
    ?assertEqual(Chunk#uacp_chunk.header_size + Chunk#uacp_chunk.locked_size,
                 iolist_size([Header, Body])),
    [Header, Body];
encode_chunk(_Space, #uacp_chunk{message_type = MsgType, chunk_type = ChunkType,
                                 state = State} = Chunk) ->
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

encode_status(StatusName, undefined) ->
    {C, _, D} = opcua_nodeset:status(StatusName),
    {C, D};
encode_status(StatusName, StatusDesc) ->
    {C, _, _} = opcua_nodeset:status(StatusName),
    {C, StatusDesc}.

decode_status(StatusCode, undefined) ->
    {_, N, D} = opcua_nodeset:status(StatusCode),
    {N, D};
decode_status(StatusCode, StatusDesc) ->
    {_, N, _} = opcua_nodeset:status(StatusCode),
    {N, StatusDesc}.
