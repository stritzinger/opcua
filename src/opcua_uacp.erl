-module(opcua_uacp).

%TODO: Check maximum number of inflight requests.
%TODO: Handle response chunking...

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Generic API Functions
-export([init/2]).
-export([version/1]).
-export([limit/2, limit/3]).
-export([can_produce/2]).
-export([produce/3]).
-export([consume/4]).
-export([handle_data/4]).
-export([continue/4]).

-export([iolist_chunk/2]).

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(DEFAULT_VER, 0).
-define(DEFAULT_MAX_CHUNK_SIZE, 65535).
-define(DEFAULT_MAX_MESSAGE_SIZE, 0).
-define(DEFAULT_MAX_CHUNK_COUNT, 0).
-define(MAX_DECODING_RETRIES, 4).

%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(inflight_input, {
    message_type :: opcua:message_type(),
    request_id :: opcua:request_id(),
    chunk_count :: pos_integer(),
    total_size :: pos_integer(),
    reversed_data :: [iodata()]
}).

-record(inflight_output, {
    message_type :: opcua:message_type(),
    request_id :: undefined | opcua:request_id(),
    data :: iodata()
}).

-record(state, {
    side                            :: server | client,
    ver = ?DEFAULT_VER              :: non_neg_integer(),
    buff = <<>>                     :: binary(),
    max_res_chunk_size = ?DEFAULT_MAX_CHUNK_SIZE    :: non_neg_integer(),
    max_req_chunk_size = ?DEFAULT_MAX_CHUNK_SIZE    :: non_neg_integer(),
    max_msg_size = ?DEFAULT_MAX_MESSAGE_SIZE        :: non_neg_integer(),
    max_chunk_count = ?DEFAULT_MAX_CHUNK_COUNT      :: non_neg_integer(),
    inflight_inputs = #{}           :: #{pos_integer() => #inflight_input{}},
    inflight_outputs = #{}          :: #{pos_integer() => #inflight_output{}},
    inflight_queue = queue:new()    :: queue:queue(),
    channel_mod                     :: module()
}).


%%% GENERIC API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Side, ChannelMod) when Side =:= client; Side =:= server ->
    {ok, #state{side = Side, channel_mod = ChannelMod}}.

version(#state{ver = V}) -> V.

limit(max_res_chunk_size, #state{max_res_chunk_size = V}) -> V;
limit(max_req_chunk_size, #state{max_req_chunk_size = V}) -> V;
limit(max_msg_size, #state{max_msg_size = V}) -> V;
limit(max_chunk_count, #state{max_chunk_count = V}) -> V.

limit(max_res_chunk_size, V, State) -> State#state{max_res_chunk_size = V};
limit(max_req_chunk_size, V, State) -> State#state{max_req_chunk_size = V};
limit(max_msg_size, V, State) -> State#state{max_msg_size = V};
limit(max_chunk_count, V, State) -> State#state{max_chunk_count = V}.

can_produce(_Conn, #state{inflight_outputs = Map}) -> maps:size(Map) > 0.

produce(Conn, Channel, #state{} = State) ->
    #state{inflight_outputs = Map, inflight_queue = Queue} = State,
    case queue:out(Queue) of
        {empty, Queue2} ->
            ?assertEqual(0, maps:size(Map)),
            {ok, Conn, Channel, State#state{inflight_queue = Queue2}};
        {{value, ReqId}, Queue2} ->
            case maps:take(ReqId, Map) of
                error ->
                    {ok, Conn, Channel, State#state{inflight_queue = Queue2}};
                {Inflight, Map2} ->
                    #inflight_output{
                        message_type = MsgType,
                        request_id = ReqId,
                        data = Data
                    } = Inflight,
                    State2 = State#state{inflight_outputs = Map2,
                                         inflight_queue = Queue2},
                    %TODO: Properly figure out the chunk overhead
                    MaxSize = limit(max_res_chunk_size, State2) - 1000,
                    {ChunkType, DataChunk, Conn4, Channel4, State4} =
                        case iolist_chunk(Data, MaxSize) of
                            {C, _Size, R} when R =:= <<>>; R =:= [] ->
                                {final, C, Conn, Channel, State2};
                            {C, _Size, R} ->
                                {ok, Conn2, Channel2, State3} =
                                    inflight_output_add(State2, Channel, Conn,
                                                        MsgType, ReqId, R),
                                {intermediate, C, Conn2, Channel2, State3}
                        end,
                    case produce_chunk(State4, Channel4, Conn4,
                                       MsgType, ChunkType, ReqId, DataChunk) of
                        {error, _Reason, _Channel5, _State5} = Error -> Error;
                        {ok, Chunk, Conn5, Channel5, State5} ->
                            Output = opcua_uacp_codec:encode_chunks(Conn, Chunk),
                            {ok, Output, Conn5, Channel5, State5}
                    end
            end
    end.

consume(#uacp_message{sender = Side, request_id = ReqId} = Msg,
        Conn, Channel, #state{side = Side} = State) ->
    #uacp_message{type = MsgType, node_id = NodeId, payload = Payload} = Msg,
    Data = opcua_uacp_codec:encode_payload(Conn, MsgType, NodeId, Payload),
    inflight_output_add(State, Channel, Conn, MsgType, ReqId, Data).

handle_data(Data, Conn, Channel, #state{buff = Buff} = State) ->
    %TODO: Handle decoding errors so it can be recoverable,
    %      updated buffer would be required to not keep decoding the
    %      same data all over again.
    TotalSize = byte_size(Data) + byte_size(Buff),
    %TODO: Figure out why being strict here just fails ? Maybe testing more clients ?
    case TotalSize > (State#state.max_req_chunk_size + 1000) of
        true ->
            {error, bad_encoding_limits_exceeded, Channel, State};
        false ->
            AllData = <<Buff/binary, Data/binary>>,
            {Chunks, Buff2} = opcua_uacp_codec:decode_chunks(Conn, AllData),
            State2 = State#state{buff = Buff2},
            handle_chunks(State2, Channel, Conn, Chunks)
    end.

continue({decode_payload, RetryCount, Msg, Data}, Conn, Channel, State) ->
    case decode_payload(State, Conn, RetryCount, Msg, Data) of
        {error, Reason} -> {error, Reason, Channel, State};
        {ok, Message, State2} -> {ok, [Message], [], Conn, Channel, State2};
        {issue, Issue, State2} -> {ok, [], [Issue], Conn, Channel, State2}
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

iolist_chunk(Data, Size) ->
    {RevChunk, RemSize, Rest} = iolist_chunk(Data, Size, []),
    {lists:reverse(RevChunk), Size - RemSize, Rest}.

iolist_chunk(Data, 0, Acc) ->
    {Acc, 0, Data};
iolist_chunk([], RemSize, Acc) ->
    {Acc, RemSize, []};
iolist_chunk(<<>>, RemSize, Acc) ->
    {Acc, RemSize, <<>>};
iolist_chunk([Int | Rest], RemSize, Acc) when is_integer(Int) ->
    iolist_chunk(Rest, RemSize - 1, [Int | Acc]);
iolist_chunk([Data | Rest], RemSize, Acc) when is_list(Data); is_binary(Data) ->
    case iolist_chunk(Data, RemSize, Acc) of
        {Acc2, 0, SubRest} ->
            iolist_chunk([SubRest | Rest], 0, Acc2);
        {Acc2, RemSize2, SubRest} when SubRest =:= <<>>; SubRest =:= [] ->
            iolist_chunk(Rest, RemSize2, Acc2)
    end;
iolist_chunk(Bin, RemSize, Acc) when is_binary(Bin) ->
    case byte_size(Bin) of
        BinSize when BinSize =< RemSize ->
            iolist_chunk(<<>>, RemSize - BinSize, [Bin | Acc]);
        _ ->
            <<Head:RemSize/binary, Tail/binary>> = Bin,
            iolist_chunk(Tail, 0, [Head | Acc])
    end.

peer_side(#state{side = client}) -> server;
peer_side(#state{side = server}) -> client.

produce_chunk(State, Channel, Conn, MsgType, ChunkType, undefined, Data)
  when MsgType =:= hello, ChunkType =:= final;
       MsgType =:= acknowledge, ChunkType =:= final;
       MsgType =:= error, ChunkType =:= final ->
    {ok, #uacp_chunk{
        message_type = MsgType,
        chunk_type = ChunkType,
        body = Data
    }, Conn, Channel, State};
produce_chunk(State, Channel, Conn,
              MsgType, ChunkType, ReqId, Data)
  when MsgType =:= channel_open, ChunkType =:= final;
       MsgType =:= channel_close, ChunkType =:= final;
       MsgType =:= channel_message ->
    Chunk = #uacp_chunk{
        message_type = MsgType,
        chunk_type = ChunkType,
        state = unlocked,
        request_id = ReqId,
        body = Data
    },
    channel_lock(State, Channel, Conn, Chunk).

handle_chunks(State, Channel, Conn, Chunks) ->
    handle_chunks(State, Channel, Conn, Chunks, [], []).

handle_chunks(State, Channel, Conn, [], MAcc, IAcc) ->
    {ok, lists:reverse(MAcc), lists:reverse(IAcc), Conn, Channel, State};
handle_chunks(State, Channel, Conn, [Chunk | Rest], MAcc, IAcc) ->
    %TODO: handle errors so non-fatal errors do not break the chunk processing
    case handle_chunk(State, Channel, Conn, Chunk) of
        {error, _Reason, _Channel2, _State2} = Error -> Error;
        {ok, Conn2, Channel2, State2} ->
            handle_chunks(State2, Channel2, Conn2, Rest, MAcc, IAcc);
        {ok, Msg, Conn2, Channel2, State2} ->
            handle_chunks(State2, Channel2, Conn2, Rest, [Msg | MAcc], IAcc);
        {issue, Issue, Conn2, Channel2, State2} ->
            handle_chunks(State2, Channel2, Conn2, Rest, MAcc, [Issue | IAcc])
    end.

handle_chunk(State, Channel, Conn, #uacp_chunk{state = undefined} = Chunk) ->
    handle_unlocked_chunk(State, Channel, Conn, Chunk);
handle_chunk(State, Channel, Conn, #uacp_chunk{state = locked} = Chunk) ->
    case channel_unlock(State, Channel, Conn, Chunk) of
        {error, _Reason, _Channel2, _State2} = Error -> Error;
        {ok, Chunk2, Conn2, Channel2, State2} ->
            handle_unlocked_chunk(State2, Channel2, Conn2, Chunk2)
    end.

handle_unlocked_chunk(State, Channel, Conn, Chunk) ->
    case inflight_input_update(State, Conn, Chunk) of
        {error, Reason} ->
            {error, Reason, Conn, Channel, State};
        {ok, State2} ->
            {ok, Conn, Channel, State2};
        {ok, Message, State2} ->
            {ok, Message, Conn, Channel, State2};
        {issue, Issue, State2} ->
            {issue, Issue, Conn, Channel, State2}
    end.

inflight_output_add(State, Channel, Conn, MsgType, ReqId, Data) ->
    #state{inflight_outputs = ResMap, inflight_queue = ResQueue} = State,
    Inflight = #inflight_output{
        message_type = MsgType,
        request_id = ReqId,
        data = Data
    },
    ?assert(not maps:is_key(ReqId, ResMap)),
    ResMap2 = maps:put(ReqId, Inflight, ResMap),
    ResQueue2 = queue:in(ReqId, ResQueue),
    State2 = State#state{inflight_outputs = ResMap2, inflight_queue = ResQueue2},
    {ok, Conn, Channel, State2}.

%TODO: Enforce that channel_open messages cannot be chunked.
inflight_input_update(State, Conn, #uacp_chunk{chunk_type = aborted} = Chunk) ->
    #state{inflight_inputs = ReqMap} = State,
    #uacp_chunk{request_id = ReqId, body = Body} = Chunk,
    case maps:take(ReqId, ReqMap) of
        error ->
            #{error := Error, reason := Reason} =
                opcua_uacp_codec:decode_error(Conn, Body),
            ?LOG_WARNING("Unknown request ~w aborted: ~s (~w)",
                         [ReqId, Reason, Error]),
            {ok, State};
        {_Inflight, ReqMap2} ->
            #{error := Error, reason := Reason} =
                opcua_uacp_codec:decode_error(Conn, Body),
            ?LOG_WARNING("Request ~w aborted: ~s (~w)",
                         [ReqId, Reason, Error]),
            {ok, State#state{inflight_inputs = ReqMap2}}
    end;
inflight_input_update(State, _Conn, #uacp_chunk{chunk_type = intermediate} = Chunk) ->
    #state{inflight_inputs = ReqMap} = State,
    #uacp_chunk{message_type = MsgType, request_id = ReqId, body = Body} = Chunk,
    case maps:find(ReqId, ReqMap) of
        error ->
            %TODO: Check for maximum number of inflight requests
            Inflight = #inflight_input{
                message_type = MsgType,
                request_id = ReqId,
                chunk_count = 1,
                total_size = iolist_size(Body),
                reversed_data = [Body]
            },
            ReqMap2 = maps:put(ReqId, Inflight, ReqMap),
            {ok, State#state{inflight_inputs = ReqMap2}};
        {ok, #inflight_input{message_type = MsgType} = Inflight} ->
            %TODO: Check for maximum number of chunks and maximum message size
            #inflight_input{
                chunk_count = Count,
                total_size = Size,
                reversed_data = Data
            } = Inflight,
            Inflight2 = Inflight#inflight_input{
                chunk_count = Count + 1,
                total_size = Size + iolist_size(Body),
                reversed_data = [Body | Data]
            },
            ReqMap2 = maps:put(ReqId, Inflight2, ReqMap),
            {ok, State#state{inflight_inputs = ReqMap2}}
    end;
inflight_input_update(State, Conn, #uacp_chunk{chunk_type = final} = Chunk) ->
    #state{inflight_inputs = ReqMap} = State,
    #uacp_chunk{message_type = MsgType, request_id = ReqId, body = Body} = Chunk,
    {Inflight2, State2} = case maps:take(ReqId, ReqMap) of
        error ->
            {#inflight_input{
                message_type = MsgType,
                request_id = ReqId,
                chunk_count = 1,
                total_size = iolist_size(Body),
                reversed_data = [Body]
            }, State};
        {#inflight_input{message_type = MsgType} = Inflight, ReqMap2} ->
            #inflight_input{
                chunk_count = Count,
                total_size = Size,
                reversed_data = Data
            } = Inflight,
            {Inflight#inflight_input{
                chunk_count = Count + 1,
                total_size = Size + iolist_size(Body),
                reversed_data = [Body | Data]
            }, State#state{inflight_inputs = ReqMap2}}
    end,
    #inflight_input{reversed_data = FinalReversedData} = Inflight2,
    FinalData = lists:reverse(FinalReversedData),
    Msg = #uacp_message{
        type = MsgType,
        sender = peer_side(State),
        request_id = ReqId
    },
    decode_payload(State2, Conn, 0, Msg, FinalData).

decode_payload(_State, _Conn, MaxRetryCount, _Msg, _Data)
  when MaxRetryCount > ?MAX_DECODING_RETRIES ->
    {error, decoding_retries_exhausted};
decode_payload(State, Conn, RetryCount,
               #uacp_message{type = MsgType} = Msg, Data) ->
    case opcua_uacp_codec:decode_payload(Conn, MsgType, Data) of
        {ok, NodeId, Payload} ->
            {ok, Msg#uacp_message{node_id = NodeId, payload = Payload}, State};
        {schema_not_found, NodeId, Payload, Schemas} ->
            PartialMsg = Msg#uacp_message{node_id = NodeId, payload = Payload},
            ContData = {decode_payload, RetryCount + 1, Msg, Data},
            Issue = {schema_not_found, PartialMsg, Schemas, ContData},
            {issue, Issue, State}
    end.

%== Channel Module Abstraction Functions =======================================

channel_lock(#state{channel_mod = Mod} = State, Channel, Conn, Chunk) ->
    case Mod:lock(Chunk, Conn, Channel) of
        {error, Reason} -> {error, Reason, Channel, State};
        {ok, Chunk2, Conn2, Channel2} -> {ok, Chunk2, Conn2, Channel2, State}
    end.

channel_unlock(#state{channel_mod = Mod} = State, Channel, Conn, Chunk) ->
    case Mod:unlock(Chunk, Conn, Channel) of
        {error, Reason} -> {error, Reason, Channel, State};
        {ok, Chunk2, Conn2, Channel2} -> {ok, Chunk2, Conn2, Channel2, State}
    end.
