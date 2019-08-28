-module(opcua_uacp).

%TODO: Split the channel handling to its own process, if resuming a channel
%      is required.
%TODO: Check maximum number of inflight requests.
%TODO: Handle response chunking...

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([new/3]).
-export([has_chunk/2]).
-export([next_chunk/2]).
-export([send/2]).
-export([handle_data/3]).
-export([terminate/3]).

%%% BEHAVIOUR opcua_uacp DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-callback init(Opts :: map()) -> State :: term().
-callback channel_allocate(term())
    -> {ok, opcua:channel_id(), State :: term()}
     | {error, Reason :: term()}.
-callback channel_release(opcua:channel_id(), term())
    -> {ok, State :: term()}
     | {error, Reason :: term()}.

-optional_callbacks([
    channel_allocate/1,
    channel_release/2
]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER_VER, 0).
-define(DEFAULT_MAX_CHUNK_SIZE, 65535).
-define(DEFAULT_MAX_MESSAGE_SIZE, 0).
-define(DEFAULT_MAX_CHUNK_COUNT, 0).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(inflight_request, {
    message_type :: opcua:message_type(),
    request_id :: opcua:request_id(),
    chunk_count :: pos_integer(),
    total_size :: pos_integer(),
    reversed_data :: [iodata()]
}).

-record(inflight_response, {
    message_type :: opcua:message_type(),
    request_id :: undefined | opcua:request_id(),
    data :: iodata()
}).

-record(state, {
    side                            :: server | client,
    buff = <<>>                     :: binary(),
    channel_id                      :: undefined | pos_integer(),
    max_res_chunk_size = ?DEFAULT_MAX_CHUNK_SIZE    :: non_neg_integer(),
    max_req_chunk_size = ?DEFAULT_MAX_CHUNK_SIZE    :: non_neg_integer(),
    max_msg_size = ?DEFAULT_MAX_MESSAGE_SIZE        :: non_neg_integer(),
    max_chunk_count = ?DEFAULT_MAX_CHUNK_COUNT      :: non_neg_integer(),
    client_ver                      :: undefined | pos_integer(),
    endpoint_url                    :: undefined | binary(),
    inflight_requests = #{}         :: #{pos_integer() => #inflight_request{}},
    inflight_responses = #{}        :: #{pos_integer() => #inflight_response{}},
    inflight_queue = queue:new()    :: queue:queue(),
    curr_token_id                   :: undefined | pos_integer(),
    temp_token_id                   :: undefined | pos_integer(),
    curr_sec                        :: term(),
    temp_sec                        :: term(),
    mod                             :: module(),
    sub                             :: term()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Side, Mod, Opts) when Side =:= client; Side =:= server ->
    case Mod:init(Opts) of
        {ok, Sub} ->  {ok, #state{side = Side, mod = Mod, sub = Sub}};
        {error, _Reason} = Error -> Error
    end.

has_chunk(_Conn, #state{inflight_responses = Map}) -> maps:size(Map) > 0.

next_chunk(_conn, #state{} = State) ->
    #state{inflight_responses = Map, inflight_queue = Queue} = State,
    case queue:out(Queue) of
        {empty, Queue2} ->
            ?assertEqual(0, maps:size(Map)),
            {ok, State#state{inflight_queue = Queue2}};
        {{value, ReqId}, Queue2} ->
            case maps:take(ReqId, Map) of
                error -> {ok, State#state{inflight_queue = Queue2}};
                {Inflight, Map2} ->
                    State2 = State#state{inflight_responses = Map2,
                                         inflight_queue = Queue2},
                    %TODO: Add support for chunking the output messages
                    #inflight_response{
                        message_type = MsgType,
                        request_id = ReqId,
                        data = Data
                    } = Inflight,
                    case produce_chunk(State2, MsgType, final, ReqId, Data) of
                        {error, _Reason, _State3} = Error -> Error;
                        {ok, Chunk, State3} ->
                            Output = opcua_uacp_codec:encode_chunks(Chunk),
                            {ok, Output, State3}
                    end
            end
    end.

send(#uacp_message{type = MsgType} = Msg, State)
  when MsgType =:= acknowledge; MsgType =:= error ->
    #uacp_message{payload = Payload} = Msg,
    Data = encode_basic_payload(MsgType, Payload),
    inflight_response_add(State, MsgType, undefined, Data);
send(#uacp_message{type = MsgType} = Msg, State)
  when MsgType =:= channel_open; MsgType =:= channel_close; MsgType =:= channel_message ->
    #uacp_message{request_id = ReqId, node_id = NodeId, payload = Payload} = Msg,
    Data = opcua_uacp_codec:encode_object(NodeId, Payload),
    inflight_response_add(State, MsgType, ReqId, Data).

handle_data(Data, Conn, #state{buff = Buff} = State) ->
    %TODO: Handle decoding errors so it can be recoverable,
    %      updated buffer would be required to not keep decoding the
    %      same data all over again.
    TotalSize = byte_size(Data) + byte_size(Buff),
    case TotalSize > State#state.max_req_chunk_size of
        true -> {error, bad_encoding_limits_exceeded, State};
        false ->
            AllData = <<Buff/binary, Data/binary>>,
            {Chunks, Buff2} = opcua_uacp_codec:decode_chunks(AllData),
            State2 = State#state{buff = Buff2},
            case handle_chunks(State2, Conn, Chunks) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, _Msgs, _State3} = Result -> Result
            end
    end.

terminate(_Reason, _Conn, State) ->
    channel_release(State),
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_chunks(State, Conn, Chunks) ->
    handle_chunks(State, Conn, Chunks, []).

handle_chunks(State, _Conn, [], Acc) ->
    {ok, Acc, State};
handle_chunks(State, Conn, [Chunk | Rest], Acc) ->
    %TODO: handle errors so non-fatal errors do not break the chunk processing
    case handle_chunk(State, Conn, Chunk) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} -> handle_chunks(State2, Conn, Rest, Acc);
        {ok, Msg, State2} -> handle_chunks(State2, Conn, Rest, [Msg | Acc])
    end.

handle_chunk(State, Conn,
             #uacp_chunk{state = undefined, message_type = MsgType,
                         chunk_type = final, body = Body}) ->
    Payload = decode_basic_payload(MsgType, Body),
    Message = #uacp_message{type = MsgType, payload = Payload},
    handle_message(State, Conn, Message);
handle_chunk(State, Conn, #uacp_chunk{state = locked} = Chunk) ->
    #uacp_chunk{message_type = MsgType, channel_id = ChannelId} = Chunk,
    channel_validate(State, MsgType, ChannelId),
    handle_locked_chunk(State, Conn, Chunk).

handle_locked_chunk(#state{channel_id = undefined} = State, Conn,
                    #uacp_chunk{message_type = channel_open,
                                chunk_type = final} = Chunk) ->
    case channel_allocate(State) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} -> handle_open_chunk(State2, Conn, Chunk)
    end;
handle_locked_chunk(State, Conn,
                    #uacp_chunk{message_type = channel_open,
                                chunk_type = final} = Chunk) ->
    handle_open_chunk(State, Conn, Chunk);
handle_locked_chunk(State, Conn,
                    #uacp_chunk{message_type = channel_close,
                                chunk_type = final} = Chunk) ->
    handle_close_chunk(State, Conn, Chunk);
handle_locked_chunk(State, Conn,
                    #uacp_chunk{message_type = channel_message} = Chunk) ->
    handle_message_chunk(State, Conn, Chunk).

handle_open_chunk(State, Conn, #uacp_chunk{security = SecPolicy} = Chunk) ->
    case security_init(State, SecPolicy) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} ->
            case security_asym_unlock(State2, Chunk) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, Chunk2, State3} ->
                    #uacp_chunk{
                        request_id = RequestId,
                        body = Body
                    } = Chunk2,
                    {NodeId, Payload} =
                        opcua_uacp_codec:decode_object(Body),
                    Message = #uacp_message{
                        type = channel_open,
                        request_id = RequestId,
                        node_id = NodeId,
                        payload = Payload
                    },
                    handle_message(State3, Conn, Message)
            end
    end.

handle_close_chunk(State, Conn, Chunk) ->
    case security_sym_unlock(State, Chunk) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Chunk2, State2} ->
            #uacp_chunk{
                request_id = RequestId,
                body = Body
            } = Chunk2,
            {NodeId, Payload} =
                opcua_uacp_codec:decode_object(Body),
            Message = #uacp_message{
                type = channel_close,
                request_id = RequestId,
                node_id = NodeId,
                payload = Payload
            },
            handle_message(State2, Conn, Message)
    end.

handle_message(State, _Conn,
               #uacp_message{type = hello, payload = Msg}) ->
    #state{
        max_res_chunk_size = ServerMaxResChunkSize,
        max_req_chunk_size = ServerMaxReqChunkSize,
        max_msg_size = ServerMaxMsgSize,
        max_chunk_count = ServerMaxChunkCount
    } = State,
    #{
        ver := ClientVersion,
        max_res_chunk_size := ClientMaxResChunkSize,
        max_req_chunk_size := ClientMaxReqChunkSize,
        max_msg_size := ClientMaxMsgSize,
        max_chunk_count := ClientMaxChunkCount,
        endpoint_url := EndPointUrl
    } = Msg,
    MaxResChunkSize = negotiate_limit(ServerMaxResChunkSize, ClientMaxResChunkSize),
    MaxReqChunkSize = negotiate_limit(ServerMaxReqChunkSize, ClientMaxReqChunkSize),
    MaxMsgSize = negotiate_limit(ServerMaxMsgSize, ClientMaxMsgSize),
    MaxChunkCount = negotiate_limit(ServerMaxChunkCount, ClientMaxChunkCount),
    ?LOG_INFO("Negociated protocol configuration: client_ver=~w, server_ver=~w, "
              "max_res_chunk_size=~w, max_req_chunk_size=~w, max_msg_size=~w, "
              "max_chunk_count=~w, endpoint_url=~p" ,
              [ClientVersion, ?SERVER_VER, MaxResChunkSize, MaxReqChunkSize,
               MaxMsgSize, MaxChunkCount, EndPointUrl]),
    State2 = State#state{
        client_ver = ClientVersion,
        max_res_chunk_size = MaxResChunkSize,
        max_req_chunk_size = MaxReqChunkSize,
        max_msg_size = MaxMsgSize,
        max_chunk_count = MaxChunkCount,
        endpoint_url = EndPointUrl
    },
    Response = #uacp_message{
        type = acknowledge,
        payload = #{
            ver => ?SERVER_VER,
            max_res_chunk_size => MaxResChunkSize,
            max_req_chunk_size => MaxReqChunkSize,
            max_msg_size => MaxMsgSize,
            max_chunk_count => MaxChunkCount
        }
    },
    send(Response, State2);
handle_message(State, _Conn, #uacp_message{type = error} = Msg) ->
    {ok, Msg, State};
handle_message(State, Conn, #uacp_message{type = channel_open} = Request) ->
    case security_open(State, Conn, Request) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Resp, State2} -> send(Resp, State2)
    end;
handle_message(State, Conn, #uacp_message{type = channel_close} = Request) ->
    case security_close(State, Conn, Request) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Resp, State2} -> send(Resp, State2)
    end;
handle_message(State, _Conn, #uacp_message{type = channel_message} = Msg) ->
    {ok, Msg, State}.

handle_message_chunk(State, _Conn, Chunk) ->
    case security_sym_unlock(State, Chunk) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Chunk2, State2} ->
            case inflight_request_update(State2, Chunk2) of
                {ok, State3} -> {ok, State3};
                {ok, Message, State3} -> {ok, Message, State3}
            end
    end.

inflight_request_update(State, #uacp_chunk{chunk_type = aborted} = Chunk) ->
    #state{inflight_requests = ReqMap} = State,
    #uacp_chunk{request_id = ReqId, body = Body} = Chunk,
    case maps:take(ReqId, ReqMap) of
        error ->
            #{error := Error, reason := Reason} =
                opcua_uacp_codec:decode_error(Body),
            ?LOG_WARNING("Unknown request ~w aborted: ~s (~w)",
                         [ReqId, Reason, Error]),
            {ok, State};
        {_Inflight, ReqMap2} ->
            #{error := Error, reason := Reason} =
                opcua_uacp_codec:decode_error(Body),
            ?LOG_WARNING("Request ~w aborted: ~s (~w)",
                         [ReqId, Reason, Error]),
            {ok, State#state{inflight_requests = ReqMap2}}
    end;
inflight_request_update(State, #uacp_chunk{chunk_type = intermediate} = Chunk) ->
    #state{inflight_requests = ReqMap} = State,
    #uacp_chunk{message_type = MsgType, request_id = ReqId, body = Body} = Chunk,
    case maps:find(ReqId, ReqMap) of
        error ->
            %TODO: Check for maximum number of inflight requests
            Inflight = #inflight_request{
                message_type = MsgType,
                request_id = ReqId,
                chunk_count = 1,
                total_size = iolist_size(Body),
                reversed_data = [Body]
            },
            ReqMap2 = maps:put(ReqId, Inflight, ReqMap),
            {ok, State#state{inflight_requests = ReqMap2}};
        {ok, #inflight_request{message_type = MsgType} = Inflight} ->
            %TODO: Check for maximum number of chunks and maximum message size
            #inflight_request{
                chunk_count = Count,
                total_size = Size,
                reversed_data = Data
            } = Inflight,
            Inflight2 = Inflight#inflight_request{
                chunk_count = Count + 1,
                total_size = Size + iolist_size(Body),
                reversed_data = [Body | Data]
            },
            ReqMap2 = maps:put(ReqId, Inflight2, ReqMap),
            {ok, State#state{inflight_requests = ReqMap2}}
    end;
inflight_request_update(State, #uacp_chunk{chunk_type = final} = Chunk) ->
    #state{inflight_requests = ReqMap} = State,
    #uacp_chunk{message_type = MsgType, request_id = ReqId, body = Body} = Chunk,
    {Inflight2, State2} = case maps:take(ReqId, ReqMap) of
        error ->
            {#inflight_request{
                message_type = MsgType,
                request_id = ReqId,
                chunk_count = 1,
                total_size = iolist_size(Body),
                reversed_data = [Body]
            }, State};
        {#inflight_request{message_type = MsgType} = Inflight, ReqMap2} ->
            #inflight_request{
                chunk_count = Count,
                total_size = Size,
                reversed_data = Data
            } = Inflight,
            {Inflight#inflight_request{
                chunk_count = Count + 1,
                total_size = Size + iolist_size(Body),
                reversed_data = [Body | Data]
            }, State#state{inflight_requests = ReqMap2}}
    end,
    #inflight_request{reversed_data = FinalReversedData} = Inflight2,
    FinalData = lists:reverse(FinalReversedData),
    {NodeId, Payload} = opcua_uacp_codec:decode_object(FinalData),
    Message = #uacp_message{
        type = MsgType,
        request_id = ReqId,
        node_id = NodeId,
        payload = Payload
    },
    {ok, Message, State2}.


inflight_response_add(State, MsgType, ReqId, Data) ->
    #state{inflight_responses = ResMap, inflight_queue = ResQueue} = State,
    Inflight = #inflight_response{
        message_type = MsgType,
        request_id = ReqId,
        data = Data
    },
    ?assert(not maps:is_key(ReqId, ResMap)),
    ResMap2 = maps:put(ReqId, Inflight, ResMap),
    ResQueue2 = queue:in(ReqId, ResQueue),
    {ok, State#state{inflight_responses = ResMap2, inflight_queue = ResQueue2}}.


produce_chunk(State, MsgType, ChunkType, undefined, Data)
  when MsgType =:= acknowledge, ChunkType =:= final;
       MsgType =:= error, ChunkType =:= final ->
    {ok, #uacp_chunk{
        message_type = MsgType,
        chunk_type = ChunkType,
        body = Data
    }, State};
produce_chunk(#state{channel_id = ChannelId} = State,
              MsgType, ChunkType, ReqId, Data)
  when ChannelId =/= undefined, MsgType =:= channel_open, ChunkType =:= final;
       ChannelId =/= undefined, MsgType =:= channel_close, ChunkType =:= final;
       ChannelId =/= undefined, MsgType =:= channel_message ->
    Chunk = #uacp_chunk{
        message_type = MsgType,
        chunk_type = ChunkType,
        channel_id = ChannelId,
        request_id = ReqId,
        body = Data
    },
    case setup_chunk_security(State, Chunk) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Chunk2, State2} ->
            Chunk3 = opcua_uacp_codec:prepare_chunks(Chunk2),
            case security_prepare(State2, Chunk3) of
                {error, _Reason, _State2} = Error -> Error;
                {ok, Chunk4, State3} ->
                    Chunk5 = opcua_uacp_codec:freeze_chunks(Chunk4),
                    security_lock(State3, Chunk5)
            end
    end.

setup_chunk_security(State, #uacp_chunk{message_type = channel_open} = Chunk) ->
    security_setup_asym(State, Chunk);
setup_chunk_security(State, Chunk) ->
    security_setup_sym(State, Chunk).

decode_basic_payload(hello, Body) ->
    opcua_uacp_codec:decode_hello(Body);
decode_basic_payload(error, Body) ->
    opcua_uacp_codec:decode_error(Body).

encode_basic_payload(acknowledge, Payload) ->
    opcua_uacp_codec:encode_acknowledge(Payload);
encode_basic_payload(error, Payload) ->
    opcua_uacp_codec:encode_error(Payload).

channel_validate(#state{channel_id = undefined}, channel_open, 0) -> ok;
channel_validate(#state{channel_id = ChannelId}, _MsgType, ChannelId) -> ok;
channel_validate(_State, _MsgType, _ChannelId) ->
    throw(bad_tcp_secure_channel_unknown).

negotiate_limit(0, B) when is_integer(B), B >= 0 -> B;
negotiate_limit(A, 0) when is_integer(A), A > 0-> A;
negotiate_limit(A, B) when is_integer(A), A > 0, is_integer(B), B > 0 -> min(A, B).


%== Security Module Abstraction Functions ======================================

security_init(#state{channel_id = ChannelId, temp_token_id = undefined} = State,
              SecurityPolicy)
  when ChannelId =/= undefined ->
    #state{curr_sec = Sub, curr_token_id = TokenId} = State,
    case opcua_security:init(ChannelId, SecurityPolicy, Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, NewTokenId, NewSub} ->
            {ok, State#state{
                curr_token_id = NewTokenId,
                curr_sec = NewSub,
                temp_token_id = TokenId,
                temp_sec = Sub
            }}
    end;
security_init(State, _SecurityPolicy) ->
    % If we already have two valid token ids, we must reject any new one
    {error, bad_security_policy_rejected, State}.

security_asym_unlock(#state{curr_sec = Sub} = State, Chunk) ->
    case opcua_security:unlock(Chunk, Sub) of
        {error, Reason} -> {error, Reason, State};
        {expired, _Sub2} -> {error, bad_internal_error, State};
        {ok, Chunk2, Sub2} -> {ok, Chunk2, State#state{curr_sec = Sub2}}
    end.

security_sym_unlock(#state{curr_token_id = Token, temp_token_id = undefined} = State,
                    #uacp_chunk{security = Token} = Chunk) ->
    #state{curr_sec = Sub} = State,
    case opcua_security:unlock(Chunk, Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, Chunk2, Sub2} ->
            {ok, Chunk2, State#state{curr_sec = Sub2}};
        {expired, _Sub2} ->
            opcua_security:cleanup(Sub),
            State2 = State#state{curr_token_id = undefined, curr_sec = undefined},
            {error, bad_secure_channel_token_unknown, State2}
    end;
security_sym_unlock(#state{curr_token_id = Token} = State,
                    #uacp_chunk{security = Token} = Chunk) ->
    #state{curr_sec = Sub, temp_sec = TempSub} = State,
    opcua_security:cleanup(TempSub),
    State2 = State#state{temp_token_id = undefined, temp_sec = undefined},
    case opcua_security:unlock(Chunk, Sub) of
        {error, Reason} -> {error, Reason, State2};
        {ok, Chunk2, Sub2} ->
            {ok, Chunk2, State2#state{curr_sec = Sub2}};
        {expired, Sub2} ->
            opcua_security:cleanup(Sub2),
            State3 = State2#state{curr_token_id = undefined, curr_sec = undefined},
            {error, bad_secure_channel_token_unknown, State3}
    end;
security_sym_unlock(#state{temp_token_id = Token, temp_sec = Sub} = State,
                    #uacp_chunk{security = Token} = Chunk) ->
    case opcua_security:unlock(Chunk, Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, Chunk2, Sub2} ->
            {ok, Chunk2, State#state{temp_sec = Sub2}};
        {expired, Sub2} ->
            opcua_security:cleanup(Sub2),
            State2 = State#state{temp_token_id = undefined, temp_sec = undefined},
            {error, bad_secure_channel_token_unknown, State2}
    end;
security_sym_unlock(State, _Chunk) ->
    {error, bad_secure_channel_token_unknown, State}.

security_open(#state{curr_token_id = Tok} = State, Conn, Req)
  when Tok =/= undefined ->
    #state{curr_sec = Sub} = State,
    case opcua_security:open(Conn, Req, Sub) of
        {error, Reason} ->
            State2 = State#state{curr_token_id = undefined, curr_sec = undefined},
            {error, Reason, State2};
        {ok, Resp, Sub2} ->
            {ok, Resp, State#state{curr_sec = Sub2}}
    end.

security_setup_asym(#state{curr_sec = Sub} = State, Chunk) ->
    case opcua_security:setup_asym(Chunk, Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, Chunk2, Sub2} -> {ok, Chunk2, State#state{curr_sec = Sub2}}
    end.

security_setup_sym(#state{curr_sec = Sub} = State, Chunk) ->
    case opcua_security:setup_sym(Chunk, Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, Chunk2, Sub2} -> {ok, Chunk2, State#state{curr_sec = Sub2}}
    end.

security_prepare(#state{curr_sec = Sub} = State, Chunk) ->
    case opcua_security:prepare(Chunk, Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, Chunk2, Sub2} -> {ok, Chunk2, State#state{curr_sec = Sub2}}
    end.

security_lock(#state{curr_sec = Sub} = State, Chunk) ->
    case opcua_security:lock(Chunk, Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, Chunk2, Sub2} -> {ok, Chunk2, State#state{curr_sec = Sub2}}
    end.

security_close(#state{curr_token_id = Tok} = State, Conn, Req)
  when Tok =/= undefined ->
    #state{curr_sec = Sub} = State,
    case opcua_security:close(Conn, Req, Sub) of
        {error, Reason} ->
            State2 = State#state{curr_token_id = undefined, curr_sec = undefined},
            {error, Reason, State2};
        {ok, Resp, Sub2} ->
            {ok, Resp, State#state{curr_sec = Sub2}}
    end.


%== Callback Module Abstraction Functions ======================================

channel_allocate(#state{channel_id = undefined, mod = Mod, sub = Sub} = State) ->
    case Mod:channel_allocate(Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, ChannelId, Sub2} ->
            {ok, State#state{channel_id = ChannelId, sub = Sub2}}
    end.

channel_release(#state{channel_id = undefined} = State) -> State;
channel_release(#state{channel_id = ChannelId, mod = Mod, sub = Sub} = State) ->
    case Mod:channel_release(ChannelId, Sub) of
        {error, Reason} -> {error, Reason, State};
        {ok, Sub2} ->
            {ok, State#state{channel_id = undefined, sub = Sub2}}
    end.
