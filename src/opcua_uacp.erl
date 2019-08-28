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
-export([new_server/1]).
-export([has_chunk/2]).
-export([next_chunk/2]).
-export([handle_data/3]).
-export([terminate/3]).


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
    sess                            :: undefined | pid()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new_server(_Opts) ->
    {ok, #state{}}.

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
                        {error, Reason, State3} -> {stop, Reason, State3};
                        {ok, Chunk, State3} ->
                            Output = opcua_uacp_codec:encode_chunks(Chunk),
                            {ok, Output, State3}
                    end
            end
    end.

handle_data(Data, Conn, #state{buff = Buff} = State) ->
    %TODO: Handle decoding errors so it can be recoverable,
    %      updated buffer would be required to not keep decoding the
    %      same data all over again.
    TotalSize = byte_size(Data) + byte_size(Buff),
    case TotalSize > State#state.max_req_chunk_size of
        true -> {stop, bad_encoding_limits_exceeded, State};
        false ->
            AllData = <<Buff/binary, Data/binary>>,
            {Chunks, Buff2} = opcua_uacp_codec:decode_chunks(AllData),
            State2 = State#state{buff = Buff2},
            case handle_chunks(State2, Conn, Chunks) of
                {error, Reason, State3} -> {stop, Reason, State3};
                {ok, State3} -> {ok, State3}
            end
    end.

terminate(_Reason, _Conn, State) ->
    channel_release(State),
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_chunks(State, _Conn, []) -> {ok, State};
handle_chunks(State, Conn, [Chunk | Rest]) ->
    %TODO: handle errors so non-fatal errors do not break the chunk processing
    case handle_chunk(State, Conn, Chunk) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} -> handle_chunks(State2, Conn, Rest)
    end.

handle_chunk(State, Conn,
             #uacp_chunk{state = undefined, message_type = MsgType,
                         chunk_type = final, body = Body}) ->
    Message = decode_basic_payload(MsgType, Body),
    Request = #uacp_message{type = MsgType, payload = Message},
    ?LOG_DEBUG("Handling request ~p", [Request]),
    handle_request(State, Conn, Request);
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
                    Request = #uacp_message{
                        type = channel_open,
                        request_id = RequestId,
                        node_id = NodeId,
                        payload = Payload
                    },
                    ?LOG_DEBUG("Handling request ~p", [Request]),
                    handle_request(State3, Conn, Request)
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
            Request = #uacp_message{
                type = channel_close,
                request_id = RequestId,
                node_id = NodeId,
                payload = Payload
            },
            ?LOG_DEBUG("Handling request ~p", [Request]),
            handle_request(State2, Conn, Request)
    end.

handle_message_chunk(State, Conn, Chunk) ->
    case security_sym_unlock(State, Chunk) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Chunk2, State2} ->
            case inflight_request_update(State2, Chunk2) of
                {ok, State3} -> {ok, State3};
                {ok, Request, State3} ->
                    ?LOG_DEBUG("Handling request ~p", [Request]),
                    handle_request(State3, Conn, Request)

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
    Request = #uacp_message{
        type = MsgType,
        request_id = ReqId,
        node_id = NodeId,
        payload = Payload
    },
    {ok, Request, State2}.

handle_request(State, _Conn, #uacp_message{type = hello, payload = Msg}) ->
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
    MaxResChunkSize = negotiate_min(ServerMaxResChunkSize, ClientMaxResChunkSize),
    MaxReqChunkSize = negotiate_min(ServerMaxReqChunkSize, ClientMaxReqChunkSize),
    MaxMsgSize = negotiate_min(ServerMaxMsgSize, ClientMaxMsgSize),
    MaxChunkCount = negotiate_min(ServerMaxChunkCount, ClientMaxChunkCount),
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
    handle_response(State2, Response);
handle_request(State, _Conn, #uacp_message{type = error, payload = Msg}) ->
    #{error := Error, reason := Reason} = Msg,
    ?LOG_ERROR("Received client error ~w: ~s", [Error, Reason]),
    {ok, State};
handle_request(State, Conn, #uacp_message{type = channel_open} = Request) ->
    case security_open(State, Conn, Request) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Resp, State2} -> handle_response(State2, Resp)
    end;
handle_request(State, Conn, #uacp_message{type = channel_close} = Request) ->
    case security_close(State, Conn, Request) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Resp, State2} -> handle_response(State2, Resp)
    end;
handle_request(#state{sess = Sess} = State, Conn,
               #uacp_message{type = channel_message} = Request) ->
    #uacp_message{node_id = NodeSpec} = Request,
    %TODO: figure a way to no hardcode the ids...
    NodeId = opcua_database:lookup_id(NodeSpec),
    Request2 = Request#uacp_message{node_id = NodeId},
    Result = case {Sess, NodeId} of
        {_, #opcua_node_id{value = 426}} -> %% GetEndpoints
            opcua_discovery:handle_request(Conn, Request2);
        {undefined, _} ->
            opcua_session_manager:handle_request(Conn, Request2);
        {Session, _} ->
            opcua_session:handle_request(Conn, Request2, Session)
    end,
    case Result of
        {error, Reason} ->
            {error, Reason, State};
        {created, Resp, _SessPid} ->
            handle_response(State, Resp);
        {bound, Resp, SessPid} ->
            handle_response(State#state{sess = SessPid}, Resp);
        {closed, Resp} ->
            handle_response(State#state{sess = undefined}, Resp);
        {reply, Resp} ->
            handle_response(State, Resp)
    end.

handle_response(State, #uacp_message{type = MsgType} = Response)
  when MsgType =:= acknowledge; MsgType =:= error ->
    #uacp_message{payload = Payload} = Response,
    Data = encode_basic_payload(MsgType, Payload),
    inflight_response_add(State, MsgType, undefined, Data);
handle_response(State, #uacp_message{type = MsgType} = Response)
  when MsgType =:= channel_open; MsgType =:= channel_close; MsgType =:= channel_message ->
    #uacp_message{request_id = ReqId, node_id = NodeId, payload = Payload} = Response,
    Data = opcua_uacp_codec:encode_object(NodeId, Payload),
    inflight_response_add(State, MsgType, ReqId, Data).

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

channel_allocate(#state{channel_id = undefined} = State) ->
    case opcua_registry:allocate_secure_channel(self()) of
        {ok, ChannelId} -> {ok, State#state{channel_id = ChannelId}};
        {error, Reason} -> {error, Reason, State}
    end.

channel_release(#state{channel_id = undefined} = State) -> State;
channel_release(#state{channel_id = ChannelId} = State) ->
    case opcua_registry:release_secure_channel(ChannelId) of
        ok -> State#state{channel_id = undefined};
        {error, Reason} ->
            ?LOG_ERROR("Error while releasing secure channel ~w: ~p",
                       [ChannelId, Reason]),
            State#state{channel_id = undefined}
    end.

negotiate_min(0, B) -> B;
negotiate_min(A, 0) -> A;
negotiate_min(A, B) -> min(A, B).


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