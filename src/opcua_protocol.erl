-module(opcua_protocol).

-behavior(ranch_protocol).

%TODO: Split the channel handling to its own process, if resuming a channel
%      is required.
%TODO: Check maximum number of inflight requests.
%TODO: Handle response chunking...

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua_protocol.hrl").
-include("opcua_codec.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Behaviour ranch_protocol Callback Functions
-export([start_link/4]).

%% API functions
-export([req2res/3]).

%% System Callback Functions
-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

%% Internal Exported Functions
-export([connection_process/4]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER_VER, 0).
-define(DEFAULT_MAX_CHUNK_SIZE, 65535).
-define(DEFAULT_MAX_MESSAGE_SIZE, 0).
-define(DEFAULT_MAX_CHUNK_COUNT, 0).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(inflight_request, {
    message_type :: message_type(),
    request_id :: request_id(),
    chunk_count :: pos_integer(),
    total_size :: pos_integer(),
    reversed_data :: [iodata()]
}).

-record(inflight_response, {
    message_type :: message_type(),
    request_id :: undefined | request_id(),
    data :: iodata()
}).

-record(state, {
    opts = #{} :: map(),
    channel_id :: undefined | pos_integer(),
    max_res_chunk_size = ?DEFAULT_MAX_CHUNK_SIZE :: non_neg_integer(),
    max_req_chunk_size = ?DEFAULT_MAX_CHUNK_SIZE :: non_neg_integer(),
    max_msg_size = ?DEFAULT_MAX_MESSAGE_SIZE :: non_neg_integer(),
    max_chunk_count = ?DEFAULT_MAX_CHUNK_COUNT :: non_neg_integer(),
    parent :: pid(),
    ref :: ranch:ref(),
    socket :: inet:socket(),
    transport :: module(),
    peer :: undefined | {inet:ip_address(), inet:port_number()},
    sock :: undefined | {inet:ip_address(), inet:port_number()},
    client_ver :: undefined | pos_integer(),
    endpoint_url :: undefined | binary(),
    inflight_requests = #{} :: #{pos_integer() => #inflight_request{}},
    inflight_responses = #{} :: #{pos_integer() => #inflight_response{}},
    inflight_queue = queue:new(),
    curr_token_id :: undefined | pos_integer(),
    temp_token_id :: undefined | pos_integer(),
    curr_sec :: term(),
    temp_sec :: term(),
    conn :: undefined | connection(),
    sess :: undefined | pid()
}).

-type state() :: #state{}.
-type message_type() :: hello | acknowledge | reverse_hello | error
                      | channel_open | channel_close | channel_message.
-type chunk_type() :: final | intermediate | aborted.
-type token_id() :: pos_integer().
-type channel_id() :: pos_integer().
-type sequence_num() :: pos_integer().
-type request_id() :: pos_integer().
-type security_policy() :: #uacp_security_policy{}.
-type chunk() :: #uacp_chunk{}.
-type message() :: #uacp_message{}.
-type connection() :: #uacp_connection{}.

-type hello_payload() :: #{
    ver := non_neg_integer(),
    max_res_chunk_size := non_neg_integer(),
    max_req_chunk_size := non_neg_integer(),
    max_msg_size := non_neg_integer(),
    max_chunk_count := non_neg_integer(),
    endpoint_url := undefined | binary()
}.

-type acknowledge_payload() :: #{
    ver := non_neg_integer(),
    max_res_chunk_size := non_neg_integer(),
    max_req_chunk_size := non_neg_integer(),
    max_msg_size := non_neg_integer(),
    max_chunk_count := non_neg_integer()
}.

-type error_payload() :: #{
    error := non_neg_integer(),
    reason := undefined | binary()
}.

-export_type([
    message_type/0,
    chunk_type/0,
    token_id/0,
    channel_id/0,
    sequence_num/0,
    request_id/0,
    security_policy/0,
    chunk/0,
    message/0,
    hello_payload/0,
    acknowledge_payload/0,
    error_payload/0
]).


%%% BEHAVIOUR ranch_protocol CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Ref, _Socket, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, connection_process,
                              [self(), Ref, Transport, Opts]),
    {ok, Pid}.


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

req2res(#uacp_message{type = T, request_id = ReqId}, NodeId, Payload) ->
    FinalPayload = case maps:is_key(response_header, Payload) of
        true -> Payload;
        false ->
            Header = #{
                timestamp => opcua_util:date_time(),
                request_handle => ReqId,
                service_result => 0,
                service_diagnostics => #diagnostic_info{},
                string_table => [],
                additional_header => #extension_object{}
            },
            Payload#{response_header => Header}
    end,
    #uacp_message{type = T, request_id = ReqId,
                  node_id = NodeId, payload = FinalPayload}.


%%% SYSTEM CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

system_continue(_, _, {State, Buff}) ->
    loop(State, Buff).

-spec system_terminate(term(), term(), term(), term()) -> no_return().
system_terminate(Reason, _, _, {State, _}) ->
    terminate({stop, {exit, Reason}, 'sys:terminate/2,3 was called.'}, State).

system_code_change(Misc, _, _, _) ->
    {ok, Misc}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec connection_process(pid(), atom(), module(), term()) -> no_return().
connection_process(Parent, Ref, Transport, Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    init(Parent, Ref, Socket, Transport, Opts).

-spec init(pid(), atom(), inet:socket(), module(), term()) -> no_return().
init(Parent, Ref, Socket, Transport, Opts) ->
    ?LOG_DEBUG("Starting protocol handler"),
    PeerRes = Transport:peername(Socket),
    SockRes = Transport:sockname(Socket),
    case {PeerRes, SockRes} of
        {{ok, Peer}, {ok, Sock}} ->
            State = #state{
                opts = Opts,
                parent = Parent,
                ref = Ref,
                socket = Socket,
                transport = Transport,
                peer = Peer,
                sock = Sock,
                conn = #uacp_connection{pid = self()}
            },
            loop(State, <<>>);
        {{error, Reason}, _} ->
            terminate(undefined, {socket_error, Reason,
                'A socket error occurred when retrieving the peer name.'});
        {_, {error, Reason}} ->
            terminate(undefined, {socket_error, Reason,
                'A socket error occurred when retrieving the sock name.'})
    end.

loop(#state{socket = S, transport = T} = State, Buff) ->
    T:setopts(S, [{active, once}]),
    loop_consume(State, Buff).

loop_consume(#state{parent = P, socket = S, transport = T} = State, Buff) ->
    {OK, Closed, Error} = T:messages(),
    Timeout = produce_timeout(State),
    receive
        {OK, S, Input} ->
            ?LOG_DEBUG("Received: ~p", [Input]),
            try handle_data(State, Input, Buff) of
                {error, Reason, Buff2, State2} ->
                    loop_error(State2, Reason, Buff2, fun loop/2);
                {ok, Buff2, State2} ->
                    loop_produce(State2, Buff2, fun loop/2)
            catch
                Class:Reason:Stack ->
                    Error = {exception, Class, Reason, Stack},
                    loop_error(State, Error, Buff, fun loop/2)
            end;
        {Closed, S} ->
            ?LOG_DEBUG("Socket closed"),
            terminate(State, {socket_error, closed, 'The socket has been closed.'});
        {Error, S, Reason} ->
            ?LOG_DEBUG("Socket error: ~p", [Reason]),
            terminate(State, {socket_error, Reason, 'An error has occurred on the socket.'});
        {'EXIT', P, Reason} ->
            ?LOG_DEBUG("Parent ~p died: ~p", [P, Reason]),
            terminate(State, {stop, {exit, Reason}, 'Parent process terminated.'});
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, P, ?MODULE, [], {State, Buff});
        Msg ->
            ?LOG_WARNING("Received unexpected message ~p", [Msg]),
            loop(State, Buff)
    after Timeout ->
        loop_produce(State, Buff, fun loop_consume/2)
    end.

-spec loop_produce(state(), binary(), function()) -> no_return().
loop_produce(#state{socket = S, transport = T} = State, Buff, Cont) ->
    try produce_data(State) of
        {error, Reason, State2} ->
            loop_error(State2, Reason, Buff, Cont);
        {ok, State2} -> Cont(State2, Buff);
        {ok, Output, State2} ->
            ?LOG_DEBUG("Sending:  ~p", [Output]),
            case T:send(S, Output) of
                ok -> Cont(State2, Buff);
                {error, Reason} ->
                    loop_error(State2, Reason, Buff, Cont)
            end
    catch
        Class:Reason:Stack ->
            Error = {exception, Class, Reason, Stack},
            loop_error(State, Error, Buff, Cont)
    end.

-spec loop_error(state(), term(), binary(), function()) -> no_return().
%%TODO: Implemente proper error handling, support non-fatal errors and send
%%      error message to the client.
loop_error(State, {exception, Class, Reason, Stack}, _Buff, _Cont) ->
    ?LOG_ERROR("Protocol ~s exception: ~p", [Class, Reason]),
    ?LOG_DEBUG("Stacktrace: ~p", [Stack]),
    terminate(State, Reason);
loop_error(State, Reason, _Buff, _Cont) ->
    ?LOG_ERROR("Protocol error: ~p", [Reason]),
    terminate(State, Reason).

terminate(undefined, Reason) ->
    exit({shutdown, Reason});
terminate(State, Reason) ->
    terminate_linger(channel_release(State)),
    exit({shutdown, Reason}).

terminate_linger(#state{socket = S, transport = T, opts = O} = State) ->
    case T:shutdown(S, write) of
        ok ->
            case maps:get(linger_timeout, O, 1000) of
                0 -> ok;
                infinity -> terminate_linger_loop(State, undefined);
                Timeout ->
                    TRef = erlang:start_timer(Timeout, self(), linger_timeout),
                    terminate_linger_loop(State, TRef)
            end;
        {error, _} ->
            ok
    end.

terminate_linger_loop(#state{socket = S, transport = T} = State, TRef) ->
    {OK, Closed, Error} = T:messages(),
    case T:setopts(S, [{active, once}]) of
        {error, _} -> ok;
        ok ->
            receive
                {Closed, S} -> ok;
                {Error, S, _} -> ok;
                {timeout, TRef, linger_timeout} -> ok;
                {OK, S, _} ->
                    terminate_linger_loop(State, TRef);
                _ ->
                    terminate_linger_loop(State, TRef)
            end
    end.

handle_data(State, Data, Buff) ->
    %TODO: Handle decoding errors so it can be recoverable,
    %      updated buffer would be required to not keep decoding the
    %      same data all over again.
    TotalSize = byte_size(Data) + byte_size(Buff),
    case TotalSize > State#state.max_req_chunk_size of
        true -> {error, bad_encoding_limits_exceeded, Buff, State};
        false ->
            AllData = <<Buff/binary, Data/binary>>,
            {Chunks, Buff2} = opcua_protocol_codec:decode_chunks(AllData),
            case handle_chunks(State, Chunks) of
                {error, Reason, State2} -> {error, Reason, Buff2, State2};
                {ok, State2} -> {ok, Buff2, State2}
            end
    end.

handle_chunks(State, []) -> {ok, State};
handle_chunks(State, [Chunk | Rest]) ->
    %TODO: handle errors so non-fatal errors do not break the chunk processing
    try handle_chunk(State, Chunk) of
        {error, _Reason, _State} = Error -> Error;
        {ok, State2} -> handle_chunks(State2, Rest)
    catch
        Class:Reason:Stack -> {error, {exception, Class, Reason, Stack}, State}
    end.

handle_chunk(State, #uacp_chunk{state = undefined, message_type = MsgType,
                                chunk_type = final, body = Body}) ->
    Message = decode_basic_payload(MsgType, Body),
    Request = #uacp_message{type = MsgType, payload = Message},
    ?LOG_DEBUG("Handling request ~p", [Request]),
    handle_request(State, Request);
handle_chunk(State, #uacp_chunk{state = locked} = Chunk) ->
    #uacp_chunk{message_type = MsgType, channel_id = ChannelId} = Chunk,
    channel_validate(State, MsgType, ChannelId),
    handle_locked_chunk(State, Chunk).

handle_locked_chunk(#state{channel_id = undefined} = State,
                    #uacp_chunk{message_type = channel_open,
                                chunk_type = final} = Chunk) ->
    case channel_allocate(State) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} -> handle_open_chunk(State2, Chunk)
    end;
handle_locked_chunk(State, #uacp_chunk{message_type = channel_open,
                                       chunk_type = final} = Chunk) ->
    handle_open_chunk(State, Chunk);
handle_locked_chunk(State, #uacp_chunk{message_type = channel_close,
                                       chunk_type = final} = Chunk) ->
    handle_close_chunk(State, Chunk);
handle_locked_chunk(State, #uacp_chunk{message_type = channel_message} = Chunk) ->
    handle_message_chunk(State, Chunk).

handle_open_chunk(State, #uacp_chunk{security = SecPolicy} = Chunk) ->
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
                        opcua_protocol_codec:decode_object(Body),
                    Request = #uacp_message{
                        type = channel_open,
                        request_id = RequestId,
                        node_id = NodeId,
                        payload = Payload
                    },
                    ?LOG_DEBUG("Handling request ~p", [Request]),
                    handle_request(State3, Request)
            end
    end.

handle_close_chunk(State, Chunk) ->
    case security_sym_unlock(State, Chunk) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Chunk2, State2} ->
            #uacp_chunk{
                request_id = RequestId,
                body = Body
            } = Chunk2,
            {NodeId, Payload} =
                opcua_protocol_codec:decode_object(Body),
            Request = #uacp_message{
                type = channel_close,
                request_id = RequestId,
                node_id = NodeId,
                payload = Payload
            },
            ?LOG_DEBUG("Handling request ~p", [Request]),
            handle_request(State2, Request)
    end.

handle_message_chunk(State, Chunk) ->
    case security_sym_unlock(State, Chunk) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Chunk2, State2} ->
            case inflight_request_update(State2, Chunk2) of
                {ok, State3} -> {ok, State3};
                {ok, Request, State3} ->
                    ?LOG_DEBUG("Handling request ~p", [Request]),
                    handle_request(State3, Request)

            end
    end.

inflight_request_update(State, #uacp_chunk{chunk_type = aborted} = Chunk) ->
    #state{inflight_requests = ReqMap} = State,
    #uacp_chunk{request_id = ReqId, body = Body} = Chunk,
    case maps:take(ReqId, ReqMap) of
        error ->
            #{error := Error, reason := Reason} =
                opcua_protocol_codec:decode_error(Body),
            ?LOG_WARNING("Unknown request ~w aborted: ~s (~w)",
                         [ReqId, Reason, Error]),
            {ok, State};
        {_Inflight, ReqMap2} ->
            #{error := Error, reason := Reason} =
                opcua_protocol_codec:decode_error(Body),
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
    {NodeId, Payload} = opcua_protocol_codec:decode_object(FinalData),
    Request = #uacp_message{
        type = MsgType,
        request_id = ReqId,
        node_id = NodeId,
        payload = Payload
    },
    {ok, Request, State2}.

handle_request(State, #uacp_message{type = hello, payload = Msg}) ->
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
handle_request(State, #uacp_message{type = error, payload = Msg}) ->
    #{error := Error, reason := Reason} = Msg,
    ?LOG_ERROR("Received client error ~w: ~s", [Error, Reason]),
    {ok, State};
handle_request(State, #uacp_message{type = channel_open} = Request) ->
    case security_open(State, Request) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Resp, State2} -> handle_response(State2, Resp)
    end;
handle_request(State, #uacp_message{type = channel_close} = Request) ->
    case security_close(State, Request) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Resp, State2} -> handle_response(State2, Resp)
    end;

handle_request(#state{conn = Conn, sess = Sess} = State,
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
    Data = opcua_protocol_codec:encode_object(NodeId, Payload),
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

produce_timeout(#state{inflight_responses = Map}) ->
    case maps:size(Map) > 0 of
        true -> 3;
        false -> infinity
    end.

produce_data(#state{inflight_responses = Map, inflight_queue = Queue} = State) ->
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
                            Output = opcua_protocol_codec:encode_chunks(Chunk),
                            {ok, Output, State3}
                    end
            end
    end.

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
            Chunk3 = opcua_protocol_codec:prepare_chunks(Chunk2),
            case security_prepare(State2, Chunk3) of
                {error, _Reason, _State2} = Error -> Error;
                {ok, Chunk4, State3} ->
                    Chunk5 = opcua_protocol_codec:freeze_chunks(Chunk4),
                    security_lock(State3, Chunk5)
            end
    end.

setup_chunk_security(State, #uacp_chunk{message_type = channel_open} = Chunk) ->
    security_setup_asym(State, Chunk);
setup_chunk_security(State, Chunk) ->
    security_setup_sym(State, Chunk).

decode_basic_payload(hello, Body) ->
    opcua_protocol_codec:decode_hello(Body);
decode_basic_payload(error, Body) ->
    opcua_protocol_codec:decode_error(Body).

encode_basic_payload(acknowledge, Payload) ->
    opcua_protocol_codec:encode_acknowledge(Payload);
encode_basic_payload(error, Payload) ->
    opcua_protocol_codec:encode_error(Payload).

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

security_open(#state{curr_token_id = Tok} = State, Req)
  when Tok =/= undefined ->
    #state{curr_sec = Sub, conn = Conn} = State,
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

security_close(#state{curr_token_id = Tok} = State, Req)
  when Tok =/= undefined ->
    #state{curr_sec = Sub, conn = Conn} = State,
    case opcua_security:close(Conn, Req, Sub) of
        {error, Reason} ->
            State2 = State#state{curr_token_id = undefined, curr_sec = undefined},
            {error, Reason, State2};
        {ok, Resp, Sub2} ->
            {ok, Resp, State#state{curr_sec = Sub2}}
    end.
