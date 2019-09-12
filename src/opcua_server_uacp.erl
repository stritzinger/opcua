-module(opcua_server_uacp).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([init/1]).
-export([can_produce/2]).
-export([produce/2]).
-export([handle_data/3]).
-export([terminate/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    client_ver                      :: undefined | pos_integer(),
    channel                         :: term(),
    proto                           :: term(),
    sess                            :: undefined | pid()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Opts) ->
    case opcua_server_channel:init() of
        {error, _Reason} = Error -> Error;
        {ok, Channel} ->
            case opcua_uacp:init(server, opcua_server_channel) of
                {error, _Reason} = Error -> Error;
                {ok, Proto} ->
                    {ok, #state{channel = Channel, proto = Proto}}
            end
    end.

can_produce(Conn, State) ->
    proto_can_produce(State, Conn).

produce(Conn, State) ->
    proto_produce(State, Conn).

handle_data(Data, Conn, State) ->
    case proto_handle_data(State, Conn, Data) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Msgs, State2} -> handle_requests(State2, Conn, Msgs)
    end.

terminate(Reason, Conn, State) ->
    channel_terminate(State, Conn, Reason).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

negotiate_limit(0, B) when is_integer(B), B >= 0 -> B;
negotiate_limit(A, 0) when is_integer(A), A > 0-> A;
negotiate_limit(A, B) when is_integer(A), A > 0, is_integer(B), B > 0 -> min(A, B).

handle_requests(State, _Conn, []) -> {ok, State};
handle_requests(State, Conn, [Msg | Rest]) ->
    ?LOG_DEBUG("Handling request ~p", [Msg]),
    case handle_request(State, Conn, Msg) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} ->
            handle_requests(State2, Conn, Rest);
        {ok, NewMsg, State2} ->
            case proto_consume(State2, Conn, NewMsg) of
                {error, _Reason, _State2} = Error -> Error;
                {ok, State3} -> handle_requests(State3, Conn, Rest)
            end
    end.

handle_request(State, _Conn, #uacp_message{type = hello, payload = Msg}) ->
    ServerVersion = proto_version(State),
    #{
        ver := ClientVersion,
        max_res_chunk_size := ClientMaxResChunkSize,
        max_req_chunk_size := ClientMaxReqChunkSize,
        max_msg_size := ClientMaxMsgSize,
        max_chunk_count := ClientMaxChunkCount,
        endpoint_url := ClientEndPointUrl
    } = Msg,

    %TODO: Validate that the client endpoint URL match the connection one.

    ServerMaxResChunkSize = proto_limit(State, max_res_chunk_size),
    ServerMaxReqChunkSize = proto_limit(State, max_req_chunk_size),
    ServerMaxMsgSize = proto_limit(State, max_msg_size),
    ServerMaxChunkCount = proto_limit(State, max_chunk_count),

    MaxResChunkSize = negotiate_limit(ServerMaxResChunkSize, ClientMaxResChunkSize),
    MaxReqChunkSize = negotiate_limit(ServerMaxReqChunkSize, ClientMaxReqChunkSize),
    MaxMsgSize = negotiate_limit(ServerMaxMsgSize, ClientMaxMsgSize),
    MaxChunkCount = negotiate_limit(ServerMaxChunkCount, ClientMaxChunkCount),
    ?LOG_INFO("Negociated protocol configuration: client_ver=~w, server_ver=~w, "
              "max_res_chunk_size=~w, max_req_chunk_size=~w, max_msg_size=~w, "
              "max_chunk_count=~w, endpoint_url=~p" ,
              [ClientVersion, ServerVersion, MaxResChunkSize, MaxReqChunkSize,
               MaxMsgSize, MaxChunkCount, ClientEndPointUrl]),

    State2 = proto_limit(State, max_res_chunk_size, MaxResChunkSize),
    State3 = proto_limit(State2, max_req_chunk_size, MaxReqChunkSize),
    State4 = proto_limit(State3, max_msg_size, MaxMsgSize),
    State5 = proto_limit(State4, max_chunk_count, MaxChunkCount),
    State6 = State5#state{client_ver = ClientVersion},

    Response = #uacp_message{
        type = acknowledge,
        sender = server,
        payload = #{
            ver => ServerVersion,
            max_res_chunk_size => MaxResChunkSize,
            max_req_chunk_size => MaxReqChunkSize,
            max_msg_size => MaxMsgSize,
            max_chunk_count => MaxChunkCount
        }
    },
    {ok, Response, State6};
handle_request(State, Conn, #uacp_message{type = MsgType} = Req)
  when MsgType =:= channel_open; MsgType =:= channel_close ->
    channel_handle_request(State, Conn, Req);
handle_request(State, _Conn, #uacp_message{type = error, payload = Payload}) ->
    #{error := Error, reason := Reason} = Payload,
    ?LOG_ERROR("Received client error ~w: ~s", [Error, Reason]),
    {ok, State};
handle_request(#state{sess = Sess} = State, Conn,
               #uacp_message{type = channel_message} = Request) ->
    #uacp_message{node_id = NodeSpec} = Request,
    %TODO: figure a way to not hardcode the ids...
    NodeId = opcua_database:lookup_id(NodeSpec),
    Request2 = Request#uacp_message{node_id = NodeId},
    Result = case {Sess, NodeId} of
        {_, #opcua_node_id{value = 426}} -> %% GetEndpoints
            opcua_server_discovery:handle_request(Conn, Request2);
        {undefined, _} ->
            opcua_server_session_manager:handle_request(Conn, Request2);
        {Session, _} ->
            opcua_server_session:handle_request(Conn, Request2, Session)
    end,
    case Result of
        {error, Reason} -> {error, Reason, State};
        {created, Resp, _SessPid} -> {ok, Resp, State};
        {bound, Resp, SessPid} -> {ok, Resp, State#state{sess = SessPid}};
        {closed, Resp} -> {ok, Resp, State#state{sess = undefined}};
        {reply, Resp} -> {ok, Resp, State}
    end.


%== Channel Module Abstraction Functions =======================================

channel_terminate(#state{channel = Channel}, Conn, Reason) ->
    opcua_server_channel:terminate(Reason, Conn, Channel).

channel_handle_request(#state{channel = Channel} = State, Conn, Req) ->
    case opcua_server_channel:handle_request(Req, Conn, Channel) of
        {error, _Reason} = Error -> Error;
        {ok, Resp, Channel2} -> {ok, Resp, State#state{channel = Channel2}}
    end.


%== Protocol Module Abstraction Functions ======================================

proto_version(#state{proto = Proto}) ->
    opcua_uacp:version(Proto).

proto_limit(#state{proto = Proto}, Name) ->
    opcua_uacp:limit(Name, Proto).

proto_limit(#state{proto = Proto} = State, Name, Value) ->
    Proto2 = opcua_uacp:limit(Name, Value, Proto),
    State#state{proto = Proto2}.

proto_can_produce(#state{proto = Proto}, Conn) ->
    opcua_uacp:can_produce(Conn, Proto).

proto_produce(#state{channel = Channel, proto = Proto} = State, Conn) ->
    case opcua_uacp:produce(Conn, Channel, Proto) of
        {error, Reason, Channel2, Proto2} ->
            {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Channel2, Proto2} ->
            {ok, State#state{channel = Channel2, proto = Proto2}};
        {ok, Output, Channel2, Proto2} ->
            {ok, Output, State#state{channel = Channel2, proto = Proto2}}
    end.

proto_handle_data(#state{channel = Channel, proto = Proto} = State, Conn, Data) ->
    case opcua_uacp:handle_data(Data, Conn, Channel, Proto) of
        {error, Reason, Channel2, Proto2} ->
            {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Messages, Channel2, Proto2} ->
            {ok, Messages, State#state{channel = Channel2, proto = Proto2}}
    end.

proto_consume(#state{channel = Channel, proto = Proto} = State, Conn, Msg) ->
    case opcua_uacp:consume(Msg, Conn, Channel, Proto) of
        {error, Reason, Channel2, Proto2} ->
            {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Channel2, Proto2} ->
            {ok, State#state{channel = Channel2, proto = Proto2}}
    end.