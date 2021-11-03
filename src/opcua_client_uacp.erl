-module(opcua_client_uacp).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([init/0]).
-export([handshake/2]).
-export([browse/4]).
-export([read/4]).
-export([write/5]).
-export([close/2]).
-export([can_produce/2]).
-export([produce/2]).
-export([handle_data/3]).
-export([terminate/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    server_ver                      :: undefined | pos_integer(),
    channel                         :: term(),
    proto                           :: term(),
    sess                            :: term()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init() ->
    case opcua_uacp:init(client, opcua_client_channel) of
        {error, _Reason} = Error -> Error;
        {ok, Proto} -> {ok, #state{proto = Proto}}
    end.

handshake(Conn, #state{proto = Proto} = State) ->
    Payload = #{
        ver => opcua_uacp:version(Proto),
        max_res_chunk_size => opcua_uacp:limit(max_res_chunk_size, Proto),
        max_req_chunk_size => opcua_uacp:limit(max_req_chunk_size, Proto),
        max_msg_size => opcua_uacp:limit(max_msg_size, Proto),
        max_chunk_count => opcua_uacp:limit(max_chunk_count, Proto),
        endpoint_url => opcua_connection:endpoint_url(Conn)
    },
    Request = opcua_connection:request(Conn, hello, undefined, undefined, Payload),
    proto_consume(State, Conn, Request).

browse(NodeId, Opts, Conn, State) ->
    case session_browse(State, Conn, NodeId, Opts) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Handle, Requests, State2} ->
            case proto_consume(State2, Conn, Requests) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, State3} -> {async, Handle, State3}
            end
    end.

read(ReadSpecs, Opts, Conn, State) ->
    case session_read(State, Conn, ReadSpecs, Opts) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Handle, Requests, State2} ->
            case proto_consume(State2, Conn, Requests) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, State3} -> {async, Handle, State3}
            end
    end.

write(NodeId, AttribValuePairs, Opts, Conn, State) ->
    case session_write(State, Conn, NodeId, AttribValuePairs, Opts) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Handle, Requests, State2} ->
            case proto_consume(State2, Conn, Requests) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, State3} -> {async, Handle, State3}
            end
    end.

close(Conn, State) ->
    case session_close(State, Conn) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Requests, State2} ->
            case proto_consume(State2, Conn, Requests) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, State3} -> {ok, State3}
            end
    end.

can_produce(Conn, State) ->
    proto_can_produce(State, Conn).

produce(Conn, State) ->
    proto_produce(State, Conn).

handle_data(Data, Conn, State) ->
    case proto_handle_data(State, Conn, Data) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Msgs, State2} -> handle_responses(State2, Conn, Msgs)
    end.

terminate(Reason, Conn, State) ->
    channel_terminate(State, Conn, Reason).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_responses(State, _Conn, Msgs) ->
    handle_responses(State, _Conn, Msgs, []).

handle_responses(State, _Conn, [], Acc) ->
    {ok, lists:append(Acc), State};
handle_responses(State, Conn, [Msg | Rest], Acc) ->
    ?DUMP("Receiving message: ~p", [Msg]),
    case handle_response(State, Conn, Msg) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} ->
            handle_responses(State2, Conn, Rest);
        {ok, Results, Requests, State2} ->
            case proto_consume(State2, Conn, Requests) of
                {error, _Reason, _State2} = Error -> Error;
                {ok, State3} ->
                    handle_responses(State3, Conn, Rest, [Results | Acc])
            end
    end.

handle_response(State, Conn, #uacp_message{type = acknowledge, payload = Payload}) ->
    ClientVersion = proto_version(State),
    #{
        ver := ServerVersion,
        max_res_chunk_size := MaxResChunkSize,
        max_req_chunk_size := MaxReqChunkSize,
        max_msg_size := MaxMsgSize,
        max_chunk_count := MaxChunkCount
    } = Payload,
    ?LOG_INFO("Negociated protocol configuration: client_ver=~w, server_ver=~w, "
              "max_res_chunk_size=~w, max_req_chunk_size=~w, max_msg_size=~w, "
              "max_chunk_count=~w" ,
              [ClientVersion, ServerVersion, MaxResChunkSize, MaxReqChunkSize,
               MaxMsgSize, MaxChunkCount]),
    State2 = proto_limit(State, max_res_chunk_size, MaxResChunkSize),
    State3 = proto_limit(State2, max_req_chunk_size, MaxReqChunkSize),
    State4 = proto_limit(State3, max_msg_size, MaxMsgSize),
    State5 = proto_limit(State4, max_chunk_count, MaxChunkCount),
    State6 = State5#state{server_ver = ServerVersion},
    channel_open(State6, Conn);
handle_response(#state{channel = undefined} = State, _Conn, _Response) ->
    %% Called when closed and receiving a message, not sure what we should be doing
    {ok, State};
handle_response(State, Conn, Response) ->
    case channel_handle_response(State, Conn, Response) of
        {error, _Reason, _State2} = Error -> Error;
        {open, State2} -> session_create(State2, Conn);
        {closed, State2} ->
            opcua_connection:notify(Conn, closed),
            {ok, State2#state{channel = undefined}};
        {forward, Response2, State2} ->
            case session_handle_response(State2, Conn, Response2) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, _Results, _Requests, _State3} = Result -> Result;
                {closed, State3} ->
                    channel_close(State3#state{sess = undefined}, Conn)
            end
    end.


%== Session Module Abstraction Functions =======================================

session_create(#state{channel = Channel, sess = undefined} = State, Conn) ->
    Sess = opcua_client_session:new(),
    case opcua_client_session:create(Conn, Channel, Sess) of
        {error, Reason} -> {error, Reason, State};
        {ok, Req, Channel2, Sess2} ->
            {ok, [], Req, State#state{channel = Channel2, sess = Sess2}}
    end.

session_browse(#state{channel = Channel, sess = Sess} = State, Conn, NodeId, Opts) ->
    case opcua_client_session:browse(NodeId, Opts, Conn, Channel, Sess) of
        {error, Reason} -> {error, Reason, State};
        {ok, Handle, Requests, Channel2, Sess2} ->
            {ok, Handle, Requests, State#state{channel = Channel2, sess = Sess2}}
    end.

session_read(#state{channel = Channel, sess = Sess} = State, Conn, ReadSpecs, Opts) ->
    case opcua_client_session:read(ReadSpecs, Opts, Conn, Channel, Sess) of
        {error, Reason} -> {error, Reason, State};
        {ok, Handle, Requests, Channel2, Sess2} ->
            {ok, Handle, Requests, State#state{channel = Channel2, sess = Sess2}}
    end.

session_write(#state{channel = Channel, sess = Sess} = State, Conn, NodeId,
              AttribValuePairs, Opts) ->
    case opcua_client_session:write(NodeId, AttribValuePairs, Opts, Conn,
                                    Channel, Sess) of
        {error, Reason} -> {error, Reason, State};
        {ok, Handle, Requests, Channel2, Sess2} ->
            {ok, Handle, Requests, State#state{channel = Channel2, sess = Sess2}}
    end.

session_close(#state{channel = Channel, sess = Sess} = State, Conn) ->
    case opcua_client_session:close(Conn, Channel, Sess) of
        {error, Reason} -> {error, Reason, State};
        {ok, Requests, Channel2, Sess2} ->
            {ok, Requests, State#state{channel = Channel2, sess = Sess2}}
    end.

session_handle_response(#state{channel = Channel, sess = Sess} = State, Conn, Response) ->
    case opcua_client_session:handle_response(Response, Conn, Channel, Sess) of
        {error, Reason} -> {error, Reason, State};
        {closed, Channel2, Sess2} ->
            {closed, State#state{channel = Channel2, sess = Sess2}};
        {ok, Results, Requests, Channel2, Sess2} ->
            {ok, Results, Requests, State#state{channel = Channel2, sess = Sess2}}
    end.


%== Channel Module Abstraction Functions =======================================

channel_open(#state{channel = undefined} = State, Conn) ->
    case opcua_client_channel:init(Conn) of
        {error, Reason} -> {error, Reason, State};
        {ok, Channel} ->
            case opcua_client_channel:open(Conn, Channel) of
                {error, _Reason} = Error -> Error;
                {ok, Req, Channel2} ->
                    {ok, [], Req, State#state{channel = Channel2}}
            end
    end.

channel_close(#state{channel = Channel} = State, Conn) ->
    case opcua_client_channel:close(Conn, Channel) of
        {error, _Reason} = Error -> Error;
        {ok, Req, Channel2} ->
            {ok, [], Req, State#state{channel = Channel2}}
    end.

channel_terminate(#state{channel = Channel}, Conn, Reason) ->
    opcua_client_channel:terminate(Reason, Conn, Channel).

channel_handle_response(#state{channel = Channel} = State, Conn, Req) ->
    case opcua_client_channel:handle_response(Req, Conn, Channel) of
        {error, Reason} -> {error, Reason, State};
        {Tag, Channel2} -> {Tag, State#state{channel = Channel2}};
        {Tag, Resp, Channel2} -> {Tag, Resp, State#state{channel = Channel2}}
    end.


%== Protocol Module Abstraction Functions ======================================

proto_version(#state{proto = Proto}) ->
    opcua_uacp:version(Proto).

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

proto_consume(#state{channel = Channel, proto = Proto} = State, Conn, Msgs) ->
    case proto_consume_loop(Proto, Channel, Conn, Msgs) of
        {error, Reason, Channel2, Proto2} ->
            {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Channel2, Proto2} ->
            {ok, State#state{channel = Channel2, proto = Proto2}}
    end.

proto_consume_loop(Proto, Channel, _Conn, []) -> {ok, Channel, Proto};
proto_consume_loop(Proto, Channel, Conn, [Msg | Rest]) ->
    case proto_consume_loop(Proto, Channel, Conn, Msg) of
        {error, _Reason, _Channel2, _Proto2} = Result -> Result;
        {ok, Channel2, Proto2} ->
            proto_consume_loop(Proto2, Channel2, Conn, Rest)
    end;
proto_consume_loop(Proto, Channel, Conn, Msg) ->
    ?DUMP("Sending message: ~p", [Msg]),
    opcua_uacp:consume(Msg, Conn, Channel, Proto).

