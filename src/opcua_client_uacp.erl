-module(opcua_client_uacp).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([init/2]).
-export([handshake/2]).
-export([browse/4]).
-export([read/4]).
-export([write/5]).
-export([close/2]).
-export([can_produce/2]).
-export([produce/2]).
-export([handle_data/3]).
-export([terminate/3]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    mode                            :: lookup_endpoint | open_session,
    server_ver                      :: undefined | pos_integer(),
    channel                         :: term(),
    proto                           :: term(),
    sess                            :: term(),
    endpoint_selector               :: function(),
    security_mode                   :: atom(),
    security_policy                 :: atom(),
    identity                        :: undefined | opcua_keychain:ident()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Mode, Opts) ->
    AllOpts = maps:merge(default_options(), Opts),
    case opcua_uacp:init(client, opcua_client_channel) of
        {error, _Reason} = Error -> Error;
        {ok, Proto} ->
            State = #state{
                mode = Mode,
                endpoint_selector = maps:get(endpoint_selector, AllOpts),
                security_mode = maps:get(mode, AllOpts),
                security_policy = maps:get(policy, AllOpts),
                identity = maps:get(identity, AllOpts),
                proto = Proto
            },
            {ok, State}
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
    maybe_consume(async, session_browse(State, Conn, NodeId, Opts), Conn).

read(ReadSpecs, Opts, Conn, State) ->
    maybe_consume(async, session_read(State, Conn, ReadSpecs, Opts), Conn).

write(NodeId, AttribValuePairs, Opts, Conn, State) ->
    maybe_consume(async, session_write(State, Conn, NodeId, AttribValuePairs, Opts), Conn).

close(Conn, #state{sess = undefined} = State) ->
    maybe_consume(ok, channel_close(State, Conn), Conn);
close(Conn, State) ->
    maybe_consume(ok, session_close(State, Conn), Conn).

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

default_options() ->
    #{mode => none, policy => none, auth => anonymous, identity => undefined}.

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
    no_result(channel_open(State6, Conn));
handle_response(#state{channel = undefined} = State, _Conn, _Response) ->
    %% Called when closed and receiving a message, not sure what we should be doing
    {ok, State};
handle_response(#state{mode = lookup_endpoint} = State, Conn, Response) ->
    case channel_handle_response(State, Conn, Response) of
        {error, _Reason, _State2} = Error -> Error;
        {open, State2} ->
            no_result(channel_get_endpoints(State2, Conn));
        {endpoints, Endpoints, State2} ->
            case select_endpoint(State2, Endpoints) of
                {error, Reason} ->
                    {error, Reason, State2};
                {ok, EndpointSpec, ProtoOpts} ->
                    opcua_connection:notify(Conn, {reconnect, EndpointSpec, ProtoOpts}),
                    {ok, State2}
            end;
        {closed, State2} ->
            opcua_connection:notify(Conn, closed),
            {ok, State2#state{channel = undefined}}
    end;
handle_response(#state{mode = open_session} = State, Conn, Response) ->
    case channel_handle_response(State, Conn, Response) of
        {error, _Reason, _State2} = Error -> Error;
        {open, #state{endpoint_selector = Selector} = State2} ->
            no_result(session_create(State2, Conn, Selector));
        {closed, State2} ->
            opcua_connection:notify(Conn, closed),
            {ok, State2#state{channel = undefined}};
        {forward, Response2, State2} ->
            case session_handle_response(State2, Conn, Response2) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, _Results, _Requests, _State3} = Result -> Result;
                {closed, State3} ->
                    no_result(channel_close(State3#state{sess = undefined}, Conn))
            end
    end.

select_endpoint(State, Endpoints) ->
    #state{endpoint_selector = Selector, identity = Identity} = State,
    case Selector(Endpoints) of
        {error, not_found} -> {error, no_compatible_server_endpoint};
        {ok, Endpoint, _AuthPolicyId, _AuthSpec} ->
            #{endpoint_url := EndPointUrl,
              security_mode := Mode,
              security_policy_uri := PolicyUri} = Endpoint,
            EndpointSpec = opcua_util:parse_endpoint(EndPointUrl),
            ProtoOpts = #{
                endpoint_selector => Selector,
                mode => Mode,
                policy => opcua_util:policy_type(PolicyUri),
                identity => Identity
            },
            {ok, EndpointSpec, ProtoOpts}
    end.


%== Utility Functions ==========================================================

maybe_consume(_Tag, {error, _Reason, _State} = Error, _Conn) -> Error;
maybe_consume(Tag, {ok, Requests, State}, Conn) ->
    case proto_consume(State, Conn, Requests) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} -> {Tag, State2}
    end;
maybe_consume(Tag, {ok, Result, Requests, State}, Conn) ->
    case proto_consume(State, Conn, Requests) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, State2} -> {Tag, Result, State2}
    end.

no_result({error, _Reason, _State} = Error) -> Error;
no_result({ok, Req, State}) -> {ok, [], Req, State}.


%== Session Module Abstraction Functions =======================================

session_create(#state{channel = Channel, sess = undefined} = State, Conn, EndpointSelector) ->
    Sess = opcua_client_session:new(EndpointSelector),
    case opcua_client_session:create(Conn, Channel, Sess) of
        {error, Reason} -> {error, Reason, State};
        {ok, Req, Channel2, Sess2} ->
            {ok, Req, State#state{channel = Channel2, sess = Sess2}}
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

channel_open(#state{channel = undefined, security_mode = Mode,
                    security_policy = Policy, identity = Identity} = State, Conn) ->
    case opcua_client_channel:init(Conn, Mode, Policy, Identity) of
        {error, Reason} ->
            {error, Reason, State};
        {ok, Channel} ->
            case opcua_client_channel:open(Conn, Channel) of
                {error, _Reason} = Error -> Error;
                {ok, Req, Channel2} ->
                    {ok, Req, State#state{channel = Channel2}}
            end
    end.

channel_get_endpoints(#state{channel = Channel} = State, Conn) ->
    {ok, Req, Channel2} = opcua_client_channel:get_endpoints(Conn, Channel),
    {ok, Req, State#state{channel = Channel2}}.

channel_close(#state{channel = Channel} = State, Conn) ->
    case opcua_client_channel:close(Conn, Channel) of
        {error, Reason} ->
            {error, Reason, State};
        {ok, Req, Channel2} ->
            {ok, Req, State#state{channel = Channel2}}
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

