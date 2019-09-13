-module(opcua_client_session).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([new/0]).
-export([id/1]).
-export([auth_token/1]).
-export([create/3]).
-export([browse/5]).
-export([read/6]).
-export([close/3]).
-export([handle_response/4]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    status                      :: undefined | creating | activating | activated,
    id                          :: opcua:node_id(),
    token                       :: opcua:node_id()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new() ->
    #state{}.

id(undefined)       -> ?UNDEF_NODE_ID;
id(#state{id = Id}) -> Id.

auth_token(undefined)             -> ?UNDEF_NODE_ID;
auth_token(#state{token = Token}) -> Token.

create(Conn, Channel, #state{status = undefined} = State) ->
    Payload = #{
        client_description => #{
            application_uri => <<"urn:stritzinger:opcua:erlang:client">>,
            product_uri => <<"urn:stritzinger.com:opcua:erlang:client">>,
            application_name => <<"Stritzinger GmbH OPCUA Client">>,
            application_type => client,
            gateway_server_uri => undefined,
            discovery_profile_uri => undefined,
            discovery_urls => []
        },
        server_uri => undefined,
        endpoint_url => opcua_connection:endpoint_url(Conn),
        session_name => <<"Stritzinger OPCUA Session 1">>,
        client_nonce => opcua_util:nonce(),
        client_certificate => undefined,
        requested_session_timeout => 3600000,
        max_response_message_size => 0
    },
    channel_make_request(State#state{status = creating}, Channel, Conn,
                         ?NID_CREATE_SESS_REQ, Payload).

browse(NodeId, Opts, Conn, Channel, #state{status = activated} = State) ->
    %TODO: Add support for batching and options for class and result filtering
    BrowseDirection = maps:get(direction, Opts, forward),
    IncludeSubtypes = maps:get(include_subtypes, Opts, false),
    ReferenceTypeId = maps:get(type, Opts, ?UNDEF_NODE_ID),
    Payload = #{
        view => #{
            view_id => ?UNDEF_NODE_ID,
            timestamp => 0,
            view_version => 0
        },
        requested_max_references_per_node => 0,
        nodes_to_browse => [#{
            node_id => NodeId,
            reference_type_id => ReferenceTypeId,
            browse_direction => BrowseDirection,
            include_subtypes => IncludeSubtypes,
            node_class_mask => 0,
            result_mask => 16#3F
        }]
    },
    {ok, Request, Channel2, State2} =
        channel_make_request(State, Channel, Conn,
                             ?NID_BROWSE_REQ, Payload),
    {ok, opcua_connection:handle(Conn, Request), [Request], Channel2, State2}.

read(NodeId, Attribs, _Opts, Conn, Channel, #state{status = activated} = State) ->
    %TODO: Add support multi-node batching and options for  age, timestamp and array slicing
    Payload = #{
        max_age => 0,
        timestamps_to_return => source,
        nodes_to_read => [
            #{
                node_id => NodeId,
                attribute_id => opcua_database_attributes:id(Attr),
                index_range => undefined,
                data_encoding => ?UNDEF_QUALIFIED_NAME
            }
        || Attr <- Attribs]
    },
    {ok, Request, Channel2, State2} =
        channel_make_request(State, Channel, Conn,
                             ?NID_READ_REQ, Payload),
    {ok, opcua_connection:handle(Conn, Request), [Request], Channel2, State2}.

close(Conn, Channel, #state{status = activated} = State) ->
    Payload = #{delete_subscriptions => true},
    {ok, Request, Channel2, State2} =
        channel_make_request(State, Channel, Conn,
                             ?NID_CLOSE_SESS_REQ, Payload),
    {ok, [Request], Channel2, State2}.

handle_response(#uacp_message{node_id = NodeId, payload = Payload} = Msg, Conn,
                Channel, #state{status = Status} = State) ->
    Handle = opcua_connection:handle(Conn, Msg),
    handle_response(State, Channel, Conn, Status, Handle, NodeId, Payload).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_response(State, Channel, Conn, creating, _Handle, ?NID_CREATE_SESS_RES, ResPayload) ->
    #{
        authentication_token := AuthToken,
        max_request_message_size := _MaxReqMsgSize,
        revised_session_timeout := _RevisedSessTimeout,
        server_certificate := undefined,
        server_endpoints := _ServerEndpointDescription,
        server_nonce := _ServerNonce,
        server_signature := _ServerSig,
        session_id := SessId
    } = ResPayload,
    State2 = State#state{status = activating, id = SessId, token = AuthToken},
    ReqPayload = #{
        client_signature => #{
            algorithm => <<"http://www.w3.org/2000/09/xmldsig#rsa-sha1">>,
            signature => undefined
        },
        client_software_certificates => [],
        locale_ids => [<<"en">>],
        user_identity_token => #opcua_extension_object{
            type_id = ?NID_ANONYMOUS_IDENTITY_TOKEN,
            encoding = byte_string,
            body = #{
                policy_id => <<"anonymous">>
            }
        },
        user_token_signature => #{
            algorithm => undefined,
            signature => undefined
        }
    },
    {ok, Req, Channel2, State3} =
        channel_make_request(State2, Channel, Conn,
                             ?NID_ACTIVATE_SESS_REQ, ReqPayload),
    {ok, [], [Req], Channel2, State3};
handle_response(State, Channel, Conn, activating, _Handle, ?NID_ACTIVATE_SESS_RES, _Payload) ->
    opcua_connection:notify(Conn, ready),
    {ok, [], [], Channel, State#state{status = activated}};
handle_response(State, Channel, _Conn, activated, Handle, ?NID_BROWSE_RES, Payload) ->
    %TODO: Add support for batching and error handling
    #{results := [#{references := RefDescs}]} = Payload,
    {ok, [{Handle, {ok, RefDescs}}], [], Channel, State};
handle_response(State, Channel, _Conn, activated, Handle, ?NID_READ_RES, Payload) ->
    %TODO: Add support for multi-node batching and error handling
    %TODO: Add option to allow per-attribute error instead of all-or-nothing
    #{results := Results} = Payload,
    {ok, [{Handle, unpack_read_results(Results)}], [], Channel, State};
handle_response(State, Channel, _Conn, activated, _Handle, ?NID_CLOSE_SESS_RES, _Payload) ->
    {closed, Channel, State};
handle_response(State, Channel, _Conn, Status, _Handle, NodeId, Payload) ->
    ?LOG_WARNING("Unexpected message while ~w: ~p ~p", [Status, NodeId, Payload]),
    {ok, [], [], Channel, State}.


%== Channel Module Abstraction Functions =======================================

unpack_read_results(Result) ->
    unpack_read_results(Result, []).

unpack_read_results([], Acc) ->
    {ok, lists:reverse(Acc)};
unpack_read_results([#opcua_data_value{status = good, value = #opcua_variant{value = Value}} | Rest], Acc) ->
    unpack_read_results(Rest, [Value | Acc]);
unpack_read_results([#opcua_data_value{status = good, value = Value} | Rest], Acc) ->
    unpack_read_results(Rest, [Value | Acc]);
unpack_read_results([#opcua_data_value{status = Status} | _], _Acc) ->
    {error, Status}.

channel_make_request(State, Channel, Conn, NodeId, Payload) ->
    {ok, Req, Channel2} =
        opcua_client_channel:make_request(channel_message, NodeId,
                                          Payload, State, Conn, Channel),
    {ok, Req, Channel2, State}.
