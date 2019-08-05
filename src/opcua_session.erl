-module(opcua_session).

-behaviour(gen_statem).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua_codec.hrl").
-include("opcua_protocol.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/2]).
-export([handle_request/3]).

%% Behaviour gen_statem callback functions
-export([init/1]).
-export([callback_mode/0]).
-export([terminate/3]).

%% Behaviour gen_statem state functions
-export([started/3]).
-export([created/3]).
-export([bound/3]).


%%% MACRO %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(data, {
    session_id :: node_id(),
    auth_token :: node_id(),
    session_name :: undefined | binary(),
    endpoint_url :: undefined | binary(),
    client_certificate :: undefined | binary(),
    client_description :: undefined | binary(),
    client_nonce :: undefined | binary(),
    server_nonce :: undefined | binary(),
    max_response_message_size :: undefined | non_neg_integer(),
    requested_session_timeout :: undefined | non_neg_integer(),
    ident :: undefined | binary(),
    local_ids :: undefined | [binary()],
    conn :: undefined | opcua_protocol:connection()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(SessId, AuthToken) ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [SessId, AuthToken], []).

handle_request(Conn, #uacp_message{} = Req, SessPid) ->
    gen_statem:call(SessPid, {request, Conn, Req}).


%%% BEHAVIOUR gen_statem CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([SessId, AuthToken]) ->
    ?LOG_DEBUG("OPCUA session ~w process starting", [SessId]),
    Data = #data{session_id = SessId, auth_token = AuthToken},
    {ok, started, Data, [{state_timeout, 1000, terminate}]}.

callback_mode() -> state_functions.

terminate(Reason, _State, _Data) ->
    ?LOG_DEBUG("OPCUA registry process terminating: ~p", [Reason]),
    ok.

%%% BEHAVIOUR gen_statem STATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

started(state_timeout, terminate, _Data) -> {stop, timeout};
started({call, From}, {request, Conn, Req}, Data) ->
    dispatch_request(Data, started, From, Conn, Req, [], #{
        459 => {next_state, created, fun session_create_command/3}
    }, 'Bad_RequestNotAllowed', [{state_timeout, 10000, terminate}]).

created(state_timeout, terminate, _Data) -> {stop, timeout};
created({call, From}, {request, Conn, Req}, Data) ->
    dispatch_request(Data, created, From, Conn, Req, [fun validate_auth/2], #{
        465 => {next_state, bound, fun session_activate_command/3}
    }, 'Bad_SessionNotActivated', []).

bound({call, From}, {request, Conn, Req}, Data) ->
    dispatch_request(Data, bound, From, Conn, Req, [fun validate_auth/2], #{
    }, 'Bad_RequestNotAllowed', []).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dispatch_request(Data, State, From, Conn, Req, Validators, HandlerMap, Reason, Extra) ->
    case validate_request(Data, Req, Validators) of
        {error, _Reason} = Error ->
            {keep_state, Data, [{reply, From, Error} | Extra]};
        ok ->
            #uacp_message{node_id = NodeSpec} = Req,
            #node_id{value = Num} = opcua_database:lookup_id(NodeSpec),
            case maps:find(Num, HandlerMap) of
                error ->
                    ?LOG_ERROR("Unexpected request ~w in state ~w: ~p",
                               [Num, State, Req]),
                    {keep_state, Data, [{reply, From, {error, Reason}} | Extra]};
                {ok, Spec} ->
                    dispatch_request(Data, Spec, From, Conn, Req, Extra)
            end
    end.

dispatch_request(Data, {keep_state, Fun}, From, Conn, Req, Extra) ->
    case Fun(Data, Conn, Req) of
        {error, _Reason} = Error ->
            {keep_state, Data, [{reply, From, Error} | Extra]};
        {Result, Data2} ->
            {keep_state, Data2, [{reply, From, Result} | Extra]}
    end;
dispatch_request(Data, {next_state, NextState, Fun}, From, Conn, Req, Extra) ->
    case Fun(Data, Conn, Req) of
        {error, _Reason} = Error ->
            {keep_state, Data, [{reply, From, Error} | Extra]};
        {Result, Data2} ->
            {next_state, NextState, Data2,
             [{reply, From, Result} | Extra]}
    end.

validate_request(_Data, _Req, []) -> ok;
validate_request(Data, Req, [Fun | Rest]) ->
    case Fun(Data, Req) of
        {error, _Reason} = Error -> Error;
        ok -> validate_request(Data, Req, Rest)
    end.

validate_auth(#data{auth_token = AuthToken},
              #uacp_message{payload = #{
                request_header := #{authentication_token := AuthToken}}}) ->
    ok;
validate_auth(_State, _Headers) ->
    {error, 'Bad_SessionIdInvalid'}.


%-- SESSION SERVICE SET --------------------------------------------------------

session_create_command(Data, Conn, #uacp_message{payload = Msg} = Req) ->
    %TODO: Probably check the request header...
    %TODO: We need some imformation about the security channel from the protocol
    %      like the transport uri, channel id, the policy URL...
    %      For now it is all hardcoded.
    #{
        client_certificate := ClientCertificate,
        client_description := ClientDescription,
        client_nonce := ClientNonce,
        endpoint_url := EndpointURL,
        max_response_message_size := MaxResponseMessageSize,
        requested_session_timeout := RequestedSessionTimeout,
        session_name := SessionName
    } = Msg,
    ServerNonce = opcua_util:nonce(),
    #data{
        session_id = SessId,
        auth_token = AuthToken
    } = Data,
    Data2 = Data#data{
        client_certificate = ClientCertificate,
        client_description = ClientDescription,
        client_nonce = ClientNonce,
        server_nonce = ServerNonce,
        endpoint_url = EndpointURL,
        max_response_message_size = MaxResponseMessageSize,
        requested_session_timeout = RequestedSessionTimeout,
        session_name = SessionName
    },
    Resp = opcua_connection:req2res(Conn, Req, 462, #{
        session_id => SessId,
        authentication_token => AuthToken,
        revised_session_timeout => RequestedSessionTimeout,
        server_nonce => ServerNonce,
        server_certificate => undefined,
        server_endpoints => [
            #{
                endpoint_url => EndpointURL,
                server => #{
                    application_uri => <<"urn:stritzinger:opcua:erlang:server">>,
                    product_uri => <<"urn:stritzinger.com:opcua:erlang:server">>,
                    application_name => <<"Stritzinger GmbH OPCUA Server">>,
                    application_type => server,
                    gateway_server_uri => undefined,
                    discovery_profile_uri => undefined,
                    discovery_urls => [
                        <<"opc.tcp://0.0.0.0:4840">>
                    ]
                },
                server_certificate => undefined,
                security_mode => none,
                security_policy_uri => <<"http://opcfoundation.org/UA/SecurityPolicy#None">>,
                user_identity_tokens => [
                    #{
                        policy_id => <<"anonymous">>,
                        token_type => anonymous,
                        issued_token_type => undefined,
                        issuer_endpoint_url => undefined,
                        security_policy_uri => undefined
                    }
                ],
                transport_profile_uri => <<"http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary">>,
                security_level => 0
            }
        ],
        server_software_certificates => [],
        server_signature => #{
            algorithm => <<"http://www.w3.org/2000/09/xmldsig#rsa-sha1">>,
            signature => undefined
        },
        max_request_message_size => 0
    }),
    {{created, Resp}, Data2}.

session_activate_command(Data, Conn, #uacp_message{payload = Msg} = Req) ->
    #{
        locale_ids := LocalIds,
        user_identity_token := IdentTokenExtObj
    } = Msg,
    case check_identity(IdentTokenExtObj) of
        {error, _Reason} = Error -> Error;
        {ok, Ident} ->
            ServerNonce = opcua_util:nonce(),
            Data2 = Data#data{
                server_nonce = ServerNonce,
                ident = Ident,
                local_ids = LocalIds,
                conn = Conn
            },
            Resp = opcua_connection:req2res(Conn, Req, 468, #{
                server_nonce => ServerNonce,
                results => [],
                diagnostic_infos => []
            }),
            {{bound, Resp}, Data2}
    end.

check_identity(ExtObj) ->
    #extension_object{type_id = NodeSpec, body = Body} = ExtObj,
    case opcua_database:lookup_id(NodeSpec) of
        #node_id{value = 319} -> %% AnonymousIdentityToken
            #{policy_id := PolicyId} = Body,
            {ok, PolicyId};
        _ ->
            {error, 'Bad_UserAccessDenied'}
    end.
