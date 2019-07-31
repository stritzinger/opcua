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
    #uacp_message{node_id = NodeSpec} = Req,
    case opcua_database:lookup_id(NodeSpec) of
        %% CreateSessionRequest
        #node_id{value = 459} ->
            case create_session(Data, Conn, Req) of
                {error, _Reason} = Error -> Error;
                {ok, Resp, Data2} ->
                    {next_state, created, Data2, [
                        {reply, From, {created, Resp}},
                        {state_timeout, 10000, terminate}
                    ]}
            end;
        #node_id{value = Num} ->
            ?LOG_ERROR("Unexpected message ~w in state 'started': ~p",
                       [Num, Req]),
            {error, 'Bad_RequestNotAllowed'}
    end.

created(state_timeout, terminate, _Data) -> {stop, timeout};
created({call, From}, {request, Conn, Req}, Data) ->
    #uacp_message{node_id = NodeSpec} = Req,
    case opcua_database:lookup_id(NodeSpec) of
        %% ActivateSessionRequest
        #node_id{value = 465} ->
            case activate_session(Data, Conn, Req) of
                {error, _Reason} = Error -> Error;
                {ok, Resp, Data2} ->
                    {next_state, bound, Data2, [
                        {reply, From, {bound, Resp}}
                    ]}
            end;
        #node_id{value = Num} ->
            ?LOG_ERROR("Unexpected message ~w in state 'created': ~p",
                       [Num, Req]),
            {error, 'Bad_RequestNotAllowed'}
    end.

bound({call, From}, {request, _Conn, _Req}, Data) ->
    Result = {error, 'Bad_RequestNotAllowed'},
    {keep_state, Data, [{reply, From, Result}]}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_session(Data, _Conn, #uacp_message{payload = Msg} = Req) ->
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
    Resp = opcua_protocol:req2res(Req, 462, #{
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
    {ok, Resp, Data2}.

activate_session(_Data, _Conn, Req) ->
    ?LOG_DEBUG(">>>>>>>>> ~p", [Req]),
    {error, 'Bad_NotImplemented'}.
