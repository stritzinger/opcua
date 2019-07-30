-module(opcua_session_manager).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua_codec.hrl").
-include("opcua_protocol.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/1]).
-export([handle_request/1]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).


%%% MACRO %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    next_session_id = 1 :: pos_integer(),
    sessions = #{} :: #{binary() => pid()}
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

handle_request(#uacp_message{node_id = NodeSpec} = Req) ->
    case opcua_database:lookup_id(NodeSpec) of
        %% CreateSessionRequest
        #node_id{value = 459} ->
            %TODO: Probably check the request header...
            gen_server:call(?SERVER, {create_session, Req});
        %% ActivateSessionRequest
        #node_id{value = 465} ->
            %TODO: Probably check the request header, and do some security checks
            %      It should be enforced that it is called in the same secure
            %      channel as the corresponding CreateSesionRequest
            gen_server:call(?SERVER, {activate_session, Req});
        #node_id{value = Num} ->
            ?LOG_DEBUG("Unexpected OPCUA request ~w: ~p", [Num, Req]),
            {error, 'Bad_RequestNotAllowed'}
    end.


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Opts) ->
    ?LOG_DEBUG("OPCUA session manager process starting with options: ~p", [Opts]),
    {ok, #state{}}.

handle_call({create_session, Req}, _From, State) ->
    case create_session(State, Req) of
        {error, _Reason} = Error -> {reply, Error, State};
        {ok, Resp, State2} -> {reply, {reply, Resp}, State2}
    end;
handle_call({activate_session, Req}, _From, State) ->
    case activate_session(State, Req) of
        {error, _Reason} = Error -> {reply, Error, State};
        {ok, Resp, SessPid, State2} -> {reply, {bind, Resp, SessPid}, State2}
    end;
handle_call(Req, From, State) ->
    ?LOG_WARNING("Unexpected gen_server call from ~p: ~p", [From, Req]),
    {reply, {error, unexpected_call}, State}.

handle_cast(Req, State) ->
    ?LOG_WARNING("Unexpected gen_server cast: ~p", [Req]),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_WARNING("Unexpected gen_server message: ~p", [Msg]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_DEBUG("OPCUA registry process terminating: ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

generate_session_auth_token() ->
    #node_id{type = opaque, value = crypto:strong_rand_bytes(32)}.

next_session_node_id(#state{next_session_id = Id} = State) ->
    {#node_id{ns = 232, value = Id}, State#state{next_session_id = Id + 1}}.

create_session(#state{sessions = Sessions} = State,
               #uacp_message{payload = Msg} = Req) ->
    %TODO: We need some imformation about the security channel from the protocol
    %      like the transport uri, channel id, the policy URL...
    %      For now it is all hardcoded.
    SessReqOpts = maps:with([
        client_certificate,
        client_description,
        client_nonce,
        endpoint_url,
        max_response_message_size,
        requested_session_timeout,
        session_name
    ], Msg),
    AuthToken = generate_session_auth_token(),
    {SessNodeId, State2} = next_session_node_id(State),
    ServerNonce = opcua_util:nonce(),
    SessOpts = SessReqOpts#{
        auth_token => AuthToken,
        node_id => SessNodeId,
        server_nonce => ServerNonce
    },
    case opcua_session_sup:start_session(SessOpts) of
        {error, _Reason} = Error -> Error;
        {ok, SessPid} ->
            Resp = opcua_protocol:req2res(Req, 462, #{
                session_id => SessNodeId,
                authentication_token => AuthToken,
                revised_session_timeout => maps:get(requested_session_timeout, SessOpts),
                server_nonce => ServerNonce,
                server_certificate => undefined,
                server_endpoints => [
                    #{
                        endpoint_url => maps:get(endpoint_url, SessOpts),
                        server => #{
                            application_uri => <<"urn:stritzinger:opcua:erlang:server">>,
                            product_uri => <<"urn:stritzinger.com:opcua:erlang:server">>,
                            application_name => <<"Stritzinger GmbH OPCUA Server">>,
                            application_type => #{name => server},
                            gateway_server_uri => undefined,
                            discovery_profile_uri => undefined,
                            discovery_urls => [
                                <<"opc.tcp://0.0.0.0:4840">>
                            ]
                        },
                        server_certificate => undefined,
                        security_mode => #{name => none},
                        security_policy_uri => <<"http://opcfoundation.org/UA/SecurityPolicy#None">>,
                        user_identity_tokens => [
                            #{
                                policy_id => <<"anonymous">>,
                                token_type => #{name => anonymous},
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
            {ok, Resp, State2#state{sessions = Sessions#{AuthToken => SessPid}}}
    end.

activate_session(_State, _Msg) ->
    {error, 'Bad_NotImplemented'}.
