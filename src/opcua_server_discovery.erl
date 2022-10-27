-module(opcua_server_discovery).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([handle_request/2]).

% Utility functions
-export([format_endopoints/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_request(Conn, #uacp_message{node_id = #opcua_node_id{value = 426}} = Req) ->
    #uacp_message{payload = #{
        endpoint_url := EndpointUrl,
        locale_ids := _LocalIds,
        profile_uris := _ProfileUris
    }} = Req,
    %TODO: Maybe check the request header,
    %      validate that the EndpointUrl is correct...
    Endpoints = format_endopoints(
                                opcua_security:supported_endpoints(EndpointUrl)),
    Resp = opcua_connection:response(Conn, Req, 429, #{
        endpoints => Endpoints
    }),
    {reply, Resp, Conn};
handle_request(_Conn, _Req) ->
    {error, bad_not_implemented}.

format_endopoints(EndpointSettings) ->
    lists:map(fun endpoint_description/1, EndpointSettings).


endpoint_description({Url, ServerCert, SecurityLevel, SecurityMode, Policy, Tokens}) ->
    #{
        endpoint_url => Url,
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
        server_certificate => ServerCert,
        security_mode => SecurityMode,
        security_policy_uri => opcua_util:policy_uri(Policy),
        user_identity_tokens => [format_token(Type, SecurityMode, Policy)
                                                            || Type <- Tokens],
        transport_profile_uri => ?TRANSPORT_PROFILE_BINARY,
        security_level => SecurityLevel
    }.

format_token(TokenType, SecurityMode, Policy) ->
    #{
        policy_id => list_to_binary(
                        string:join([
                                atom_to_list(TokenType),
                                atom_to_list(SecurityMode),
                                atom_to_list(Policy)], "-")),
        token_type => TokenType,
        issued_token_type => undefined,
        issuer_endpoint_url => undefined,
        security_policy_uri => opcua_util:policy_uri(Policy)
    }.
