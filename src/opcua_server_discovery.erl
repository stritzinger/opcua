-module(opcua_server_discovery).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([handle_request/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_request(Conn, #uacp_message{node_id = #opcua_node_id{value = 426}} = Req) ->
    #uacp_message{payload = #{
        endpoint_url := EndpointUrl,
        locale_ids := _LocalIds,
        profile_uris := _ProfileUris
    }} = Req,
    %TODO: Maybe check the request header,
    %      validate that the EndpointUrl is correct...
    Resp = opcua_connection:response(Conn, Req, 429, #{
        endpoints => [ %% Duplicated in opcua_server_session
            #{
                endpoint_url => EndpointUrl,
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
        ]
    }),
    {reply, Resp};
handle_request(_Conn, _Req) ->
    {error, bad_not_implemented}.
