-module(codec_binary_tests).

-include_lib("eunit/include/eunit.hrl").
-include("opcua_codec.hrl").

t_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      fun open_secure_channel_request/0,
      fun open_secure_channel_response/0,
      fun create_session_request/0,
      fun create_session_response/0,
      fun activate_session_request/0
      %fun activate_session_response/0,
      %fun read_request/0,
      %fun read_response/0,
      %fun browse_request/0,
      %fun browse_response/0
     ]}.

setup() ->
    {ok, _} = application:ensure_all_started(opcua).

cleanup(_) ->
    ok = application:stop(opcua).

%% This little helper does the major testing:
%%
%% 1. encode some data according to a NodeId
%% 2. decode the result again and check if it
%%    matches the original data
%% 3. decode another example from the python
%%    implementation which encoded the same
%%    data and check if the data matches
%%    again (hex dumps are from wireshark)
%%
%% Why not compare the encoded binaries
%% directly? Because NodeIds can be encoded
%% in different ways and hence you can get
%% binaries holding the same data even though
%% the binaries are not the same!
assert_codec(NodeId, ToBeEncoded, EncodedComp) ->
    {Encoded, _} = opcua_codec_binary:encode(NodeId, ToBeEncoded),
    {Decoded, EmptyBin} = opcua_codec_binary:decode(NodeId, Encoded),
    ?assertEqual(<<>>, EmptyBin),
    ?assertEqual(ToBeEncoded, Decoded),
    BinEncodedComp = opcua_util:hex_to_bin(EncodedComp),
    {DecodedComp, EmptyBin1} = opcua_codec_binary:decode(NodeId, BinEncodedComp),
    ?assertEqual(<<>>, EmptyBin1),
    ?assertEqual(ToBeEncoded, DecodedComp).

open_secure_channel_request() ->
    NodeId = #node_id{value = 444},
    ToBeEncoded = #{
        request_header => #{
          authentication_token => #node_id{value = 0},
          timestamp => 132061913263422630,
          request_handle => 1,
          return_diagnostics => 0,
          audit_entry_id => undefined,
          timeout_hint => 1000,
          additional_header => #extension_object{}
        },
        client_protocol_version => 0,
        request_type => issue,
        security_mode => none,
        client_nonce => <<>>,
        requested_lifetime => 3600000
    },
    Encoded = "0000a6bc6c449c2dd5010100000000000000ffff"
              "ffffe80300000000000000000000000000010000"
              "000000000080ee3600",
    assert_codec(NodeId, ToBeEncoded, Encoded).

open_secure_channel_response() ->
    NodeId = #node_id{value = 447},
    ToBeEncoded = #{
        response_header => #{
          timestamp => 132061913263430080,
          request_handle => 1,
          service_result => 0,
          service_diagnostics => #diagnostic_info{},
          string_table => [],
          additional_header => #extension_object{}
        },
        server_protocol_version => 0,
        security_token => #{
          channel_id => 6,
          token_id => 14,
          created_at => 132061913263429970,
          revised_lifetime => 3600000
        },
        server_nonce => <<>>
    },
    Encoded = "c0d96c449c2dd501010000000000000000000000"
              "0000000000000000060000000e00000052d96c44"
              "9c2dd50180ee360000000000",
    assert_codec(NodeId, ToBeEncoded, Encoded).

create_session_request() ->
    NodeId = #node_id{value = 459},
    ToBeEncoded = #{
        client_certificate => undefined,
        client_description => #{
            application_name => #localized_text{text = <<"Pure Python Client">>},
            application_type => client,
            application_uri => <<"urn:freeopcua:client">>,
            discovery_profile_uri => undefined,
            discovery_urls => [],
            gateway_server_uri => undefined,
            product_uri => <<"urn:freeopcua.github.io:client">>
        },
        client_nonce => opcua_util:hex_to_bin("dcb709b91898921af025dabacbfdcfaa4891d0cd9fe09a3addb2e094db4048dc"),
        endpoint_url => <<"opc.tcp://localhost:4840">>,
        max_response_message_size => 0,
        request_header => #{
            additional_header => #extension_object{},
            audit_entry_id => undefined,
            authentication_token => #node_id{},
            request_handle => 2,
            return_diagnostics => 0,
            timeout_hint => 1000,
            timestamp => 132061913263439110
        },
        requested_session_timeout => 3600000.0,
        server_uri => undefined,
        session_name => <<"Pure Python Client Session1">>
     },
    Encoded = "000006fd6c449c2dd5010200000000000000ffff"
              "ffffe80300000000001400000075726e3a667265"
              "656f706375613a636c69656e741e00000075726e"
              "3a667265656f706375612e6769746875622e696f"
              "3a636c69656e7402120000005075726520507974"
              "686f6e20436c69656e7401000000ffffffffffff"
              "ffff00000000ffffffff180000006f70632e7463"
              "703a2f2f6c6f63616c686f73743a343834301b00"
              "00005075726520507974686f6e20436c69656e74"
              "2053657373696f6e3120000000dcb709b9189892"
              "1af025dabacbfdcfaa4891d0cd9fe09a3addb2e0"
              "94db4048dcffffffff0000000040774b41000000"
              "00",
    assert_codec(NodeId, ToBeEncoded, Encoded).

create_session_response() ->
    NodeId = #node_id{value = 462},
    ToBeEncoded = #{
        authentication_token => #node_id{value = 1001},
        max_request_message_size => 65536,
        response_header => #{
            additional_header => #extension_object{},
            request_handle => 2,
            service_diagnostics => #diagnostic_info{},
            service_result => 0,
            string_table => [],
            timestamp => 132061913263448480
        },
        revised_session_timeout => 3600000.0,
        server_certificate => undefined,
        server_endpoints => [#{
            endpoint_url => <<"opc.tcp://127.0.0.1:4840/freeopcua/server/">>,
            security_level => 0,
            security_mode => none,
            security_policy_uri => <<"http://opcfoundation.org/UA/SecurityPolicy#None">>,
            server => #{
                application_name => #localized_text{text = <<"FreeOpcUa Python Server">>},
                application_type => client_and_server,
                application_uri => <<"urn:freeopcua:python:server">>,
                discovery_profile_uri => undefined,
                discovery_urls => [<<"opc.tcp://0.0.0.0:4840/freeopcua/server/">>],
                gateway_server_uri => undefined,
                product_uri => <<"urn:freeopcua.github.io:python:server">>
            },
            server_certificate => undefined,
            transport_profile_uri => <<"http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary">>,
            user_identity_tokens => [
                #{
                    issued_token_type => undefined,
                    issuer_endpoint_url => undefined,
                    security_policy_uri => undefined,
                    policy_id => <<"anonymous">>,
                    token_type => anonymous
                },
                #{
                    issued_token_type => undefined,
                    issuer_endpoint_url => undefined,
                    security_policy_uri => undefined,
                    policy_id => <<"certificate_basic256sha256">>,
                    token_type => certificate
                },
                #{
                    issued_token_type => undefined,
                    issuer_endpoint_url => undefined,
                    security_policy_uri => undefined,
                    policy_id => <<"username">>,
                    token_type => user_name
                }
            ]
        }],
        server_nonce => opcua_util:hex_to_bin("68924a95b8434526a36cb9373085289748b9dd60fbdff38153339e7844ef8c14"),
        server_signature => #{
            algorithm => <<"http://www.w3.org/2000/09/xmldsig#rsa-sha1">>,
            signature => <<>>
        },
        server_software_certificates => [],
        session_id => #node_id{value = 11}
    },
    Encoded = "a0216d449c2dd501020000000000000000000000"
              "000000000200000b000000020000e90300000000"
              "000040774b412000000068924a95b8434526a36c"
              "b9373085289748b9dd60fbdff38153339e7844ef"
              "8c14ffffffff010000002a0000006f70632e7463"
              "703a2f2f3132372e302e302e313a343834302f66"
              "7265656f706375612f7365727665722f1b000000"
              "75726e3a667265656f706375613a707974686f6e"
              "3a7365727665722500000075726e3a667265656f"
              "706375612e6769746875622e696f3a707974686f"
              "6e3a7365727665720217000000467265654f7063"
              "556120507974686f6e2053657276657202000000"
              "ffffffffffffffff01000000280000006f70632e"
              "7463703a2f2f302e302e302e303a343834302f66"
              "7265656f706375612f7365727665722fffffffff"
              "010000002f000000687474703a2f2f6f7063666f"
              "756e646174696f6e2e6f72672f55412f53656375"
              "72697479506f6c696379234e6f6e650300000009"
              "000000616e6f6e796d6f757300000000ffffffff"
              "ffffffffffffffff1a0000006365727469666963"
              "6174655f62617369633235367368613235360200"
              "0000ffffffffffffffffffffffff080000007573"
              "65726e616d6501000000ffffffffffffffffffff"
              "ffff41000000687474703a2f2f6f7063666f756e"
              "646174696f6e2e6f72672f55412d50726f66696c"
              "652f5472616e73706f72742f75617463702d7561"
              "73632d756162696e61727900000000002a000000"
              "687474703a2f2f7777772e77332e6f72672f3230"
              "30302f30392f786d6c64736967237273612d7368"
              "61310000000000000100",
    assert_codec(NodeId, ToBeEncoded, Encoded).

activate_session_request() ->
    NodeId = #node_id{value = 465},
    ToBeEncoded = #{
        client_signature => #{
            algorithm => <<"http://www.w3.org/2000/09/xmldsig#rsa-sha1">>,
            signature => <<>>
        },
        client_software_certificates => [],
        locale_ids => [<<"en">>],
        request_header => #{
            additional_header => #extension_object{},
            audit_entry_id => undefined,
            authentication_token => #node_id{value = 1001},
            request_handle => 3,
            return_diagnostics => 0,
            timeout_hint => 1000,
            timestamp => 132061913263467530
        },
        user_identity_token => #extension_object{
                                type_id = #node_id{value = 319},
                                encoding = byte_string,
                                body = #{policy_id => <<"anonymous">>}
                               },
        user_token_signature => #{
            algorithm => undefined,
            signature => undefined
        }
    },
    Encoded = "020000e90300000a6c6d449c2dd5010300000000"
              "000000ffffffffe80300000000002a0000006874"
              "74703a2f2f7777772e77332e6f72672f32303030"
              "2f30392f786d6c64736967237273612d73686131"
              "00000000000000000100000002000000656e0100"
              "4101010d00000009000000616e6f6e796d6f7573"
              "ffffffffffffffff",
    assert_codec(NodeId, ToBeEncoded, Encoded).
