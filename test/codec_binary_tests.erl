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
      fun create_session_request/0
      %fun create_session_response/0,
      %fun activate_session_request/0,
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

%% some little helper
assert_codec(NodeId, ToBeEncoded, Encoded) ->
    {Bin, _} = opcua_codec_binary:encode(NodeId, ToBeEncoded),
    ?assertEqual(Encoded, string:to_lower(opcua_util:bin_to_hex(Bin))),
    {Decoded, EmptyBin} = opcua_codec_binary:decode(NodeId, Bin),
    ?assertEqual(<<>>, EmptyBin),
    ?assertEqual(ToBeEncoded, Decoded).

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
