-module(codec_binary_tests).

-include_lib("eunit/include/eunit.hrl").
-include("opcua_codec.hrl").

t_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      fun open_secure_channel_request/0,
      fun open_secure_channel_response/0
      %fun create_session_request/0,
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
