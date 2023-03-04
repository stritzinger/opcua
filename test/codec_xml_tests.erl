-module(codec_xml_tests).

-include_lib("eunit/include/eunit.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").

-define(assertDecode(T, O, I),
    ?assertEqual(O, opcua_codec_xml:decode(T, I))).
-define(assertDecodeValue(T, O, I),
    ?assertEqual(O, opcua_codec_xml:decode_value(T, I))).

t_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        fun decode_numbers/0,
        fun decode_number_values/0,
        fun decode_number_value_lists/0,
        fun decode_localized_text/0,
        fun decode_node_id/0,
        fun decode_expanded_node_id/0
     ]}.

setup() ->
    {ok, Apps} = application:ensure_all_started(opcua),
    Apps.

cleanup(Apps) ->
    opcua_test_util:without_error_logger(fun() ->
        [ok = application:stop(A) || A <- Apps]
    end).

decode_numbers() ->
    ?assertDecode(byte, 0, [<<"0">>]),
    ?assertDecode(sbyte, -10, [<<"-10">>]),
    ?assertDecode(uint16, 60000, [<<"60000">>]),
    %TODO: Write more test for all the types of numbers
    ok.

decode_number_values() ->
    ?assertDecodeValue(byte, 0, [{<<"Byte">>, #{}, [<<"0">>]}]),
    ?assertDecodeValue(undefined, 42, [{<<"Byte">>, #{}, [<<"42">>]}]),
    ?assertDecodeValue(int32, -1000, [{<<"Int32">>, #{}, [<<"-1000">>]}]),
    ?assertDecodeValue(undefined, 10000, [{<<"Int32">>, #{}, [<<"10000">>]}]),
    %TODO: Write more test for all the types of numbers
    ok.

decode_number_value_lists() ->
    ?assertDecodeValue(undefined, [], [{<<"ListOfByte">>, #{}, []}]),
    ?assertDecodeValue(byte, [], [{<<"ListOfByte">>, #{}, []}]),
    ?assertDecodeValue(undefined, [0, 42],
                       [{<<"ListOfByte">>, #{}, [
                            {<<"Byte">>, #{}, [<<"0">>]},
                            {<<"Byte">>, #{}, [<<"42">>]}]}]),
    ?assertDecodeValue(byte, [0, 42],
                       [{<<"ListOfByte">>, #{}, [
                            {<<"Byte">>, #{}, [<<"0">>]},
                            {<<"Byte">>, #{}, [<<"42">>]}]}]),
    %TODO: Write more test for all the types of numbers
    ok.

decode_localized_text() ->
    ?assertDecode(localized_text, #opcua_localized_text{}, []),
    ?assertDecode(localized_text, #opcua_localized_text{text = <<"foo">>},
                  [{<<"Text">>, #{}, [<<"foo">>]}]),
    ?assertDecode(localized_text, #opcua_localized_text{locale = <<"en">>},
                  [{<<"Locale">>, #{}, [<<"en">>]}]),
    ?assertDecode(localized_text, #opcua_localized_text{locale = <<"en">>, text = <<"foo">>},
                  [{<<"Locale">>, #{}, [<<"en">>]},
                    {<<"Text">>, #{}, [<<"foo">>]}]),
    ok.

decode_node_id() ->
    ?assertDecode(node_id, ?NNID(123),
                  [{<<"Identifier">>, #{}, [<<"i=123">>]}]),
    ?assertDecode(node_id, ?NNID(42, 123),
                  [{<<"Identifier">>, #{}, [<<"ns=42;i=123">>]}]),
    ?assertDecode(node_id, #opcua_node_id{type = string, value = <<"Test">>},
                  [{<<"Identifier">>, #{}, [<<"s=Test">>]}]),
    ?assertDecode(node_id, #opcua_node_id{ns = 42, type = string, value = <<"Test">>},
                  [{<<"Identifier">>, #{}, [<<"ns=42;s=Test">>]}]),
    ok.

decode_expanded_node_id() ->
    ?assertDecode(expanded_node_id, ?XID(?NNID(123)),
                  [{<<"Identifier">>, #{}, [<<"i=123">>]}]),
    ?assertDecode(expanded_node_id, ?XID(?NNID(42, 123)),
                  [{<<"Identifier">>, #{}, [<<"ns=42;i=123">>]}]),
    ?assertDecode(expanded_node_id, ?XID(#opcua_node_id{type = string, value = <<"Test">>}),
                  [{<<"Identifier">>, #{}, [<<"s=Test">>]}]),
    ?assertDecode(expanded_node_id, ?XID(#opcua_node_id{ns = 42, type = string, value = <<"Test">>}),
                  [{<<"Identifier">>, #{}, [<<"ns=42;s=Test">>]}]),
    ?assertDecode(expanded_node_id, ?XID(18, ?NNID(123)),
                  [{<<"Identifier">>, #{}, [<<"svr=18;i=123">>]}]),
    ok.