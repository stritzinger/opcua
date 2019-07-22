-module(codec_tests).

-include_lib("eunit/include/eunit.hrl").
-include("opcua_codec.hrl").

basic_test() ->
    %% schema for a structure which only contains one int32 field
    Structure = #structure{
                   node_id = #node_id{type = numeric, value = 0},
                   with_options = false,
                   fields = [#field{
                               name = some_integer,
                               node_id = #node_id{type = numeric, value = 6},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined},
                             #field{
                               name = some_string,
                               node_id = #node_id{type = numeric, value = 12},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined}]},

    %% some setup for things we don't have yet 
    meck:new(opcua_schema, [non_strict]),
    meck:expect(opcua_schema, resolve, fun(#node_id{value = 0}) -> Structure end),

    %% the node id of the structure
    NodeId = #node_id{type = numeric, value = 0},

    %% encode the structure
    ToBeEncoded = #{some_integer => 1, some_string => <<"Hello!">>},
    Bin = opcua_codec_binary:encode(NodeId, ToBeEncoded),

    %% decode and check
    {Decoded, Bin1} = opcua_codec_binary:decode(NodeId, Bin),
    ?assertEqual(ToBeEncoded, Decoded),
    ?assertEqual(<<>>, Bin1),

    meck:unload(opcua_schema).
