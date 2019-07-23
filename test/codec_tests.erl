-module(codec_tests).

-include_lib("eunit/include/eunit.hrl").
-include("opcua_codec.hrl").

structure_test() ->
    InnerField = #structure{
                   node_id = #node_id{type = numeric, value = 100},
                   with_options = false,
                   fields = [#field{
                               name = some_boolean,
                               node_id = #node_id{type = numeric, value = 1},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined}]},
    Structure = #structure{
                   node_id = #node_id{type = numeric, value = 101},
                   with_options = false,
                   fields = [#field{
                               name = some_integer,
                               node_id = #node_id{type = string, value = int32},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined},
                             #field{
                               name = inner_field,
                               node_id = #node_id{type = numeric, value = 100},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined}]},
    %% some setup for things we don't have yet 
    meck:new(opcua_schema, [non_strict]),
    meck:expect(opcua_schema, resolve, fun(#node_id{value = 101}) -> Structure;
                                          (#node_id{value = 100}) -> InnerField end),
    NodeId = #node_id{type = numeric, value = 101},
    ToBeEncoded = #{some_integer => 1, inner_field => #{some_boolean => true}},
    Bin = opcua_codec_binary:encode(NodeId, ToBeEncoded),
    {Decoded, Bin1} = opcua_codec_binary:decode(NodeId, Bin),
    ?assertEqual(ToBeEncoded, Decoded),
    ?assertEqual(<<>>, Bin1),
    meck:unload(opcua_schema).


structure_with_options_test() ->
    Structure = #structure{
                   node_id = #node_id{type = numeric, value = 101},
                   with_options = true,
                   fields = [#field{
                               name = some_integer,
                               node_id = #node_id{type = string, value = int32},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined},
                             #field{
                               name = some_string,
                               node_id = #node_id{type = numeric, value = 12},
                               value_rank = -1,
                               is_optional = true,
                               value = 1}]},
    %% some setup for things we don't have yet 
    meck:new(opcua_schema, [non_strict]),
    meck:expect(opcua_schema, resolve, fun(#node_id{value = 101}) -> Structure end),
    NodeId = #node_id{type = numeric, value = 101},

    %% check WITHOUT optional field
    ToBeEncodedWithoutOptions = #{some_integer => 1},
    BinWithoutOptions = opcua_codec_binary:encode(NodeId, ToBeEncodedWithoutOptions),
    {DecodedWithoutOptions, Rest} = opcua_codec_binary:decode(NodeId, BinWithoutOptions),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(ToBeEncodedWithoutOptions, DecodedWithoutOptions),

    %% check WITH optional field
    ToBeEncodedWithOptions = #{some_integer => 1, some_string => <<"Hello!">>},
    BinWithOptions = opcua_codec_binary:encode(NodeId, ToBeEncodedWithOptions),
    {DecodedWithOptions, Rest1} = opcua_codec_binary:decode(NodeId, BinWithOptions),
    ?assertEqual(<<>>, Rest1),
    ?assertEqual(ToBeEncodedWithOptions, DecodedWithOptions),
    meck:unload(opcua_schema).

enum_test() ->
    Enum = #enum{
               node_id = #node_id{type = numeric, value = 101},
               fields = [#field{
                           name = field_1,
                           value = 1},
                         #field{
                           name = field_2,
                           value = 2}]},
    %% some setup for things we don't have yet 
    meck:new(opcua_schema, [non_strict]),
    meck:expect(opcua_schema, resolve, fun(#node_id{value = 101}) -> Enum end),
    NodeId = #node_id{type = numeric, value = 101},
    ToBeEncoded = #{name => field_2},
    Bin = opcua_codec_binary:encode(NodeId, ToBeEncoded),
    {Decoded, Bin1} = opcua_codec_binary:decode(NodeId, Bin),
    ?assertEqual(ToBeEncoded, Decoded),
    ?assertEqual(<<>>, Bin1),
    meck:unload(opcua_schema).

union_test() ->
    Union = #union{
                node_id = #node_id{type = numeric, value = 101},
                fields = [#field{
                            name = some_integer,
                            node_id = #node_id{type = string, value = int32},
                            value_rank = -1,
                            is_optional = true,
                            value = 1},
                          #field{
                            name = some_string,
                            node_id = #node_id{type = numeric, value = 12},
                            value_rank = -1,
                            is_optional = true,
                            value = 2}]},
    %% some setup for things we don't have yet 
    meck:new(opcua_schema, [non_strict]),
    meck:expect(opcua_schema, resolve, fun(#node_id{value = 101}) -> Union end),
    NodeId = #node_id{type = numeric, value = 101},
    ToBeEncoded = #{some_string => <<"Hello!">>},
    Bin = opcua_codec_binary:encode(NodeId, ToBeEncoded),
    {Decoded, Bin1} = opcua_codec_binary:decode(NodeId, Bin),
    ?assertEqual(ToBeEncoded, Decoded),
    ?assertEqual(<<>>, Bin1),
    meck:unload(opcua_schema).
