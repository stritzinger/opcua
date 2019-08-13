-module(codec_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-include("opcua_database.hrl").
-include("opcua_codec.hrl").

t_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [fun structure/0,
      fun structure_with_options/0,
      fun enum/0,
      fun union/0,
      fun builtin/0]}.

setup() ->
    meck:new(opcua_database, [non_strict]).

cleanup(_) ->
    meck:unload(opcua_database).

%% some little helper
assert_codec(NodeId, ToBeEncoded) ->
    {Bin, _} = opcua_codec_binary:encode(NodeId, ToBeEncoded),
    {Decoded, Bin1} = opcua_codec_binary:decode(NodeId, Bin),
    ?assertEqual(ToBeEncoded, Decoded),
    ?assertEqual(<<>>, Bin1).

structure() ->
    InnerField = #structure{
                   node_id = #opcua_node_id{type = numeric, value = 100},
                   with_options = false,
                   fields = [#field{
                               name = some_boolean,
                               node_id = #opcua_node_id{type = numeric, value = 1},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined}]},
    Structure = #structure{
                   node_id = #opcua_node_id{type = numeric, value = 101},
                   with_options = false,
                   fields = [#field{
                               name = some_integer,
                               node_id = #opcua_node_id{type = string, value = int32},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined},
                             #field{
                               name = inner_field,
                               node_id = #opcua_node_id{type = numeric, value = 100},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined}]},
    meck:expect(opcua_database, lookup_schema,
        fun(#opcua_node_id{value = 101}) -> Structure;
           (#opcua_node_id{value = 100}) -> InnerField end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = #{some_integer => 1, inner_field => #{some_boolean => true}},
    assert_codec(NodeId, ToBeEncoded).

structure_with_options() ->
    Structure = #structure{
                   node_id = #opcua_node_id{type = numeric, value = 101},
                   with_options = true,
                   fields = [#field{
                               name = some_integer,
                               node_id = #opcua_node_id{type = string, value = int32},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined},
                             #field{
                               name = some_string,
                               node_id = #opcua_node_id{type = numeric, value = 12},
                               value_rank = -1,
                               is_optional = true,
                               value = 1}]},
    meck:expect(opcua_database, lookup_schema,
        fun(#opcua_node_id{value = 101}) -> Structure end),
    NodeId = #opcua_node_id{type = numeric, value = 101},

    %% check WITHOUT optional field
    ToBeEncodedWithoutOptions = #{some_integer => 1},
    assert_codec(NodeId, ToBeEncodedWithoutOptions),

    %% check WITH optional field
    ToBeEncodedWithOptions = #{some_integer => 1, some_string => <<"Hello!">>},
    assert_codec(NodeId, ToBeEncodedWithOptions).

enum() ->
    Enum = #enum{
               node_id = #opcua_node_id{type = numeric, value = 101},
               fields = [#field{
                           name = field_1,
                           value = 1},
                         #field{
                           name = field_2,
                           value = 2}]},
    meck:expect(opcua_database, lookup_schema,
        fun(#opcua_node_id{value = 101}) -> Enum end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = field_2,
    assert_codec(NodeId, ToBeEncoded).

union() ->
    Union = #union{
                node_id = #opcua_node_id{type = numeric, value = 101},
                fields = [#field{
                            name = some_integer,
                            node_id = #opcua_node_id{type = string, value = int32},
                            value_rank = -1,
                            is_optional = true,
                            value = 1},
                          #field{
                            name = some_string,
                            node_id = #opcua_node_id{type = numeric, value = 12},
                            value_rank = -1,
                            is_optional = true,
                            value = 2}]},
    meck:expect(opcua_database, lookup_schema,
        fun(#opcua_node_id{value = 101}) -> Union end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = #{some_string => <<"Hello!">>},
    assert_codec(NodeId, ToBeEncoded).

builtin() ->
    Builtin = #builtin{
                node_id = #opcua_node_id{type = numeric, value = 101},
                builtin_node_id = #opcua_node_id{type = numeric, value = 6}},
    meck:expect(opcua_database, lookup_schema,
        fun(#opcua_node_id{value = 101}) -> Builtin end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = 123,
    assert_codec(NodeId, ToBeEncoded).
