-module(codec_schema_tests).

-include_lib("eunit/include/eunit.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").

t_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [fun structure/0,
      fun structure_with_options/0,
      fun enum/0,
      fun union/0,
      fun option_set/0,
      fun builtin/0]}.

setup() ->
    meck:new(opcua_nodeset, [non_strict]).

cleanup(_) ->
    meck:unload(opcua_nodeset).

%% some little helper
assert_codec(NodeId, ToBeEncoded) ->
    {Bin, _} = opcua_codec_binary:encode(NodeId, ToBeEncoded),
    {Decoded, Bin1} = opcua_codec_binary:decode(NodeId, Bin),
    ?assertEqual(ToBeEncoded, Decoded),
    ?assertEqual(<<>>, Bin1).

structure() ->
    InnerFieldSchema = #opcua_structure{
       node_id = #opcua_node_id{type = numeric, value = 100},
       with_options = false,
       fields = [#opcua_field{
                   tag = some_boolean,
                   node_id = #opcua_node_id{type = numeric, value = 1},
                   value_rank = -1,
                   is_optional = false,
                   value = undefined}]},
    StructureSchema = #opcua_structure{
       node_id = #opcua_node_id{type = numeric, value = 101},
       with_options = false,
       fields = [#opcua_field{
                   tag = some_integer,
                   node_id = #opcua_node_id{type = string, value = int32},
                   value_rank = -1,
                   is_optional = false,
                   value = undefined},
                 #opcua_field{
                   tag = inner_field,
                   node_id = #opcua_node_id{type = numeric, value = 100},
                   value_rank = -1,
                   is_optional = false,
                   value = undefined}]},
    meck:expect(opcua_nodeset, schema,
        fun(#opcua_node_id{value = 101}) -> StructureSchema;
           (#opcua_node_id{value = 100}) -> InnerFieldSchema end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = #{some_integer => 1, inner_field => #{some_boolean => true}},
    assert_codec(NodeId, ToBeEncoded).

structure_with_options() ->
    StructureSchema = #opcua_structure{
       node_id = #opcua_node_id{type = numeric, value = 101},
       with_options = true,
       fields = [#opcua_field{
                   tag = some_integer,
                   node_id = #opcua_node_id{type = string, value = int32},
                   value_rank = -1,
                   is_optional = false,
                   value = undefined},
                 #opcua_field{
                   tag = some_string,
                   node_id = #opcua_node_id{type = numeric, value = 12},
                   value_rank = -1,
                   is_optional = true,
                   value = 1}]},
    meck:expect(opcua_nodeset, schema,
        fun(#opcua_node_id{value = 101}) -> StructureSchema end),
    NodeId = #opcua_node_id{type = numeric, value = 101},

    %% check WITHOUT optional field
    ToBeEncodedWithoutOptions = #{some_integer => 1},
    assert_codec(NodeId, ToBeEncodedWithoutOptions),

    %% check WITH optional field
    ToBeEncodedWithOptions = #{some_integer => 1, some_string => <<"Hello!">>},
    assert_codec(NodeId, ToBeEncodedWithOptions).

enum() ->
    EnumSchema = #opcua_enum{
       node_id = #opcua_node_id{type = numeric, value = 101},
       fields = [#opcua_field{
                   tag = field_1,
                   value = 1},
                 #opcua_field{
                   tag = field_2,
                   value = 2}]},
    meck:expect(opcua_nodeset, schema,
        fun(#opcua_node_id{value = 101}) -> EnumSchema end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = field_2,
    assert_codec(NodeId, ToBeEncoded).

union() ->
    UnionSchema = #opcua_union{
        node_id = #opcua_node_id{type = numeric, value = 101},
        fields = [#opcua_field{
                    tag = some_integer,
                    node_id = #opcua_node_id{type = string, value = int32},
                    value_rank = -1,
                    is_optional = true,
                    value = 1},
                  #opcua_field{
                    tag = some_string,
                    node_id = #opcua_node_id{type = numeric, value = 12},
                    value_rank = -1,
                    is_optional = true,
                    value = 2}]},
    meck:expect(opcua_nodeset, schema,
        fun(#opcua_node_id{value = 101}) -> UnionSchema end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = #{some_string => <<"Hello!">>},
    assert_codec(NodeId, ToBeEncoded).

option_set() ->
    OptionSetSchema = #opcua_option_set{
        node_id = #opcua_node_id{type = numeric, value = 101},
        mask_type = #opcua_node_id{value = 3},
        fields = [#opcua_field{tag = option_0, value = 0},
                  #opcua_field{tag = option_1, value = 1},
                  #opcua_field{tag = option_2, value = 2}]},
    meck:expect(opcua_nodeset, schema,
        fun(#opcua_node_id{value = 101}) -> OptionSetSchema end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = [option_0, option_2],
    assert_codec(NodeId, ToBeEncoded).

builtin() ->
    BuiltinSchema = #opcua_builtin{
        node_id = #opcua_node_id{type = numeric, value = 101},
        builtin_node_id = #opcua_node_id{type = numeric, value = 6}},
    meck:expect(opcua_nodeset, schema,
        fun(#opcua_node_id{value = 101}) -> BuiltinSchema end),
    NodeId = #opcua_node_id{type = numeric, value = 101},
    ToBeEncoded = 123,
    assert_codec(NodeId, ToBeEncoded).
