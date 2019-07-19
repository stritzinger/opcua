-module(codec_tests).

-include_lib("eunit/include/eunit.hrl").
-include("opcua_codec.hrl").

basic_test() ->
    %% schema for a structure which only contains one int32 field
    Structure = #data_type{
                   node_id = #node_id{type = numeric, value = 0},
                   type = structure,
                   with_options = false,
                   fields = [#field{
                               name = some_integer,
                               node_id = #node_id{type = numeric, value = 6},
                               value_rank = -1,
                               is_optional = false,
                               value = undefined}]},

    %% the schema for the int32 type, note the {builtin, int32} tuple
    Int32 = #data_type{
               node_id = #node_id{type = numeric, value = 6},
               type = {builtin, int32},
               with_options = false,
               fields = []},

    %% some setup for things we don't have yet 
    meck:new(opcua_schema, [non_strict]),
    meck:expect(opcua_schema, resolve, fun(#node_id{value = 0}) -> Structure;
                                          (#node_id{value = 6}) -> Int32
                                       end),

    %% this is supposed to be the encoded structure with one int32 element
    Bin = <<1:32/little-signed-integer>>,
    %% the node id of the structure
    NodeId = #node_id{type = numeric, value = 0},
    %% what we expect to get from the decoder
    Expected = #{some_integer => 1},

    %% decode and check
    {Decoded, Bin1} = opcua_codec_binary:decode(NodeId, Bin),
    ?assertEqual(Expected, Decoded),
    ?assertEqual(<<>>, Bin1),

    meck:unload(opcua_schema).
