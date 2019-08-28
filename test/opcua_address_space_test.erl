-module(opcua_address_space_test).

-include_lib("eunit/include/eunit.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").

-import(opcua_address_space, [add_nodes/1, get_node/1]).


%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_node_test_() ->
    setup_teardown([
        {"add and read", fun() ->
            ?assertEqual(ok, add_nodes([#opcua_node{}])),
            ?assertEqual(#opcua_node{}, get_node(undefined))
        end}
    ]).


%%% HELPERS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup_teardown(Tests) ->
    {
        foreach,
        fun() -> opcua_address_space:start_link() end,
        fun({ok, Pid}) -> gen_server:stop(Pid) end,
        Tests
    }.
