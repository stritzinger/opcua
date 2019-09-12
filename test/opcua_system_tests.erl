-module(opcua_system_tests).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("eunit/include/eunit.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

basic_client_server_connection_test_() ->
    setup_teardown([
        {"basic client/server connection", fun() ->
            application:ensure_all_started(opcua),
            Client = opcua_client:connect(<<"opc.tcp://127.0.0.1">>),
            ?assertMatch(#opcua_qualified_name{ns = 0, name = <<"Server">>},
                         opcua_client:read(Client, server, browse_name)),
            %FIXME: This should actually return 'object' for node class...
            ?assertMatch([?NNID(?OBJ_SERVER), 1],
                         opcua_client:read(Client, server, [node_id, node_class])),
            Refs = opcua_client:browse(Client, root, #{
                direction => forward,
                type => ?REF_ORGANIZES,
                include_subtypes => true}),
            ?assertMatch([
                #{browse_name := #opcua_qualified_name{name = <<"Objects">>}},
                #{browse_name := #opcua_qualified_name{name = <<"Types">>}},
                #{browse_name := #opcua_qualified_name{name = <<"Views">>}}
            ], lists:sort(Refs)),
            ?assertMatch(ok, opcua_client:close(Client)),
            application:stop(opcua)
        end}
    ]).


%%% HELPERS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup_teardown(Tests) ->
    {
        foreach,
        fun() -> application:ensure_all_started(opcua) end,
        fun(_) -> application:stop(opcua) end,
        Tests
    }.
