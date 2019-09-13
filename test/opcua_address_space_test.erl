-module(opcua_address_space_test).

-include_lib("eunit/include/eunit.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").

-import(opcua_address_space, [add_nodes/2, get_node/2]).


%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cleanup_test() ->
    {ok, Supervisor} = opcua_address_space_sup:start_link(),
    unlink(Supervisor),
    PersistentTerms = persistent_term:get(),
    ok = opcua_address_space:create(?MODULE),
    opcua_test_util:without_error_logger(fun() ->
        exit(Supervisor, test_kill),
        timer:sleep(100) % persistent_term deletion is asynchronous, wait until done
    end),
    ?assertEqual(PersistentTerms, persistent_term:get()).

add_node_test_() ->
    setup_teardown([
        {"add and read", fun() ->
            ?assertEqual(ok, add_nodes(?MODULE, [#opcua_node{}])),
            ?assertEqual(#opcua_node{}, get_node(?MODULE, undefined))
        end}
    ]).


%%% HELPERS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup_teardown(Tests) ->
    {
        foreach,
        fun() ->
            {ok, Supervisor} = opcua_address_space_sup:start_link(),
            opcua_address_space:create(?MODULE),
            Supervisor
        end,
        fun(Supervisor) ->
            opcua_address_space:destroy(?MODULE),
            unlink(Supervisor),
            exit(Supervisor, normal)
        end,
        Tests
    }.
