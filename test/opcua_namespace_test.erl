-module(opcua_namespace_test).

-include_lib("eunit/include/eunit.hrl").

-import(opcua_namespace, [all/0, add/1, index/1, uri/1]).


%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

type_test_() ->
    setup_teardown([
        ?_assertError(function_clause, add(1))
    ]).

default_test_() ->
    setup_teardown([
        ?_assertEqual(#{undefined => 0}, all()),
        ?_assertEqual(0, index(undefined)),
        ?_assertEqual(undefined, uri(0)),
        ?_assertError({namespace_exists, undefined, 0}, add(undefined))
    ]).

add_test_() ->
    setup_teardown([
        {"add and read", fun() ->
            ?assertEqual(1, add(<<"ns">>)),
            ?assertEqual(1, index(<<"ns">>)),
            ?assertEqual(<<"ns">>, uri(1))
        end},
        {"add twice", fun() ->
            ?assertEqual(1, add(<<"ns">>)),
            ?assertError({namespace_exists, <<"ns">>, 1}, add(<<"ns">>))
        end}
    ]).

read_test_() ->
    setup_teardown([
        ?_assertError({unknown_index, 1}, uri(1)),
        ?_assertError({unknown_uri, <<"foo">>}, index(<<"foo">>))
    ]).


%%% HELPERS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup_teardown(Tests) ->
    {
        foreach,
        fun() -> ok end,
        fun(ok) -> persistent_term:erase(opcua_namespace) end,
        Tests
    }.
