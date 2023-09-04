-module(opcua_template_test).

-include_lib("eunit/include/eunit.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(NS, 1).
-define(LNNID(V), ?NNID(?NS, V)).
-define(STATIC1, ?NNID(42)).
-define(STATIC2, ?NNID(66)).

-define(assertTemplate(SPACE, RESULT, NODES, REFS, TEMPLATE), (fun() ->
    Template = TEMPLATE,
    Checkpoint = save(),
    Return1 = prepare(SPACE, Template),
    ?assertMatch({_, _, _}, Return1),
    {Result1, Nodes1, Refs1} = Return1,
    ?assertMatch({RESULT, NODES, REFS},
                 {Result1, lists:sort(Nodes1), lists:sort(Refs1)}),
    {Result1, _, _} = Return1,
    restore(Checkpoint), % So the generated node ids are the same
    Result2 = ensure(SPACE, Template),
    ?assertMatch(RESULT, Result2),
    ?assertEqual(Result1, Result2),
    Return3 = prepare(SPACE, Template),
    ?assertMatch({RESULT, [], []}, Return3),
    {Result3, _, _} = Return3,
    ?assertEqual(Result1, Result3)
end)()).


%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

explicit_id_test() ->
    Space = new_space(),
    ?assertTemplate(Space,
        [{?LNNID(1000), []}],
        [#opcua_node{node_id = ?LNNID(1000),
                     browse_name = <<"FooBar">>}],
        [], [#{
            node_id => ?LNNID(1000),
            browse_name => <<"FooBar">>
        }]),
    opcua_space_backend:terminate(Space),
    ok.

simple_single_forward_ref_test() ->
    Space = new_space(),
    ?assertTemplate(Space,
        [{?LNNID(1), [{?STATIC1, []}]}],
        [#opcua_node{node_id = ?LNNID(1),
                     browse_name = <<"FooBar">>}],
        [#opcua_reference{type_id = ?NNID(33),
                          source_id = ?LNNID(1),
                          target_id = ?STATIC1}],
        [#{
            namespace => ?NS,
            browse_name => <<"FooBar">>,
            references => [
                {forward, ?NNID(33), ?STATIC1}
            ]
        }]),
    opcua_space_backend:terminate(Space),
    ok.

simple_single_inverse_ref_test() ->
    Space = new_space(),
    ?assertTemplate(Space,
        [{?LNNID(1), [{?STATIC1, []}]}],
        [#opcua_node{node_id = ?LNNID(1),
                     browse_name = <<"FooBar">>}],
        [#opcua_reference{type_id = ?NNID(33),
                          source_id = ?STATIC1,
                          target_id = ?LNNID(1)}],
        [#{
            namespace => ?NS,
            browse_name => <<"FooBar">>,
            references => [
                {inverse, ?NNID(33), ?STATIC1}
            ]
        }]),
    opcua_space_backend:terminate(Space),
    ok.

two_layer_forward_ref_test() ->
    Space = new_space(),
    ?assertTemplate(Space,
        [{?LNNID(1), [{?LNNID(2), []}, {?LNNID(3), [{?STATIC1, []}]}]}],
        [#opcua_node{node_id = ?LNNID(1), browse_name = <<"Foo">>},
         #opcua_node{node_id = ?LNNID(2), browse_name = <<"Bar">>},
         #opcua_node{node_id = ?LNNID(3), browse_name = <<"Buz">>}],
        [#opcua_reference{type_id = ?NNID(33),
                          source_id = ?LNNID(1),
                          target_id = ?LNNID(2)},
         #opcua_reference{type_id = ?NNID(33),
                          source_id = ?LNNID(1),
                          target_id = ?LNNID(3)},
         #opcua_reference{type_id = ?NNID(33),
                          source_id = ?LNNID(3),
                          target_id = ?STATIC1}],
        [#{
            namespace => ?NS,
            browse_name => <<"Foo">>,
            references => [
                {forward, ?NNID(33), #{
                    namespace => ?NS,
                    browse_name => <<"Bar">>
                }},
                {forward, ?NNID(33), #{
                    namespace => ?NS,
                    browse_name => <<"Buz">>,
                    references => [
                        {forward, ?NNID(33), ?STATIC1}
                    ]
                }}
            ]
        }]),
    opcua_space_backend:terminate(Space),
    ok.

two_layer_inverse_ref_test() ->
    Space = new_space(),
    ?assertTemplate(Space,
        [{?LNNID(1), [{?LNNID(2), []}, {?LNNID(3), [{?STATIC1, []}]}]}],
        [#opcua_node{node_id = ?LNNID(1), browse_name = <<"Foo">>},
         #opcua_node{node_id = ?LNNID(2), browse_name = <<"Bar">>},
         #opcua_node{node_id = ?LNNID(3), browse_name = <<"Buz">>}],
        [#opcua_reference{type_id = ?NNID(33),
                          source_id = ?STATIC1,
                          target_id = ?LNNID(3)},
         #opcua_reference{type_id = ?NNID(33),
                          source_id = ?LNNID(2),
                          target_id = ?LNNID(1)},
         #opcua_reference{type_id = ?NNID(33),
                          source_id = ?LNNID(3),
                          target_id = ?LNNID(1)}],
        [#{
            namespace => ?NS,
            browse_name => <<"Foo">>,
            references => [
                {inverse, ?NNID(33), #{
                    namespace => ?NS,
                    browse_name => <<"Bar">>
                }},
                {inverse, ?NNID(33), #{
                    namespace => ?NS,
                    browse_name => <<"Buz">>,
                    references => [
                        {inverse, ?NNID(33), ?STATIC1}
                    ]
                }}
            ]
        }]),
    opcua_space_backend:terminate(Space),
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new_space() ->
    Space = opcua_space_backend:new(),
    0 = opcua_space:add_namespace(Space, <<"http://foo.com/UA/">>),
    ?NS = opcua_space:add_namespace(Space, <<"http://bar.com/UA/">>),
    put({opcua_space_test, next}, 1),
    % Add some static nodes
    opcua_space:add_nodes(Space, [
        #opcua_node{
            node_id = ?STATIC1,
            browse_name = <<"Static1">>,
            node_class = #opcua_variable{}
        },
        #opcua_node{
            node_id = ?STATIC2,
            browse_name = <<"Static2">>,
            node_class = #opcua_variable{}
        }]),
    Space.

save() ->
    get({opcua_space_test, next}).

restore(Id) ->
    put({opcua_space_test, next}, Id).

next_node_id() ->
    Id = get({opcua_space_test, next}),
    put({opcua_space_test, next}, Id + 1),
    ?LNNID(Id).

prepare(Space, Template) ->
    Opts = #{node_id_fun => fun next_node_id/0},
    opcua_template:prepare(Space, Template, Opts).

ensure(Space, Template) ->
    Opts = #{node_id_fun => fun next_node_id/0},
    opcua_template:ensure(Space, Template, Opts).
