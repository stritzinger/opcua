-module(opcua_template_test).

-include_lib("eunit/include/eunit.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(NS, 1).
-define(LNNID(V), ?NNID(?NS, V)).
-define(STATIC_VAR1, ?NNID(42)).
-define(STATIC_VAR2, ?NNID(66)).
-define(STATIC_OBJ_TYPE, ?NNID(33)).

% Asserts the template returns given result, and creates given nodes and refs.
% In addition, it asserts that the second time it is applied, no new nodes or
% refs are created, but the result is the same.
-define(assertTemplate(RESULT, NODES, REFS, TEMPLATE), (fun() ->
    Template = TEMPLATE,
    Checkpoint = save(),
    Return1 = prepare(opcua_server_space, Template),
    ?assertMatch({_, _, _}, Return1),
    {Result1, Nodes1, Refs1} = Return1,
    ?assertMatch({RESULT, NODES, REFS},
                 {Result1, lists:sort(Nodes1), lists:sort(Refs1)}),
    {Result1, _, _} = Return1,
    restore(Checkpoint), % So the generated node ids are the same
    Result2 = ensure(opcua_server_space, Template),
    ?assertMatch(RESULT, Result2),
    ?assertEqual(Result1, Result2),
    Return3 = prepare(opcua_server_space, Template),
    ?assertMatch({RESULT, [], []}, Return3),
    {Result3, _, _} = Return3,
    ?assertEqual(Result1, Result3)
end)()).


%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup() ->
    PrivDir = code:priv_dir(opcua),
    NodeSetDir = filename:join([PrivDir, "nodeset", "data"]),
    opcua_nodeset:start_link(NodeSetDir),
    {ok, ServerSpaceProc} = opcua_server_space:start_link(),
    ServerSpaceProc.


teardown(ServerSpaceProc) ->
    unlink(ServerSpaceProc),
    ServerSpaceMon = erlang:monitor(process, ServerSpaceProc),
    erlang:exit(ServerSpaceProc, shutdown),
    receive {'DOWN', ServerSpaceMon, _, _, _} -> ok end,
    ok.

all_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        fun test_explicit_id/0,
        fun test_simple_single_forward_ref/0,
        fun test_simple_single_inverse_ref/0,
        fun test_two_layer_forward_ref/0,
        fun test_two_layer_inverse_ref/0,
        fun test_aliases/0,
        fun test_reference_type_alias/0
     ]}.

test_explicit_id() ->
    ?assertTemplate(
        [{?LNNID(1000), []}],
        [#opcua_node{node_id = ?LNNID(1000),
                     browse_name = <<"FooBar">>}],
        [], [#{
            node_id => ?LNNID(1000),
            browse_name => <<"FooBar">>
        }]),
    ok.

test_simple_single_forward_ref() ->
    ?assertTemplate(
        [{?LNNID(1), [{?NNID(?TYPE_BASE_OBJECT), []}]}],
        [#opcua_node{node_id = ?LNNID(1),
                     browse_name = <<"FooBar">>}],
        [#opcua_reference{type_id = ?NNID(?REF_HAS_TYPE_DEFINITION),
                          source_id = ?LNNID(1),
                          target_id = ?NNID(?TYPE_BASE_OBJECT)}],
        [#{
            namespace => ?NS,
            browse_name => <<"FooBar">>,
            references => [
                {forward, has_type_definition, base_object_type}
            ]
        }]),
    ok.

test_simple_single_inverse_ref() ->
    ?assertTemplate(
        [{?LNNID(1), [{?NNID(?OBJ_OBJECTS_FOLDER), []}]}],
        [#opcua_node{node_id = ?LNNID(1),
                     browse_name = <<"FooBar">>}],
        [#opcua_reference{type_id = ?NNID(?REF_HAS_CHILD),
                          source_id = ?NNID(?OBJ_OBJECTS_FOLDER),
                          target_id = ?LNNID(1)}],
        [#{
            namespace => ?NS,
            browse_name => <<"FooBar">>,
            references => [
                {inverse, has_child, objects}
            ]
        }]),
    ok.

test_two_layer_forward_ref() ->
    ?assertTemplate(
        [{?LNNID(1), [{?LNNID(2), []}, {?LNNID(3), [{?NNID(?TYPE_BASE_OBJECT), []}]}]}],
        [#opcua_node{node_id = ?LNNID(1), browse_name = <<"Foo">>},
         #opcua_node{node_id = ?LNNID(2), browse_name = <<"Bar">>},
         #opcua_node{node_id = ?LNNID(3), browse_name = <<"Buz">>}],
        [#opcua_reference{type_id = ?NNID(?REF_ORGANIZES),
                          source_id = ?LNNID(1),
                          target_id = ?LNNID(2)},
         #opcua_reference{type_id = ?NNID(?REF_ORGANIZES),
                          source_id = ?LNNID(1),
                          target_id = ?LNNID(3)},
         #opcua_reference{type_id = ?NNID(?REF_HAS_TYPE_DEFINITION),
                          source_id = ?LNNID(3),
                          target_id = ?NNID(?TYPE_BASE_OBJECT)}],
        [#{
            namespace => ?NS,
            browse_name => <<"Foo">>,
            references => [
                {forward, organizes, #{
                    namespace => ?NS,
                    browse_name => <<"Bar">>
                }},
                {forward, organizes, #{
                    namespace => ?NS,
                    browse_name => <<"Buz">>,
                    references => [
                        {forward, has_type_definition, base_object_type}
                    ]
                }}
            ]
        }]),
    ok.

test_two_layer_inverse_ref() ->
    ?assertTemplate(
        [{?LNNID(1), [{?LNNID(2), []}, {?LNNID(3), [{?NNID(?OBJ_OBJECTS_FOLDER), []}]}]}],
        [#opcua_node{node_id = ?LNNID(1), browse_name = <<"Foo">>},
         #opcua_node{node_id = ?LNNID(2), browse_name = <<"Bar">>},
         #opcua_node{node_id = ?LNNID(3), browse_name = <<"Buz">>}],
        [#opcua_reference{type_id = ?NNID(?REF_HAS_CHILD),
                          source_id = ?NNID(?OBJ_OBJECTS_FOLDER),
                          target_id = ?LNNID(3)},
         #opcua_reference{type_id = ?NNID(?REF_ORGANIZES),
                          source_id = ?LNNID(2),
                          target_id = ?LNNID(1)},
         #opcua_reference{type_id = ?NNID(?REF_ORGANIZES),
                          source_id = ?LNNID(3),
                          target_id = ?LNNID(1)}],
        [#{
            namespace => ?NS,
            browse_name => <<"Foo">>,
            references => [
                {inverse, organizes, #{
                    namespace => ?NS,
                    browse_name => <<"Bar">>
                }},
                {inverse, organizes, #{
                    namespace => ?NS,
                    browse_name => <<"Buz">>,
                    references => [
                        {inverse, has_child, objects}
                    ]
                }}
            ]
        }]),
    ok.

test_aliases() ->
    ?assertTemplate(
       [{?LNNID(1), []},{?LNNID(2), [{?LNNID(1), []},{?NNID(?TYPE_BASE_OBJECT), []}]}],
       [#opcua_node{node_id = ?LNNID(1), browse_name = <<"Foo">>},
        #opcua_node{node_id = ?LNNID(2), browse_name = <<"Bar">>}],
       [#opcua_reference{type_id = ?NNID(?REF_ORGANIZES),
                         source_id = ?LNNID(2),
                         target_id = ?LNNID(1)},
        #opcua_reference{type_id = ?NNID(?REF_HAS_TYPE_DEFINITION),
                         source_id = ?LNNID(2),
                         target_id = ?NNID(?TYPE_BASE_OBJECT)}
        ],
        [#{
            alias => foo,
            namespace => ?NS,
            browse_name => <<"Foo">>
        },
        #{
            namespace => ?NS,
            browse_name => <<"Bar">>,
            references => [
                {forward, organizes, foo},
                 % The anchor to prevent it to be created every time:
                {forward, has_type_definition, base_object_type}
            ]
        }]),
    ok.

test_reference_type_alias() ->
    ?assertTemplate(
       [{?LNNID(1), [{?NNID(?TYPE_REFERENCES), []}]},
        {?LNNID(2), [{?LNNID(3), []},{?NNID(?OBJ_OBJECTS_FOLDER), []}]}],
       [#opcua_node{node_id = ?LNNID(1), browse_name = <<"MyRefType">>},
        #opcua_node{node_id = ?LNNID(2), browse_name = <<"Foo">>},
        #opcua_node{node_id = ?LNNID(3), browse_name = <<"Bar">>}],
       [#opcua_reference{type_id = ?NNID(?REF_HAS_SUBTYPE),
                         source_id = ?NNID(?TYPE_REFERENCES),
                         target_id = ?LNNID(1)},
        #opcua_reference{type_id = ?LNNID(1),
                         source_id = ?LNNID(2),
                         target_id = ?NNID(?OBJ_OBJECTS_FOLDER)},
        #opcua_reference{type_id = ?LNNID(1),
                         source_id = ?LNNID(2),
                         target_id = ?LNNID(3)}],
        [#{
            alias => my_ref_type,
            namespace => ?NS,
            node_class => reference_type,
            browse_name => <<"MyRefType">>,
            references => [
                {inverse, has_subtype, references}
            ]
        },
        #{
            namespace => ?NS,
            browse_name => <<"Foo">>,
            references => [
                {forward, my_ref_type, #{
                    namespace => ?NS,
                    browse_name => <<"Bar">>
                }},
                % The anchor to prevent it to be created everyt time:
                {forward, my_ref_type, objects}
            ]
        }]),
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

save() ->
    get({?MODULE, next}).

restore(Id) ->
    put({?MODULE, next}, Id).

next_node_id() ->
    Id = case get({?MODULE, next}) of
        undefined -> 1;
        V -> V
    end,
    put({?MODULE, next}, Id + 1),
    ?LNNID(Id).

prepare(Space, Template) ->
    Opts = #{node_id_fun => fun next_node_id/0},
    opcua_template:prepare(Space, Template, Opts).

ensure(Space, Template) ->
    Opts = #{node_id_fun => fun next_node_id/0},
    opcua_template:ensure(Space, Template, Opts).
