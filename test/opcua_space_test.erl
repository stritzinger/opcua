-module(opcua_space_test).

-include_lib("eunit/include/eunit.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").

-import(opcua_space, [add_nodes/2]).
-import(opcua_space, [del_nodes/2]).
-import(opcua_space, [add_references/2]).
-import(opcua_space, [del_references/2]).
-import(opcua_space, [data_type/2]).
-import(opcua_space, [type_descriptor/3]).
-import(opcua_space, [references/3]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(NODE(N, B), #opcua_node{
    node_id = opcua_node:id(N),
    browse_name = B
}).

-define(REF(TYPE, SOURCE, TARGET), #opcua_reference{
    type_id = opcua_node:id(TYPE),
    source_id = opcua_node:id(SOURCE),
    target_id = opcua_node:id(TARGET)
}).

% Can be used in pattern-matching, but do not resolve node specs
-define(MREF(TYPE, SOURCE, TARGET), #opcua_reference{
    type_id = ?NNID(TYPE),
    source_id = ?NNID(SOURCE),
    target_id = ?NNID(TARGET)
}).


%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

type_descriptor_test() ->
    Space = opcua_space_backend:new(),

    add_nodes(Space, [?NODE(100, <<"My Type">>)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, binary)),
    ?assertMatch(undefined, type_descriptor(Space, 100, xml)),
    ?assertMatch(undefined, type_descriptor(Space, 100, json)),

    % Adding order: Node, has_encoding, has_type_definition
    add_nodes(Space, [?NODE(101, <<"Default Binary">>)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, binary)),
    add_references(Space, [?REF(has_encoding, 100, 101)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, binary)),
    add_references(Space, [?REF(has_type_definition, 101, ?NID_DATA_TYPE_ENCODING_TYPE)]),
    ?assertMatch(?NNID(101), type_descriptor(Space, 100, binary)),

    % Adding order: has_type_definition, Node, has_encoding
    add_references(Space, [?REF(has_type_definition, 102, ?NID_DATA_TYPE_ENCODING_TYPE)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, xml)),
    add_nodes(Space, [?NODE(102, <<"Default XML">>)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, xml)),
    add_references(Space, [?REF(has_encoding, 100, 102)]),
    ?assertMatch(?NNID(102), type_descriptor(Space, 100, xml)),

    % Adding order: has_encoding, has_type_definition, Node
    add_references(Space, [?REF(has_encoding, 100, 103)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, json)),
    add_references(Space, [?REF(has_type_definition, 103, ?NID_DATA_TYPE_ENCODING_TYPE)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, json)),
    add_nodes(Space, [?NODE(103, <<"Default JSON">>)]),
    ?assertMatch(?NNID(103), type_descriptor(Space, 100, json)),

    ?assertMatch(?NNID(101), type_descriptor(Space, 100, binary)),
    ?assertMatch(?NNID(102), type_descriptor(Space, 100, xml)),
    ?assertMatch(?NNID(103), type_descriptor(Space, 100, json)),
    ?assertMatch({?NNID(100), binary}, data_type(Space, 101)),
    ?assertMatch({?NNID(100), xml}, data_type(Space, 102)),
    ?assertMatch({?NNID(100), json}, data_type(Space, 103)),

    del_nodes(Space, [?NNID(101)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, binary)),
    del_references(Space, [?REF(has_type_definition, 102, ?NID_DATA_TYPE_ENCODING_TYPE)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, xml)),
    del_references(Space, [?REF(has_encoding, 100, 103)]),
    ?assertMatch(undefined, type_descriptor(Space, 100, json)),

    opcua_space_backend:terminate(Space),
    ok.

reference_subtypes_link_test() ->
    % Test then createing and breaking a link of subtype has the expected effect
    % 100 -2-> 101 -3-> 102 -1-> 103
    S = opcua_space_backend:new(),
    add_references(S, [?REF(has_subtype, 102, 103),
                       ?REF(has_subtype, 100, 101)]),
    ?assertEqual(true, opcua_space:is_subtype(S, 103, 102)),
    ?assertEqual(false, opcua_space:is_subtype(S, 103, 101)),
    ?assertEqual(false, opcua_space:is_subtype(S, 103, 100)),
    ?assertEqual(false, opcua_space:is_subtype(S, 102, 101)),
    ?assertEqual(false, opcua_space:is_subtype(S, 102, 100)),
    ?assertEqual(true, opcua_space:is_subtype(S, 101, 100)),
    add_references(S, [?REF(has_subtype, 101, 102)]),
    ?assertEqual(true, opcua_space:is_subtype(S, 103, 102)),
    ?assertEqual(true, opcua_space:is_subtype(S, 103, 101)),
    ?assertEqual(true, opcua_space:is_subtype(S, 103, 100)),
    ?assertEqual(true, opcua_space:is_subtype(S, 102, 101)),
    ?assertEqual(true, opcua_space:is_subtype(S, 102, 100)),
    ?assertEqual(true, opcua_space:is_subtype(S, 101, 100)),
    del_references(S, [?REF(has_subtype, 101, 102)]),
    ?assertEqual(true, opcua_space:is_subtype(S, 103, 102)),
    ?assertEqual(false, opcua_space:is_subtype(S, 103, 101)),
    ?assertEqual(false, opcua_space:is_subtype(S, 103, 100)),
    ?assertEqual(false, opcua_space:is_subtype(S, 102, 101)),
    ?assertEqual(false, opcua_space:is_subtype(S, 102, 100)),
    ?assertEqual(true, opcua_space:is_subtype(S, 101, 100)),
    ok.

reference_subtypes_test() ->
    A = opcua_space_backend:new(a, []),
    B = opcua_space_backend:new(b, A),
    C = opcua_space_backend:new(c, B),

    % The reference type hierarchy used in this test:
    %
    %               10011
    %      / 1001 <
    %     /         10012
    % 100
    %     \         10021
    %      \ 1002 <
    %               10022
    %
    % Each layers contains a reference from node 1 to another node with each of
    % the possible reference types.
    %
    % This test will be defining and modifying this type hierarchy and test
    % the references returned are the expected ones.

    add_references(A, [?REF(100, 1, 2), ?REF(1001, 1, 2), ?REF(1002, 1, 2),
                       ?REF(10011, 1, 2), ?REF(10012, 1, 2),
                       ?REF(10021, 1, 2), ?REF(10022, 1, 2)]),
    add_references(B, [?REF(100, 1, 3), ?REF(1001, 1, 3), ?REF(1002, 1, 3),
                       ?REF(10011, 1, 3), ?REF(10012, 1, 3),
                       ?REF(10021, 1, 3), ?REF(10022, 1, 3)]),
    add_references(C, [?REF(100, 1, 4), ?REF(1001, 1, 4), ?REF(1002, 1, 4),
                       ?REF(10011, 1, 4), ?REF(10012, 1, 4),
                       ?REF(10021, 1, 4), ?REF(10022, 1, 4)]),

    % All references types of space A
    ?assertMatch([?MREF(100, 1, 2), ?MREF(1001, 1, 2), ?MREF(1002, 1, 2),
                  ?MREF(10011, 1, 2), ?MREF(10012, 1, 2),
                  ?MREF(10021, 1, 2), ?MREF(10022, 1, 2)],
                 lists:sort(references(A, 1, #{direction => forward}))),
    ?assertMatch([],
                 lists:sort(references(A, 1, #{direction => inverse}))),
    ?assertMatch([],
                 lists:sort(references(A, 2, #{direction => forward}))),
    ?assertMatch([?MREF(100, 1, 2), ?MREF(1001, 1, 2), ?MREF(1002, 1, 2),
                  ?MREF(10011, 1, 2), ?MREF(10012, 1, 2),
                  ?MREF(10021, 1, 2), ?MREF(10022, 1, 2)],
                 lists:sort(references(A, 2, #{direction => inverse}))),

    % Only root reference type of space A (no reference hierarchy defined)
    ?assertMatch([?MREF(100, 1, 2)],
                 lists:sort(references(A, 1,
                    #{direction => forward, type => 100}))),
    ?assertMatch([],
                 lists:sort(references(A, 1,
                    #{direction => inverse, type => 100}))),
    ?assertMatch([],
                 lists:sort(references(A, 2,
                    #{direction => forward, type => 100}))),
    ?assertMatch([?MREF(100, 1, 2)],
                 lists:sort(references(A, 2,
                    #{direction => inverse, type => 100}))),

    % Root reference type and subtypes of space A (no reference hierarchy defined)
    ?assertMatch([?MREF(100, 1, 2)],
                 lists:sort(references(A, 1,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(A, 1,
                    #{direction => inverse, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(A, 2,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(100, 1, 2)],
                 lists:sort(references(A, 2,
                    #{direction => inverse, type => 100,
                      include_subtypes => true}))),

    % Only Half-way reference type of space A (no reference hierarchy defined)
    ?assertMatch([?MREF(1001, 1, 2)],
                 lists:sort(references(A, 1,
                    #{direction => forward, type => 1001}))),
    ?assertMatch([],
                 lists:sort(references(A, 1,
                    #{direction => inverse, type => 1001}))),
    ?assertMatch([],
                 lists:sort(references(A, 2,
                    #{direction => forward, type => 1001}))),
    ?assertMatch([?MREF(1001, 1, 2)],
                 lists:sort(references(A, 2,
                    #{direction => inverse, type => 1001}))),

    % Half-way reference type and subtypes of space A (no reference hierarchy defined)
    ?assertMatch([?MREF(1001, 1, 2)],
                 lists:sort(references(A, 1,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(A, 1,
                    #{direction => inverse, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(A, 2,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1001, 1, 2)],
                 lists:sort(references(A, 2,
                    #{direction => inverse, type => 1001,
                      include_subtypes => true}))),

    % Defining part of the reference type hierarchy

    add_references(A, [?REF(has_subtype, 100, 1001),
                       ?REF(has_subtype, 1001, 10011),
                       ?REF(has_subtype, 1001, 10012)]),

    % Only root reference type of space A
    ?assertMatch([?MREF(100, 1, 2)],
                 lists:sort(references(A, 1,
                    #{direction => forward, type => 100}))),
    ?assertMatch([],
                 lists:sort(references(A, 1,
                    #{direction => inverse, type => 100}))),
    ?assertMatch([],
                 lists:sort(references(A, 2,
                    #{direction => forward, type => 100}))),
    ?assertMatch([?MREF(100, 1, 2)],
                 lists:sort(references(A, 2,
                    #{direction => inverse, type => 100}))),

    % Root reference type and subtypes of space A
    ?assertMatch([?MREF(100, 1, 2), ?MREF(1001, 1, 2),
                  ?MREF(10011, 1, 2), ?MREF(10012, 1, 2)],
                 lists:sort(references(A, 1,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(A, 1,
                    #{direction => inverse, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(A, 2,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(100, 1, 2), ?MREF(1001, 1, 2),
                  ?MREF(10011, 1, 2), ?MREF(10012, 1, 2)],
                 lists:sort(references(A, 2,
                    #{direction => inverse, type => 100,
                      include_subtypes => true}))),

    % Only half-way reference type of space A
    ?assertMatch([?MREF(1001, 1, 2)],
                 lists:sort(references(A, 1,
                    #{direction => forward, type => 1001}))),
    ?assertMatch([],
                 lists:sort(references(A, 1,
                    #{direction => inverse, type => 1001}))),
    ?assertMatch([],
                 lists:sort(references(A, 2,
                    #{direction => forward, type => 1001}))),
    ?assertMatch([?MREF(1001, 1, 2)],
                 lists:sort(references(A, 2,
                    #{direction => inverse, type => 1001}))),

    % Half-way reference type and subtypes of space A
    ?assertMatch([?MREF(1001, 1, 2), ?MREF(10011, 1, 2), ?MREF(10012, 1, 2)],
                 lists:sort(references(A, 1,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(A, 1,
                    #{direction => inverse, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(A, 2,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1001, 1, 2), ?MREF(10011, 1, 2), ?MREF(10012, 1, 2)],
                 lists:sort(references(A, 2,
                    #{direction => inverse, type => 1001,
                      include_subtypes => true}))),

    % Only root reference type of space B/A
    ?assertMatch([?MREF(100, 1, 2), ?MREF(100, 1, 3)],
                 lists:sort(references(B, 1,
                    #{direction => forward, type => 100}))),
    ?assertMatch([],
                 lists:sort(references(B, 1,
                    #{direction => inverse, type => 100}))),
    ?assertMatch([],
                 lists:sort(references(B, 2,
                    #{direction => forward, type => 100}))),
    ?assertMatch([?MREF(100, 1, 2)],
                 lists:sort(references(B, 2,
                    #{direction => inverse, type => 100}))),
    ?assertMatch([],
                 lists:sort(references(B, 3,
                    #{direction => forward, type => 100}))),
    ?assertMatch([?MREF(100, 1, 3)],
                 lists:sort(references(B, 3,
                    #{direction => inverse, type => 100}))),

    % Root reference type and subtypes of space B/A
    ?assertMatch([?MREF(100, 1, 2), ?MREF(100, 1, 3),
                  ?MREF(1001, 1, 2), ?MREF(1001, 1, 3),
                  ?MREF(10011, 1, 2), ?MREF(10011, 1, 3),
                  ?MREF(10012, 1, 2), ?MREF(10012, 1, 3)],
                 lists:sort(references(B, 1,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(B, 1,
                    #{direction => inverse, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(B, 2,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(100, 1, 2), ?MREF(1001, 1, 2),
                  ?MREF(10011, 1, 2), ?MREF(10012, 1, 2)],
                 lists:sort(references(B, 2,
                    #{direction => inverse, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(B, 3,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(100, 1, 3), ?MREF(1001, 1, 3),
                  ?MREF(10011, 1, 3), ?MREF(10012, 1, 3)],
                 lists:sort(references(B, 3,
                    #{direction => inverse, type => 100,
                      include_subtypes => true}))),

    % Only half-way reference type of space A
    ?assertMatch([?MREF(1001, 1, 2), ?MREF(1001, 1, 3)],
                 lists:sort(references(B, 1,
                    #{direction => forward, type => 1001}))),
    ?assertMatch([],
                 lists:sort(references(B, 1,
                    #{direction => inverse, type => 1001}))),
    ?assertMatch([],
                 lists:sort(references(B, 2,
                    #{direction => forward, type => 1001}))),
    ?assertMatch([?MREF(1001, 1, 2)],
                 lists:sort(references(B, 2,
                    #{direction => inverse, type => 1001}))),
    ?assertMatch([],
                 lists:sort(references(B, 3,
                    #{direction => forward, type => 1001}))),
    ?assertMatch([?MREF(1001, 1, 3)],
                 lists:sort(references(B, 3,
                    #{direction => inverse, type => 1001}))),

    % Half-way reference type and subtypes of space A
    ?assertMatch([?MREF(1001, 1, 2), ?MREF(1001, 1, 3),
                  ?MREF(10011, 1, 2), ?MREF(10011, 1, 3),
                  ?MREF(10012, 1, 2), ?MREF(10012, 1, 3)],
                 lists:sort(references(B, 1,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(B, 1,
                    #{direction => inverse, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(B, 2,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1001, 1, 2), ?MREF(10011, 1, 2), ?MREF(10012, 1, 2)],
                 lists:sort(references(B, 2,
                    #{direction => inverse, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([],
                 lists:sort(references(B, 3,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1001, 1, 3), ?MREF(10011, 1, 3), ?MREF(10012, 1, 3)],
                 lists:sort(references(B, 3,
                    #{direction => inverse, type => 1001,
                      include_subtypes => true}))),

    % Defining more of the reference type hierarchy in the sub-space B/A,
    % deleting some defined in the super-space.

    add_references(B, [?REF(has_subtype, 100, 1002),
                       ?REF(has_subtype, 1002, 10021)]),
    del_references(B, [?REF(has_subtype, 1001, 10012)]),

    ?assertMatch([?MREF(100, 1, 2), ?MREF(100, 1, 3),
                  ?MREF(1001, 1, 2), ?MREF(1001, 1, 3),
                  ?MREF(1002, 1, 2), ?MREF(1002, 1, 3),
                  ?MREF(10011, 1, 2), ?MREF(10011, 1, 3),
                  ?MREF(10021, 1, 2), ?MREF(10021, 1, 3)],
                 lists:sort(references(B, 1,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1001, 1, 2), ?MREF(1001, 1, 3),
                  ?MREF(10011, 1, 2), ?MREF(10011, 1, 3)],
                 lists:sort(references(B, 1,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1002, 1, 2), ?MREF(1002, 1, 3),
                  ?MREF(10021, 1, 2), ?MREF(10021, 1, 3)],
                 lists:sort(references(B, 1,
                    #{direction => forward, type => 1002,
                      include_subtypes => true}))),


    % Defining more of the reference type hierarchy in the sub-space C/B/A,
    % deleting some defined in the super-spaces.

    add_references(C, [?REF(has_subtype, 1002, 10022),
                       ?REF(has_subtype, 1001, 10012)]),
    del_references(C, [?REF(has_subtype, 1001, 10011),
                       ?REF(has_subtype, 1002, 10021)]),

    ?assertMatch([?MREF(100, 1, 2), ?MREF(100, 1, 3), ?MREF(100, 1, 4),
                  ?MREF(1001, 1, 2), ?MREF(1001, 1, 3), ?MREF(1001, 1, 4),
                  ?MREF(1002, 1, 2), ?MREF(1002, 1, 3), ?MREF(1002, 1, 4),
                  ?MREF(10012, 1, 2), ?MREF(10012, 1, 3), ?MREF(10012, 1, 4),
                  ?MREF(10022, 1, 2), ?MREF(10022, 1, 3), ?MREF(10022, 1, 4)],
                 lists:sort(references(C, 1,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1001, 1, 2), ?MREF(1001, 1, 3), ?MREF(1001, 1, 4),
                  ?MREF(10012, 1, 2), ?MREF(10012, 1, 3), ?MREF(10012, 1, 4)],
                 lists:sort(references(C, 1,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1002, 1, 2), ?MREF(1002, 1, 3), ?MREF(1002, 1, 4),
                  ?MREF(10022, 1, 2), ?MREF(10022, 1, 3), ?MREF(10022, 1, 4)],
                 lists:sort(references(C, 1,
                    #{direction => forward, type => 1002,
                      include_subtypes => true}))),

    % Adding back all the removed reference types

    add_references(C, [?REF(has_subtype, 1001, 10011),
                       ?REF(has_subtype, 1002, 10021)]),

    ?assertMatch([?MREF(100, 1, 2), ?MREF(100, 1, 3), ?MREF(100, 1, 4),
                  ?MREF(1001, 1, 2), ?MREF(1001, 1, 3), ?MREF(1001, 1, 4),
                  ?MREF(1002, 1, 2), ?MREF(1002, 1, 3), ?MREF(1002, 1, 4),
                  ?MREF(10011, 1, 2), ?MREF(10011, 1, 3), ?MREF(10011, 1, 4),
                  ?MREF(10012, 1, 2), ?MREF(10012, 1, 3), ?MREF(10012, 1, 4),
                  ?MREF(10021, 1, 2), ?MREF(10021, 1, 3), ?MREF(10021, 1, 4),
                  ?MREF(10022, 1, 2), ?MREF(10022, 1, 3), ?MREF(10022, 1, 4)],
                 lists:sort(references(C, 1,
                    #{direction => forward, type => 100,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1001, 1, 2), ?MREF(1001, 1, 3), ?MREF(1001, 1, 4),
                  ?MREF(10011, 1, 2), ?MREF(10011, 1, 3), ?MREF(10011, 1, 4),
                  ?MREF(10012, 1, 2), ?MREF(10012, 1, 3), ?MREF(10012, 1, 4)],
                 lists:sort(references(C, 1,
                    #{direction => forward, type => 1001,
                      include_subtypes => true}))),
    ?assertMatch([?MREF(1002, 1, 2), ?MREF(1002, 1, 3), ?MREF(1002, 1, 4),
                  ?MREF(10021, 1, 2), ?MREF(10021, 1, 3), ?MREF(10021, 1, 4),
                  ?MREF(10022, 1, 2), ?MREF(10022, 1, 3), ?MREF(10022, 1, 4)],
                 lists:sort(references(C, 1,
                    #{direction => forward, type => 1002,
                      include_subtypes => true}))),

    opcua_space_backend:terminate(C),
    opcua_space_backend:terminate(B),
    opcua_space_backend:terminate(A),
    ok.
