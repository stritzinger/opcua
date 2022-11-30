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
