-module(opcua_database_data_types).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([lookup/1]).
-export([generate_schemas/1]).
-export([setup/0]).
-export([store/2]).
-export([example/1]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

lookup(NodeId) ->
    proplists:get_value(NodeId, ets:lookup(?MODULE, NodeId)).

generate_schemas(DataTypeNodes) ->
    Digraph = generate_data_types_digraph(DataTypeNodes),
    SortedNodes = digraph_utils:topsort(Digraph),
    generate_schemas(Digraph, SortedNodes, #{}).

setup() ->
    _Tid = ets:new(?MODULE, [named_table]).

store(Keys, DataType) ->
    KeyValuePairs = [{Key, DataType} || Key <- Keys],
    true = ets:insert(?MODULE, KeyValuePairs).

example(#opcua_node_id{value = Id}) when ?IS_BUILTIN_TYPE_ID(Id) ->
    opcua_codec:builtin_type_name(Id);
example(NodeId = #opcua_node_id{}) ->
    example(lookup(NodeId));
example(#opcua_structure{fields = Fields}) ->
    lists:foldl(fun(#opcua_field{name=Name, node_id=NodeId, value_rank=N}, Map) when N==-1 ->
                        maps:put(Name, example(NodeId), Map);
                   (#opcua_field{name=Name, node_id=NodeId, value_rank=N}, Map) when N>0 ->
                        maps:put(Name, [example(NodeId)], Map)
                end, #{}, Fields);
example(#opcua_enum{fields = [#opcua_field{name=Name}|_]}) ->
    Name; %% just take the first element as example
example(#opcua_union{fields = [#opcua_field{name=Name, node_id = NodeId}|_]}) ->
    #{Name => example(NodeId)}; %% just take the first element as example
example(#opcua_builtin{builtin_node_id = #opcua_node_id{value = Id}}) ->
    opcua_codec:builtin_type_name(Id);
example(Id) ->
    example(opcua_codec:node_id(Id)).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

generate_schemas(_Digraph, [], Acc) ->
    maps:values(Acc);
generate_schemas(Digraph, [#opcua_node{node_id = #opcua_node_id{value = Id}} | Nodes], Acc)
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    generate_schemas(Digraph, Nodes, Acc);
generate_schemas(Digraph, [Node | Nodes], Acc) ->
    [#opcua_node{node_id = ParentNodeId}] = digraph:in_neighbours(Digraph, Node),
    #opcua_node{node_id = NodeId, browse_name = BrowseName, node_class = NodeClass} = Node,
    DataTypeDefinition = case NodeClass#opcua_data_type.data_type_definition of
                             undefined  -> #{};
                             Map        -> Map
                         end,
    Fields = maps:get(fields, DataTypeDefinition, []),
    IsUnion = maps:get(is_union, DataTypeDefinition, false),
    IsOptionSet = maps:get(is_option_set, DataTypeDefinition, false),
    RecordFields = [field_to_record(Field) || Field <- Fields],
    {RootNodeId, NewFields} = resolve_inheritance(ParentNodeId, RecordFields, Acc),
    DataType = resolve_type(RootNodeId, NodeId, NewFields, IsUnion, IsOptionSet),
    StringNodeId = #opcua_node_id{type = string, value = BrowseName},
    Keys = [StringNodeId, NodeId, {0, BrowseName}, {0, NodeId#opcua_node_id.value}],
    generate_schemas(Digraph, Nodes, maps:put(NodeId, {Keys, DataType}, Acc)).

field_to_record({Name, Attrs}) ->
    #opcua_field{name       = Name,
                 node_id    = maps:get(data_type, Attrs, #opcua_node_id{value = 24}),
                 value_rank = maps:get(value_rank, Attrs, -1),
                 value      = maps:get(value, Attrs, undefined)}.

generate_data_types_digraph(DataTypeNodes) ->
    NodesPropList = [{NodeId, Node} || Node = #opcua_node{node_id = NodeId} <- DataTypeNodes],
    DataTypeNodesMap = maps:from_list(NodesPropList),
    Digraph = digraph:new([acyclic]),
    fill_data_types(Digraph, DataTypeNodesMap, DataTypeNodes).

fill_data_types(Digraph, _NodesMap, []) ->
    Digraph;
fill_data_types(Digraph, NodesMap, [Node = #opcua_node{references = References} | Nodes]) ->
    case get_sub_type_reference(References) of
        {SourceNodeId, TargetNodeId} ->
            Source = maps:get(SourceNodeId, NodesMap),
            Target = maps:get(TargetNodeId, NodesMap),
            %% NOTE: we might add nodes multiple times here,
            %% but the add operation is idempotent anyway
            digraph:add_vertex(Digraph, Source),
            digraph:add_vertex(Digraph, Target),
            digraph:add_edge(Digraph, Source, Target);
        undefined ->
            digraph:add_vertex(Digraph, Node) %% we got a root node
    end,
    fill_data_types(Digraph, NodesMap, Nodes).

get_sub_type_reference(References) ->
    SubTypeRefs = [Ref || Ref = #opcua_reference{reference_type_id = #opcua_node_id{value = 45}} <- References],
    case SubTypeRefs of
        [] ->
            undefined;
        [#opcua_reference{source_id = SourceNodeId, target_id = TargetNodeId}] ->
            {SourceNodeId, TargetNodeId}
    end.

resolve_type(#opcua_node_id{value = 22}, NodeId, Fields, false, IsOptionSet) ->
    #opcua_structure{node_id = NodeId, with_options = IsOptionSet, fields = Fields};
resolve_type(#opcua_node_id{value = 22}, NodeId, Fields, true, _IsOptionSet) ->
    #opcua_union{node_id = NodeId, fields = Fields};
resolve_type(#opcua_node_id{value = 29}, NodeId, Fields, _IsUnion, _IsOptionSet) ->
    #opcua_enum{node_id = NodeId, fields = Fields};
resolve_type(BuiltinNodeId = #opcua_node_id{value = Id}, NodeId, _Fields, _IsUnion, _IsOptionSet)
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    #opcua_builtin{node_id = NodeId, builtin_node_id = BuiltinNodeId}.

resolve_inheritance(ParentNodeId, Fields, DataTypes) ->
    {RootNodeId, NewFields} = resolve_inheritance1(ParentNodeId, DataTypes),
    {RootNodeId, NewFields ++ Fields}.

resolve_inheritance1(NodeId = #opcua_node_id{value = Id}, _DataTypes)
  when Id=:=29 ->
    {NodeId, []};
resolve_inheritance1(NodeId = #opcua_node_id{value = Id}, _DataTypes) 
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    {NodeId, []};
resolve_inheritance1(NodeId, DataTypes) ->
    {_Keys, DataType} = maps:get(NodeId, DataTypes),
    case DataType of
        #opcua_structure{fields = Fields} -> {#opcua_node_id{value = 22}, Fields};
        #opcua_union{fields = Fields} -> {#opcua_node_id{value = 22}, Fields};
        #opcua_enum{fields = Fields} -> {#opcua_node_id{value = 29}, Fields};
        #opcua_builtin{builtin_node_id = BuiltinNodeId} -> {BuiltinNodeId, []}
    end.
