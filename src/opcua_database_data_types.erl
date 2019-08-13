-module(opcua_database_data_types).

-export([lookup/1, setup/1, example/1]).

%% event handler for sax parser
-export([parse/3]).

-include("opcua_database.hrl").
-include("opcua_codec.hrl").

-define(DB_DATA_TYPES, db_data_types).


%% PUBLIC API

lookup(NodeId) ->
    proplists:get_value(NodeId, ets:lookup(?DB_DATA_TYPES, NodeId)).

setup(File) ->
    _Tid = ets:new(?DB_DATA_TYPES, [named_table]),
    xmerl_sax_parser:file(File, [{event_fun, fun parse/3}]).

example(#opcua_node_id{value = Id}) when ?IS_BUILTIN_TYPE_ID(Id) ->
    opcua_codec:builtin_type_name(Id);
example(NodeId = #opcua_node_id{}) ->
    example(lookup(NodeId));
example(#structure{fields = Fields}) ->
    lists:foldl(fun(#field{name=Name, node_id=NodeId, value_rank=N}, Map) when N==-1 ->
                        maps:put(Name, example(NodeId), Map);
                   (#field{name=Name, node_id=NodeId, value_rank=N}, Map) when N>0 ->
                        maps:put(Name, [example(NodeId)], Map)
                end, #{}, Fields);
example(#enum{fields = [#field{name=Name}|_]}) ->
    Name; %% just take the first element as example
example(#union{fields = [#field{name=Name, node_id = NodeId}|_]}) ->
    #{Name => example(NodeId)}; %% just take the first element as example
example(#builtin{builtin_node_id = #opcua_node_id{value = Id}}) ->
    opcua_codec:builtin_type_name(Id);
example(Id) ->
    example(opcua_codec:node_id(Id)).


%% INTERNAL

parse({startElement, _, "UADataType", _, Attributes}, _Loc, _State) ->
    NodeId = opcua_util:get_node_id("NodeId", Attributes),
    Name = opcua_util:convert_name(opcua_util:get_attr("BrowseName", Attributes)),
    #{node_id => NodeId, name => Name, fields => []};
parse({startElement, _, "Definition", _, Attributes}, _Loc, DataTypeMap) ->
    IsOptionSet = opcua_util:get_attr("IsOptionSet", Attributes, false) == "true",
    maps:put(is_option_set, IsOptionSet, DataTypeMap);
parse({startElement, _, "Reference", _, Attributes}, _Loc, DataTypeMap)
  when is_map(DataTypeMap) ->
    case opcua_util:get_attr("ReferenceType", Attributes) of
        "HasSubtype" -> {await_subtype, DataTypeMap};
        _ -> DataTypeMap
    end;
parse({characters, Chars}, _Loc, {await_subtype, DataTypeMap}) ->
    maps:put(parent_node_id, opcua_util:parse_node_id(Chars), DataTypeMap);
parse({startElement, _, "Field", _, Attributes}, _Loc, DataTypeMap) ->
    Name = opcua_util:convert_name(opcua_util:get_attr("Name", Attributes)),
    NodeId = opcua_util:get_node_id("DataType", Attributes),
    Value = opcua_util:get_int("Value", Attributes),
    ValueRank = opcua_util:get_int("ValueRank", Attributes, -1),
    NewField = #field{name = Name,
                      node_id = NodeId,
                      value_rank = ValueRank,
                      value = Value},
    NewFields = maps:get(fields, DataTypeMap) ++ [NewField],
    maps:put(fields, NewFields, DataTypeMap);
parse({endElement, _, "UADataType", _}, _Loc, #{node_id := #opcua_node_id{value =Id}})
  when ?IS_BUILTIN_TYPE_ID(Id) -> ok;
parse({endElement, _, "UADataType", _}, _Loc, DataTypeMap = #{node_id := NodeId, name := Name}) ->
    {RootNodeId, Fields} = resolve_inheritance(DataTypeMap),
    DataType = resolve_type(RootNodeId, DataTypeMap, Fields),
    store_data_type(NodeId, Name, DataType);
parse(_Event, _Loc, State) ->
    State.

resolve_type(#opcua_node_id{value = 22}, Map = #{node_id := NodeId}, Fields) ->
    #structure{node_id = NodeId, with_options = maps:get(is_option_set, Map, false), fields = Fields};
resolve_type(#opcua_node_id{value = 29}, #{node_id := NodeId}, Fields) ->
    #enum{node_id = NodeId, fields = Fields};
resolve_type(#opcua_node_id{value = 12756}, #{node_id := NodeId}, Fields) ->
    #union{node_id = NodeId, fields = Fields};
resolve_type(BuiltinNodeId = #opcua_node_id{value = Id}, #{node_id := NodeId}, _Fields)
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    #builtin{node_id = NodeId, builtin_node_id = BuiltinNodeId}.

resolve_inheritance(#{parent_node_id := ParentNodeId, fields := Fields}) ->
    {RootNodeId, NewFields} = resolve_inheritance1(ParentNodeId),
    {RootNodeId, NewFields ++ Fields}.

resolve_inheritance1(NodeId = #opcua_node_id{value = Id})
  when Id=:=29;Id=:=12756 ->
    {NodeId, []};
resolve_inheritance1(NodeId = #opcua_node_id{value = Id}) 
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    {NodeId, []};
resolve_inheritance1(NodeId) ->
    DataType = lookup(NodeId),
    case DataType of
        #structure{fields = Fields} -> {#opcua_node_id{value = 22}, Fields};
        #enum{fields = Fields} -> {#opcua_node_id{value = 29}, Fields};
        #union{fields = Fields} -> {#opcua_node_id{value = 12756}, Fields};
        #builtin{builtin_node_id = BuiltinNodeId} -> {BuiltinNodeId, []}
    end.

store_data_type(NodeId = #opcua_node_id{value = Id}, Name, DataType) ->
    StringNodeId = #opcua_node_id{type = string, value = Name},
    KeyValuePairs = [{StringNodeId, DataType}, {NodeId, DataType},
                     {{0, Name}, DataType}, {{0, Id}, DataType}],
    ets:insert(?DB_DATA_TYPES, KeyValuePairs).
