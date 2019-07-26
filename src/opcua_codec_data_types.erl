-module(opcua_codec_data_types).

-export([lookup/1, setup/1, convert_name/1]).

%% event handler for sax parser
-export([data_types/3]).

-include("opcua_codec.hrl").

-define(DB_DATA_TYPES, db_data_types).


%% PUBLIC API

lookup(NodeId) ->
    proplists:get_value(NodeId, ets:lookup(?DB_DATA_TYPES, NodeId)).

setup(File) ->
    _Tid = ets:new(?DB_DATA_TYPES, [named_table]),
    xmerl_sax_parser:file(File, [{event_fun, fun data_types/3}]).


%% INTERNAL

data_types({startElement, _, "UADataType", _, Attributes}, _Loc, _State) ->
    NodeId = get_node_id("NodeId", Attributes),
    Name = convert_name(get_attr("BrowseName", Attributes)),
    #{node_id => NodeId, name => Name, fields => []};
data_types({startElement, _, "Definition", _, Attributes}, _Loc, DataTypeMap) ->
    IsOptionSet = get_attr("IsOptionSet", Attributes, false) == "true",
    maps:put(is_option_set, IsOptionSet, DataTypeMap);
data_types({startElement, _, "Reference", _, Attributes}, _Loc, DataTypeMap)
  when is_map(DataTypeMap) ->
    case get_attr("ReferenceType", Attributes) of
        "HasSubtype" -> {await_subtype, DataTypeMap};
        _ -> DataTypeMap
    end;
data_types({characters, Chars}, _Loc, {await_subtype, DataTypeMap}) ->
    maps:put(parent_node_id, parse_node_id(Chars), DataTypeMap);
data_types({startElement, _, "Field", _, Attributes}, _Loc, DataTypeMap) ->
    Name = convert_name(get_attr("Name", Attributes)),
    NodeId = get_node_id("DataType", Attributes),
    Value = get_attr("Value", Attributes),
    ValueRank = get_attr("ValueRank", Attributes, -1),
    NewField = #{node_id => NodeId, value => Value, value_rank => ValueRank},
    NewFields = maps:get(fields, DataTypeMap) ++ [{Name, NewField}],
    maps:put(fields, NewFields, DataTypeMap);
data_types({endElement, _, "UADataType", _}, _Loc, #{node_id := #node_id{value =Id}})
  when ?IS_BUILTIN_TYPE_ID(Id) -> ok;
data_types({endElement, _, "UADataType", _}, _Loc, DataTypeMap = #{node_id := NodeId, name := Name}) ->
    {RootNodeId, Fields} = resolve_inheritance(DataTypeMap),
    DataType = resolve_type(RootNodeId, DataTypeMap, Fields),
    store_data_type(NodeId, Name, DataType);
data_types(_Event, _Loc, State) ->
    State.

resolve_type(#node_id{value = 22}, Map = #{node_id := NodeId}, Fields) ->
    #structure{node_id = NodeId, with_options = maps:get(is_option_set, Map, false), fields = Fields};
resolve_type(#node_id{value = 29}, #{node_id := NodeId}, Fields) ->
    #enum{node_id = NodeId, fields = Fields};
resolve_type(#node_id{value = 12756}, #{node_id := NodeId}, Fields) ->
    #union{node_id = NodeId, fields = Fields};
resolve_type(BuiltinNodeId = #node_id{value = Id}, #{node_id := NodeId}, _Fields)
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    #builtin{node_id = NodeId, builtin_node_id = BuiltinNodeId}.

resolve_inheritance(#{parent_node_id := ParentNodeId, fields := Fields}) ->
    {RootNodeId, NewFields} = resolve_inheritance1(ParentNodeId),
    {RootNodeId, NewFields ++ Fields}.

resolve_inheritance1(NodeId = #node_id{value = Id})
  when Id=:=29;Id=:=12756 ->
    {NodeId, []};
resolve_inheritance1(NodeId = #node_id{value = Id}) 
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    {NodeId, []};
resolve_inheritance1(NodeId) ->
    DataType = lookup(NodeId),
    case DataType of
        #structure{fields = Fields} -> {#node_id{value = 22}, Fields};
        #enum{fields = Fields} -> {#node_id{value = 29}, Fields};
        #union{fields = Fields} -> {#node_id{value = 12756}, Fields};
        #builtin{builtin_node_id = BuiltinNodeId} -> {BuiltinNodeId, []}
    end.

store_data_type(NodeId = #node_id{value = Id}, Name, DataType) ->
    StringNodeId = #node_id{type = string, value = Name},
    KeyValuePairs = [{StringNodeId, DataType}, {NodeId, DataType},
                     {{0, Name}, DataType}, {{0, Id}, DataType}],
    ets:insert(?DB_DATA_TYPES, KeyValuePairs).

get_node_id(Key, Attributes) ->
    case get_attr(Key, Attributes) of
        undefined       -> undefined;
        NodeIdString    -> parse_node_id(NodeIdString)
    end.

parse_node_id(String) ->
    [_, String1] = string:split(String, "="),
    opcua_codec:node_id(list_to_integer(String1)).

get_attr(Key, Attributes) ->
    get_attr(Key, Attributes, undefined).

get_attr(Key, Attributes, Default) ->
    case lists:keyfind(Key, 3, Attributes) of
        false -> Default;
        Value -> element(4, Value)
    end.

%% converts CamelCase strings to snake_case atoms
convert_name([FirstLetter|Rest]) ->
    list_to_atom(
      string:lowercase([FirstLetter]) ++ 
        lists:flatten(
          lists:map(fun(Char) ->
              case string:uppercase([Char]) of
                  [Char]  -> "_" ++ string:lowercase([Char]);
                  _     -> Char
              end
          end, Rest))).
