-module(opcua_codec_encodings).

-export([resolve/1, lookup/2, setup/1]).

%% event handler for sax parser
-export([parse/3]).

-include("opcua_codec.hrl").

-define(DB_ENCODINGS, db_encodings).


%% PUBLIC API

resolve(NodeId) ->
    proplists:get_value(NodeId, ets:lookup(?DB_ENCODINGS, NodeId)).

lookup(NodeId, Encoding) ->
    Key = {NodeId, Encoding},
    proplists:get_value(Key, ets:lookup(?DB_ENCODINGS, Key)).

setup(File) ->
    _Tid = ets:new(?DB_ENCODINGS, [named_table]),
    xmerl_sax_parser:file(File, [{event_fun, fun parse/3}]).


%% INTERNAL

parse({startElement, _, "UAObject", _, Attributes}, _Loc, _State) ->
    NodeId = opcua_util:get_node_id("NodeId", Attributes),
    case opcua_util:get_attr("BrowseName", Attributes) of
        "Default Binary"    -> #{node_id => NodeId, type => binary};
        _                   -> undefined
    end;
parse({startElement, _, "Reference", _, Attributes}, _Loc, EncodingTypeMap)
  when is_map(EncodingTypeMap) ->
    case opcua_util:get_attr("ReferenceType", Attributes) of
        "HasEncoding"   -> {await_target_node_id, EncodingTypeMap};
        _               -> EncodingTypeMap
    end;
parse({characters, Chars}, _Loc, {await_target_node_id, EncodingTypeMap}) ->
    maps:put(target_node_id, opcua_util:parse_node_id(Chars), EncodingTypeMap);
parse({endElement, _, "UAObject", _}, _Loc, #{node_id := NodeId, target_node_id := TargetNodeId, type := Encoding}) ->
    store_encoding(NodeId, TargetNodeId, Encoding);
parse(_Event, _Loc, State) ->
    State.

store_encoding(NodeId, TargetNodeId, Encoding) ->
    ets:insert(?DB_ENCODINGS, [{NodeId, {TargetNodeId, Encoding}},
                               {{TargetNodeId, Encoding}, NodeId}]).
