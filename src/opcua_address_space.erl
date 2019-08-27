-module(opcua_address_space).

-behavior(gen_server).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/0]).
-export([add_nodes/1]).
-export([del_nodes/1]).
-export([add_references/1]).
-export([get_node/1]).
-export([get_references/1, get_references/2]).
-export([is_subtype/2]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

-ignore_xref([{?MODULE, start_link, 0}]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SUBTYPES_CACHE, opcua_address_space_subtypes_cache).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, undefined, []).

add_nodes(Nodes) ->
    gen_server:call(?MODULE, {add_nodes, Nodes}).

del_nodes(NodeIds) ->
    gen_server:call(?MODULE, {del_nodes, NodeIds}).

add_references(References) ->
    gen_server:call(?MODULE, {add_references, References}).

get_node(NodeId) ->
    case digraph:vertex(persistent_term:get(?MODULE), NodeId) of
        {NodeId, Node} -> Node;
        _ -> undefined
    end.

get_references(OriginId) ->
    get_references(OriginId, #{}).

get_references(OriginId, Opts) ->
    Graph = persistent_term:get(?MODULE),
    Subtypes = maps:get(include_subtypes, Opts, false),
    RefTypeId = maps:get(type, Opts, undefined),
    Direction = maps:get(direction, Opts, forward),
    {GetEdgesFun, FilterFun} = case {RefTypeId, Subtypes, Direction} of
        {undefined, _, forward} ->
            {fun digraph:out_edges/2, fun(_) -> true end};
        {undefined, _, inverse} ->
            {fun digraph:in_edges/2, fun(_) -> true end};
        {undefined, _, _} ->
            {fun digraph:edges/2, fun(_) -> true end};
        {Id, false, forward} ->
            {fun digraph:out_edges/2, fun({_, _, _, T}) -> T =:= Id end};
        {Id, false, inverse} ->
            {fun digraph:in_edges/2, fun({_, _, _, T}) -> T =:= Id end};
        {Id, false, _} ->
            {fun digraph:edges/2, fun({_, _, _, T}) -> T =:= Id end};
        {Id, true, forward} ->
            RefTypes = (subtypes(Id))#{Id => true},
            {fun digraph:out_edges/2, fun({_, _, _, T}) -> maps:is_key(T, RefTypes) end};
        {Id, true, inverse} ->
            RefTypes = (subtypes(Id))#{Id => true},
            {fun digraph:in_edges/2, fun({_, _, _, T}) -> maps:is_key(T, RefTypes) end};
        {Id, true, _} ->
            RefTypes = (subtypes(Id))#{Id => true},
            {fun digraph:edges/2, fun({_, _, _, T}) -> maps:is_key(T, RefTypes) end}
    end,
    Edges = [digraph:edge(Graph, I) || I <- GetEdgesFun(Graph, OriginId)],
    [edge_to_ref(E) || E <- Edges, FilterFun(E)].

%% @doc Returns if the given OPCUA type node id is a subtype of the second
%% given OPCUA type node id.
-spec is_subtype(opcua:node_id(), opcua:node_id()) -> boolean().
is_subtype(TypeId, TypeId) -> true;
is_subtype(TypeId, SuperTypeId) ->
    maps:is_key(TypeId, subtypes(SuperTypeId)).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(undefined) ->
    G = digraph:new([cyclic, protected]),
    persistent_term:put(?MODULE, G),
    persistent_term:put(?SUBTYPES_CACHE, #{}),
    {ok, G}.

handle_call({add_nodes, Nodes}, _From, G) ->
    [digraph:add_vertex(G, Node#opcua_node.node_id, Node) || Node <- Nodes],
    {reply, ok, G};
handle_call({del_nodes, NodeIds}, _From, G) ->
    [digraph:del_vertex(G, NodeId) || NodeId <- NodeIds],
    {reply, ok, G};
handle_call({add_references, References}, _From, G) ->
    [digraph:add_edge(G, N1, N2, Type) || {N1, #opcua_reference{target_id = N2, reference_type_id = Type}} <- References],
    {reply, ok, G}.

handle_cast({cache_subtypes, Type, Subtypes}, State) ->
    Cache = persistent_term:get(?SUBTYPES_CACHE),
    persistent_term:put(?SUBTYPES_CACHE, Cache#{Type => Subtypes}),
    {noreply, State};
handle_cast(Request, _State) ->
    error({unknown_cast, Request}).

handle_info(Info, _State) ->
    error({unknown_info, Info}).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Returns a map where the keys are the node id of all the OPCUA subtypes
%% of the given type id.
subtypes(TypeNodeId) ->
    Cache = persistent_term:get(?SUBTYPES_CACHE),
    case maps:find(TypeNodeId, Cache) of
        {ok, Value} -> Value;
        error ->
            Value = subtypes(TypeNodeId, #{}),
            gen_server:cast(?MODULE, {cache_subtypes, TypeNodeId, Value}),
            Value
    end.

subtypes(TypeNodeId, Acc) ->
    RefOpts = #{
        type => ?NNID(?REF_HAS_SUBTYPE),
        direction => forward,
        include_subtypes => false
    },
    lists:foldl(fun(#opcua_reference{target_id = Id}, Map) ->
        case maps:is_key(Id, Map) of
            false -> subtypes(Id, Map#{Id => true});
            true -> Map
        end
    end, Acc, get_references(TypeNodeId, RefOpts)).

edge_to_ref({_, SourceNodeId, TargetNodeId, Type}) ->
    #opcua_reference{
        source_id = SourceNodeId,
        target_id = TargetNodeId,
        reference_type_id = Type
    }.
