-module(opcua_address_space).

-behavior(gen_server).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/0]).
-export([add_nodes/1]).
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

%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type expand_fun() :: fun((term()) -> [term()]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, undefined, []).

add_nodes(Nodes) ->
    gen_server:call(?MODULE, {add_nodes, Nodes}).

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
    FilterFun = make_reference_filter(OriginId, Opts),
    Edges = [digraph:edge(Graph, I) || I <- digraph:edges(Graph, OriginId)],
    [edge_to_ref(OriginId, E) || E <- Edges, FilterFun(E)].

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
    {ok, G}.

handle_call({add_nodes, Nodes}, _From, G) ->
    [digraph:add_vertex(G, Node#opcua_node.node_id, Node) || Node <- Nodes],
    {reply, ok, G};
handle_call({add_references, References}, _From, G) ->
    [digraph:add_edge(G, N1, N2, Type) || {N1, #opcua_reference{target_id = N2, reference_type_id = Type}} <- References],
    {reply, ok, G}.

handle_cast(Request, _State) ->
    error({unknown_cast, Request}).

handle_info(Info, _State) ->
    error({unknown_info, Info}).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Returns a map where the keys are the node id of all the OPCUA subtypes
%% of the given type id.
subtypes(TypenodeId) -> subtypes(TypenodeId, #{}).

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

edge_to_ref(OriginId, {_, From, OriginId, Type}) ->
    #opcua_reference{
        target_id = From,
        reference_type_id = Type,
        is_forward = false
    };
edge_to_ref(OriginId, {_, OriginId, To, Type}) ->
    #opcua_reference{
        target_id = To,
        reference_type_id = Type,
        is_forward = true
    }.

make_reference_filter(OriginId, Opts) ->
    Subtypes = maps:get(include_subtypes, Opts, false),
    RefTypeId = maps:get(type, Opts, undefined),
    Direction = maps:get(direction, Opts, forward),
    case {RefTypeId, Subtypes, Direction} of
        {undefined, _, forward} ->
            fun({_, I, _, _}) -> I =:= OriginId end;
        {undefined, _, inverse} ->
            fun({_, _, I, _}) -> I =:= OriginId end;
        {undefined, _, _} ->
            fun({_, _, _, _}) -> true end;
        {Id, false, forward} ->
            fun({_, I, _, T}) -> I =:= OriginId andalso T =:= Id end;
        {Id, false, inverse} ->
            fun({_, _, I, T}) -> I =:= OriginId andalso T =:= Id end;
        {Id, false, _} ->
            fun({_, _, _, T}) -> T =:= Id end;
        {Id, true, forward} ->
            RefTypes = (subtypes(Id))#{Id => true},
            fun({_, I, _, T}) -> I =:= OriginId andalso maps:is_key(T, RefTypes) end;
        {Id, true, inverse} ->
            RefTypes = (subtypes(Id))#{Id => true},
            fun({_, _, I, T}) -> I =:= OriginId andalso maps:is_key(T, RefTypes) end;
        {Id, true, _} ->
            RefTypes = (subtypes(Id))#{Id => true},
            fun({_, _, _, T}) -> maps:is_key(T, RefTypes) end
    end.