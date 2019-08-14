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
    get_references(OriginId, []).

get_references(OriginId, Opts) ->
    Graph = persistent_term:get(?MODULE),
    FilterFun = make_reference_filter(OriginId, Opts),
    MakeRefFun = fun({_, From, To, Type}) ->
        #opcua_reference{
            target_id = To,
            reference_type_id = Type,
            is_forward = From =:= OriginId
        }
    end,
    Edges = [digraph:edge(Graph, I) || I <- digraph:edges(Graph, OriginId)],
    [MakeRefFun(E) || E <- Edges, FilterFun(E)].

%% @doc Returns if the given OPCUA type node id is a subtype of the second
%% given OPCUA type node id.
-spec is_subtype(opcua:node_id(), opcua:node_id()) -> boolean().
is_subtype(TypeId, SuperTypeId) -> maps:is_key(SuperTypeId, supertypes(TypeId)).


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

%% Call given function for all given values expanding the value to a new set
%% of value until there is nothing more to expand.
-spec expand(expand_fun(), [term()]) -> [term()].
expand(ExpFun, Values) ->
    expand(ExpFun, Values, #{}).

expand(_ExpFun, [], Acc) -> Acc;
expand(ExpFun, [V | Rest], Acc) ->
    expand(ExpFun, Rest, expand(ExpFun, ExpFun(V), Acc#{V => true})).


%% Returns a map where the leys are the node id of all the OPCUA supertypes
%% of the given type id, including the given type id.
-spec supertypes(opcua:node_id()) -> #{opcua:node_id() => true}.
supertypes(TypeNodeId) ->
    expand(fun(Id) ->
        #opcua_node{references = Refs} = opcua_address_space:get_node(Id),
        [SubId || #opcua_reference{
            reference_type_id = ?NNID(?REF_HAS_SUBTYPE),
            is_forward = false,
            target_id = SubId} <- Refs]
    end, [TypeNodeId]).

make_reference_filter(OriginId, Opts) ->
    Subtypes = proplists:is_defined(include_subtypes, Opts),
    RefTypeId = proplists:get_value(type, Opts),
    Direction = proplists:get_value(direction, Opts),
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
            fun({_, I, _, T}) -> I =:= OriginId andalso is_subtype(T, Id) end;
        {Id, true, inverse} ->
            fun({_, _, I, T}) -> I =:= OriginId andalso is_subtype(T, Id) end;
        {Id, true, _} ->
            fun({_, _, _, T}) -> is_subtype(T, Id) end
    end.