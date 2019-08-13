-module(opcua_address_space).

-behavior(gen_server).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/0]).
-export([add_nodes/1]).
-export([add_references/1]).
-export([get_node/1]).
-export([is_subtype/2]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

-ignore_xref([{?MODULE, start_link, 0}]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua_codec.hrl").


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

is_subtype(A, B) -> maps:is_key(B, supertypes_map(A)).


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

expand(ExpFun, Values) ->
    expand(ExpFun, Values, #{}).

expand(_ExpFun, [], Acc) -> Acc;
expand(ExpFun, [V | Rest], Acc) ->
    expand(ExpFun, Rest, expand(ExpFun, ExpFun(V), Acc#{V => true})).


supertypes_map(NodeId) ->
    expand(fun(Id) ->
        #opcua_node{references = Refs} = opcua_address_space:get_node(Id),
        [SubId || #opcua_reference{
            reference_type_id = ?NNID(45), %
            is_forward = false,
            target_id = SubId} <- Refs]
    end, [NodeId]).
