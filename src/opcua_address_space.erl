-module(opcua_address_space).

-behavior(gen_server).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([create/1]).
-export([destroy/1]).
-export([start_link/1]).
-export([add_nodes/2]).
-export([del_nodes/2]).
-export([add_references/2]).
-export([get_node/2]).
-export([get_references/2]).
-export([get_references/3]).
-export([is_subtype/3]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-ignore_xref([{?MODULE, start_link, 0}]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(node, {
    id       :: opcua:node_id(),
    instance :: #opcua_node{} | pid()
}).

-record(reference, {
    index  :: {opcua:node_id(), opcua:node_id(), forward | inverse},
    target :: opcua:node_id()
}).

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SUBTYPES_CACHE, opcua_address_space_subtypes_cache).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create(Context) ->
    opcua_address_space_sup:start_child(Context).

destroy(Context) ->
    gen_server:call(proc(Context), stop).

start_link(Context) ->
    gen_server:start_link(?MODULE, {Context}, []).

add_nodes(Context, Nodes) ->
    gen_server:call(proc(Context), {add_nodes, Nodes}).

del_nodes(Context, NodeIds) ->
    gen_server:call(proc(Context), {del_nodes, NodeIds}).

add_references(Context, References) ->
    gen_server:call(proc(Context), {add_references, References}).

get_node(Context, NodeId) ->
    case ets:lookup(persistent_term:get(key(Context, nodes)), NodeId) of
        [#node{instance = Node}] -> Node;
        []                       -> undefined
    end.

get_references(Context, OriginId) ->
    get_references(Context, OriginId, #{}).

get_references(Context, OriginId, Opts) ->
    Graph = persistent_term:get(?MODULE),
    Subtypes = maps:get(include_subtypes, Opts, false),
    Direction = maps:get(direction, Opts, forward),
    RefTypeId = case maps:get(type, Opts, undefined) of
        undefined -> undefined;
        ?UNDEF_NODE_ID -> undefined;
        #opcua_node_id{} = NodeId -> NodeId
    end,
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
-spec is_subtype(term(), opcua:node_id(), opcua:node_id()) -> boolean().
is_subtype(Context, TypeId, TypeId) -> true;
is_subtype(Context, TypeId, SuperTypeId) ->
    maps:is_key(TypeId, subtypes(SuperTypeId)).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Context}) ->
    NodesTable = ets:new(opcua_address_space_nodes, [
        {keypos, #node.id}
    ]),
    ReferencesTable = ets:new(opcua_address_space_references, [
        ordered_set,
        {keypos, #reference.index}
    ]),

    ProcKey = key(Context),
    NodesKey = key(Context, nodes),
    ReferencesKey = key(Context, references),
    Keys = [ProcKey, NodesKey, ReferencesKey],

    State = #{
        nodes => NodesTable,
        references => ReferencesTable,
        keys => Keys
    },

    spawn_cleanup_proc(self(), Keys),

    persistent_term:put(ProcKey, self()),
    persistent_term:put(NodesKey, NodesTable),
    persistent_term:put(ReferencesKey, ReferencesTable),

    persistent_term:put(?SUBTYPES_CACHE, #{}), % FIXME: Remove?
    {ok, State}.

handle_call({add_nodes, Nodes}, _From, #{nodes := NodesTable} = State) ->
    true = ets:insert_new(NodesTable, [#node{id = ID, instance = Node} ||
        #opcua_node{node_id = ID} = Node <- Nodes
    ]),
    {reply, ok, State};
handle_call({del_nodes, NodeIDs}, _From, #{nodes := NodesTable} = State) ->
    [ets:delete(NodesTable, NodeID) || NodeID <- NodeIDs],
    {reply, ok, State};
handle_call({add_references, References}, _From, #{references := ReferencesTable} = State) ->
    [ets:insert_new(ReferencesTable, R) || R <- expand_references(References)],
    {reply, ok, State};
handle_call(stop, From, State) ->
    {stop, normal, maps:put(from, From, State)}.

handle_cast({cache_subtypes, Type, Subtypes}, State) ->
    Cache = persistent_term:get(?SUBTYPES_CACHE),
    persistent_term:put(?SUBTYPES_CACHE, Cache#{Type => Subtypes}),
    {noreply, State};
handle_cast(Request, _State) ->
    error({unknown_cast, Request}).

handle_info(Info, _State) ->
    error({unknown_info, Info}).

terminate(normal, #{from := From} = State) ->
    true = ets:delete(maps:get(nodes, State)),
    true = ets:delete(maps:get(references, State)),
    cleanup_persitent_terms(maps:get(keys, State)),
    gen_server:reply(From, ok).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cleanup_persitent_terms(Keys) ->
    [persistent_term:erase(K) || K <- Keys].

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
        type_id = Type
    }.

proc(Context) -> persistent_term:get(key(Context)).

key(Context) -> {?MODULE, Context}.

key(Context, Type) -> {?MODULE, {Context, Type}}.

expand_references(References) ->
    [expand_reference(R) || R <- References].

expand_reference(#opcua_reference{type_id = TypeID} = Ref) ->
    #reference{
        index = {Ref#opcua_reference.source_id, TypeID, forward},
        target = Ref#opcua_reference.target_id
    }.

spawn_cleanup_proc(Pid, Keys) ->
    spawn(fun() ->
        Ref = erlang:monitor(process, Pid),
        receive
            {'DOWN', Ref, process, Pid, _Reason} ->
                cleanup_persitent_terms(Keys)
        end
    end).
