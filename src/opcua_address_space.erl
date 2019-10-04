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

-record(refs, {
    index   :: {opcua:node_id(), opcua:node_id(), forward | inverse},
    targets :: opcua:node_id()
}).

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(BROWSE_DEFAULT_OPTS, #{
    include_subtypes => false,
    type             => undefined,
    direction        => forward
}).

-define(is_node(N), is_record(N, opcua_node_id)).

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
    case ets:lookup(table(Context, nodes), NodeId) of
        [#node{instance = Node}] -> Node;
        []                       -> undefined
    end.

get_references(Context, OriginNode) when ?is_node(OriginNode) ->
    get_references(Context, OriginNode, #{}).

get_references(Context, OriginNode, Opts) when ?is_node(OriginNode) ->
    #{direction := Dir} = FullOpts = maps:merge(?BROWSE_DEFAULT_OPTS, Opts),
    Table = table(Context, references),
    {TypeSpec, Filter} = type_filter(Context, FullOpts),
    Index = {OriginNode, TypeSpec, spec_dir(Dir)},
    Spec = [{#refs{index = Index, targets = '_'}, [], ['$_']}],
    lists:flatten([to_references(R) || R <- ets:select(Table, Spec), Filter(R)]).

%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Context}) ->
    NodesTable = ets:new(opcua_address_space_nodes, [
        {keypos, #node.id}
    ]),
    ReferencesTable = ets:new(opcua_address_space_references, [
        ordered_set,
        {keypos, #refs.index}
    ]),
    SubtypesTable = ets:new(opcua_address_space_subtypes, []),

    ProcKey = key(Context),
    NodesKey = key(Context, nodes),
    ReferencesKey = key(Context, references),
    SubtypesKey = key(Context, subtypes),
    Keys = [ProcKey, NodesKey, ReferencesKey, SubtypesKey],

    State = #{
        context => Context,
        nodes => NodesTable,
        references => ReferencesTable,
        subtypes => SubtypesTable,
        keys => Keys
    },

    spawn_cleanup_proc(self(), Keys),

    persistent_term:put(ProcKey, self()),
    persistent_term:put(NodesKey, NodesTable),
    persistent_term:put(ReferencesKey, ReferencesTable),
    persistent_term:put(SubtypesKey, SubtypesTable),

    {ok, State}.

handle_call({add_nodes, Nodes}, _From, #{nodes := NodesTable} = State) ->
    true = ets:insert_new(NodesTable, [#node{id = ID, instance = Node} ||
        #opcua_node{node_id = ID} = Node <- Nodes
    ]),
    {reply, ok, State};
handle_call({del_nodes, NodeIDs}, _From, #{nodes := NodesTable} = State) ->
    [ets:delete(NodesTable, NodeID) || NodeID <- NodeIDs],
    {reply, ok, State};
handle_call({add_references, References}, _From, State) ->
    insert_references(References, State),
    {reply, ok, State};
handle_call(stop, From, State) ->
    {stop, normal, maps:put(from, From, State)}.

handle_cast(Request, _State) ->
    error({unknown_cast, Request}).

handle_info(Info, _State) ->
    error({unknown_info, Info}).

terminate(normal, #{from := From} = State) ->
    true = ets:delete(maps:get(nodes, State)),
    true = ets:delete(maps:get(references, State)),
    cleanup_persistent_terms(maps:get(keys, State)),
    gen_server:reply(From, ok).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cleanup_persistent_terms(Keys) ->
    [persistent_term:erase(K) || K <- Keys].

table(Context, Type) -> persistent_term:get(key(Context, Type)).

proc(Context) -> persistent_term:get(key(Context)).

key(Context) -> {?MODULE, Context}.

key(Context, Type) -> {?MODULE, {Context, Type}}.

insert_references([], _State) ->
    [];
insert_references([Ref|References], #{references := Table} = State) ->
    #opcua_reference{
        source_id = Source,
        target_id = Target,
        type_id = Type
    } = Ref,
    insert_reference(Table, Source, Type, forward, Target),
    insert_reference(Table, Target, Type, inverse, Source),
    cache_subtypes(Type, Source, Target, State),
    insert_references(References, State).

insert_reference(Table, Source, Type, Direction, Target) ->
    Key = {Source, Type, Direction},
    Targets = case ets:lookup(Table, Key) of
        [#refs{index = Key, targets = Ts}] -> Ts;
        []                                 -> []
    end,
    ets:insert(Table, #refs{index = Key, targets = Targets ++ [Target]}).

cache_subtypes(?NNID(?REF_HAS_SUBTYPE), Source, Target, State) ->
    Table = maps:get(subtypes, State),
    % Add target to existing subtypes
    SubTypes = case ets:lookup(Table, Source) of
        [{Source, Map}] -> Map;
        []              -> #{}
    end,
    ets:insert(Table, {Source, maps:put(Target, true, SubTypes)}),
    % Add target to all super types as well
    SuperTypes = get_references(maps:get(context, State), Source, #{
        type => ?NNID(?REF_HAS_SUBTYPE),
        direction => inverse
    }),
    lists:map(fun(#opcua_reference{source_id = S}) ->
        cache_subtypes(?NNID(?REF_HAS_SUBTYPE), S, Target, State)
    end, SuperTypes);
cache_subtypes(_Type, _Source, _Target, _State) ->
    ok.

is_subtype(_Context, Type, Type) ->
    true;
is_subtype(Context, Type, SuperType) ->
    case ets:lookup(table(Context, subtypes), SuperType) of
        [{SuperType, SubTypes}] -> maps:is_key(Type, SubTypes);
        []                      -> false
    end.

spawn_cleanup_proc(Pid, Keys) ->
    spawn(fun() ->
        Ref = erlang:monitor(process, Pid),
        receive
            {'DOWN', Ref, process, Pid, _Reason} ->
                cleanup_persistent_terms(Keys)
        end
    end).

type_filter(Context, #{include_subtypes := true, type := Type})
  when ?is_node(Type) ->
    {
        '_',
        fun(#refs{index = {_, T, _}}) -> is_subtype(Context, T, Type) end
    };
type_filter(_Context, #{type := Type}) ->
    {
        spec_type(Type),
        fun(_) -> true end
    }.

spec_type(undefined)                -> '_';
spec_type(?UNDEF_NODE_ID)           -> '_';
spec_type(Type) when ?is_node(Type) -> Type.

spec_dir(both) -> '_';
spec_dir(Dir)  -> Dir.

to_references(#refs{index = {Source, Type, forward}, targets = Targets}) ->
    [#opcua_reference{source_id = Source, type_id = Type, target_id = T} || T <- Targets];
to_references(#refs{index = {Source, Type, inverse}, targets = Targets}) ->
    [#opcua_reference{source_id = T, type_id = Type, target_id = Source} || T <- Targets].
