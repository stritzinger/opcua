%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA address space data structures
%%%
%%% Uses tombstones instead of deleting anything. This is needed for layered
%%% spaces, so each connections can share a base space but changes are isolated.
%%%
%%% get_node and get_reference functions should be used with al list of space
%%% references to take advantage of the layers.
%%%
%%% add_references and del_references should be called with all the layers, even
%%% though only the last one (the list head) will ever be modified.
%%% This is because the full stack of layers are needed to properly maintains
%%% the reference sub-type cache when adding and deleting references.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_space).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions to be called only from owning process
-export([init/0, init/1]).
-export([terminate/1]).
-export([add_nodes/2]).
-export([del_nodes/2]).
-export([add_references/2]).
-export([del_references/2]).

%% API functions for shared reference
-export([get_node/2]).
-export([get_references/2]).
-export([get_references/3]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type space() :: atom() | reference().

-record(space_node, {
    id       :: opcua:node_id(),
    instance :: deleted | #opcua_node{} | pid()
}).

-record(space_refs, {
    index   :: {opcua:node_id(), opcua:node_id(), forward | inverse},
    targets :: #{opcua:node_id() := true | deleted}
             | '_' % to use as ETS spec and make dialyzer happy
}).

-type get_references_options() :: #{
    include_subtypes => boolean(),
    type => undefined | opcua:node_spec(),
    direction => opcua:direction()
}.


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(GETREF_DEFAULT_OPTS, #{
    include_subtypes => false,
    type             => undefined,
    direction        => forward
}).


%%% API FUNCTIONS FOR OWNING PROCESS ONLY %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init() -> space().
init() ->
    init(make_ref()).

-spec init(space()) -> space().
init(Space) ->
    NodesTable = ets:new(opcua_space_nodes, [{keypos, #space_node.id}]),
    RefsTable = ets:new(opcua_references,
                        [ordered_set, {keypos, #space_refs.index}]),
    SubtypesTable = ets:new(opcua_space_subtypes, []),

    NodesKey = key(Space, nodes),
    ReferencesKey = key(Space, references),
    SubtypesKey = key(Space, subtypes),
    Keys = [NodesKey, ReferencesKey, SubtypesKey],

    spawn_cleanup_proc(self(), Keys),

    persistent_term:put(NodesKey, NodesTable),
    persistent_term:put(ReferencesKey, RefsTable),
    persistent_term:put(SubtypesKey, SubtypesTable),
    Space.

-spec terminate(space()) -> ok.
terminate(Space) ->
    NodesKey = key(Space, nodes),
    ReferencesKey = key(Space, references),
    SubtypesKey = key(Space, subtypes),
    ets:delete(persistent_term:get(NodesKey)),
    ets:delete(persistent_term:get(ReferencesKey)),
    ets:delete(persistent_term:get(SubtypesKey)),
    cleanup_persistent_terms([NodesKey, ReferencesKey, SubtypesKey]),
    ok.

% @doc Adds a node to a space, if multiple layers of space are given, only
% the last one is modified (the head).
-spec add_nodes(space() | [space()], [opcua:node_rec()]) -> ok.
add_nodes([Space | _], Nodes) ->
    NodesTable = table(Space, nodes),
    SpaceNodes = [#space_node{id = Id, instance = Node} ||
                  #opcua_node{node_id = Id} = Node <- Nodes],
    ets:insert(NodesTable, SpaceNodes),
    ok;
add_nodes(Space, Nodes) ->
    add_nodes([Space], Nodes).

% @doc Deletes a node from a space, if multiple layers of space are given, only
% the last one is modified (the head).
-spec del_nodes(space() | [space()], [opcua:node_id()]) -> ok.
del_nodes([Space | _], NodeIds) ->
    NodesTable = table(Space, nodes),
    SpaceNodes = [#space_node{id = Id, instance = deleted} || Id <- NodeIds],
    ets:insert(NodesTable, SpaceNodes),
    ok;
del_nodes(Space, NodeIds) ->
    del_nodes([Space], NodeIds).

% @doc Adds a reference to a space, if multiple layers of space are given, only
% the last one is modified (the head).
% When using layered spaces, the references should always be added specifying
% all the layers, as they are used to properly maintain the reference
% lookup table.
-spec add_references(space() | [space()], [opcua:node_ref()]) -> ok.
add_references([_Space | _], []) -> ok;
add_references([Space | _] = Spaces, [Ref | Rest]) ->
    #opcua_reference{source_id = Source, target_id = Target, type_id = Type} = Ref,
    RefTable = table(Space, references),
    mark_reference(RefTable, Source, Type, forward, Target, true),
    mark_reference(RefTable, Target, Type, inverse, Source, true),
    update_reference_cache(Spaces, added, Source, Type, Target),
    add_references(Spaces, Rest);
add_references(Space, Refs) ->
    add_references([Space], Refs).

% @doc Adds a reference from a space, if multiple layers of space are given, only
% the last one is modified (the head).
% When using layered spaces, the references should always be deleted specifying
% all the layers, as they are used to properly maintain the reference
% lookup table.
-spec del_references(space() | [space()], [opcua:node_ref()]) -> ok.
del_references([_Space | _], []) -> ok;
del_references([Space | _] = Spaces, [Ref | Rest]) ->
    #opcua_reference{source_id = Source, target_id = Target, type_id = Type} = Ref,
    RefTable = table(Space, references),
    mark_reference(RefTable, Source, Type, forward, Target, deleted),
    mark_reference(RefTable, Target, Type, inverse, Source, deleted),
    update_reference_cache(Spaces, deleted, Source, Type, Target),
    del_references(Spaces, Rest);
del_references(Space, Refs) ->
    del_references([Space], Refs).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_node(space() | [space()], opcua:node_spec()) ->
    opcua:node_rec() | pid | undefined.
get_node([], _NodeSpec) ->
    undefined;
get_node([Space | Rest], NodeSpec) ->
    NodeId = opcua_node:id(NodeSpec),
    case ets:lookup(table(Space, nodes), NodeId) of
        [#space_node{instance = deleted}] -> undefined;
        [#space_node{instance = Result}] -> Result;
        [] -> get_node(Rest, NodeSpec)
    end;
get_node(Space, NodeSpec) ->
    get_node([Space], NodeSpec).

-spec get_references(space() | [space()], opcua:node_spec()) ->
    [opcua:node_ref()].
get_references(Spaces, OriginNodeSpec) when is_list(Spaces) ->
    get_refs(Spaces, opcua_node:id(OriginNodeSpec), #{});
get_references(Space, OriginNodeSpec) ->
    get_refs([Space], opcua_node:id(OriginNodeSpec), #{}).

-spec get_references(space() | [space()], opcua:node_spec(),
                     get_references_options()) ->
    [opcua:node_ref()].
get_references(Spaces, OriginNodeSpec, Opts) when is_list(Spaces) ->
    get_refs(Spaces, opcua_node:id(OriginNodeSpec), Opts);
get_references(Space, OriginNodeSpec, Opts) ->
    get_refs([Space], opcua_node:id(OriginNodeSpec), Opts).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

spawn_cleanup_proc(Pid, Keys) ->
    spawn(fun() ->
        MonRef = erlang:monitor(process, Pid),
        receive
            {'DOWN', MonRef, process, Pid, _Reason} ->
                cleanup_persistent_terms(Keys)
        end
    end).

cleanup_persistent_terms(Keys) ->
    [persistent_term:erase(K) || K <- Keys].

table(Context, Type) -> persistent_term:get(key(Context, Type)).

key(Context, Type) -> {?MODULE, {Context, Type}}.

getref_options(Opts) ->
    #{type := Type} = FullOpts = maps:merge(?GETREF_DEFAULT_OPTS, Opts),
    FullOpts#{type := opcua_node:id(Type)}.

get_refs(Spaces, Origin, PartialOpts) ->
    #{direction := Dir} = Opts = getref_options(PartialOpts),
    {TypeSpec, Filter} = type_filter(Spaces, Opts),
    Index = {Origin, TypeSpec, spec_dir(Dir)},
    Select = [{#space_refs{index = Index, targets = '_'}, [], ['$_']}],
    get_refs(Spaces, Select, Filter, #{}, #{}).

get_refs([], _Select, _Filter, Acc, _Del) ->
    maps:keys(Acc);
get_refs([Space | Rest], Select, Filter, Acc, Del) ->
    RefTable = table(Space, references),
    SpaceRefs = [R || R <- ets:select(RefTable, Select), Filter(R)],
    {Acc2, Del2} = expand_space_refs(SpaceRefs, Acc, Del),
    get_refs(Rest, Select, Filter, Acc2, Del2).

expand_space_refs([], Acc, Del) -> {Acc, Del};
expand_space_refs([#space_refs{index = {Source, Type, Dir}, targets = Targets} | Rest], Acc, Del) ->
    {Acc2, Del2} = expand_space_ref(Source, Type, Dir, maps:to_list(Targets), Acc, Del),
    expand_space_refs(Rest, Acc2, Del2).

expand_space_ref(_Source, _Type, _Dir, [], Acc, Del) -> {Acc, Del};
expand_space_ref(Source, Type, forward, [{Target, Mark} | Rest], Acc, Del) ->
    {Acc2, Del2} = expand_ref(Mark, Source, Type, Target, Acc, Del),
    expand_space_ref(Source, Type, forward, Rest, Acc2, Del2);
expand_space_ref(Source, Type, inverse, [{Target, Mark} | Rest], Acc, Del) ->
    {Acc2, Del2} = expand_ref(Mark, Target, Type, Source, Acc, Del),
    expand_space_ref(Source, Type, inverse, Rest, Acc2, Del2).

expand_ref(true, Source, Type, Target, Acc, Del) ->
    Ref = #opcua_reference{source_id = Source, type_id = Type, target_id = Target},
    case maps:is_key(Ref, Del) of
        true -> {Acc, Del};
        false -> {Acc#{Ref => true}, Del}
    end;
expand_ref(deleted, Source, Type, Target, Acc, Del) ->
    Ref = #opcua_reference{source_id = Source, type_id = Type, target_id = Target},
    Del2 = Del#{Ref => true},
    {Acc, Del2}.

update_reference_cache(Spaces, added, Source, ?NNID(?REF_HAS_SUBTYPE), Target) ->
    add_subtype(Spaces, Source, Target);
update_reference_cache(Spaces, deleted, Source, ?NNID(?REF_HAS_SUBTYPE), Target) ->
    del_subtype(Spaces, Source, Target);
update_reference_cache(_Spaces, _Action, _Source, _Type, _Target) ->
    ok.

add_subtype([Space | _] = Spaces, Type, SubType) ->
    SubTable = table(Space, subtypes),
    mark_subtype(SubTable, Type, SubType, true),
    SuperTypes = get_references(Spaces, Type, #{
        type => ?NNID(?REF_HAS_SUBTYPE),
        direction => inverse
    }),
    lists:map(fun(#opcua_reference{source_id = SuperType}) ->
        add_subtype(Spaces, SuperType, SubType)
    end, SuperTypes).

del_subtype([Space | _] = Spaces, Type, SubType) ->
    SubTable = table(Space, subtypes),
    mark_subtype(SubTable, Type, SubType, false),
    SuperTypes = get_references(Spaces, Type, #{
        type => ?NNID(?REF_HAS_SUBTYPE),
        direction => inverse
    }),
    lists:map(fun(#opcua_reference{source_id = SuperType}) ->
        del_subtype(Spaces, SuperType, SubType)
    end, SuperTypes).

mark_reference(RefTable, Source, Type, Direction, Target, Mark) ->
    Key = {Source, Type, Direction},
    SpaceRefs = case ets:lookup(RefTable, Key) of
        [] -> #space_refs{index = Key, targets = #{Target => Mark}};
        [#space_refs{targets = Ts} = Refs] ->
            Refs#space_refs{targets = Ts#{Target => Mark}}
    end,
    ets:insert(RefTable, SpaceRefs).

mark_subtype(SubTable, SuperType, SubType, Mark) ->
    SubTypes = case ets:lookup(SubTable, SuperType) of
        [{_, M}] -> M#{SubType => Mark};
        [] -> #{SubType => Mark}
    end,
    ets:insert(SubTable, {SuperType, SubTypes}),
    ok.

is_subtype(_Spaces, Type, Type) ->
    true;
is_subtype([], _SubType, _SuperType) ->
    false;
is_subtype([Space | Rest], SubType, SuperType) ->
    SubTable = table(Space, subtypes),
    case ets:lookup(SubTable, SuperType) of
        [{SuperType, #{SubType := Result}}] -> Result;
        _ -> is_subtype(Rest, SubType, SuperType)
    end.

type_filter(Spaces, #{include_subtypes := true, type := #opcua_node_id{} = Type}) ->
    {
        '_',
        fun(#space_refs{index = {_, T, _}}) -> is_subtype(Spaces, T, Type) end
    };
type_filter(_Spaces, #{include_subtypes := false, type := Type}) ->
    {
        spec_type(Type),
        fun(_) -> true end
    }.

spec_type(?UNDEF_NODE_ID)       -> '_';
spec_type(#opcua_node_id{} = Type) -> Type.

spec_dir(both) -> '_';
spec_dir(Dir)  -> Dir.
