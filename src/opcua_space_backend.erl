%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA address space data structures
%%%
%%% Uses tombstones instead of deleting anything. This is needed for layered
%%% spaces, so each connections can share a base space but changes are isolated.
%%%
%%% node/2 andreference/2,3 functions should be used with al list of space
%%% references to take advantage of the layers.
%%%
%%% add_references and del_references should be called with all the layers, even
%%% though only the last one (the list head) will ever be modified.
%%% This is because the full stack of layers are needed to properly maintains
%%% the reference sub-type cache when adding and deleting references.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_space_backend).

-behavior(opcua_space).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions to be called only from owning process
-export([new/0, new/1]).
-export([init/0, init/1]).
-export([terminate/1]).
-export([add_nodes/2]).
-export([del_nodes/2]).
-export([add_references/2]).
-export([del_references/2]).

%% API functions for shared reference
-export([node/2]).
-export([references/3]).
-export([data_type/2]).
-export([type_descriptor/3]).
-export([schema/2]).
-export([namespace_uri/2]).
-export([namespace_id/2]).
-export([namespaces/1]).

%% Persistence API Functions
-export([fold/3]).
-export([store/2]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type space() :: atom() | reference().
-type spaces() :: [space()].

-record(space_node, {
    id       :: opcua:node_id(),
    instance :: deleted | #opcua_node{}
}).

-record(space_refs, {
    index   :: {opcua:node_id(), opcua:node_id(), forward | inverse},
    targets :: #{opcua:node_id() := true | deleted}
             | '_' % to use as ETS spec and make dialyzer happy
}).

-export_type([space/0, spaces/0]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(GETREF_DEFAULT_OPTS, #{
    include_subtypes => false,
    type             => undefined,
    direction        => forward
}).


%%% API FUNCTIONS FOR OWNING PROCESS ONLY %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new() -> opcua_space:state().
new() ->
    new([]).

-spec new(spaces()) -> opcua_space:state().
new(Parent) when is_list(Parent) ->
    {?MODULE, [init() | Parent]}.

-spec init() -> space().
init() ->
    init(make_ref()).

-spec init(space()) -> space().
init(Space) ->
    NodesTable = ets:new(opcua_space_nodes,
        [{read_concurrency, true}, {keypos, #space_node.id}]),
    RefsTable = ets:new(opcua_references,
        [{read_concurrency, true}, {keypos, #space_refs.index}, ordered_set]),
    RefSubTable = ets:new(opcua_space_ref_subtypes,
        [{read_concurrency, true}]),
    EncodingTable = ets:new(opcua_space_encodings,
        [{read_concurrency, true}]),
    DataTypesTable = ets:new(opcua_space_datatypes,
        [{read_concurrency, true}]),
    NamespaceIdsTable = ets:new(opcua_space_namespace_ids,
        [{read_concurrency, true}, {keypos, 2}]),
    NamespaceUrisTable = ets:new(opcua_space_namespace_uris,
        [{read_concurrency, true}, {keypos, 1}]),

    NodesKey = key(Space, nodes),
    ReferencesKey = key(Space, references),
    RefSubKey = key(Space, ref_subtypes),
    EncodingKey = key(Space, encodings),
    DataTypesKey = key(Space, datatypes),
    NamespaceIdsKey = key(Space, namespace_ids),
    NamespaceUrisKey = key(Space, namespace_uris),
    Keys = [NodesKey, ReferencesKey, RefSubKey, EncodingKey,
            DataTypesKey, NamespaceIdsKey, NamespaceUrisKey],

    spawn_cleanup_proc(self(), Keys),

    persistent_term:put(NodesKey, NodesTable),
    persistent_term:put(ReferencesKey, RefsTable),
    persistent_term:put(RefSubKey, RefSubTable),
    persistent_term:put(EncodingKey, EncodingTable),
    persistent_term:put(DataTypesKey, DataTypesTable),
    persistent_term:put(NamespaceIdsKey, NamespaceIdsTable),
    persistent_term:put(NamespaceUrisKey, NamespaceUrisTable),

    Space.

-spec terminate(space()) -> ok.
terminate(Space) ->
    NodesKey = key(Space, nodes),
    ReferencesKey = key(Space, references),
    RefSubKey = key(Space, ref_subtypes),
    EncodingKey = key(Space, encodings),
    DataTypesKey = key(Space, datatypes),
    NamespaceIdsKey = key(Space, namespace_ids),
    NamespaceUrisKey = key(Space, namespace_uris),
    Keys = [NodesKey, ReferencesKey, RefSubKey, EncodingKey,
            DataTypesKey, NamespaceIdsKey, NamespaceUrisKey],
    ets:delete(persistent_term:get(NodesKey)),
    ets:delete(persistent_term:get(ReferencesKey)),
    ets:delete(persistent_term:get(RefSubKey)),
    ets:delete(persistent_term:get(EncodingKey)),
    ets:delete(persistent_term:get(DataTypesKey)),
    ets:delete(persistent_term:get(NamespaceIdsKey)),
    ets:delete(persistent_term:get(NamespaceUrisKey)),
    cleanup_persistent_terms(Keys),
    ok.

% @doc Adds a node to a space, if multiple layers of space are given, only
% the last one is modified (the head).
-spec add_nodes(space() | spaces(), [opcua:node_rec()]) -> ok.
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
-spec del_nodes(space() | spaces(), [opcua:node_id()]) -> ok.
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
-spec add_references(space() | spaces(), [opcua:node_ref()]) -> ok.
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
-spec del_references(space() | spaces(), [opcua:node_ref()]) -> ok.
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

-spec node(space() | spaces(), opcua:node_spec()) ->
    opcua:node_rec() | undefined.
node([], _NodeSpec) ->
    undefined;
node([Space | Rest], NodeSpec) ->
    NodeId = opcua_node:id(NodeSpec),
    case ets:lookup(table(Space, nodes), NodeId) of
        [#space_node{instance = deleted}] -> undefined;
        [#space_node{instance = Result}] -> Result;
        [] -> node(Rest, NodeSpec)
    end;
node(Space, NodeSpec) ->
    node([Space], NodeSpec).

-spec references(space() | spaces(), opcua:node_spec(),
                 opcua:references_options()) ->
    [opcua:node_ref()].
references(Spaces, OriginNodeSpec, Opts) when is_list(Spaces) ->
    get_refs(Spaces, opcua_node:id(OriginNodeSpec), Opts);
references(Space, OriginNodeSpec, Opts) ->
    get_refs([Space], opcua_node:id(OriginNodeSpec), Opts).

-spec data_type(space() | spaces(), opcua:node_spec()) ->
    {opcua:node_id(), opcua:stream_encoding()} | undefined.
data_type([_Space | _Rest] = Spaces, TypeDescriptorNodeSpec) ->
    NodeId = opcua_node:id(TypeDescriptorNodeSpec),
    get_data_type(Spaces, NodeId);
data_type(Space, TypeDescriptorNodeSpec) ->
    NodeId = opcua_node:id(TypeDescriptorNodeSpec),
    get_data_type([Space], NodeId).

-spec type_descriptor(space() | spaces(), opcua:node_spec(), opcua:stream_encoding()) ->
    opcua:node_id() | undefined.
type_descriptor([_Space | _Rest] = Spaces, DataTypeNodeSpec, Encoding) ->
    NodeId = opcua_node:id(DataTypeNodeSpec),
    get_type_descriptor(Spaces, NodeId, Encoding);
type_descriptor(Space, DataTypeNodeSpec, Encoding) ->
    NodeId = opcua_node:id(DataTypeNodeSpec),
    get_type_descriptor([Space], NodeId, Encoding).

-spec schema(space() | spaces(), opcua:node_spec()) -> term() | undefined.
schema([_Space | _Rest] = Spaces, NodeSpec) ->
    NodeId = opcua_node:id(NodeSpec),
    get_schema(Spaces, NodeId);
schema(Space, NodeSpec) ->
    NodeId = opcua_node:id(NodeSpec),
    get_schema([Space], NodeId).

-spec namespace_uri(space() | spaces(), non_neg_integer()) -> binary() | undefined.
namespace_uri([_Space | _Rest] = Spaces, Id) when is_integer(Id), Id >= 0 ->
    get_namespace_uri(Spaces, Id);
namespace_uri(Space, Id) when is_integer(Id), Id >= 0 ->
    get_namespace_uri([Space], Id).

-spec namespace_id(space() | spaces(), binary()) -> non_neg_integer() | undefined.
namespace_id([_Space | _Rest] = Spaces, Uri) when is_binary(Uri) ->
    get_namespace_id(Spaces, Uri);
namespace_id(Space, Uri) when is_binary(Uri) ->
    get_namespace_id([Space], Uri).

-spec namespaces(space() | spaces()) -> #{non_neg_integer() => binary()}.
namespaces([_Space | _Rest] = Spaces) ->
    get_namespaces(Spaces);
namespaces(Space) ->
    get_namespaces([Space]).


%%% STORAGE API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold([Space | _], Fun, Acc) ->
    fold(Space, Fun, Acc);
fold(Space, Fun, Acc) ->
    TableSpecs = [
        {nodes, node},
        {references, ref},
        {ref_subtypes, ref_subtypes},
        {encodings, encoding},
        {datatypes, datatype},
        {namespace_ids, namespace}],
    fold(Space, TableSpecs, Fun, Acc).

store([Space | _], Item) ->
    store(Space, Item);
store(Space, {node, Term}) ->
    ets:insert(table(Space, nodes), Term),
    ok;
store(Space, {ref, Term}) ->
    ets:insert(table(Space, references), Term),
    ok;
store(Space, {ref_subtypes, Term}) ->
    ets:insert(table(Space, ref_subtypes), Term),
    ok;
store(Space, {encoding, Term}) ->
    ets:insert(table(Space, encodings), Term),
    ok;
store(Space, {datatype, Term}) ->
    ets:insert(table(Space, datatypes), Term),
    ok;
store(Space, {namespace, Term}) ->
    ets:insert(table(Space, namespace_uris), Term),
    ets:insert(table(Space, namespace_ids), Term),
    ok.


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

fold(_Space, [], _Fun, Acc) -> Acc;
fold(Space, [{TableKey, Prefix} | Rest], Fun, Acc) ->
    Table = table(Space, TableKey),
    Acc2 = ets:foldl(fun(T, A) -> Fun({Prefix, T}, A) end, Acc, Table),
    fold(Space, Rest, Fun, Acc2).

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
    SubTable = table(Space, ref_subtypes),
    mark_subtype(SubTable, Type, SubType, true),
    SuperTypes = references(Spaces, Type, #{
        type => ?NNID(?REF_HAS_SUBTYPE),
        direction => inverse
    }),
    lists:map(fun(#opcua_reference{source_id = SuperType}) ->
        add_subtype(Spaces, SuperType, SubType)
    end, SuperTypes).

del_subtype([Space | _] = Spaces, Type, SubType) ->
    SubTable = table(Space, ref_subtypes),
    mark_subtype(SubTable, Type, SubType, false),
    SuperTypes = references(Spaces, Type, #{
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
    SubTable = table(Space, ref_subtypes),
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

get_data_type([], _NodeId) ->
    undefined;
get_data_type([Space | Rest], NodeId) ->
    EncTable = table(Space, encodings),
    case ets:lookup(EncTable, NodeId) of
        [{_, Result}] -> Result;
        [] -> get_data_type(Rest, NodeId)
    end.

get_type_descriptor([], _NodeId, _Encoding) ->
    undefined;
get_type_descriptor([Space | Rest], NodeId, Encoding) ->
    Key = {NodeId, Encoding},
    EncTable = table(Space, encodings),
    case ets:lookup(EncTable, Key) of
        [{_, Result}] -> Result;
        [] -> get_type_descriptor(Rest, NodeId, Encoding)
    end.

get_schema([], _NodeId) -> undefined;
get_schema([Space | Rest], NodeId) ->
    TypeTable = table(Space, datatypes),
    case ets:lookup(TypeTable, NodeId) of
        [{_, Result}] -> Result;
        [] -> get_schema(Rest, NodeId)
    end.

get_namespace_uri([], _Id) -> undefined;
get_namespace_uri([Space | Rest], Id) ->
    NsTable = table(Space, namespace_uris),
    case ets:lookup(NsTable, Id) of
        [{_, Uri}] -> Uri;
        [] -> get_namespace_uri(Rest, Id)
    end.

get_namespace_id([], _Uri) -> undefined;
get_namespace_id([Space | Rest], Uri) ->
    NsTable = table(Space, namespace_ids),
    case ets:lookup(NsTable, Uri) of
        [{Id, _}] -> Id;
        [] -> get_namespace_id(Rest, Uri)
    end.

get_namespaces(Spaces) ->
    get_namespaces(Spaces, #{}).

get_namespaces([], Acc) -> Acc;
get_namespaces([Space | Rest], Acc) ->
    NsTable = table(Space, namespace_uris),
    Namespaces =  maps:from_list(ets:tab2list(NsTable)),
    get_namespaces(Rest, maps:merge(Namespaces, Acc)).
