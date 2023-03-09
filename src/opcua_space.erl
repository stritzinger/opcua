%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA address space interface module.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_space).

%% TODO %%
%%
%% - Add data type definition generate schemas dynamically
%% - If not data type definition is found use EnumStrings, EnumValues
%%   or OptionSetValues and OptionSetLength to deduce a proper schema,
%%   as some servers seem to not always provide a data type definition
%%   for enums and option sets.
%% - Properly update the node version when modifying a data type references.
%%   We probably wnat this to be optional so it is not done when the space
%%   is used as a cache for the server space in the client.
%% - Schema generation is sub-optimal and generation code is triggered
%%   multiple times when adding all the relwevent nodes and references.

%%% BEHAVIOUR opcua_database DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-callback init() -> State
    when State :: term().

-callback add_namespace(State, Id, Uri) -> ok
    when State :: term(), Id :: non_neg_integer(), Uri :: binary().

-callback add_nodes(State, [Node]) -> ok
    when State :: term(), Node :: opcua:node_rec().

-callback del_nodes(State, [NodeSpec]) -> ok
    when State :: term(), NodeSpec :: opcua:node_spec().

-callback add_references(State, [Reference]) -> ok
    when State :: term(), Reference :: opcua:node_ref().

-callback del_references(State, [Reference]) -> ok
    when State :: term(), Reference :: opcua:node_ref().

-callback add_descriptor(State, TypeDescriptorSpec, TypeSpec, Encoding) -> ok
    when State :: term(), TypeSpec :: opcua:node_spec(),
         Encoding :: opcua:stream_encoding(),
         TypeDescriptorSpec :: opcua:stream_encoding().

-callback del_descriptor(State, TypeDescriptorSpec) -> ok
    when State :: term(), TypeDescriptorSpec :: opcua:stream_encoding().

-callback add_subtype(State, TypeSpec, SubTypeSpec) -> ok
    when State :: term(), TypeSpec :: opcua:node_spec(),
         SubTypeSpec :: opcua:node_spec().

-callback del_subtype(State, TypeSpec, SubTypeSpec) -> ok
    when State :: term(), TypeSpec :: opcua:node_spec(),
         SubTypeSpec  :: opcua:node_spec().

-callback add_schema(State, Schema) -> ok
    when State :: term(), Schema :: opcua:codec_schema().

-callback del_schema(State, TypeSpec) -> ok
    when State :: term(), TypeSpec :: opcua:node_spec().

-callback node(State, NodeSpec) -> Result
    when State :: term(), NodeSpec :: opcua:node_spec(),
         Result :: undefined | opcua:node_rec().

-callback references(State, OriginNodeSpec, Options) -> Result
    when State :: term(), OriginNodeSpec :: opcua:node_spec(),
         Options :: opcua:references_options(),
         Result :: [opcua:node_ref()].

-callback data_type(State, TypeDescriptorSpec) -> Result
    when State :: term(), TypeDescriptorSpec :: opcua:node_spec(),
         Result :: undefined | {TypeId, Encoding},
         TypeId :: opcua:node_id(), Encoding :: opcua:stream_encoding().

-callback type_descriptor(State, TypeSpec, Encoding) -> Result
    when State :: term(), TypeSpec :: opcua:node_spec(),
         Encoding :: opcua:stream_encoding(),
         Result :: undefined | TypeDescriptorId,
         TypeDescriptorId :: opcua:node_id().

-callback schema(State, TypeSpec) -> Result
    when State :: term(), TypeSpec :: opcua:node_spec(),
         Result :: undefined | opcua:codec_schema().

-callback namespace_uri(State, NamespaceId) -> undefined | NamespaceUri
    when State :: term(), NamespaceId :: non_neg_integer(),
         NamespaceUri :: binary().

-callback namespace_id(State, NamespaceUri) -> undefined | NamespaceId
    when State :: term(), NamespaceId :: non_neg_integer(),
         NamespaceUri :: binary().

-callback namespaces(State) -> #{NamespaceId => NamespaceUri}
    when State :: term(), NamespaceId :: non_neg_integer(),
         NamespaceUri :: binary().

-callback is_subtype(State, SubTypeSpec, SuperTypeSpec) -> boolean()
    when State :: term(), SubTypeSpec :: opcua:node_spec(),
         SuperTypeSpec :: opcua:node_spec().


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([add_namespace/3]).
-export([add_nodes/2]).
-export([del_nodes/2]).
-export([add_references/2]).
-export([del_references/2]).
-export([node/2]).
-export([references/2, references/3]).
-export([data_type/2]).
-export([type_descriptor/3]).
-export([schema/2]).
-export([namespace_uri/2]).
-export([namespace_id/2]).
-export([namespaces/1]).
-export([is_subtype/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type state() :: module() | {module(), term()} | opcua:connection().

-export_type([state/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Adds a namespace to given space.
% Depending on the backend, may only be allowed from the owning process.
add_namespace(#uacp_connection{space = Space}, Id, Uri) ->
    add_namespace(Space, Id, Uri);
add_namespace({Mod, Sub}, Id, Uri) ->
    Mod:add_namespace(Sub, Id, Uri);
add_namespace(Mod, Id, Uri) ->
    % When delegating to a module with its own state,
    % we are delegating the side-effects too.
    Mod:add_namespace(Id, Uri).

% @doc Adds given nodes to the given database.
% Depending on the backend, may only be allowed from the owning process.
add_nodes(#uacp_connection{space = Space} = State, Nodes) ->
    add_nodes(Space, Nodes),
    nodes_added(State, Nodes);
add_nodes({Mod, Sub} = State, Nodes) ->
    Mod:add_nodes(Sub, Nodes),
    nodes_added(State, Nodes);
add_nodes(Mod, Nodes) ->
    % When delegating to a module with its own state,
    % we are delegating the side-effects too.
    Mod:add_nodes(Nodes).

% @doc Removes the nodes with given node identifier from the given database.
% Depending on the backend, may only be allowed from the owning process.
del_nodes(#uacp_connection{space = Space} = State, NodeIds) ->
    nodes_deleted(State, NodeIds),
    del_nodes(Space, NodeIds);
del_nodes({Mod, Sub} = State, NodeIds) ->
    nodes_deleted(State, NodeIds),
    Mod:del_nodes(Sub, NodeIds);
del_nodes(Mod, NodeIds) ->
    % When delegating to a module with its own state,
    % we are delegating the side-effects too.
    Mod:del_nodes(NodeIds).

% @doc Adds given references to the given database.
% Depending on the backend, may only be allowed from the owning process.
add_references(#uacp_connection{space = Space} = State, References) ->
    add_references(Space, References),
    references_added(State, References);
add_references({Mod, Sub} = State, References) ->
    Mod:add_references(Sub, References),
    references_added(State, References);
add_references(Mod, References) ->
    % When delegating to a module with its own state,
    % we are delegating the side-effects too.
    Mod:add_references(References).

% @doc Removes given references from given database.
% Depending on the backend, may only be allowed from the owning process.
del_references(#uacp_connection{space = Space} = State, References) ->
    references_deleted(State, References),
    del_references(Space, References);
del_references({Mod, Sub} = State, References) ->
    references_deleted(State, References),
    Mod:del_references(Sub, References);
del_references(Mod, References) ->
    % When delegating to a module with its own state,
    % we are delegating the side-effects too.
    Mod:del_references(References).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Retrieves a node record from given database.
node(#uacp_connection{space = Space}, NodeSpec) ->
    node(Space, NodeSpec);
node({Mod, Sub}, NodeSpec) ->
    Mod:node(Sub, NodeSpec);
node(Mod, NodeSpec) ->
    Mod:node(NodeSpec).

% @doc Retrieves a node forward references from given database.
references(#uacp_connection{space = Space}, OriginNode) ->
    references(Space, OriginNode);
references({Mod, Sub}, OriginNode) ->
    Mod:references(Sub, OriginNode);
references(Mod, OriginNode) ->
    Mod:references(OriginNode).

% @doc Retrieves a node references from given database with given options.
references(#uacp_connection{space = Space}, OriginNode, Opts) ->
    references(Space, OriginNode, Opts);
references({Mod, Sub}, OriginNode, Opts) ->
    Mod:references(Sub, OriginNode, Opts);
references(Mod, OriginNode, Opts) ->
    Mod:references(OriginNode, Opts).

% @doc Retrives a data type and encoding type from a type descriptor identifier.
data_type(#uacp_connection{space = Space}, TypeDescriptorSpec) ->
    data_type(Space, TypeDescriptorSpec);
data_type({Mod, Sub}, TypeDescriptorSpec) ->
    Mod:data_type(Sub, TypeDescriptorSpec);
data_type(Mod, TypeDescriptorSpec) ->
    Mod:data_type(TypeDescriptorSpec).

% @doc Retrieves a type descriptor identifier from a data type and an encoding type.
type_descriptor(#uacp_connection{space = Space}, NodeSpec, Encoding) ->
    type_descriptor(Space, NodeSpec, Encoding);
type_descriptor({Mod, Sub}, NodeSpec, Encoding) ->
    Mod:type_descriptor(Sub, NodeSpec, Encoding);
type_descriptor(Mod, NodeSpec, Encoding) ->
    Mod:type_descriptor(NodeSpec, Encoding).

% @doc Retrieves a data schema from a data type.
schema(#uacp_connection{space = Space}, TypeSpec) ->
    schema(Space, TypeSpec);
schema({Mod, Sub}, TypeSpec) ->
    Mod:schema(Sub, TypeSpec);
schema(Mod, TypeSpec) ->
    Mod:schema(TypeSpec).

% @doc Retrieves a namespace URI from its identifier.
namespace_uri(#uacp_connection{space = Space}, Id) ->
    namespace_uri(Space, Id);
namespace_uri({Mod, Sub}, Id) ->
    Mod:namespace_uri(Sub, Id);
namespace_uri(Mod, Id) ->
    Mod:namespace_uri(Id).

% @doc Retrieve a namespace identifier from its URI.
namespace_id(#uacp_connection{space = Space}, Uri) ->
    namespace_id(Space, Uri);
namespace_id({Mod, Sub}, Uri) ->
    Mod:namespace_id(Sub, Uri);
namespace_id(Mod, Uri) ->
    Mod:namespace_id(Uri).

% @doc Retrieves all the defined namespaces as a map.
namespaces(#uacp_connection{space = Space}) ->
    namespaces(Space);
namespaces({Mod, Sub}) ->
    Mod:namespaces(Sub);
namespaces(Mod) ->
    Mod:namespaces().

% @doc Checks if the given type is a subtype of given super type.
is_subtype(#uacp_connection{space = Space}, SubTypeSpec, SuperTypeSpec) ->
    is_subtype(Space, SubTypeSpec, SuperTypeSpec);
is_subtype({Mod, Sub}, SubTypeSpec, SuperTypeSpec) ->
    Mod:is_subtype(Sub, SubTypeSpec, SuperTypeSpec);
is_subtype(Mod, SubTypeSpec, SuperTypeSpec) ->
    Mod:is_subtype(SubTypeSpec, SuperTypeSpec).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

nodes_added(_Space, []) -> ok;
nodes_added(Space, [Node | Rest]) ->
    node_added(Space, Node),
    nodes_added(Space, Rest).

node_added(Space, #opcua_node{node_id = NodeId,
                    node_class = #opcua_data_type{is_abstract = false}}) ->
    maybe_add_schema(Space, NodeId);
node_added(Space, #opcua_node{node_id = NodeId, browse_name = <<"EnumValues">>,
                              node_class = #opcua_object{}}) ->
    HasPropOpts = #{direction => inverse, type => has_property},
    case references(Space, NodeId, HasPropOpts) of
        [#opcua_reference{source_id = NodeId}] ->
            maybe_add_schema(Space, NodeId);
        _ -> ok
    end;
node_added(Space, #opcua_node{node_id = DescId, browse_name = Name})
  when Name =:= <<"Default Binary">>; Name =:= <<"Default XML">>;
       Name =:= <<"Default JSON">> ->
    maybe_add_descriptor(Space, DescId);
node_added(_Space, #opcua_node{}) ->
    ok.

nodes_deleted(_Space, []) -> ok;
nodes_deleted(Space, [NodeId | Rest]) ->
    node_deleted(Space, NodeId, node(Space, NodeId)),
    nodes_deleted(Space, Rest).

node_deleted(Space, NodeId, #opcua_node{node_id = NodeId,
        node_class = #opcua_data_type{is_abstract = false}}) ->
    backend_del_schema(Space, NodeId);
node_deleted(Space, DescId, #opcua_node{node_id = DescId, browse_name = Name})
  when Name =:= <<"Default Binary">>; Name =:= <<"Default XML">>;
       Name =:= <<"Default JSON">> ->
    backend_del_descriptor(Space, DescId);
node_deleted(_Space, _NodeId, _Node) ->
    ok.

references_added(_Space, []) -> ok;
references_added(Space, [Ref | Rest]) ->
    reference_added(Space, Ref),
    references_added(Space, Rest).

reference_added(Space, #opcua_reference{
        type_id = ?NID_HAS_ENCODING,
        source_id = _SourceId,
        target_id = TargetId}) ->
    maybe_add_descriptor(Space, TargetId);
reference_added(Space, #opcua_reference{
        type_id = ?NID_HAS_TYPE_DEFINITION,
        source_id = DescId,
        target_id = ?NID_DATA_TYPE_ENCODING_TYPE}) ->
    maybe_add_descriptor(Space, DescId);
reference_added(Space, #opcua_reference{
        type_id = ?NID_HAS_SUBTYPE,
        source_id = SourceId,
        target_id = TargetId}) ->
    add_subtype(Space, SourceId, TargetId),
    maybe_add_schema(Space, TargetId);
reference_added(Space, #opcua_reference{
        type_id = ?NID_HAS_PROPERTY,
        source_id = SourceId,
        target_id = _TargetId}) ->
    %FIXME: This is ineficient, adding any property will always trigger this,
    %       and this is only used for detecting EnumValues objects.
    maybe_add_schema(Space, SourceId);
reference_added(_Space, #opcua_reference{}) ->
    ok.

references_deleted(_Space, []) -> ok;
references_deleted(Space, [Ref | Rest]) ->
    reference_deleted(Space, Ref),
    references_deleted(Space, Rest).

reference_deleted(Space, #opcua_reference{
        type_id = ?NID_HAS_ENCODING,
        target_id = DescId}) ->
    backend_del_descriptor(Space, DescId);
reference_deleted(Space, #opcua_reference{
        type_id = ?NID_HAS_TYPE_DEFINITION,
        source_id = DescId,
        target_id = ?NID_DATA_TYPE_ENCODING_TYPE}) ->
    backend_del_descriptor(Space, DescId);
reference_deleted(Space, #opcua_reference{
        type_id = ?NID_HAS_SUBTYPE,
        source_id = Type,
        target_id = SubType}) ->
    del_subtype(Space, Type, SubType);
reference_deleted(_Space, #opcua_reference{}) ->
    ok.

maybe_add_descriptor(Space, DescId) ->
    DescNode = node(Space, DescId),
    TypeDefRefs = references(Space, DescId, #{type => has_type_definition}),
    HasEncRefs = references(Space, DescId, #{type => has_encoding, direction => inverse}),
    maybe_add_descriptor(Space, DescId, DescNode, TypeDefRefs, HasEncRefs).

maybe_add_descriptor(Space, DescId,
                  #opcua_node{node_id = DescId, browse_name = Name},
                  [#opcua_reference{type_id = ?NID_HAS_TYPE_DEFINITION,
                                    source_id = DescId,
                                    target_id = ?NID_DATA_TYPE_ENCODING_TYPE}],
                  [#opcua_reference{type_id = ?NID_HAS_ENCODING,
                                    source_id = TypeId,
                                    target_id = DescId}]) ->
    backend_add_descriptor(Space, DescId, TypeId, descriptor_encoding(Name));
maybe_add_descriptor(_Space, _DescId, _Node, _TypeDefRefs, _HasEncRefs) ->
    ok.

descriptor_encoding(<<"Default Binary">>) -> binary;
descriptor_encoding(<<"Default XML">>) -> xml;
descriptor_encoding(<<"Default JSON">>) -> json;
descriptor_encoding(_) -> unknown.

maybe_add_schema(Space, TypeId) ->
    % We generate a schema from:
    %  - Data type node's identifier
    %  - Data type node's definition
    %  - Data type node's browse name
    %  - Data type node's root-type identifier (builtin type)
    %  - Data type node's property named EnumValues
    % We want to be extra flexible to be able to decode types even when
    % there is slight inconsistencies in the model:
    %  - If the data type definition contains a default encoding id that doesn't
    %    exist in the space, we add a data descriptor for it.
    %  - If data type with structure type definition do not have any super type,
    %    we assume it doesn't inherit any fields.
    case node(Space, TypeId) of
        #opcua_node{browse_name = Name,
                    node_class = #opcua_data_type{
                        is_abstract = false,
                        data_type_definition = TypeDef}} ->
            HasPropOpts = #{type => has_property, direction => forward},
            PropRefs = references(Space, TypeId, HasPropOpts),
            EnumValues = lists:foldl(fun
                (#opcua_reference{type_id = ?NID_HAS_PROPERTY,
                                  source_id = S, target_id = T}, Result)
                  when S =:= TypeId ->
                    case node(Space, T) of
                        #opcua_node{browse_name = <<"EnumValues">>,
                                    node_class = #opcua_variable{value = Value}} ->
                            Value;
                        _ ->
                            Result
                    end;
                (_, Result) -> Result
            end, undefined, PropRefs),
            RootTypeId = lookup_root_type(Space, TypeId),
            case generate_schema(TypeId, Name, TypeDef, RootTypeId, EnumValues) of
                undefined -> ok;
                {undefined, Schema} ->
                    backend_add_schema(Space, Schema);
                {DescId, Schema} ->
                    backend_add_descriptor(Space, DescId, TypeId, binary),
                    backend_add_schema(Space, Schema)
            end;
        _ ->
            ok
    end.

% Gives the root builtin type (plus enumeration and union) or undefined.
lookup_root_type(_Space, ?NNID(12756) = TypeId) -> TypeId;
lookup_root_type(_Space, ?NNID(29) = TypeId) -> TypeId;
lookup_root_type(_Space, ?NNID(Id) = TypeId)
  when ?IS_BUILTIN_TYPE_ID(Id) -> TypeId;
lookup_root_type(Space, TypeId) ->
    HasSubTypeOpts = #{direction => inverse, type => has_subtype},
    case references(Space, TypeId, HasSubTypeOpts) of
        [#opcua_reference{source_id = SuperTypeId}] ->
            lookup_root_type(Space, SuperTypeId);
        _ -> undefined
    end.

add_subtype(Space, Type, SubType) ->
    backend_add_subtype(Space, Type, SubType),
    SubTypes = references(Space, SubType, #{
        type => ?NID_HAS_SUBTYPE,
        direction => forward
    }),
    lists:map(fun
        (#opcua_reference{target_id = T}) ->
            add_subtype(Space, Type, T)
    end, SubTypes),
    SuperTypes = references(Space, Type, #{
        type => ?NID_HAS_SUBTYPE,
        direction => inverse
    }),
    lists:map(fun
        (#opcua_reference{source_id = S}) ->
            add_subtype(Space, S, SubType)
    end, SuperTypes).

del_subtype(Space, Type, SubType) ->
    backend_del_subtype(Space, Type, SubType),
    SubTypes = references(Space, SubType, #{
        type => ?NNID(?REF_HAS_SUBTYPE),
        direction => forward
    }),
    lists:map(fun(#opcua_reference{target_id = T}) ->
        del_subtype(Space, Type, T)
    end, SubTypes),
    SuperTypes = references(Space, Type, #{
        type => ?NNID(?REF_HAS_SUBTYPE),
        direction => inverse
    }),
    lists:map(fun(#opcua_reference{source_id = S}) ->
        del_subtype(Space, S, SubType)
    end, SuperTypes).

generate_schema(TypeId, _, undefined, TypeId, _EnumValues) ->
    % Builtin types
    {undefined, #opcua_builtin{node_id = TypeId, builtin_node_id = TypeId}};
generate_schema(TypeId, Name, #{structure_type := T,
                                base_data_type := BaseTypeId,
                                default_encoding_id := DescId,
                                fields := Fields},
                RootTypeId, _EnumValues)
  when T =:= structure, RootTypeId =:= ?NNID(22);
       T =:= structure, BaseTypeId =:= ?NNID(22);
       T =:= structure_with_optional_fields, RootTypeId =:= ?NNID(22);
       T =:= structure_with_optional_fields, BaseTypeId =:= ?NNID(22);
       T =:= structure_with_subtyped_values, RootTypeId =:= ?NNID(22);
       T =:= structure_with_subtyped_values, BaseTypeId =:= ?NNID(22) ->
    % Structure
    WithOpts = T =:= structure_with_optional_fields,
    AllowSubTypes = T =:= structure_with_subtyped_values,
    {if_defined(DescId), #opcua_structure{
        node_id = TypeId,
        name = Name,
        with_options = WithOpts,
        allow_subtypes = AllowSubTypes,
        fields = fields_from_struct_typedef(Fields, WithOpts or AllowSubTypes)
    }};
generate_schema(TypeId, Name, #{structure_type := T,
                                base_data_type := BaseTypeId,
                                default_encoding_id := DescId,
                                fields := Fields},
                RootTypeId, _)
  when T =:= union, BaseTypeId =:= ?NNID(12756);
       T =:= union, RootTypeId =:= ?NNID(12756);
       T =:= union_with_subtyped_values, BaseTypeId =:= ?NNID(12756);
       T =:= union_with_subtyped_values, RootTypeId =:= ?NNID(12756) ->
    % Union
    AllowSubTypes = T =:= union_with_subtyped_values,
    {if_defined(DescId), #opcua_union{
        node_id = TypeId,
        name = Name,
        allow_subtypes = AllowSubTypes,
        fields = fields_from_struct_typedef(Fields, AllowSubTypes)
    }};
generate_schema(TypeId, _, undefined, ?NNID(29), EnumValues)
  when is_list(EnumValues) ->
    % Enum without data type definition but with enum values property
    %TODO: We could generate the missing data type definition...
    {undefined, #opcua_enum{
        node_id = TypeId,
        fields = fields_from_enum_values(EnumValues)
    }};
generate_schema(TypeId, _, #{fields := Fields}, ?NNID(29), _) ->
    % Enum with data type definition and optional enum values property
    {undefined, #opcua_enum{
        node_id = TypeId,
        fields = fields_from_enum_typedef(Fields)
    }};
generate_schema(TypeId, _, #{fields := Fields}, RootTypeId, _)
  when RootTypeId =:= ?NNID(3); RootTypeId =:= ?NNID(5); RootTypeId =:= ?NNID(7) ->
    % OptionSet with data type definition
    {undefined, #opcua_option_set{
        node_id = TypeId,
        mask_type = opcua_codec:builtin_type_name(RootTypeId),
        fields = fields_from_enum_typedef(Fields)
    }};
generate_schema(TypeId, _, undefined, ?NNID(Id) = RootTypeId, _)
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    % Subtype of some builtin types
    {undefined, #opcua_builtin{node_id = TypeId, builtin_node_id = RootTypeId}};
generate_schema(_TypeId, _Name, _TypeDef, _RootTypeId, _EnumValues) ->
    undefined.

if_defined(#opcua_node_id{value = Value} = NodeId)
  when Value =/= undefined -> NodeId;
if_defined(_) -> undefined.

fields_from_enum_values(EnumValues) ->
    fields_from_enum_values(EnumValues, []).

fields_from_enum_values([], Acc) -> lists:reverse(Acc);
fields_from_enum_values([Info | Rest], Acc) ->
    #{value := V, display_name := #opcua_localized_text{text = N}} = Info,
    Field = #opcua_field{
        tag = opcua_util:convert_name(N),
        name = N,
        value = V
    },
    fields_from_enum_values(Rest, [Field | Acc]).

fields_from_enum_typedef(FieldDefs) ->
    fields_from_enum_typedef(FieldDefs, []).

fields_from_enum_typedef([], Acc) -> lists:reverse(Acc);
fields_from_enum_typedef([FieldDef | Rest], Acc) ->
    #{value := V, name := N} = FieldDef,
    Field = #opcua_field{
        tag = opcua_util:convert_name(N),
        name = N,
        value = V
    },
    fields_from_enum_typedef(Rest, [Field | Acc]).

% Set the value of optional field to the bit index.
% TODO: This is a hack, and it should be removed at some point.
fields_from_struct_typedef(FieldDefs, WithOpts) ->
    fields_from_struct_typedef(FieldDefs, WithOpts, 1, []).

fields_from_struct_typedef([], _WithOpts, _BitIdx, Acc) -> lists:reverse(Acc);
fields_from_struct_typedef([#{is_optional := true} = FieldDef | Rest],
                           true, BitIdx, Acc) ->
    #{data_type := T, name := N, value_rank := R} = FieldDef,
    Field = #opcua_field{
        tag = opcua_util:convert_name(N),
        name = N,
        node_id = T,
        value_rank = R,
        is_optional = true,
        value = BitIdx
    },
    fields_from_struct_typedef(Rest, true, BitIdx + 1, [Field | Acc]);
fields_from_struct_typedef([#{is_optional := false} = FieldDef | Rest],
                           WithOpts, BitIdx, Acc) ->
    #{data_type := T, name := N, value_rank := R} = FieldDef,
    Field = #opcua_field{
        tag = opcua_util:convert_name(N),
        name = N,
        node_id = T,
        value_rank = R,
        is_optional = false
    },
    fields_from_struct_typedef(Rest, WithOpts, BitIdx, [Field | Acc]).

%-- BACKEND INTERFACE FUNCTIONS ------------------------------------------------

backend_add_descriptor(#uacp_connection{space = Space},
                       DescSpec, TypeSpec, Encoding) ->
    backend_add_descriptor(Space, DescSpec, TypeSpec, Encoding);
backend_add_descriptor({Mod, Sub}, DescSpec, TypeSpec, Encoding) ->
    Mod:add_descriptor(Sub, DescSpec, TypeSpec, Encoding).

backend_del_descriptor(#uacp_connection{space = Space}, DescSpec) ->
    backend_del_descriptor(Space, DescSpec);
backend_del_descriptor({Mod, Sub}, DescSpec) ->
    Mod:del_descriptor(Sub, DescSpec).

backend_add_subtype(#uacp_connection{space = Space}, Type, SubType) ->
    backend_add_subtype(Space, Type, SubType);
backend_add_subtype({Mod, Sub}, Type, SubType) ->
    Mod:add_subtype(Sub, Type, SubType).

backend_del_subtype(#uacp_connection{space = Space}, Type, SubType) ->
    backend_del_subtype(Space, Type, SubType);
backend_del_subtype({Mod, Sub}, Type, SubType) ->
    Mod:del_subtype(Sub, Type, SubType).

backend_add_schema(#uacp_connection{space = Space}, Schema) ->
    backend_add_schema(Space, Schema);
backend_add_schema({Mod, Sub}, Schema) ->
    Mod:add_schema(Sub, Schema).

backend_del_schema(#uacp_connection{space = Space}, TypeSpec) ->
    backend_del_schema(Space, TypeSpec);
backend_del_schema({Mod, Sub}, TypeSpec) ->
    Mod:del_schema(Sub, TypeSpec).
