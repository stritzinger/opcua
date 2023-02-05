%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA address space interface module.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_space).


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
    Mod:add_namespace(Id, Uri).

% @doc Adds given nodes to the given database.
% Depending on the backend, may only be allowed from the owning process.
add_nodes(#uacp_connection{space = Space} = State, Nodes) ->
    add_nodes(Space, Nodes),
    nodes_added(State, Nodes);
add_nodes({Mod, Sub} = State, Nodes) ->
    Mod:add_nodes(Sub, Nodes),
    nodes_added(State, Nodes);
add_nodes(Mod = State, Nodes) ->
    Mod:add_nodes(Nodes),
    nodes_added(State, Nodes).

% @doc Removes the nodes with given node identifier from the given database.
% Depending on the backend, may only be allowed from the owning process.
del_nodes(#uacp_connection{space = Space} = State, NodeIds) ->
    nodes_deleted(State, NodeIds),
    del_nodes(Space, NodeIds);
del_nodes({Mod, Sub} = State, NodeIds) ->
    nodes_deleted(State, NodeIds),
    Mod:del_nodes(Sub, NodeIds);
del_nodes(Mod = State, NodeIds) ->
    nodes_deleted(State, NodeIds),
    Mod:del_nodes(NodeIds).

% @doc Adds given references to the given database.
% Depending on the backend, may only be allowed from the owning process.
add_references(#uacp_connection{space = Space} = State, References) ->
    add_references(Space, References),
    references_added(State, References);
add_references({Mod, Sub} = State, References) ->
    Mod:add_references(Sub, References),
    references_added(State, References);
add_references(Mod = State, References) ->
    Mod:add_references(References),
    references_added(State, References).

% @doc Removes given references from given database.
% Depending on the backend, may only be allowed from the owning process.
del_references(#uacp_connection{space = Space} = State, References) ->
    references_deleted(State, References),
    del_references(Space, References);
del_references({Mod, Sub} = State, References) ->
    references_deleted(State, References),
    Mod:del_references(Sub, References);
del_references(Mod = State, References) ->
    references_deleted(State, References),
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
schema(#uacp_connection{space = Space}, NodeSpec) ->
    schema(Space, NodeSpec);
schema({Mod, Sub}, NodeSpec) ->
    Mod:schema(Sub, NodeSpec);
schema(Mod, NodeSpec) ->
    Mod:schema(NodeSpec).

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


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

nodes_added(_Space, []) -> ok;
nodes_added(Space, [Node | Rest]) ->
    node_added(Space, Node),
    nodes_added(Space, Rest).

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

node_deleted(Space, DescId, #opcua_node{node_id = DescId, browse_name = Name})
  when Name =:= <<"Default Binary">>; Name =:= <<"Default XML">>;
       Name =:= <<"Default JSON">> ->
    del_descriptor(Space, DescId);
node_deleted(_Space, _NodeId, _Node) ->
    ok.

references_added(_Space, []) -> ok;
references_added(Space, [Ref | Rest]) ->
    reference_added(Space, Ref),
    references_added(Space, Rest).

reference_added(Space, #opcua_reference{
        type_id = ?NID_HAS_ENCODING,
        target_id = DescId}) ->
    maybe_add_descriptor(Space, DescId);
reference_added(Space, #opcua_reference{
        type_id = ?NID_HAS_TYPE_DEFINITION,
        source_id = DescId,
        target_id = ?NID_DATA_TYPE_ENCODING_TYPE}) ->
    maybe_add_descriptor(Space, DescId);
reference_added(_Space, #opcua_reference{}) ->
    ok.

references_deleted(_Space, []) -> ok;
references_deleted(Space, [Ref | Rest]) ->
    reference_deleted(Space, Ref),
    references_deleted(Space, Rest).

reference_deleted(Space, #opcua_reference{
        type_id = ?NID_HAS_ENCODING,
        target_id = DescId}) ->
    del_descriptor(Space, DescId);
reference_deleted(Space, #opcua_reference{
        type_id = ?NID_HAS_TYPE_DEFINITION,
        source_id = DescId,
        target_id = ?NID_DATA_TYPE_ENCODING_TYPE}) ->
    del_descriptor(Space, DescId);
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
    add_descriptor(Space, DescId, TypeId, descriptor_encoding(Name));
maybe_add_descriptor(_Space, _DescId, _Node, _TypeDefRefs, _HasEncRefs) ->
    ok.

descriptor_encoding(<<"Default Binary">>) -> binary;
descriptor_encoding(<<"Default XML">>) -> xml;
descriptor_encoding(<<"Default JSON">>) -> json;
descriptor_encoding(_) -> unknown.


%-- BACKEND INTERFACE FUNCTIONS ------------------------------------------------

add_descriptor(#uacp_connection{space = Space}, DescSpec, TypeSpec, Encoding) ->
    add_descriptor(Space, DescSpec, TypeSpec, Encoding);
add_descriptor({Mod, Sub}, DescSpec, TypeSpec, Encoding) ->
    Mod:add_descriptor(Sub, DescSpec, TypeSpec, Encoding).

del_descriptor(#uacp_connection{space = Space}, DescSpec) ->
    del_descriptor(Space, DescSpec);
del_descriptor({Mod, Sub}, DescSpec) ->
    Mod:del_descriptor(Sub, DescSpec).
