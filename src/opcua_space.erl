%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA address space interface module.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_space).


%%% BEHAVIOUR opcua_database DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-callback init() -> State
    when State :: term().

-callback add_nodes(State, [Node]) -> ok
    when State :: term(), Node :: opcua:node_rec().

-callback del_nodes(State, [NodeSpec]) -> ok
    when State :: term(), NodeSpec :: opcua:node_spec().

-callback add_references(State, [Reference]) -> ok
    when State :: term(), Reference :: opcua:node_ref().

-callback del_references(State, [Reference]) -> ok
    when State :: term(), Reference :: opcua:node_ref().

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

% @doc Adds given nodes to the given database.
% Depending on the backend, may only be allowed from the owning process.
add_nodes(#uacp_connection{space = Space}, Nodes) ->
    add_nodes(Space, Nodes);
add_nodes({Mod, Sub}, Nodes) ->
    Mod:add_nodes(Sub, Nodes);
add_nodes(Mod, Nodes) ->
    Mod:add_nodes(Nodes).

% @doc Removes the nodes with given node identifier from the given database.
% Depending on the backend, may only be allowed from the owning process.
del_nodes(#uacp_connection{space = Space}, NodeIds) ->
    del_nodes(Space, NodeIds);
del_nodes({Mod, Sub}, NodeIds) ->
    Mod:del_nodes(Sub, NodeIds);
del_nodes(Mod, NodeIds) ->
    Mod:del_nodes(NodeIds).

% @doc Adds given references to the given database.
% Depending on the backend, may only be allowed from the owning process.
add_references(#uacp_connection{space = Space}, References) ->
    add_references(Space, References);
add_references({Mod, Sub}, References) ->
    Mod:add_references(Sub, References);
add_references(Mod, References) ->
    Mod:add_references(References).

% @doc Removes given references from given database.
% Depending on the backend, may only be allowed from the owning process.
del_references(#uacp_connection{space = Space}, References) ->
    del_references(Space, References);
del_references({Mod, Sub}, References) ->
    Mod:del_references(Sub, References);
del_references(Mod, References) ->
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
