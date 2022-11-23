%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA database backend.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_local_database).


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


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Adds given nodes to the given database.
% Depending on the backend, may only be allowed from the owning process.
add_nodes({Mod, Sub}, Nodes) ->
    Mod:add_nodes(Sub, Nodes);
add_nodes(Mod, Nodes) ->
    Mod:add_nodes(Nodes).

% @doc Removes the nodes with given node identifier from the given database.
% Depending on the backend, may only be allowed from the owning process.
del_nodes({Mod, Sub}, NodeIds) ->
    Mod:del_nodes(Sub, NodeIds);
del_nodes(Mod, NodeIds) ->
    Mod:del_nodes(NodeIds).

% @doc Adds given references to the given database.
% Depending on the backend, may only be allowed from the owning process.
add_references({Mod, Sub}, References) ->
    Mod:add_references(Sub, References);
add_references(Mod, References) ->
    Mod:add_references(References).

% @doc Removes given references from given database.
% Depending on the backend, may only be allowed from the owning process.
del_references({Mod, Sub}, References) ->
    Mod:del_references(Sub, References);
del_references(Mod, References) ->
    Mod:del_references(References).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Retrieves a node record from given database.
node({Mod, Sub}, NodeSpec) ->
    Mod:node(Sub, NodeSpec);
node(Mod, NodeSpec) ->
    Mod:node(NodeSpec).

% @doc Retrieves a node forward references from given database.
references({Mod, Sub}, OriginNode) ->
    Mod:references(Sub, OriginNode);
references(Mod, OriginNode) ->
    Mod:references(OriginNode).

% @doc Retrieves a node references from given database with given options.
references({Mod, Sub}, OriginNode, Opts) ->
    Mod:references(Sub, OriginNode, Opts);
references(Mod, OriginNode, Opts) ->
    Mod:references(OriginNode, Opts).

% @doc Retrives a data type and encoding type from a type descriptor identifier.
data_type({Mod, Sub}, TypeDescriptorSpec) ->
    Mod:data_type(Sub, TypeDescriptorSpec);
data_type(Mod, TypeDescriptorSpec) ->
    Mod:data_type(TypeDescriptorSpec).

% @doc Retrieves a type descriptor identifier from a data type and an encoding type.
type_descriptor({Mod, Sub}, NodeSpec, Encoding) ->
    Mod:type_descriptor(Sub, NodeSpec, Encoding);
type_descriptor(Mod, NodeSpec, Encoding) ->
    Mod:type_descriptor(NodeSpec, Encoding).

% @doc Retrieves a data schema from a data type.
schema({Mod, Sub}, NodeSpec) ->
    Mod:schema(Sub, NodeSpec);
schema(Mod, NodeSpec) ->
    Mod:schema(NodeSpec).

% @doc Retrieves a namespace URI from its identifier.
namespace_uri({Mod, Sub}, Id) ->
    Mod:namespace_uri(Sub, Id);
namespace_uri(Mod, Id) ->
    Mod:namespace_uri(Id).

% @doc Retrieve a namespace identifier from its URI.
namespace_id({Mod, Sub}, Uri) ->
    Mod:namespace_id(Sub, Uri);
namespace_id(Mod, Uri) ->
    Mod:namespace_id(Uri).

% @doc Retrieves all the defined namespaces as a map.
namespaces({Mod, Sub}) ->
    Mod:namespaces(Sub);
namespaces(Mod) ->
    Mod:namespaces().
