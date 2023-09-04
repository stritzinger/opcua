%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Helper function for OPCUA servers
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_server).

%% TODO %%
%%
%% - Implemente a plugable service API that whould handle all the nodes under
%%   some mount point node, delegating all node handling to some callback module.
%% - Add suport for method calls.
%% - Add support for object instanciation from defined type, with instanciation
%%   of all properties and components defined.
%% - Add support for object deletion with all its properties.
%% - Add some notifications of changes so a plugable service could react
%%   to changes made to there managed nodes. Ideally, there should be a way
%%   for the service to be able to validate the changes before they even take
%%   place, so the service can ensure consistency and access managment.


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API
-export([ensure/1, ensure/2]).
-export([list_nodes/1, list_nodes/2]).
-export([add_object/2, add_object/3]).
-export([del_object/1]).
-export([add_variable/5]).
-export([add_property/4]).
-export([set_value/2]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(DEFAULT_ENSURE_OPTS, #{
    namespace_aliases => #{server => opcua_server_registry:namespace_id()},
    node_id_fun => fun opcua_server_registry:next_node_id/0
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ensure(Templates) ->
    Opts = ?DEFAULT_ENSURE_OPTS,
    opcua_template:ensure(opcua_server_space, Templates, Opts).

ensure(Templates, Opts) ->
    Opts2 = maps:merge(?DEFAULT_ENSURE_OPTS, Opts),
    opcua_template:ensure(opcua_server_space, Templates, Opts2).

list_nodes(ParentSpec) ->
    list_nodes(ParentSpec, ?NNID(?REF_HIERARCHICAL)).

list_nodes(ParentSpec, RefTypeSpec) ->
    Opts = #{
        direction => forward,
        type => RefTypeSpec,
        include_subtypes => true
    },
    [opcua_server_space:node(N)
     || #opcua_reference{target_id = N}
     <- opcua_server_space:references(ParentSpec, Opts)].

add_object(Name, TypeSpec) ->
    add_object(?OBJ_OBJECTS_FOLDER, Name, TypeSpec).

add_object(ParentSpec, Name, TypeSpec) when is_binary(Name) ->
    NodeTemplate = #opcua_node{
        browse_name = Name,
        node_class = #opcua_object{}},
    add_object(ParentSpec, NodeTemplate, TypeSpec);
add_object(ParentSpec, #opcua_node{} = NodeTemplate, TypeSpec) ->
    NodeId = case NodeTemplate of
        #opcua_node{node_id = undefined} ->
            opcua_server_registry:next_node_id();
        #opcua_node{node_id = Id} ->
            Id
    end,
    TypeId = opcua_node:id(TypeSpec),
    ParentId = opcua_node:id(ParentSpec),
    opcua_server_space:add_nodes([NodeTemplate#opcua_node{
        node_id = NodeId,
        origin = local
    }]),
    opcua_server_space:add_references([
        #opcua_reference{
            type_id = ?NNID(?REF_HAS_TYPE_DEFINITION),
            source_id = NodeId,
            target_id = TypeId
        },
        #opcua_reference{
            type_id = ?NNID(?REF_HAS_CHILD),
            source_id = ParentId,
            target_id = NodeId
        }
    ]),
    NodeId.

del_object(ObjSpec) ->
    NodeId = opcua_node:id(ObjSpec),
    opcua_server_space:del_nodes([NodeId]),
    NodeId.

add_variable(ObjSpec, Name, VarTypeSpec, ValueTypeSpec, Value) ->
    NodeId = opcua_server_registry:next_node_id(),
    ParentId = opcua_node:id(ObjSpec),
    VarTypeId = opcua_node:id(VarTypeSpec),
    ValueTypeId = opcua_node:id(ValueTypeSpec),
    opcua_server_space:add_nodes([#opcua_node{
        node_id = NodeId,
        origin = local,
        browse_name = Name,
        node_class = #opcua_variable{
            data_type = ValueTypeId,
            value = Value
        }
    }]),
    opcua_server_space:add_references([
        #opcua_reference{
            type_id = ?NNID(?REF_HAS_TYPE_DEFINITION),
            source_id = NodeId,
            target_id = VarTypeId
        },
        #opcua_reference{
            type_id = ?NNID(?REF_HAS_PROPERTY),
            source_id = ParentId,
            target_id = NodeId
        }
    ]),
    NodeId.

add_property(ObjSpec, Name, ValueTypeSpec, Value) ->
    add_variable(ObjSpec, Name, ?TYPE_PROPERTY, ValueTypeSpec, Value).

set_value(VarSpec, Value) ->
    VarId = opcua_node:id(VarSpec),
    Node = opcua_server_space:node(VarId),
    #opcua_node{node_class = Variable} = Node,
    %TODO: Maybe some type checking here ?
    opcua_server_space:add_nodes([Node#opcua_node{
        node_class = Variable#opcua_variable{
            value = Value
        }
    }]).
