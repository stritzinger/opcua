%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Helper function for OPCUA servers
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_server).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API
-export([add_object/2, add_object/3]).
-export([del_object/1]).
-export([add_variable/5]).
-export([add_property/4]).
-export([set_value/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_object(Name, TypeSpec) ->
    add_object(?OBJ_OBJECTS_FOLDER, Name, TypeSpec).

add_object(ParentSpec, Name, TypeSpec) when is_binary(Name) ->
    NodeId = opcua_server_registry:next_node_id(),
    TypeId = opcua_database:lookup_id(TypeSpec),
    ParentId = opcua_database:lookup_id(ParentSpec),
    opcua_address_space:add_nodes(default, [#opcua_node{
        node_id = NodeId,
        browse_name = Name,
        node_class = #opcua_object{}
    }]),
    opcua_address_space:add_references(default, [
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
    NodeId = opcua_database:lookup_id(ObjSpec),
    opcua_address_space:del_nodes(default, [NodeId]),
    NodeId.

add_variable(ObjSpec, Name, VarTypeSpec, ValueTypeSpec, Value) ->
    NodeId = opcua_server_registry:next_node_id(),
    ParentId = opcua_database:lookup_id(ObjSpec),
    VarTypeId = opcua_database:lookup_id(VarTypeSpec),
    ValueTypeId = opcua_database:lookup_id(ValueTypeSpec),
    opcua_address_space:add_nodes(default, [#opcua_node{
        node_id = NodeId,
        browse_name = Name,
        node_class = #opcua_variable{
            data_type = ValueTypeId,
            value = Value
        }
    }]),
    opcua_address_space:add_references(default, [
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
    VarId = opcua_database:lookup_id(VarSpec),
    Node = opcua_address_space:get_node(default, VarId),
    #opcua_node{node_class = Variable} = Node,
    %TODO: Maybe some type checking here ?
    opcua_address_space:add_nodes(default, [Node#opcua_node{
        node_class = Variable#opcua_variable{
            value = Value
        }
    }]).
