-module(opcua_node).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua_node.hrl").
-include("opcua_codec.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([class/1]).
-export([attribute/2]).
-export([attribute_type/2]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type reference_type() ::   references |
                            hierarchical_references |
                            non_hierarchical_references |
                            has_event_source |
                            has_child |
                            organizes |
                            has_notifier |
                            aggregates |
                            has_subtype |
                            has_property |
                            has_component |
                            has_ordered_component |
                            generates_event |
                            always_generates_event |
                            has_encoding |
                            has_modelling_rule |
                            has_type_definition.

-type permission() ::       browse |
                            read_role_permissions |
                            write_attribute |
                            write_role_permissions |
                            write_historizing |
                            read |
                            write |
                            read_history |
                            insert_history |
                            modify_history |
                            delete_history |
                            receive_events |
                            call |
                            add_reference |
                            remove_reference |
                            delete_node |
                            add_node.

-type node_class() ::       object |
                            variable |
                            method |
                            object_type |
                            variable_type |
                            reference_type |
                            data_type |
                            view.

-type node_class_rec() ::   #opcua_object{} |
                            #opcua_variable{} |
                            #opcua_method{} |
                            #opcua_object_type{} |
                            #opcua_variable_type{} |
                            #opcua_reference_type{} |
                            #opcua_data_type{} |
                            #opcua_view{}.

-type node_id() :: #opcua_node_id{}.
-type permissions() :: [permission()].
-type role_permission() :: #role_permission{}.
-type role_permissions() :: [role_permission()].
-type ref() :: #opcua_reference{}.
-type refs() :: [ref()].

-export_type([
    node_id/0,
    reference_type/0,
    permission/0,
    permissions/0,
    role_permission/0,
    role_permissions/0,
    node_class/0,
    node_class_rec/0,
    ref/0,
    refs/0
]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

class(#opcua_node{node_class = #opcua_object{}})         -> object;
class(#opcua_node{node_class = #opcua_variable{}})       -> variable;
class(#opcua_node{node_class = #opcua_method{}})         -> method;
class(#opcua_node{node_class = #opcua_object_type{}})    -> object_type;
class(#opcua_node{node_class = #opcua_variable_type{}})  -> variable_type;
class(#opcua_node{node_class = #opcua_reference_type{}}) -> reference_type;
class(#opcua_node{node_class = #opcua_data_type{}})      -> data_type;
class(#opcua_node{node_class = #opcua_view{}})           -> view.

%% Generic Nodes attributes
attribute(node_id, #opcua_node{node_id = NodeId}) ->
    NodeId;
attribute(node_class, Node) ->
    class(Node);
attribute(browse_name, #opcua_node{node_id = ?NID_NS(NS), browse_name = V}) ->
    #qualified_name{ns = NS, name = V};
attribute(display_name, #opcua_node{display_name = V}) ->
    #localized_text{text = V};
attribute(description, #opcua_node{description = V}) ->
    #localized_text{text = V}.
% attribute(write_mask, #opcua_node{write_mask = V}) -> V;
% attribute(user_write_mask, #opcua_node{user_write_mask = V}) -> V;
% attribute(role_permissions, #opcua_node{role_permissions = V}) -> V;
% attribute(user_role_permissions, #opcua_node{user_role_permissions = V}) -> V;
% attribute(access_restrictions, #opcua_node{access_restrictions = V}) -> V;
% %% Object Node Class Attributes
% attribute(event_notifier, #opcua_node{node_class = #opcua_object{event_notifier = V}}) -> V;
% %% Variable Node Class Attributes
% attribute(value, #opcua_node{node_class = #opcua_variable{value = V}}) -> V;
% attribute(data_type, #opcua_node{node_class = #opcua_variable{data_type = V}}) -> V;
% attribute(value_rank, #opcua_node{node_class = #opcua_variable{value_rank = V}}) -> V;
% attribute(array_dimensions, #opcua_node{node_class = #opcua_variable{array_dimensions = V}}) -> V;
% attribute(access_level, #opcua_node{node_class = #opcua_variable{access_level = V}}) -> V;
% attribute(user_access_level, #opcua_node{node_class = #opcua_variable{user_access_level = V}}) -> V;
% attribute(minimum_sampling_interval, #opcua_node{node_class = #opcua_variable{minimum_sampling_interval = V}}) -> V;
% attribute(historizing, #opcua_node{node_class = #opcua_variable{historizing = V}}) -> V;
% attribute(access_level_ex, #opcua_node{node_class = #opcua_variable{access_level_ex = V}}) -> V;
% %% Method Node Class Attributes
% attribute(executable, #opcua_node{node_class = #opcua_method{executable = V}}) -> V;
% attribute(user_executable, #opcua_node{node_class = #opcua_method{user_executable = V}}) -> V;
% %% Object Type Node Class Attributes
% attribute(is_abstract, #opcua_node{node_class = #opcua_object_type{is_abstract = V}}) -> V;
% %% Variable Type Node Class Attributes
% attribute(value, #opcua_node{node_class = #opcua_variable_type{value = V}}) -> V;
% attribute(data_type, #opcua_node{node_class = #opcua_variable_type{data_type = V}}) -> V;
% attribute(value_rank, #opcua_node{node_class = #opcua_variable_type{value_rank = V}}) -> V;
% attribute(array_dimensions, #opcua_node{node_class = #opcua_variable_type{array_dimensions = V}}) -> V;
% attribute(is_abstract, #opcua_node{node_class = #opcua_variable_type{is_abstract = V}}) -> V;
% %% Data Type Node Class Attributes
% attribute(is_abstract, #opcua_node{node_class = #opcua_data_type{is_abstract = V}}) -> V;
% attribute(data_type_definition, #opcua_node{node_class = #opcua_data_type{data_type_definition = V}}) -> V;
% %% Reference Type Node Class Attributes
% attribute(is_abstract, #opcua_node{node_class = #opcua_reference_type{is_abstract = V}}) -> V;
% attribute(symmetric, #opcua_node{node_class = #opcua_reference_type{symmetric = V}}) -> V;
% attribute(inverse_name, #opcua_node{node_class = #opcua_reference_type{inverse_name = V}}) -> V;
% %% View Node Class Attributes
% attribute(contains_no_loops, #opcua_node{node_class = #opcua_view{contains_no_loops = V}}) -> V;
% attribute(event_notifier, #opcua_node{node_class = #opcua_view{event_notifier = V}}) -> V.

%TODO: figure out if these types are correct, and fill in the undefined ones
attribute_type(node_id, _Node) -> node_id;
attribute_type(node_class, _Node) -> ?NNID(257);
attribute_type(browse_name, _Node) -> qualified_name;
attribute_type(display_name, _Node) -> localized_text;
attribute_type(description, _Node) -> localized_text.
% attribute_type(write_mask, _Node) -> undefined;
% attribute_type(user_write_mask, _Node) -> undefined;
% attribute_type(is_abstract, _Node) -> boolean;
% attribute_type(symmetric, _Node) -> boolean;
% attribute_type(inverse_name, _Node) -> boolean;
% attribute_type(contains_no_loops, _Node) -> boolean;
% attribute_type(event_notifier, _Node) -> byte_string;
% attribute_type(value, _Node) -> variant;
% attribute_type(data_type, _Node) -> int32;
% attribute_type(value_rank, _Node) -> int32;
% attribute_type(array_dimensions, _Node) -> int32;
% attribute_type(access_level, _Node) -> undefined;
% attribute_type(user_access_level, _Node) -> undefined;
% attribute_type(minimum_sampling_interval, _Node) -> undefined;
% attribute_type(historizing, _Node) -> undefined;
% attribute_type(executable, _Node) -> undefined;
% attribute_type(user_executable, _Node) -> undefined;
% attribute_type(data_type_definition, _Node) -> undefined;
% attribute_type(role_permissions, _Node) -> undefined;
% attribute_type(user_role_permissions, _Node) -> undefined;
% attribute_type(access_restrictions, _Node) -> undefined;
% attribute_type(access_level_ex, _Node) -> undefined.
