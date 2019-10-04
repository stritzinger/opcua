-module(opcua).

-behaviour(application).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API
-export([node_id/1]).
-export([add_object/2, add_object/3]).
-export([del_object/1]).
-export([add_variable/5]).
-export([add_property/4]).
-export([set_value/2]).

%% BEHAVIOUR application CALLBACK FUNCTIONS
-export([start/2]).
-export([stop/1]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-- Generic Types --------------------------------------------------------------

-type uint32() :: 0..4294967295.
-type optional(Type) :: undefined | Type.

-export_type([
    uint32/0,
    optional/1
]).

%-- OPCUA Types ----------------------------------------------------------------

-type endpoint() :: #opcua_endpoint{}.
-type node_id_type() :: numeric | string | guid | opaque.
-type node_id() :: #opcua_node_id{}.
-type expanded_node_id() :: #opcua_expanded_node_id{}.
-type qualified_name() :: #opcua_qualified_name{}.
-type localized_text() :: #opcua_localized_text{}.
-type extension_object() :: #opcua_extension_object{}.
-type variant() :: #opcua_variant{}.
-type data_value() :: #opcua_data_value{}.
-type diagnostic_info() :: #opcua_diagnostic_info{}.
-type node_spec() :: non_neg_integer() | atom() | binary() | node_id().
-type codec_spec() :: node_spec() | [node_spec()] | [{atom(), node_spec()}].
-type stream_encoding() :: binary.
-type extobj_encoding() :: xml | byte_string.
-type codec_schema() :: term().
-type field() :: #opcua_field{}.
-type fields() :: [field()].
-type value_rank() :: -1 | pos_integer().
-type builtin_type() :: boolean | byte | sbyte | uint16 | uint32 | uint64
                      | int16 | int32 | int64 | float | double | string
                      | date_time | guid | xml | status_code | byte_string
                      | node_id | expanded_node_id | diagnostic_info
                      | qualified_name | localized_text | extension_object
                      | variant | data_value.

-export_type([
    endpoint/0,
    node_id_type/0,
    node_id/0,
    expanded_node_id/0,
    qualified_name/0,
    localized_text/0,
    extension_object/0,
    variant/0,
    data_value/0,
    diagnostic_info/0,
    node_spec/0,
    codec_spec/0,
    stream_encoding/0,
    extobj_encoding/0,
    codec_schema/0,
    field/0,
    fields/0,
    value_rank/0,
    builtin_type/0
]).


%-- Node Model Types -----------------------------------------------------------

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

-type permissions() :: [permission()].
-type role_permission() :: #opcua_role_permission{}.
-type role_permissions() :: [role_permission()].
-type node_ref() :: #opcua_reference{}.
-type node_refs() :: [node_ref()].

-export_type([
    reference_type/0,
    permission/0,
    node_class/0,
    node_class_rec/0,
    permissions/0,
    role_permission/0,
    role_permissions/0,
    node_ref/0,
    node_refs/0
]).


%-- Node Commands Types --------------------------------------------------------

-type timestamp_type() :: source | server | both | neither | invalid.
-type max_age() :: cached | newest | pos_integer().
-type direction() :: forward | inverse | both.
-type range() :: Index :: non_neg_integer()
               | {Min :: non_neg_integer(), Max :: non_neg_integer()}.
-type reference_description() :: #{
    node_id := opcua:node_id(),
    reference_type_id => opcua:node_id(),
    is_forward => boolean(),
    browse_name => #opcua_qualified_name{},
    display_name => #opcua_localized_text{},
    node_class => atom(),
    type_definition => #opcua_expanded_node_id{}
}.

-type read_options() :: #{
    max_age => max_age(),
    timestamp_type => timestamp_type()
}.
-type read_result() :: #opcua_data_value{}.

-type browse_options() :: #{
    max_refs => non_neg_integer()
}.
-type browse_result() :: #{
    status => atom(),
    references => [reference_description()]
}.

-export_type([
    timestamp_type/0,
    max_age/0,
    direction/0,
    range/0,
    reference_description/0,
    read_options/0,
    read_result/0,
    browse_options/0,
    browse_result/0
]).


%-- Protocol Types -------------------------------------------------------------

-type message_type() :: hello | acknowledge | reverse_hello | error
                      | channel_open | channel_close | channel_message.
-type message_sender() :: client | server.
-type chunk_type() :: final | intermediate | aborted.
-type chunk_state() :: undefined | locked | unlocked.
-type token_id() :: pos_integer().
-type channel_id() :: pos_integer().
-type sequence_num() :: pos_integer().
-type request_id() :: pos_integer().
-type security_policy() :: #uacp_security_policy{}.
-type chunk() :: #uacp_chunk{}.
-type message() :: #uacp_message{}.
-type connection() :: #uacp_connection{}.

-type hello_payload() :: #{
    ver := non_neg_integer(),
    max_res_chunk_size := non_neg_integer(),
    max_req_chunk_size := non_neg_integer(),
    max_msg_size := non_neg_integer(),
    max_chunk_count := non_neg_integer(),
    endpoint_url := undefined | binary()
}.

-type acknowledge_payload() :: #{
    ver := non_neg_integer(),
    max_res_chunk_size := non_neg_integer(),
    max_req_chunk_size := non_neg_integer(),
    max_msg_size := non_neg_integer(),
    max_chunk_count := non_neg_integer()
}.

-type error_payload() :: #{
    error := non_neg_integer(),
    reason := undefined | binary()
}.

-export_type([
    message_type/0,
    chunk_type/0,
    chunk_state/0,
    token_id/0,
    channel_id/0,
    sequence_num/0,
    request_id/0,
    security_policy/0,
    chunk/0,
    message/0,
    connection/0,
    hello_payload/0,
    acknowledge_payload/0,
    error_payload/0
]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

node_id(root)     -> ?NNID(?OBJ_ROOT_FOLDER);
node_id(objects)  -> ?NNID(?OBJ_OBJECTS_FOLDER);
node_id(server)   -> ?NNID(?OBJ_SERVER);
node_id(NodeSpec) -> opcua_database:lookup_id(NodeSpec).

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


%%% BEHAVIOUR application CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start(_StartType, _StartArgs) ->
    {ok, Pid} = opcua_sup:start_link(),
    {ok, _} = opcua_server_ranch_protocol:start_listener(),
    {ok, Pid}.

stop(_State) ->
    opcua_server_ranch_protocol:stop_listener().


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
