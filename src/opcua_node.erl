-module(opcua_node).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([id/1]).
-export([spec/1]).
-export([format/1, format/2]).
-export([parse/1]).
-export([class/1]).
-export([from_attributes/1]).
-export([attribute_value/2]).
-export([attribute_type/2]).
-export([attribute_rank/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec id(opcua:node_spec() | opcua:node_rec()) -> opcua:node_id().
id(undefined) -> ?UNDEF_NODE_ID;
id(#opcua_node{node_id = NodeId}) -> NodeId;
% Common base nodes
id(root) -> ?NNID(?OBJ_ROOT_FOLDER);
id(objects) -> ?NNID(?OBJ_OBJECTS_FOLDER);
id(types) -> ?NNID(?OBJ_TYPES_FOLDER);
id(object_types) -> ?NNID(?OBJ_OBJECT_TYPES_FOLDER);
id(variable_types) -> ?NNID(?OBJ_VARIABLE_TYPES_FOLDER);
id(data_types) -> ?NNID(?OBJ_DATA_TYPES_FOLDER);
id(reference_types) -> ?NNID(?OBJ_REFERENCE_TYPES_FOLDER);
id(server) -> ?NNID(?OBJ_SERVER);
% Modeling rules
id(mandatory) -> ?NNID(?OBJ_MANDATORY);
id(optional) -> ?NNID(?OBJ_OPTIONAL);
id(exposes_its_array) -> ?NNID(?OBJ_EXPOSES_ITS_ARRAY);
id(optional_placeholder) -> ?NNID(?OBJ_OPTIONAL_PLACEHOLDER);
id(mandatory_placeholder) -> ?NNID(?OBJ_MANDATORY_PLACEHOLDER);
% Common types
id(references) -> ?NNID(?TYPE_REFERENCES);
id(base_object_type) -> ?NNID(?TYPE_BASE_OBJECT);
id(folder_type) -> ?NNID(?TYPE_FOLDER);
id(property_type) -> ?NNID(?TYPE_PROPERTY);
% Explicit data types beside builtins
id(enumeration) -> ?NNID(?DATATYPE_ENUMERATION);
id(enum_value) -> ?NNID(?DATAYPE_ENUM_VALUE);
% Common reference types
id(has_child) -> ?NID_HAS_CHILD;
id(organizes) -> ?NID_ORGANIZES;
id(has_modeling_rule) -> ?NNID(?REF_HAS_MODELING_RULE);
id(has_encoding) -> ?NID_HAS_ENCODING;
id(has_description) -> ?NID_HAS_DESCRIPTION;
id(has_type_definition) -> ?NID_HAS_TYPE_DEFINITION;
id(has_subtype) -> ?NID_HAS_SUBTYPE;
id(has_property) -> ?NID_HAS_PROPERTY;
id(has_component) -> ?NID_HAS_COMPONENT;
id(has_notifier) -> ?NID_HAS_NOTIFIER;
%
id(#opcua_node_id{} = NodeId) -> NodeId;
id(Num) when is_integer(Num), Num >= 0 -> #opcua_node_id{value = Num};
id(Name) when is_atom(Name), ?IS_BUILTIN_TYPE_NAME(Name) ->
    #opcua_node_id{value = opcua_codec:builtin_type_id(Name)};
id(Name) when is_atom(Name) -> #opcua_node_id{type = string, value = Name};
id(Name) when is_binary(Name) -> #opcua_node_id{type = string, value = Name};
id({NS, Num}) when is_integer(NS), is_integer(Num), NS >= 0, Num > 0 ->
    #opcua_node_id{ns = NS, value = Num};
id({NS, Name}) when is_integer(NS), is_atom(Name), NS >= 0 ->
    #opcua_node_id{type = string, ns = NS, value = Name};
id({NS, Name}) when is_integer(NS), is_binary(Name), NS >= 0 ->
    #opcua_node_id{type = string, ns = NS, value = Name}.

-spec spec(opcua:node_id() | opcua:node_ref() | opcua:expanded_node_id()) ->
    opcua:node_spec().
spec(undefined) -> undefined;
spec(?UNDEF_NODE_ID) -> undefined;
spec(#opcua_node{node_id = NodeId}) -> spec(NodeId);
% Common base nodes
spec(?NNID(?OBJ_ROOT_FOLDER)) -> root;
spec(?NNID(?OBJ_OBJECTS_FOLDER)) -> objects;
spec(?NNID(?OBJ_TYPES_FOLDER)) -> types;
spec(?NNID(?OBJ_OBJECT_TYPES_FOLDER)) -> object_types;
spec(?NNID(?OBJ_VARIABLE_TYPES_FOLDER)) -> variable_types;
spec(?NNID(?OBJ_DATA_TYPES_FOLDER)) -> data_types;
spec(?NNID(?OBJ_REFERENCE_TYPES_FOLDER)) -> reference_types;
spec(?NNID(?OBJ_SERVER)) -> server;
% Modeling rules
spec(?NNID(?OBJ_MANDATORY)) -> mandatory;
spec(?NNID(?OBJ_OPTIONAL)) -> optional;
spec(?NNID(?OBJ_EXPOSES_ITS_ARRAY)) -> exposes_its_array;
spec(?NNID(?OBJ_OPTIONAL_PLACEHOLDER)) -> optional_placeholder;
spec(?NNID(?OBJ_MANDATORY_PLACEHOLDER)) -> mandatory_placeholder;
% Common types
spec(?NNID(?TYPE_REFERENCES)) -> references;
spec(?NNID(?TYPE_BASE_OBJECT)) -> base_object_type;
spec(?NNID(?TYPE_FOLDER)) -> folder_type;
spec(?NNID(?TYPE_PROPERTY)) -> property_type;
% Explicit data types beside builtins
spec(?NNID(?DATATYPE_ENUMERATION)) -> enumeration;
spec(?NNID(?DATAYPE_ENUM_VALUE)) -> enum_value;
% Common reference types
spec(?NID_HAS_CHILD) -> has_child;
spec(?NID_ORGANIZES) -> organizes;
spec(?NNID(?REF_HAS_MODELING_RULE)) -> has_modeling_rule;
spec(?NID_HAS_ENCODING) -> has_encoding;
spec(?NID_HAS_DESCRIPTION) -> has_description;
spec(?NID_HAS_TYPE_DEFINITION) -> has_type_definition;
spec(?NID_HAS_SUBTYPE) -> has_subtype;
spec(?NID_HAS_PROPERTY) -> has_property;
spec(?NID_HAS_COMPONENT) -> has_component;
spec(?NID_HAS_NOTIFIER) -> has_notifier;
% Builtin types
spec(#opcua_node_id{ns = 0, type = numeric, value = Id})
  when ?IS_BUILTIN_TYPE_ID(Id) ->
    opcua_codec:builtin_type_name(Id);
% Numerical with default namespace
spec(#opcua_node_id{ns = 0, type = numeric, value = Id})
  when is_integer(Id) -> Id;
% Numerical with explicit namespace
spec(#opcua_node_id{ns = NS, type = numeric, value = Id})
  when is_integer(NS), is_integer(Id) -> {NS, Id};
% String with default namespace
spec(#opcua_node_id{ns = 0, type = string, value = Id})
  when is_binary(Id) -> Id;
% String with explicit namespace
spec(#opcua_node_id{ns = NS, type = numeric, value = Id})
  when is_integer(NS), is_binary(Id) -> {NS, Id};
% Otherwise, the node id itself...
spec(#opcua_node_id{} = NodeId) ->
  NodeId.

%TODO: Add support for URI namespaces (nsu=)
format(List)
  when is_list(List) ->
    format(",", List);
format(undefined) ->
    undefined;
format(Builtin)
  when is_atom(Builtin) ->
    Builtin;
format(V)
  when is_integer(V) ->
    iolist_to_binary(io_lib:format("i=~w", [V]));
format({N, V})
  when is_integer(N), is_integer(V) ->
    iolist_to_binary(io_lib:format("ns=~w;i=~w", [N, V]));
format(V)
  when is_binary(V) ->
    iolist_to_binary(io_lib:format("s=~s", [V]));
format({N, V})
  when is_integer(N), is_binary(V) ->
    iolist_to_binary(io_lib:format("ns=~w;s=~s", [N, V]));
format(#opcua_node_id{ns = 0, type = numeric, value = V})
  when is_integer(V) ->
    iolist_to_binary(io_lib:format("i=~w", [V]));
format(#opcua_node_id{ns = N, type = numeric, value = V})
  when is_integer(N), is_integer(V) ->
    iolist_to_binary(io_lib:format("ns=~w;i=~w", [N, V]));
format(#opcua_node_id{ns = 0, type = string, value = V})
  when is_binary(V) ->
    iolist_to_binary(io_lib:format("s=~s", [V]));
format(#opcua_node_id{ns = N, type = string, value = V})
  when is_integer(N), is_binary(V) ->
    iolist_to_binary(io_lib:format("ns=~w;s=~s", [N, V]));
format(#opcua_node_id{ns = 0, type = guid, value = V})
  when is_binary(V) ->
    Id = uuid:uuid_to_string(V, binary_standard),
    iolist_to_binary(io_lib:format("g=~s", [Id]));
format(#opcua_node_id{ns = N, type = guid, value = V})
  when is_integer(N), is_binary(V) ->
    Id = uuid:uuid_to_string(V, binary_standard),
    iolist_to_binary(io_lib:format("ns=~w;g=~s", [N, Id]));
format(#opcua_node_id{ns = 0, type = opaque, value = V})
  when is_binary(V) ->
    Id = base64:encode(V),
    iolist_to_binary(io_lib:format("b=~s", [Id]));
format(#opcua_node_id{ns = N, type = opaque, value = V})
  when is_integer(N), is_binary(V) ->
    Id = base64:encode(V),
    iolist_to_binary(io_lib:format("ns=~w;b=~s", [N, Id]));
format(#opcua_node{node_id = Id}) ->
    format(Id).

format(Sep, Items) ->
    iolist_to_binary(lists:join(Sep, [format(I) || I <- Items])).

%TODO: Add support for URI namespaces (nsu=)
parse(<<"svr=", Rest/binary>>) ->
    [SVRBin, Rest2] = binary:split(Rest, <<";">>),
    case string:to_integer(SVRBin) of
        {SVR, <<>>} ->
            NID = parse(Rest2),
            #opcua_expanded_node_id{server_index = SVR, node_id = NID};
        _ ->
            erlang:error(badarg)
    end;
parse(<<"i=", Rest/binary>>) ->
    case string:to_integer(Rest) of
        {Id, <<>>} ->
            #opcua_node_id{ns = 0, type = numeric, value = Id};
        _ ->
            erlang:error(badarg)
    end;
parse(<<"s=", Id/binary>>) ->
    #opcua_node_id{ns = 0, type = string, value = Id};
parse(<<"g=", Rest/binary>>) ->
    Id = uuid:string_to_uuid(Rest),
    #opcua_node_id{ns = 0, type = guid, value = Id};
parse(<<"b=", Rest/binary>>) ->
    Id = base64:decode(Rest),
    #opcua_node_id{ns = 0, type = opaque, value = Id};
parse(<<"ns=", Rest/binary>>) ->
    [NSBin, Rest2] = binary:split(Rest, <<";">>),
    case string:to_integer(NSBin) of
        {NS, <<>>} ->
            NID = parse(Rest2),
            NID#opcua_node_id{ns = NS};
        _ ->
            erlang:error(badarg)
    end;
parse(_Data) ->
    erlang:error(badarg).

from_attributes(#{node_id := #opcua_error{} = Error}) -> Error;
from_attributes(Attribs) ->
    #{
        node_id := NodeId,
        node_class := NodeClass,
        %TODO: We should keep the namespace of the browse name
        browse_name := #opcua_qualified_name{name = BrowseName},
        display_name := #opcua_localized_text{text = DisplayName},
        description := DescriptionRec,
        write_mask := WriteMask,
        user_write_mask := UserWriteMask,
        role_permissions := RolePermissions,
        user_role_permissions := UserRolePermissions,
        access_restrictions := AccessRestrictions
    } = Attribs,
    #opcua_node{
        node_id = NodeId,
        node_class = node_class_from_attributes(NodeClass, Attribs),
        origin = remote,
        browse_name = BrowseName,
        display_name = DisplayName,
        description = text_if_defined(DescriptionRec),
        write_mask = if_defined(WriteMask),
        user_write_mask = if_defined(UserWriteMask),
        role_permissions = if_defined(RolePermissions),
        user_role_permissions = if_defined(UserRolePermissions),
        access_restrictions = if_defined(AccessRestrictions)
    }.

class(#opcua_node{node_class = #opcua_object{}})         -> object;
class(#opcua_node{node_class = #opcua_variable{}})       -> variable;
class(#opcua_node{node_class = #opcua_method{}})         -> method;
class(#opcua_node{node_class = #opcua_object_type{}})    -> object_type;
class(#opcua_node{node_class = #opcua_variable_type{}})  -> variable_type;
class(#opcua_node{node_class = #opcua_reference_type{}}) -> reference_type;
class(#opcua_node{node_class = #opcua_data_type{}})      -> data_type;
class(#opcua_node{node_class = #opcua_view{}})           -> view.

%% Generic Nodes attributes
attribute_value(node_id, #opcua_node{node_id = NodeId}) ->
   NodeId;
attribute_value(node_class, Node) ->
    class(Node);
attribute_value(browse_name, #opcua_node{node_id = ?NID_NS(NS), browse_name = V})
  when V =:= undefined; is_binary(V) ->
    %TODO: Properties shouldn't use the node namespace for the browse name
    #opcua_qualified_name{ns = NS, name = V};
attribute_value(display_name, #opcua_node{display_name = undefined, browse_name = V})
  when V =:= undefined; is_binary(V) ->
    #opcua_localized_text{text = V};
attribute_value(display_name, #opcua_node{display_name = V})
  when V =:= undefined; is_binary(V) ->
    #opcua_localized_text{text = V};
attribute_value(description, #opcua_node{description = V})
  when V =:= undefined; is_binary(V) ->
    #opcua_localized_text{text = V};
attribute_value(write_mask, #opcua_node{write_mask = V})
  when V =/= undefined -> V;
attribute_value(user_write_mask, #opcua_node{user_write_mask = V})
  when V =/= undefined -> V;
attribute_value(role_permissions, #opcua_node{role_permissions = V})
  when V =/= undefined -> V;
attribute_value(user_role_permissions, #opcua_node{user_role_permissions = V})
  when V =/= undefined -> V;
attribute_value(access_restrictions, #opcua_node{access_restrictions = V})
  when V =/= undefined -> V;

%% Object Node Class Attributes
attribute_value(event_notifier, #opcua_node{node_class = #opcua_object{event_notifier = V}})
  when V =/= undefined -> V;

%% Variable Node Class Attributes
attribute_value(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= string; T =:= byte_string; T =:= xml -> undefined;
attribute_value(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= node_id -> ?UNDEF_NODE_ID;
attribute_value(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= expanded_node_id -> ?UNDEF_EXT_NODE_ID;
attribute_value(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= localized_text -> ?UNDEF_LOCALIZED_TEXT;
attribute_value(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= qualified_name -> ?UNDEF_QUALIFIED_NAME;
attribute_value(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= extension_object -> ?UNDEF_EXT_OBJ;
attribute_value(value, #opcua_node{node_class = #opcua_variable{value = V}}) ->
  V;
attribute_value(data_type, #opcua_node{node_class = #opcua_variable{data_type = V}})
  when V =/= undefined -> V;
attribute_value(value_rank, #opcua_node{node_class = #opcua_variable{value_rank = V}})
  when V =/= undefined -> V;
attribute_value(array_dimensions, #opcua_node{node_class = #opcua_variable{array_dimensions = V}})
  when is_list(V) -> V;
attribute_value(access_level, #opcua_node{node_class = #opcua_variable{access_level = V}})
  when V =/= undefined -> V;
attribute_value(user_access_level, #opcua_node{node_class = #opcua_variable{user_access_level = V}})
  when V =/= undefined -> V;
attribute_value(minimum_sampling_interval, #opcua_node{node_class = #opcua_variable{minimum_sampling_interval = V}})
  when V =/= undefined -> V;
attribute_value(historizing, #opcua_node{node_class = #opcua_variable{historizing = V}})
  when V =/= undefined -> V;
attribute_value(access_level_ex, #opcua_node{node_class = #opcua_variable{access_level_ex = V}})
  when V =/= undefined -> V;

%% Method Node Class Attributes
attribute_value(executable, #opcua_node{node_class = #opcua_method{executable = V}})
  when V =/= undefined -> V;
attribute_value(user_executable, #opcua_node{node_class = #opcua_method{user_executable = V}})
  when V =/= undefined -> V;

%% Object Type Node Class Attributes
attribute_value(is_abstract, #opcua_node{node_class = #opcua_object_type{is_abstract = V}})
  when V =/= undefined -> V;

%% Variable Type Node Class Attributes
attribute_value(value, #opcua_node{node_class = #opcua_variable_type{value = V}}) ->
    V;
attribute_value(data_type, #opcua_node{node_class = #opcua_variable_type{data_type = V}})
  when V =/= undefined -> V;
attribute_value(value_rank, #opcua_node{node_class = #opcua_variable_type{value_rank = V}})
  when V =/= undefined -> V;
attribute_value(array_dimensions, #opcua_node{node_class = #opcua_variable_type{array_dimensions = V}})
  when V =/= undefined -> V;
attribute_value(is_abstract, #opcua_node{node_class = #opcua_variable_type{is_abstract = V}})
  when V =/= undefined -> V;

%% Data Type Node Class Attributes
attribute_value(is_abstract, #opcua_node{node_class = #opcua_data_type{is_abstract = V}})
  when V =/= undefined -> V;
attribute_value(data_type_definition, #opcua_node{node_class = #opcua_data_type{data_type_definition = V}})
  when V =/= undefined -> V;

%% Reference Type Node Class Attributes
attribute_value(is_abstract, #opcua_node{node_class = #opcua_reference_type{is_abstract = V}})
  when V =/= undefined -> V;
attribute_value(symmetric, #opcua_node{node_class = #opcua_reference_type{symmetric = V}})
  when V =/= undefined -> V;
attribute_value(inverse_name, #opcua_node{node_class = #opcua_reference_type{inverse_name = V}})
  when V =:= undefined; is_binary(V) ->
    #opcua_localized_text{text = V};

%% View Node Class Attributes
attribute_value(contains_no_loops, #opcua_node{node_class = #opcua_view{contains_no_loops = V}})
  when V =/= undefined -> V;
attribute_value(event_notifier, #opcua_node{node_class = #opcua_view{event_notifier = V}})
  when V =/= undefined -> V;
attribute_value(_Attr, #opcua_node{}) ->
    error(bad_attribute_id_invalid).

attribute_type(value, #opcua_node{node_class = #opcua_variable{data_type = T}}) -> T;
attribute_type(data_type_definition, #opcua_node{node_class =
        #opcua_data_type{data_type_definition = #{structure_type := _, fields := _}}}) ->
    ?NNID(99);
attribute_type(data_type_definition, #opcua_node{node_class =
        #opcua_data_type{data_type_definition = #{fields := _}}}) ->
    ?NNID(100);
attribute_type(data_type_definition, #opcua_node{node_class = #opcua_data_type{}}) ->
    ?NNID(97);
attribute_type(Name, _Node) -> opcua_nodeset:attribute_type(Name).

attribute_rank(value, #opcua_node{node_class = #opcua_variable{value_rank = V}}) -> V;
attribute_rank(value, #opcua_node{node_class = #opcua_variable_type{value_rank = V}}) -> V;
attribute_rank(_, #opcua_node{}) -> -1.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

text_if_defined(#opcua_localized_text{text = Text}) -> Text;
text_if_defined(#opcua_error{status = bad_attribute_id_invalid}) -> undefined;
text_if_defined(Value) -> Value.

if_defined(#opcua_error{status = bad_attribute_id_invalid}) -> undefined;
if_defined(Value) -> Value.

node_class_from_attributes(object, Attribs) ->
    #{event_notifier := EventNotifier} = Attribs,
    #opcua_object{event_notifier = if_defined(EventNotifier)};
node_class_from_attributes(variable, Attribs) ->
    #{value := Value,
      data_type := DataType,
      value_rank := ValueRank,
      array_dimensions := ArrayDimensions,
      access_level := AccessLevel,
      user_access_level := UserAccessLevel,
      minimum_sampling_interval := MinimumSamplingInterval,
      historizing := Historizing,
      access_level_ex := AccessLevelEx
    } = Attribs,
    #opcua_variable{
        value = Value,
        data_type = DataType,
        value_rank = if_defined(ValueRank),
        array_dimensions =  if_defined(ArrayDimensions),
        access_level =  if_defined(AccessLevel),
        user_access_level =  if_defined(UserAccessLevel),
        minimum_sampling_interval =  if_defined(MinimumSamplingInterval),
        historizing =  if_defined(Historizing),
        access_level_ex =  if_defined(AccessLevelEx)
    };
node_class_from_attributes(method, Attribs) ->
    #{executable := Executable,
      user_executable :=  UserExecutable
    } = Attribs,
    #opcua_method{
        executable =  if_defined(Executable),
        user_executable =  if_defined(UserExecutable)
    };
node_class_from_attributes(object_type, Attribs) ->
    #{is_abstract :=  IsAbstract} = Attribs,
    #opcua_object_type{is_abstract =  if_defined(IsAbstract)};
node_class_from_attributes(variable_type, Attribs) ->
    #{value := Value,
      data_type := DataType,
      value_rank :=  ValueRank,
      array_dimensions :=  ArrayDimensions,
      is_abstract :=  IsAbstract
    } = Attribs,
    #opcua_variable_type{
        value = Value,
        data_type = DataType,
        value_rank =  if_defined(ValueRank),
        array_dimensions =  if_defined(ArrayDimensions),
        is_abstract =  if_defined(IsAbstract)
    };
node_class_from_attributes(reference_type, Attribs) ->
    #{is_abstract := IsAbstract,
      symmetric := Symmetric,
      inverse_name := InverseName
    } = Attribs,
    %TODO: Maybe we want to convert the localized text to simple binary ?
    #opcua_reference_type{
        is_abstract =  if_defined(IsAbstract),
        symmetric =  if_defined(Symmetric),
        inverse_name =  if_defined(InverseName)
    };
node_class_from_attributes(data_type, Attribs) ->
    #{is_abstract := IsAbstract,
      data_type_definition := DataTypeDefinition
    } = Attribs,
    #opcua_data_type{
        is_abstract =  if_defined(IsAbstract),
        data_type_definition = DataTypeDefinition
    };
node_class_from_attributes(view, Attribs) ->
    #{contains_no_loops := ContainsNoLoops,
      event_notifier := EventNotifier
    } = Attribs,
    #opcua_view{
        contains_no_loops =  if_defined(ContainsNoLoops),
        event_notifier =  if_defined(EventNotifier)
    }.
