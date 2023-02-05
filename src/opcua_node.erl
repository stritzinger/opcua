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
-export([attribute/2]).
-export([attribute_type/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec id(opcua:node_spec() | opcua:node_rec()) -> opcua:node_id().
id(undefined) -> ?UNDEF_NODE_ID;
id(#opcua_node{node_id = NodeId}) -> NodeId;
% Common base nodes
id(root) -> ?NNID(?OBJ_ROOT_FOLDER);
id(objects) -> ?NNID(?OBJ_OBJECTS_FOLDER);
id(server) -> ?NNID(?OBJ_SERVER);
% Common reference types
id(has_component) -> ?NID_HAS_COMPONENT;
id(has_property) -> ?NID_HAS_PROPERTY;
id(organizes) -> ?NID_ORGANIZES;
id(has_notifier) -> ?NID_HAS_NOTIFIER;
id(has_subtype) -> ?NID_HAS_SUBTYPE;
id(has_type_definition) -> ?NID_HAS_TYPE_DEFINITION;
id(has_encoding) -> ?NID_HAS_ENCODING;
id(has_description) -> ?NID_HAS_DESCRIPTION;
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

-spec spec(opcua:node_id() | opcua:node_ref()) -> opcua:node_spec().
spec(?UNDEF_NODE_ID) -> undefined;
spec(#opcua_node{node_id = NodeId}) -> spec(NodeId);
% Common base nodes
spec(?NNID(?OBJ_ROOT_FOLDER)) -> root;
spec(?NNID(?OBJ_OBJECTS_FOLDER)) -> objects;
spec(?NNID(?OBJ_SERVER)) -> server;
% Common reference types
spec(?NID_HAS_COMPONENT) -> has_component;
spec(?NID_HAS_PROPERTY) -> has_property;
spec(?NID_ORGANIZES) -> organizes;
spec(?NID_HAS_NOTIFIER) -> has_notifier;
spec(?NID_HAS_SUBTYPE) -> has_subtype;
spec(?NID_HAS_TYPE_DEFINITION) -> has_type_definition;
spec(?NID_HAS_ENCODING) -> has_encoding;
spec(?NID_HAS_DESCRIPTION) -> has_description;
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
% OTherwise, the node id itself...
spec(#opcua_node_id{} = NodeId) ->
  NodeId.

%TODO: Add support for URI namespaces (nsu=)
format(List)
  when is_list(List) ->
    format(",", List);
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

from_attributes(Attribs) ->
    #{
        node_id := NodeId,
        node_class := NodeClass,
        %TODO: We should keep the namespace of the browse name
        browse_name := #opcua_qualified_name{name = BrowseName},
        display_name := #opcua_localized_text{text = DisplayName},
        description := #opcua_localized_text{text = Description},
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
        description = Description,
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
attribute(node_id, #opcua_node{node_id = NodeId}) ->
    NodeId;
attribute(node_class, Node) ->
    class(Node);
attribute(browse_name, #opcua_node{node_id = ?NID_NS(NS), browse_name = V})
  when V =:= undefined; is_binary(V) ->
    %TODO: Properties shouldn't use the node namespace for the browse name
    #opcua_qualified_name{ns = NS, name = V};
attribute(display_name, #opcua_node{display_name = undefined, browse_name = V})
  when V =:= undefined; is_binary(V) ->
    #opcua_localized_text{text = V};
attribute(display_name, #opcua_node{display_name = V})
  when V =:= undefined; is_binary(V) ->
    #opcua_localized_text{text = V};
attribute(description, #opcua_node{description = V})
  when V =:= undefined; is_binary(V) ->
    #opcua_localized_text{text = V};
attribute(write_mask, #opcua_node{write_mask = V}) -> V;
attribute(user_write_mask, #opcua_node{user_write_mask = V}) -> V;
% attribute(role_permissions, #opcua_node{role_permissions = V}) -> V;
% attribute(user_role_permissions, #opcua_node{user_role_permissions = V}) -> V;
% attribute(access_restrictions, #opcua_node{access_restrictions = V}) -> V;
% %% Object Node Class Attributes
% attribute(event_notifier, #opcua_node{node_class = #opcua_object{event_notifier = V}}) -> V;
%% Variable Node Class Attributes
attribute(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= string; T =:= byte_string; T =:= xml -> undefined;
attribute(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= node_id -> ?UNDEF_NODE_ID;
attribute(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= expanded_node_id -> ?UNDEF_EXT_NODE_ID;
attribute(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= localized_text -> ?UNDEF_LOCALIZED_TEXT;
attribute(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= qualified_name -> ?UNDEF_QUALIFIED_NAME;
attribute(value, #opcua_node{node_class = #opcua_variable{data_type = T, value = undefined}})
  when T =:= extension_object -> ?UNDEF_EXT_OBJ;
attribute(value, #opcua_node{node_class = #opcua_variable{value = V}})
  when V =/= undefined -> V;
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
% attribute(event_notifier, #opcua_node{node_class = #opcua_view{event_notifier = V}}) -> V;
attribute(_Attr, #opcua_node{}) -> error(bad_attribute_id_invalid).

attribute_type(value, #opcua_node{node_class = #opcua_variable{data_type = T}}) -> T;
attribute_type(Name, _Node) -> opcua_nodeset:attribute_type(Name).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
      data_type_definition := DataTypeDef
    } = Attribs,
    #opcua_data_type{
        is_abstract =  if_defined(IsAbstract),
        data_type_definition =  if_defined(DataTypeDef)
    };
node_class_from_attributes(view, Attribs) ->
    #{contains_no_loops := ContainsNoLoops,
      event_notifier := EventNotifier
    } = Attribs,
    #opcua_view{
        contains_no_loops =  if_defined(ContainsNoLoops),
        event_notifier =  if_defined(EventNotifier)
    }.
