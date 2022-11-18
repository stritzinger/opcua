-module(opcua_node).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([id/1]).
-export([format/1, format/2]).
-export([parse/1]).
-export([class/1]).
-export([attribute/2]).
-export([attribute_type/2]).
-export([parse_access_level/1]).
-export([format_access_level/1]).


%%% MACROS FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ACCESS_LEVEL_SPEC, [
    % https://reference.opcfoundation.org/v104/Core/docs/Part3/8.57/
    {current_read,          2#00000000001},
    {current_write,         2#00000000010},
    {history_read,          2#00000000100},
    {history_write,         2#00000001000},
    {semantic_change,       2#00000010000},
    {status_write,          2#00000100000},
    {timestamp_write,       2#00001000000},
    % https://reference.opcfoundation.org/v104/Core/docs/Part3/8.58/
    {nonatomic_read,        2#00010000000},
    {nonatomic_write,       2#00100000000},
    {write_full_array_only, 2#01000000000}
]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec id(opcua:node_spec()) -> opcua:node_id().
id(root) -> ?NNID(?OBJ_ROOT_FOLDER);
id(objects) -> ?NNID(?OBJ_OBJECTS_FOLDER);
id(server) -> ?NNID(?OBJ_SERVER);
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
% attribute(write_mask, #opcua_node{write_mask = V}) -> V;
% attribute(user_write_mask, #opcua_node{user_write_mask = V}) -> V;
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

%TODO: figure out if these types are correct, and fill in the undefined ones
attribute_type(node_id, _Node) -> node_id;
attribute_type(node_class, _Node) -> ?NNID(257);
attribute_type(browse_name, _Node) -> qualified_name;
attribute_type(display_name, _Node) -> localized_text;
attribute_type(description, _Node) -> localized_text;
% attribute_type(write_mask, _Node) -> undefined;
% attribute_type(user_write_mask, _Node) -> undefined;
% attribute_type(is_abstract, _Node) -> boolean;
% attribute_type(symmetric, _Node) -> boolean;
% attribute_type(inverse_name, _Node) -> boolean;
% attribute_type(contains_no_loops, _Node) -> boolean;
% attribute_type(event_notifier, _Node) -> byte_string;
attribute_type(value, #opcua_node{node_class = #opcua_variable{data_type = T}}) -> T;
attribute_type(data_type, _Node) -> node_id;
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
% attribute_type(access_level_ex, _Node) -> undefined;
attribute_type(_Attr, _Node) -> error(bad_attribute_id_invalid).

parse_access_level(Flags) when is_integer(Flags) ->
    lists:foldl(fun({Key, Mask}, Result) ->
        Result#{Key => ((Flags band Mask) =/= 0)}
    end, #{}, ?ACCESS_LEVEL_SPEC).

format_access_level(#{} = Map) ->
    lists:foldl(fun({Key, Mask}, Result) ->
        case maps:find(Key, Map) of
            error -> Result;
            {ok, false} -> Result;
            {ok, true} -> Result band Mask
        end
    end, 0, ?ACCESS_LEVEL_SPEC).
