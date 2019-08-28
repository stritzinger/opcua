-module(opcua_database_nodes).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([setup/1]).
-export([parse/1]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(IS_NODE(Name),
    Name =:= <<"UAObject">>;
    Name =:= <<"UAVariable">>;
    Name =:= <<"UAMethod">>;
    Name =:= <<"UAView">>;
    Name =:= <<"UAObjectType">>;
    Name =:= <<"UAVariableType">>;
    Name =:= <<"UADataType">>;
    Name =:= <<"UAReferenceType">>
).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup(Dir) ->
    opcua_database_encodings:setup(),
    opcua_database_data_types:setup(),
    ?LOG_INFO("Loading OPCUA nodes..."),
    load_nodes(Dir),
    ?LOG_INFO("Loading OPCUA references..."),
    load_references(Dir),
    ?LOG_INFO("Loading OPCUA data type schemas..."),
    load_data_type_schemas(Dir),
    ?LOG_INFO("Loading OPCUA encoding specifications..."),
    load_encodings(Dir).

parse(File) ->
    {XML, []} = xmerl_scan:file(File, [{space, normalize}]),
    Root = filename:rootname(File),
    NodesProplist = parse_node_set(xml_to_simple(XML)),
    DataTypesSchemas = extract_data_type_schemas(NodesProplist),
    Encodings = extract_encodings(NodesProplist),
    Nodes = extract_nodes(NodesProplist),
    References = extract_references(NodesProplist),
    ?LOG_INFO("Saving OPCUA data type schemas..."),
    write_terms(Root, "data_type_schemas", DataTypesSchemas),
    ?LOG_INFO("Saving OPCUA encoding specifications..."),
    write_terms(Root, "encodings", Encodings),
    ?LOG_INFO("Saving OPCUA nodes..."),
    write_terms(Root, "nodes", Nodes),
    ?LOG_INFO("Saving OPCUA references..."),
    write_terms(Root, "references", References),
    ok.


%%% XML SAX PARSER CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_node_set({<<"UANodeSet">>, _Attrs, Content}) ->
    Aliases = parse_aliases(get_value([<<"Aliases">>], Content)),
    Meta = #{aliases => Aliases},
    {Meta, Nodes} = lists:foldl(fun parse_node/2, {Meta, []}, Content),
    lists:reverse(Nodes).

parse_aliases(Aliases) ->
    maps:from_list([{Name, parse(node_id, ID, #{aliases => #{}})} || {<<"Alias">>, #{<<"Alias">> := Name}, [ID]} <- Aliases]).

parse_node({Tag, Attrs, Content} = Elem, {Meta, Nodes}) when ?IS_NODE(Tag) ->
    NodeId = get_attr(<<"NodeId">>, Attrs, node_id, Meta),
    Node = #opcua_node{
        node_id = NodeId,
        browse_name = maps:get(<<"BrowseName">>, Attrs, undefined),
        display_name = hd(get_value([<<"DisplayName">>], Content)),
        node_class = parse_node_class(Elem, Meta)
    },
    Refs = parse_references(get_value([<<"References">>], Content), NodeId, Meta),
    {Meta, [{NodeId, {Node, Refs}}|Nodes]};
parse_node(_Element, State) ->
    State.

parse_references(Refs, NodeId, Meta) ->
    [parse_reference(R, NodeId, Meta) || R <- Refs].

parse_reference({<<"Reference">>, Attrs, [Peer]}, BaseNodeId, Meta) ->
    PeerNodeId = parse(node_id, Peer, Meta),
    {SourceNodeId, TargetNodeId} = case get_attr(<<"IsForward">>, Attrs, boolean, Meta, true) of
                                       true     -> {BaseNodeId, PeerNodeId};
                                       false    -> {PeerNodeId, BaseNodeId}
                                   end,
    #opcua_reference{
        reference_type_id = get_attr(<<"ReferenceType">>, Attrs, node_id, Meta),
        source_id = SourceNodeId,
        target_id = TargetNodeId
    }.

parse_node_class({<<"UAObject">>, Attrs, _Content}, Meta) ->
    #opcua_object{
        event_notifier = get_attr(<<"EventNotifier">>, Attrs, integer, Meta, undefined)
    };
parse_node_class({<<"UADataType">>, Attrs, Content}, Meta) ->
    #opcua_data_type{
        is_abstract = get_attr(<<"IsAbstract">>, Attrs, boolean, Meta, false),
        data_type_definition = parse_data_type_definition(get_in([<<"Definition">>], Content, undefined), Meta)
    };
parse_node_class({<<"UAReferenceType">>, Attrs, Content}, Meta) ->
    #opcua_reference_type{
        is_abstract = get_attr(<<"IsAbstract">>, Attrs, boolean, Meta, false),
        symmetric = get_attr(<<"Symmetric">>, Attrs, boolean, Meta, false),
        inverse_name = get_value([<<"InverseName">>], Content, undefined)
    };
parse_node_class({<<"UAObjectType">>, Attrs, _Content}, Meta) ->
    #opcua_object_type{
        is_abstract = get_attr(<<"IsAbstract">>, Attrs, boolean, Meta, false)
    };
parse_node_class({<<"UAVariableType">>, Attrs, _Content}, Meta) ->
    #opcua_variable_type{
        data_type = get_attr(<<"DataType">>, Attrs, node_id, Meta, undefined),
        value_rank = get_attr(<<"ValueRank">>, Attrs, integer, Meta, undefined),
        array_dimensions = get_attr(<<"ArrayDimensions">>, Attrs, array_dimensions, Meta, undefined),
        is_abstract = get_attr(<<"IsAbstract">>, Attrs, boolean, Meta, false)
    };
parse_node_class({<<"UAVariable">>, Attrs, _Content}, Meta) ->
    #opcua_variable{
        data_type = get_attr(<<"DataType">>, Attrs, node_id, Meta, undefined),
        value_rank = get_attr(<<"ValueRank">>, Attrs, integer, Meta, undefined),
        array_dimensions = get_attr(<<"ArrayDimensions">>, Attrs, array_dimensions, Meta, undefined),
        access_level = get_attr(<<"AccessLevel">>, Attrs, byte, Meta, undefined),
        user_access_level = get_attr(<<"UserAccessLevel">>, Attrs, byte, Meta, undefined),
        minimum_sampling_interval = get_attr(<<"MinimumSamplingInterval">>, Attrs, float, Meta, undefined),
        historizing = get_attr(<<"Historizing">>, Attrs, boolean, Meta, false),
        access_level_ex = get_attr(<<"AccessLevelEx">>, Attrs, uint32, Meta, undefined)
    };
parse_node_class({<<"UAMethod">>, Attrs, _Content}, Meta) ->
    #opcua_method{
        executable = get_attr(<<"Executable">>, Attrs, boolean, Meta, true),
        user_executable = get_attr(<<"UserExecutable">>, Attrs, boolean, Meta, true)
    }.

parse(node_id, <<"i=", Bin/binary>>, _Meta) ->
    {ID, <<>>} = string:to_integer(Bin),
    #opcua_node_id{ns = 0, type = numeric, value = ID};
parse(node_id, Alias, #{aliases := Aliases}) ->
    maps:get(Alias, Aliases);
parse(boolean, <<"true">>, _Meta) ->
    true;
parse(boolean, <<"false">>, _Meta) ->
    false;
parse(integer, Bin, _Meta) ->
    {Int, <<>>} = string:to_integer(Bin),
    Int;
parse(uint32, Bin, Meta) ->
    parse({integer, uint32, 0, 4294967295}, Bin, Meta);
parse(byte, Bin, Meta) ->
    parse({integer, byte, 0, 255}, Bin, Meta);
parse(float, Duration, Meta) ->
    case string:to_float(Duration) of
        {Float, <<>>}     -> Float;
        {error, no_float} -> parse(integer, Duration, Meta) * 1.0
    end;
parse({integer, Name, Lower, Upper}, Bin, _Meta) ->
    case string:to_integer(Bin) of
        {Int, <<>>} when Int >= Lower, Int =< Upper -> Int;
        _Other                                      -> {invalid_type, Name, Bin}
    end;
parse(array_dimensions, Dimensions, Meta) ->
    [parse(integer, D, Meta) || D <- binary:split(Dimensions, <<$,>>, [global])].

parse_data_type_definition(undefined, _Meta) ->
    undefined;
parse_data_type_definition({_Tag, Attrs, Fields}, Meta) ->
    ParsedFields = [parse_data_type_definition_field(F, Meta) || F <- Fields],
    IsUnion = get_attr(<<"IsUnion">>, Attrs, boolean, Meta, false),
    IsOptionSet = get_attr(<<"IsOptionSet">>, Attrs, boolean, Meta, false),
    #{fields => ParsedFields,
      is_union => IsUnion,
      is_option_set => IsOptionSet}.

parse_data_type_definition_field({<<"Field">>, Attrs, _Content}, Meta) ->
    {opcua_util:convert_name(maps:get(<<"Name">>, Attrs)), maps:from_list(lists:foldl(
        fun({Attr, Key, Type}, Result) ->
            try
                [{Key, get_attr(Attr, Attrs, Type, Meta)}|Result]
            catch
                error:{attr_not_found, Attr} ->
                    Result
            end
         end,
        [],
        [
            {<<"DataType">>, data_type, node_id},
            {<<"Value">>, value, integer},
            {<<"ValueRank">>, value_rank, integer},
            {<<"IsOptional">>, is_optional, boolean}
        ]
    ))}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_attr(Name, Attrs, Type, Meta) ->
    get_attr(Name, Attrs, Type, Meta, fun() -> error({attr_not_found, Name}) end).

get_attr(Name, Attrs, Type, Meta, Default) ->
    case maps:find(Name, Attrs) of
        {ok, Value}                     -> parse(Type, Value, Meta);
        error when is_function(Default) -> Default();
        error                           -> Default
    end.

get_value(Keys, Content) ->
    get_value(Keys, Content, fun() -> error({value_not_found, Keys, Content}) end).

get_value(Keys, Content, Default) ->
    case get_in(Keys, Content, Default) of
        {_Tag, _Attrs, Value} -> Value;
        Default               -> Default
    end.

get_in([K|Keys], [Tuple|_List], Default) when element(1, Tuple) =:= K ->
    get_in(Keys, Tuple, Default);
get_in(Keys, [_Tuple|List], Default) ->
    get_in(Keys, List, Default);
get_in([], Item, _Default) ->
    Item;
get_in(_Keys, [], Default) when is_function(Default) ->
    Default();
get_in(_Keys, [], Default) ->
    Default.

xml_to_simple(#xmlElement{} = E) ->
    {
        atom_to_binary(E#xmlElement.name, utf8),
        xml_attrs_to_map(E#xmlElement.attributes),
        xml_to_simple(E#xmlElement.content)
    };
xml_to_simple(Elements) when is_list(Elements) ->
    [S || E <- Elements, S <- [xml_to_simple(E)], S =/= <<" ">>];
xml_to_simple(#xmlText{value = Value}) ->
    iolist_to_binary(Value).

xml_attrs_to_map(Attrs) ->
    maps:from_list([
        {atom_to_binary(Name, utf8), iolist_to_binary(Value)}
        ||
        #xmlAttribute{name = Name, value = Value} <- Attrs
    ]).

extract_references(NodesProplist) ->
    [Ref || {_NodeId, {_Node, Refs}} <- NodesProplist, Ref <- Refs].

extract_nodes(NodesProplist) ->
    [Node || {_NodeId, {Node, _Refs}} <- NodesProplist].

extract_data_type_schemas(NodesProplist) ->
    DataTypeNodesProplist = lists:filter(fun({_, {Node, _}}) ->
        is_record(Node#opcua_node.node_class, opcua_data_type)
    end, NodesProplist),
    opcua_database_data_types:generate_schemas(DataTypeNodesProplist).

extract_encodings(NodesProplist) ->
    [{TargetNodeId, {SourceNodeId, binary}} ||
        {_NodeId,
         {
            #opcua_node{browse_name = <<"Default Binary">>},
            References
         }
        } <- NodesProplist,
        #opcua_reference{
            reference_type_id = #opcua_node_id{value = 38},
            source_id = SourceNodeId,
            target_id = TargetNodeId
        } <- References
    ].

load_nodes(Dir) ->
    load_all_terms(Dir, "nodes", fun(Node) ->
        opcua_address_space:add_nodes([Node])
    end).

load_references(Dir) ->
    load_all_terms(Dir, "references", fun(Reference) ->
        opcua_address_space:add_references([Reference])
    end).

load_data_type_schemas(Dir) ->
    load_all_terms(Dir, "data_type_schemas", fun({Keys, DataType}) ->
        opcua_database_data_types:store(Keys, DataType)
    end).

load_encodings(Dir) ->
    load_all_terms(Dir, "encodings", fun({NodeId, {TargetNodeId, Encoding}}) ->
        opcua_database_encodings:store(NodeId, TargetNodeId, Encoding)
    end).

load_all_terms(DirPath, Tag, Fun) ->
    Pattern = filename:join(DirPath, "**/*." ++ Tag ++ ".bterm"),
    NoAccCB = fun(V, C) ->
        Fun(V),
        case C rem 500 of
            0 ->
                ?LOG_DEBUG("Loaded ~w ~s; memory: ~.3f MB",
                           [C, Tag, erlang:memory(total)/(1024*1024)]);
            _ -> ok
        end,
        C + 1
    end,
    NoAccFun = fun(F, C) -> opcua_util_bterm:fold(F, NoAccCB, C) end,
    Count = lists:foldl(NoAccFun, 0, filelib:wildcard(Pattern)),
    ?LOG_DEBUG("Loaded ~w ~s terms", [Count, Tag]),
    ok.

write_terms(BasePath, Tag, Terms) ->
    FilePath = BasePath ++ "." ++ Tag ++ ".bterm",
    opcua_util_bterm:save(FilePath, Terms).
