%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA Node Set parser.
%%%
%%% Parse OPCUA Node Set files and generate internal database format.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_nodeset_parser).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([parse/0]).


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

parse() ->
    PrivDir = code:priv_dir(opcua),
    BaseDir = filename:join([PrivDir, "nodeset"]),
    InputDir = filename:join([BaseDir, "reference"]),
    OutputDir = filename:join([BaseDir, "data"]),
    parse_attributes(InputDir, OutputDir),
    parse_status(InputDir, OutputDir),
    parse_nodeset(InputDir, OutputDir).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_attributes(InputDir, OutputDir) ->
    InputPath = filename:join([InputDir, "Schema", "AttributeIds.csv"]),
    OutputPath = filename:join([OutputDir, "nodeset.attributes.bterm"]),
    ?LOG_INFO("Loading OPCUA attributes mapping from ~s ...", [InputPath]),
    Terms = opcua_util_csv:fold(InputPath,
        fun([NameStr, IdStr], Acc) ->
            Name = opcua_util:convert_name(NameStr),
            Id = list_to_integer(IdStr),
            [{Id, Name} | Acc]
        end, []),
    ?LOG_INFO("Saving OPCUA attributes mapping into ~s ...", [OutputPath]),
    opcua_util_bterm:save(OutputPath, Terms).

parse_status(InputDir, OutputDir) ->
    InputPath = filename:join([InputDir, "Schema", "StatusCode.csv"]),
    OutputPath = filename:join([OutputDir, "nodeset.status.bterm"]),
    ?LOG_INFO("Loading OPCUA status code mapping from ~s ...", [InputPath]),
    Terms = opcua_util_csv:fold(InputPath,
        fun([NameStr, "0x" ++ CodeStr | DescList], Acc) ->
            Name = opcua_util:convert_name(NameStr),
            Code = list_to_integer(CodeStr, 16),
            Desc = iolist_to_binary(lists:join("," , DescList)),
            [{Code, Name, Desc} | Acc]
        end, []),
    ?LOG_INFO("Saving OPCUA status code mapping into ~s ...", [OutputPath]),
    opcua_util_bterm:save(OutputPath, Terms).

parse_nodeset(InputDir, OutputDir) ->
    Namespaces = #{0 => <<"http://opcfoundation.org/UA/">>},
    Meta = #{namespaces => Namespaces},
    BaseInputPath = filename:join([InputDir, "Schema", "Opc.Ua.NodeSet2.Services.xml"]),
    InputPatternPath = filename:join([InputDir, "*", "*.NodeSet2.xml"]),
    InputPathList = [BaseInputPath | filelib:wildcard(InputPatternPath)],
    parse(OutputDir, InputPathList, Meta, []).

parse(DestDir, [], Meta, Nodes) ->
    NodesProplist = lists:reverse(Nodes),
    DataTypesSchemas = extract_data_type_schemas(NodesProplist),
    Encodings = extract_encodings(NodesProplist),
    NodeSpecs = extract_nodes(NodesProplist),
    References = extract_references(NodesProplist),
    Namespaces = maps:to_list(maps:get(namespaces, Meta, #{})),
    write_terms(DestDir, "OPCUA data type schemas", "datatypes", DataTypesSchemas),
    write_terms(DestDir, "OPCUA encoding specifications", "encodings", Encodings),
    write_terms(DestDir, "OPCUA nodes", "nodes", NodeSpecs),
    write_terms(DestDir, "OPCUA references", "references", References),
    write_terms(DestDir, "OPCUA namespaces", "namespaces", Namespaces),
    ok;
parse(DestDir, [File | Files], Meta, Nodes) ->
    ?LOG_INFO("Parsing OPCUA nodeset file ~s ...", [File]),
    {XML, []} = xmerl_scan:file(File, [{space, normalize}]),
    {Meta2, Nodes2} = parse_node_set(xml_to_simple(XML), Meta, Nodes),
    parse(DestDir, Files, Meta2, Nodes2).


%%% XML SAX PARSER CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_node_set({<<"UANodeSet">>, _Attrs, Content}, Meta, Nodes) ->
    Namespaces = parse_namespaces(get_value([<<"NamespaceUris">>], Content, [])),
    Aliases = parse_aliases(get_value([<<"Aliases">>], Content)),
    Meta2 = Meta#{
        namespaces => maps:merge(maps:get(namespaces, Meta, #{}), Namespaces),
        aliases => maps:merge(maps:get(aliases, Meta, #{}), Aliases)
    },
    lists:foldl(fun parse_node/2, {Meta2, Nodes}, Content).

parse_namespaces(Namespaces) ->
    URIs = [URI || {<<"Uri">>, _, [URI]} <- Namespaces],
    maps:from_list(lists:zip(lists:seq(1, length(URIs)), URIs)).

parse_aliases(Aliases) ->
    maps:from_list([{Name, parse(node_id, ID, #{aliases => #{}})} || {<<"Alias">>, #{<<"Alias">> := Name}, [ID]} <- Aliases]).

parse_node({Tag, Attrs, Content} = Elem, {Meta, Nodes}) when ?IS_NODE(Tag) ->
    NodeId = get_attr(<<"NodeId">>, Attrs, node_id, Meta),
    Node = #opcua_node{
        node_id = NodeId,
        origin = standard,
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
        type_id = get_attr(<<"ReferenceType">>, Attrs, node_id, Meta),
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

parse(node_id, Data, #{aliases := Aliases}) ->
    try opcua_node:parse(Data)
    catch _:badarg -> maps:get(Data, Aliases)
    end;
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
    opcua_nodeset_datatypes:generate_schemas(DataTypeNodesProplist).

extract_encodings(NodesProplist) ->
    [{TargetNodeId, {SourceNodeId, binary}} ||
        {_NodeId,
         {
            #opcua_node{browse_name = <<"Default Binary">>},
            References
         }
        } <- NodesProplist,
        #opcua_reference{
            type_id = #opcua_node_id{value = 38},
            source_id = SourceNodeId,
            target_id = TargetNodeId
        } <- References
    ].

write_terms(BaseDir, Desc, Tag, Terms) ->
    OutputPath = filename:join([BaseDir, "nodeset."++ Tag ++ ".bterm"]),
    ?LOG_INFO("Saving ~s into ~s ...", [Desc, OutputPath]),
    opcua_util_bterm:save(OutputPath, Terms).
