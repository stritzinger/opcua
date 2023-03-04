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
    % Create an index of all nodes definitions
    NodeMap = maps:from_list([{NodeId, Node} || #{node_id := NodeId} = Node <- Nodes]),
    % Create an index of all node references
    RefIdx = build_ref_index(Nodes),
    % Finalize data type definitions
    NodeMap2 = post_process_data_types(NodeMap, RefIdx),
    % Create a space and add the namespaces from the meta data
    Space = opcua_space_backend:new(),
    lists:foreach(fun({Id, Uri}) ->
        opcua_space:add_namespace(Space, Id, Uri)
    end, maps:to_list(maps:get(namespaces, Meta, #{}))),
    % First add all the nodes and references required to generate the decoding
    % schemas. That includes all the data type nodes, all the encoding
    % descriptor nodes, all the relevent HasSubType, HasEncoding
    % and HasTypeDefinition references.
    finalize_typedata(Space, NodeMap2, RefIdx),
    % Now that all the data type has been added, all the decoding schemas
    % should have been generated, so we can proceed with post-processing the
    % variable and variable type nodes values.
    NodeMap3 = post_process_values(Space, NodeMap2),
    % Now we add all the remaining nodes and reference to the space.
    finalize_remaining(Space, NodeMap3, RefIdx),
    % Save the space into the cache file
    OutputPath = filename:join([DestDir, "nodeset.space.bterm"]),
    ?LOG_INFO("Saving nodeset into ~s ...", [OutputPath]),
    FoldFun = fun(F, Acc) -> opcua_space_backend:fold(Space, F, Acc) end,
    opcua_util_bterm:save(OutputPath, FoldFun),
    ok;
parse(DestDir, [File | Files], Meta, Nodes) ->
    ?LOG_INFO("Parsing OPCUA nodeset file ~s ...", [File]),
    {XML, []} = xmerl_scan:file(File, [{space, normalize}]),
    {Meta2, Nodes2} = parse_node_set(xml_to_simple(XML), Meta, Nodes),
    parse(DestDir, Files, Meta2, Nodes2).


%%% XML SAX PARSER CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_node_set({<<"UANodeSet">>, _Attrs, Content}, Meta, Nodes) ->
    %FIXME: This is probably wrong, checks that the namespaces and alias are
    % not supposed to be local to a nodeset definition, if they are we shouldn't
    % merge them into the global map.
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

parse_node({Tag, _, Content} = Elem, {Meta, Nodes}) when ?IS_NODE(Tag) ->
    BaseNode = collect(#{}, Elem, Meta, [
        {required, attrib, node_id, <<"NodeId">>, node_id},
        {required, attrib, browse_name, <<"BrowseName">>, string},
        {optional, attrib, symbolic_name, <<"SymbolicName">>, string},
        {optional, attrib, release_status, <<"ReleaseStatus">>, string},
        {required, value, display_name, [<<"DisplayName">>], string}
    ]),
    ClassNode = parse_node_class(Elem, BaseNode, Meta),
    #{node_id := BaseNodeId} = BaseNode,
    Refs = parse_references(BaseNodeId, get_value([<<"References">>], Content), Meta),
    Node = ClassNode#{references => Refs},
    {Meta, [Node | Nodes]};
parse_node(_Element, State) ->
    State.

parse_references(BaseNodeId, Refs, Meta) ->
    [parse_reference(BaseNodeId, R, Meta) || R <- Refs].

parse_reference(BaseNodeId, {<<"Reference">>, Attrs, [Peer]}, Meta) ->
    PeerNodeId = parse(node_id, Peer, Meta),
    {SourceNodeId, TargetNodeId} =
        case get_attr(<<"IsForward">>, Attrs, boolean, Meta, true) of
            true -> {BaseNodeId, PeerNodeId};
            false -> {PeerNodeId, BaseNodeId}
    end,
    #opcua_reference{
        type_id = get_attr(<<"ReferenceType">>, Attrs, node_id, Meta),
        source_id = SourceNodeId,
        target_id = TargetNodeId
    }.

parse_node_class({<<"UAObject">>, _, _} = Elem, Node, Meta) ->
    collect(Node, Elem, Meta, [
        {set, node_class, object},
        {required, attrib, parent_node_id, <<"ParentNodeId">>, node_id, undefined},
        {required, attrib, event_notifier, <<"EventNotifier">>, string, undefined}
    ]);
parse_node_class({<<"UADataType">>, _, Content} = Elem, Node, Meta) ->
    Node2 = collect(Node, Elem, Meta, [
        {set, node_class, data_type},
        {required, attrib, is_abstract, <<"IsAbstract">>, boolean, false},
        {optional, attrib, purpose, <<"Purpose">>, string}
    ]),
    DefContent = get_in([<<"Definition">>], Content, undefined),
    parse_data_type_definition(Node2, DefContent, Meta);
parse_node_class({<<"UAReferenceType">>, _, _} = Elem, Node, Meta) ->
    collect(Node, Elem, Meta, [
        {set, node_class, reference_type},
        {required, attrib, is_abstract, <<"IsAbstract">>, boolean, false},
        {required, attrib, symmetric, <<"Symmetric">>, boolean, false},
        {required, value, inverse_name, [<<"InverseName">>], string, undefined}
    ]);
parse_node_class({<<"UAObjectType">>, _, _} = Elem, Node, Meta) ->
    collect(Node, Elem, Meta, [
        {set, node_class, object_type},
        {required, attrib, is_abstract, <<"IsAbstract">>, boolean, false}
    ]);
parse_node_class({<<"UAVariableType">>, _, Content} = Elem, Node, Meta) ->
    Node2 = collect(Node, Elem, Meta, [
        {set, node_class, variable_type},
        {required, attrib, is_abstract, <<"IsAbstract">>, boolean, false},
        {required, attrib, data_type, <<"DataType">>, node_id, undefined},
        {required, attrib, value_rank, <<"ValueRank">>, integer, -1},
        {required, attrib, array_dimensions, <<"ArrayDimensions">>, array_dimensions, []}
    ]),
    ValueContent = get_in([<<"Value">>], Content, undefined),
    parse_variable_value(Node2, ValueContent, Meta);
parse_node_class({<<"UAVariable">>, _, Content} = Elem, Node, Meta) ->
    Node2 = collect(Node, Elem, Meta, [
        {set, node_class, variable},
        {required, attrib, parent_node_id, <<"ParentNodeId">>, node_id, undefined},
        {required, attrib, data_type, <<"DataType">>, node_id, undefined},
        {required, attrib, value_rank, <<"ValueRank">>, integer, -1},
        {required, attrib, array_dimensions, <<"ArrayDimensions">>, array_dimensions, []},
        {required, attrib, access_level, <<"AccessLevel">>, byte, 0},
        {required, attrib, user_access_level, <<"UserAccessLevel">>, byte, 0},
        {required, attrib, minimum_sampling_interval, <<"MinimumSamplingInterval">>, integer, -1},
        {required, attrib, historizing, <<"Historizing">>, boolean, false},
        {required, attrib, access_level_ex, <<"AccessLevelEx">>, uint32, 0},
        {required, attrib, user_access_level_ex, <<"UserAccessLevelEx">>, uint32, 0}
    ]),
    ValueContent = get_in([<<"Value">>], Content, undefined),
    parse_variable_value(Node2, ValueContent, Meta);
parse_node_class({<<"UAMethod">>, _, _} = Elem, Node, Meta) ->
    collect(Node, Elem, Meta, [
        {set, node_class, method},
        {required, attrib, parent_node_id, <<"ParentNodeId">>, node_id, undefined},
        {required, attrib, executable, <<"Executable">>, boolean, true},
        {required, attrib, user_executable, <<"UserExecutable">>, boolean, true}
    ]).

parse(string, Bin, _Meta) when is_binary(Bin) -> Bin;
parse(string, List, _Meta) when is_list(List) ->
    iolist_to_binary(lists:join(<<" ">>, List));
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
parse({integer, Name, Lower, Upper}, Bin, _Meta) ->
    case string:to_integer(Bin) of
        {Int, <<>>} when Int >= Lower, Int =< Upper -> Int;
        _Other                                      -> {invalid_type, Name, Bin}
    end;
parse(array_dimensions, Dimensions, Meta) ->
    [parse(integer, D, Meta) || D <- binary:split(Dimensions, <<$,>>, [global])].

parse_data_type_definition(Node, undefined, _Meta) ->
    Node#{partial_definition => undefined};
parse_data_type_definition(Node, {_Tag, _, Fields} = Elem, Meta) ->
    Def = collect(#{}, Elem, Meta, [
        {required, attrib, name, <<"Name">>, string},
        {required, attrib, is_union, <<"IsUnion">>, boolean, false},
        {required, attrib, is_option_set, <<"IsOptionSet">>, boolean, false}
    ]),
    ParsedFields = [parse_data_type_definition_field(F, Meta) || F <- Fields],
    Node#{partial_definition => Def#{fields => ParsedFields}}.

parse_data_type_definition_field({<<"Field">>, _, _} = Elem, Meta) ->
    collect(#{}, Elem, Meta, [
        {required, attrib, name, <<"Name">>, string},
        {optional, attrib, data_type, <<"DataType">>, node_id},
        {optional, attrib, value, <<"Value">>, integer},
        {optional, attrib, value_rank, <<"ValueRank">>, integer},
        {optional, attrib, is_optional, <<"IsOptional">>, boolean},
        {optional, attrib, allow_subtypes, <<"AllowSubTypes">>, boolean},
        {optional, value, description, [<<"Description">>], string}
    ]).

parse_variable_value(Node, undefined, _Meta) ->
    Node#{value => undefined};
parse_variable_value(Node, {<<"Value">>, _, XMLValue}, _Meta) ->
    Node#{xml_value => XMLValue};
parse_variable_value(Node, Unexpected, _Meta) ->
    io:format("Unexpected variable value: ~p~n", [Unexpected]),
    Node#{value => undefined}.

post_process_data_types(NodeMap, RefIdx) ->
    post_process_data_types(NodeMap, RefIdx, maps:keys(NodeMap)).

post_process_data_types(NodeMap, _RefIdx, []) -> NodeMap;
post_process_data_types(NodeMap, RefIdx, [NodeId | Rest]) ->
    NodeMap2 = case maps:find(NodeId, NodeMap) of
        {ok, #{node_class := data_type} = Node} ->
            post_process_data_type(NodeMap, RefIdx, Node);
        _ -> NodeMap
    end,
    post_process_data_types(NodeMap2, RefIdx, Rest).

post_process_data_type(NodeMap, _RefIdx, #{definition := _Def}) -> NodeMap;
post_process_data_type(NodeMap, RefIdx, #{node_id := NodeId, partial_definition := _Def} = Node) ->
    EncNodeId = lookup_encoding_type(NodeMap, RefIdx, NodeId, <<"Default Binary">>),
    {NodeMap2, SuperNode} = case refidx_supertype(RefIdx, NodeId) of
        undefined -> {NodeMap, undefined};
        SuperNodeId ->
            {ok, S} = maps:find(SuperNodeId, NodeMap),
            M = post_process_data_type(NodeMap, RefIdx, S),
            {ok, N} = maps:find(SuperNodeId, M),
            {M, N}
    end,
    % ?debug(29, NodeId, ">>>>>>>>>> ~p: ", [NodeId]),
    Node2 = finalize_type_definition(Node, EncNodeId, SuperNode, RefIdx),
    % ?debug(29, NodeId, "~p~n", [Node2]),
    NodeMap2#{NodeId => Node2}.

finalize_type_definition(#{node_id := NodeId, is_abstract := true} = Node,
                         _EncNodeId, _SuperNode, _RefIdx)
  when NodeId =:= ?NNID(24); NodeId =:= ?NNID(29);
       NodeId =:= ?NNID(22); NodeId =:= ?NNID(12756) ->
    % We reached a base type
    Node#{definition => undefined};
finalize_type_definition(#{partial_definition := undefined} = Node,
                         _EncNodeId, #{definition := SuperDef}, _RefIdx) ->
    Node#{definition => SuperDef};
finalize_type_definition(#{node_id := NodeId,
                           partial_definition := #{
                                is_option_set := IsOptionSet,
                                is_union := IsUnion} = PartDef} = Node,
                         EncNodeId, SuperType, RefIdx) ->
    BaseType = refidx_base_type(RefIdx, NodeId),
    FinalDef = finalize_type_definition(NodeId, PartDef,
                    IsOptionSet, IsUnion, BaseType, EncNodeId, SuperType),
    Node#{definition => FinalDef}.

finalize_type_definition(_NodeId, PartDef, false, false, enum,
                         _EncNodeId, SuperType) ->
    finalize_enum(PartDef, SuperType);
finalize_type_definition(_NodeId, PartDef, false, false, extension_object,
                         EncNodeId, SuperType) ->
    finalize_structure(PartDef, EncNodeId, false, SuperType);
finalize_type_definition(_NodeId, PartDef, true, false, BaseType,
                         _EncNodeId, SuperType)
  when BaseType =:= byte; BaseType =:= uint16; BaseType =:= uint32 ->
    finalize_enum(PartDef, SuperType);
finalize_type_definition(?NNID(12756), PartDef, false, false, union,
                         EncNodeId, SuperType) ->
    % Special case for the base abstract uion type
    finalize_structure(PartDef, EncNodeId, true, SuperType);
finalize_type_definition(_NodeId, PartDef, false, true, union,
                         EncNodeId, SuperType) ->
    finalize_structure(PartDef, EncNodeId, true, SuperType).

finalize_enum(#{fields := Fields}, #{definition := undefined}) ->
    #{fields => [
        #{name => N,
          value => V,
          display_name => #opcua_localized_text{text = N},
          description => #opcua_localized_text{text = maps:get(description, F, undefined)}
         } || #{name := N, value := V} = F <- Fields
    ]}.

finalize_structure(#{fields := Fields}, EncNodeId, IsUnion,
                   #{node_id := BaseTypeId, is_abstract := true,
                     definition := undefined}) ->
    finalize_structure(Fields, EncNodeId, IsUnion, BaseTypeId, structure, []);
finalize_structure(#{fields := Fields}, EncNodeId, IsUnion,
                   #{node_id := BaseTypeId,
                     definition := #{structure_type := SuperStructType,
                                     fields := SuperFields}}) ->
    finalize_structure(Fields, EncNodeId, IsUnion, BaseTypeId,
                       SuperStructType, SuperFields).

finalize_structure(Fields, EncNodeId, IsUnion, BaseTypeId,
                   SuperStructType, SuperFields) ->
    {RevFields, OptCount, SubTypeCount} = finalize_fields(Fields),
    StructType =
        case {OptCount, SubTypeCount, IsUnion, SuperStructType} of
            {0, 0, false, structure} -> structure;
            {_, 0, false, structure} -> structure_with_optional_fields;
            {0, _, false, structure} -> structure_with_subtyped_values;
            {_, 0, false, structure_with_optional_fields} -> structure_with_optional_fields;
            {0, _, false, structure_with_subtyped_values} -> structure_with_subtyped_values;
            {0, 0, true, union} -> union;
            {0, _, true, union} -> union_with_subtyped_values;
            {0, _, true, union_with_subtyped_values} -> union_with_subtyped_values;
            _ -> throw(bad_invalid_structure_type)
        end,
    % From the ref it seems it should be the parent type that may be anything:
    % https://reference.opcfoundation.org/Core/Part3/v105/docs/8.48
    #{base_data_type => BaseTypeId,
      default_encoding_id => EncNodeId,
      structure_type => StructType,
      fields => SuperFields ++ lists:reverse(RevFields)}.

finalize_fields(Fields) ->
    lists:foldl(fun(F, {Acc, O, S}) ->
        Name = maps:get(name, F),
        {IsOpt, O2} = case maps:get(is_optional, F, false) of
            true -> {true, O + 1};
            false -> {false, O}
        end,
        {AllowSubTypes, S2} = case maps:get(allow_subtypes, F, false) of
            true -> {true, S + 1};
            false -> {false, S}
        end,
        Desc = maps:get(description, F, undefined),
        ValueRank = maps:get(value_rank, F, -1),
        Dimensions = case ValueRank > 0 of
            true -> [0 || _ <- lists:seq(1, ValueRank)];
            false -> []
        end,
        DataType = maps:get(data_type, F, undefined),
        Field = #{
            name => Name,
            description => #opcua_localized_text{text = Desc},
            data_type => DataType,
            value_rank => ValueRank,
            array_dimensions => Dimensions,
            is_optional => IsOpt or AllowSubTypes,
            max_string_length => 0
        },
        {[Field | Acc], O2, S2}
    end, {[], 0, 0}, Fields).

post_process_values(Space, NodeMap) ->
    post_process_values(Space, NodeMap, maps:keys(NodeMap)).

post_process_values(_Space, NodeMap, []) ->
    NodeMap;
post_process_values(Space, NodeMap, [NodeId | Rest]) ->
    NodeMap2 = case maps:find(NodeId, NodeMap) of
        {ok, #{node_id := NodeId, node_class := Class} = Node}
          when Class =:= variable; Class =:= variable_type ->
            NewValue = post_process_value(Space, Node),
            NodeMap#{NodeId => Node#{value => NewValue}};
        _ ->
            NodeMap
    end,
    post_process_values(Space, NodeMap2, Rest).

post_process_value(_Space, #{value := Value}) ->
    Value;
post_process_value(Space, #{data_type := TypeId, value_rank := Rank, xml_value := XML}) ->
    DecoderOpts = #{space => Space},
    % We do some basic consistency checking on value rank
    case {Rank, opcua_codec_xml:decode_value(TypeId, XML, DecoderOpts)} of
        {1, Array} when is_list(Array) -> Array;
        {-1, Any} -> Any
    end.

% Add all the data type nodes, all their HasSubType, HasEncoding
% and HasTypeDefinition references, and all the referenced nodes.
finalize_typedata(Space, NodeMap, RefIdx) ->
    % First we add the root type definition node DataTypeEncodingType
    {ok, #{references := Refs} = Node} =
        maps:find(?NID_DATA_TYPE_ENCODING_TYPE, NodeMap),
    FilteredRefs =
        [R || #opcua_reference{type_id = ?NID_HAS_TYPE_DEFINITION} = R <- Refs],
    NodeRec = map_to_node(Space, Node),
    opcua_space:add_nodes(Space, [NodeRec]),
    opcua_space:add_nodes(Space, FilteredRefs),
    finalize_typedata_loop(Space, NodeMap, RefIdx, #{}, maps:keys(NodeMap)).

finalize_typedata_loop(_Space, _NodeMap, _RefIdx, Added, []) ->
    maps:keys(Added);
finalize_typedata_loop(Space, NodeMap, RefIdx, Added, [NodeId | Rest]) ->
    case maps:find(NodeId, Added) of
        {ok, _} -> finalize_typedata_loop(Space, NodeMap, RefIdx, Added, Rest);
        error ->
            Added2 = generate_datatype(Space, NodeMap, RefIdx, Added, NodeId),
            finalize_typedata_loop(Space, NodeMap, RefIdx, Added2, Rest)
    end.

generate_datatype(Space, NodeMap, RefIdx, Added, #opcua_node_id{} = NodeId) ->
    {ok, Node} = maps:find(NodeId, NodeMap),
    generate_datatype(Space, NodeMap, RefIdx, Added, Node);
generate_datatype(Space, NodeMap, RefIdx, Added,
                  #{node_class := data_type} = Node) ->
    #{node_id := NodeId, references := TypeRefs} = Node,
    TypeRec = map_to_node(Space, Node),
    case maps:is_key(NodeId, Added) of
        true -> Added;
        false ->
            % First generate the super-type if needed
            Added2 = case refidx_supertype(RefIdx, NodeId) of
                undefined -> Added;
                TypeId ->
                    generate_datatype(Space, NodeMap, RefIdx, Added, TypeId)
            end,
            % Then collect all the encoding type descriptor nodes
            TypeDescNodes = [maps:get(I, NodeMap)
                             || I <- refidx_encoding_types(RefIdx, NodeId)],
            {TypeDescRecs, TypeDescListOfRefs} = lists:foldl(fun
                (#{node_class := object, references := Refs} = M, {N, R}) ->
                    {[map_to_node(Space, M) | N], [Refs | R]};
                (_, Params) ->
                    Params
            end, {[], []}, TypeDescNodes),
            AllRefs = lists:append([TypeRefs | TypeDescListOfRefs]),
            AllRecs = [TypeRec | TypeDescRecs],
            % Only keep the HasSubType, HasEncoding and HasTypeDef references
            FilteredRefs = [R || #opcua_reference{type_id = T} = R <- AllRefs,
                                 T =:= ?NID_HAS_SUBTYPE
                                 orelse T =:= ?NID_HAS_ENCODING
                                 orelse T =:= ?NID_HAS_TYPE_DEFINITION],
            % Add all the nodes and reference to the space
            opcua_space:add_nodes(Space, AllRecs),
            opcua_space:add_references(Space, FilteredRefs),
            AddedIds = [I || #opcua_node{node_id = I} <- AllRecs],
            maps:merge(Added2, maps:from_list([{I, true} || I <- AddedIds]))
    end;
generate_datatype(_Space, _NodeMap, _RefIdx, Added, #{node_class := _}) ->
    Added.

% Add all remaining nodes and references.
% For now we add everything back, even though adding the type data
% nodes and references could be avoided...
finalize_remaining(Space, NodeMap, RefIdx) ->
    finalize_remaining(Space, NodeMap, RefIdx, maps:keys(NodeMap), #{}).

finalize_remaining(_Space, _NodeMap, _RefIdx, [], Added) ->
    maps:keys(Added);
finalize_remaining(Space, NodeMap, RefIdx, [NodeId | Rest], Added) ->
    {ok, #{references := Refs} = Node} = maps:find(NodeId, NodeMap),
    NodeRec = map_to_node(Space, Node),
    opcua_space:add_nodes(Space, [NodeRec]),
    opcua_space:add_references(Space, Refs),
    finalize_remaining(Space, NodeMap, RefIdx, Rest, Added#{NodeId => true}).

% Need the space to decode user access level.
map_to_node(Space, Node) ->
    #{
        node_id := NodeId,
        node_class := NodeClassType,
        browse_name := BrowseName,
        display_name := DisplayName
    } = Node,
    NodeClassRec = map_to_node_class(Space, NodeClassType, Node),
    #opcua_node{
        node_id = NodeId,
        node_class = NodeClassRec,
        origin = standard,
        browse_name = BrowseName,
        display_name = DisplayName
    }.

map_to_node_class(_Space, object, Node) ->
    #{
        event_notifier := EventNotifier
    } = Node,
    #opcua_object{
        event_notifier = EventNotifier
    };
map_to_node_class(Space, variable, Node)
  when Space =/= undefined ->
    #{
        value := Value,
        data_type := DataType,
        value_rank := ValueRank,
        array_dimensions := ArrayDimensions,
        access_level := AccessLevel,
        user_access_level := UserAccessLevel,
        minimum_sampling_interval := MinimumSamplingInterval,
        historizing := Historizing,
        access_level_ex := AccessLevelEx
    } = Node,
    #opcua_variable{
        value = Value,
        data_type = DataType,
        value_rank = ValueRank,
        array_dimensions = ArrayDimensions,
        access_level = unpack_option_set(Space, ?NNID(15031), AccessLevel),
        user_access_level = unpack_option_set(Space, ?NNID(15031), UserAccessLevel),
        minimum_sampling_interval = MinimumSamplingInterval,
        historizing = Historizing,
        access_level_ex =  unpack_option_set(Space, ?NNID(15406), AccessLevelEx)
    };
map_to_node_class(_Space, method, Node) ->
    #{
        executable := Executable,
        user_executable := UserExecutable
    } = Node,
    #opcua_method{
        executable = Executable,
        user_executable = UserExecutable
    };
map_to_node_class(_Space, object_type, Node) ->
    #{
        is_abstract := IsAbstract
    } = Node,
    #opcua_object_type{
        is_abstract = IsAbstract
    };
map_to_node_class(_Space, variable_type, Node) ->
    #{
        value := Value,
        data_type := DataType,
        value_rank := ValueRank,
        array_dimensions := ArrayDimensions,
        is_abstract := IsAbstract
    } = Node,
    #opcua_variable_type{
        value = Value,
        data_type = DataType,
        value_rank = ValueRank,
        array_dimensions = ArrayDimensions,
        is_abstract = IsAbstract
    };
map_to_node_class(_Space, data_type, Node) ->
    #{
        is_abstract := IsAbstract,
        definition := Definition
    } = Node,
    #opcua_data_type{
        is_abstract = IsAbstract,
        data_type_definition = Definition
    };
map_to_node_class(_Space, reference_type, Node) ->
    #{
        is_abstract := IsAbstract,
        symmetric := Symmetric,
        inverse_name := InverseName
    } = Node,
    #opcua_reference_type{
        is_abstract = IsAbstract,
        symmetric = Symmetric,
        inverse_name = InverseName
    }.

unpack_option_set(Space, TypeId, Value) ->
    case opcua_space:schema(Space, TypeId) of
        undefined -> throw(bad_decoding_error);
        Schema -> opcua_codec:unpack_option_set(Schema, Value)
    end.

lookup_encoding_type(NodeSet, RefIdx, NodeId, EncodingName) ->
    EncNodeIds = refidx_encoding_types(RefIdx, NodeId),
    EncNodes = [maps:get(I, NodeSet) || I <- EncNodeIds],
    case [I || #{node_id := I, browse_name := N} <- EncNodes,
               N =:= EncodingName] of
        [] -> undefined;
        [EncNodeId | _] -> EncNodeId
    end.

build_ref_index(Nodes) ->
    build_ref_index(Nodes, #{}, #{}).

build_ref_index([], SourceIndex, TargetIndex) ->
    {SourceIndex, TargetIndex};
build_ref_index([#{references := Refs} | Rest], SourceIndex, TargetIndex) ->
    {SourceIndex2, TargetIndex2} = lists:foldl(fun(Ref, {SI, TI}) ->
        #opcua_reference{source_id = S, target_id = T} = Ref,
        SL = maps:get(S, SI, []),
        TL = maps:get(T, TI, []),
        {SI#{S => [Ref | SL]}, TI#{T => [Ref | TL]}}
    end, {SourceIndex, TargetIndex}, Refs),
    build_ref_index(Rest, SourceIndex2, TargetIndex2).

refidx_supertype(Idx, NodeId) ->
    case refidx_lookup_source(Idx, NodeId, ?NID_HAS_SUBTYPE) of
        [] -> undefined;
        [Result] -> Result
    end.

refidx_encoding_types(Idx, NodeId) ->
    refidx_lookup_target(Idx, NodeId, ?NID_HAS_ENCODING).

refidx_base_type(_Idx, undefined) -> undefined;
refidx_base_type(_Idx, ?NNID(29)) -> enum;
refidx_base_type(_Idx, ?NNID(12756)) -> union;
refidx_base_type(_Idx, ?NNID(Id)) when ?IS_BUILTIN_TYPE_ID(Id) ->
    opcua_codec:builtin_type_name(Id);
refidx_base_type(Idx, NodeId) ->
    refidx_base_type(Idx, refidx_supertype(Idx, NodeId)).

refidx_lookup_source({_, TargetIndex}, NodeId, RefType) ->
    case maps:find(NodeId, TargetIndex) of
        error -> [];
        {ok, Refs} ->
            [I || #opcua_reference{source_id = I, type_id = T} <- Refs,
                  T =:= RefType]
    end.

refidx_lookup_target({SourceIndex, _}, NodeId, RefType) ->
    case maps:find(NodeId, SourceIndex) of
        error -> [];
        {ok, Refs} ->
            [I || #opcua_reference{target_id = I, type_id = T} <- Refs,
                  T =:= RefType]
    end.

collect(Map, _Elem, _Meta, []) -> Map;
collect(Map, Elem, Meta, [{set, Key, Value} | Rest]) ->
    collect(Map#{Key => Value}, Elem, Meta, Rest);
collect(Map, {_, Attrs, _} = Elem, Meta, [{required, attrib, Key, Name, Type, Default} | Rest]) ->
    Map2 = case find_attr(Name, Attrs, Type, Meta) of
        error -> Map#{Key => Default};
        {ok, Value} -> Map#{Key => Value}
    end,
    collect(Map2, Elem, Meta, Rest);
collect(Map, {_, Attrs, _} = Elem, Meta, [{required, attrib, Key, Name, Type} | Rest]) ->
    Map2 = Map#{Key => get_attr(Name, Attrs, Type, Meta)},
    collect(Map2, Elem, Meta, Rest);
collect(Map, {_, Attrs, _} = Elem, Meta, [{optional, attrib, Key, Name, Type} | Rest]) ->
    Map2 = case find_attr(Name, Attrs, Type, Meta) of
        error -> Map;
        {ok, Value} -> Map#{Key => Value}
    end,
    collect(Map2, Elem, Meta, Rest);
collect(Map, {_, _, Content} = Elem, Meta, [{required, value, Key, Path, Type, Default} | Rest]) ->
    Map2 = case find_value(Path, Content, Type, Meta) of
        error -> Map#{Key => Default};
        {ok, Value} -> Map#{Key => Value}
    end,
    collect(Map2, Elem, Meta, Rest);
collect(Map, {_, _, Content} = Elem, Meta, [{required, value, Key, Path, Type} | Rest]) ->
    Map2 = Map#{Key => get_value(Path, Content, Type, Meta)},
    collect(Map2, Elem, Meta, Rest);
collect(Map, {_, _, Content} = Elem, Meta, [{optional, value, Key, Path, Type} | Rest]) ->
    Map2 = case find_value(Path, Content, Type, Meta) of
        error -> Map;
        {ok, Value} -> Map#{Key => Value}
    end,
    collect(Map2, Elem, Meta, Rest).

get_attr(Name, Attrs, Type, Meta) ->
    case find_attr(Name, Attrs, Type, Meta) of
        error -> error({attr_not_found, Name});
        {ok, Value} -> Value
    end.

get_attr(Name, Attrs, Type, Meta, Default) ->
    case find_attr(Name, Attrs, Type, Meta) of
        error -> Default;
        {ok, Value} -> Value
    end.

find_attr(Name, Attrs, Type, Meta) ->
    case maps:find(Name, Attrs) of
        {ok, Value} -> {ok, parse(Type, Value, Meta)};
        error -> error
    end.

get_value(Keys, Content) ->
    case find_in(Keys, Content) of
        error -> error({value_not_found, Keys, Content});
        {ok, {_Tag, _Attrs, Value}} -> Value
    end.

get_value(Keys, Content, Default) ->
    case find_in(Keys, Content) of
        error -> Default;
        {ok, {_Tag, _Attrs, Value}} -> Value
    end.

get_value(Keys, Content, Type, Meta) ->
    case find_value(Keys, Content, Type, Meta) of
        error -> error({value_not_found, Keys, Content});
        {ok, Value} -> Value
    end.

find_value(Keys, Content, Type, Meta) ->
    case find_in(Keys, Content) of
        error -> error;
        {ok, {_Tag, _Attrs, Value}} -> {ok, parse(Type, Value, Meta)}
    end.

get_in(Keys, Items, Default) ->
    case find_in(Keys, Items) of
        error -> Default;
        {ok, Value} -> Value
    end.

find_in([K | Keys], [Tuple | _List]) when element(1, Tuple) =:= K ->
    find_in(Keys, Tuple);
find_in(Keys, [_Tuple | List]) ->
    find_in(Keys, List);
find_in([], Item) ->
    {ok, Item};
find_in(_Keys, []) ->
    error.

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
