%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA Node Set parser.
%%%
%%% Parse OPCUA Node Set files and generate internal database format.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_nodeset_parser).

%% TODO %%
%%
%% - When opcua_space has been updated to handle the generation of all the
%%   metadata dynamically (subtypes, type definition...), use a space and
%%   store it.
%% - Properly parse XML values from the NodeSet XML file.
%%   e.g. Variables like EnumValues (i=15633) should have a value.
%% - The namespace handling is probably wrong, check how loading multiple
%%   nodesets with a different list of namespaces would work.


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([parse/0]).

%% Limited opcua_space callback functions for the XML decoder
-export([node/2]).


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


%%% LIMITED opcua_space CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

node(NodeRecMap, TypeId) ->
    maps:get(TypeId, NodeRecMap, undefined).


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
    Meta = #{namespaces => Namespaces, partial_types => [], unparsed_values => []},
    BaseInputPath = filename:join([InputDir, "Schema", "Opc.Ua.NodeSet2.Services.xml"]),
    InputPatternPath = filename:join([InputDir, "*", "*.NodeSet2.xml"]),
    InputPathList = [BaseInputPath | filelib:wildcard(InputPatternPath)],
    parse(OutputDir, InputPathList, Meta, []).

parse(DestDir, [], Meta, Nodes) ->
    #{partial_types := PartialTypes, unparsed_values := UnparsedValues} = Meta,
    NodeMap = maps:from_list([{NodeId, Node} || #{node_id := NodeId} = Node <- Nodes]),
    RefIdx = build_ref_index(Nodes),
    NodeMap2 = post_process_data_types(NodeMap, RefIdx, PartialTypes),
    {NodeRecMap, FinalRefs} = finalize_nodes_and_refs(NodeMap2),
    FinalNodeRecMap = post_process_values(NodeRecMap, Meta, UnparsedValues),

    Space = opcua_space_backend:new(),
    lists:foreach(fun({Id, Uri}) ->
        opcua_space:add_namespace(Space, Id, Uri)
    end, maps:to_list(maps:get(namespaces, Meta, #{}))),
    opcua_space:add_nodes(Space, maps:values(FinalNodeRecMap)),
    opcua_space:add_references(Space, FinalRefs),

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
    {ClassNode, Meta2} = parse_node_class(Elem, Meta, BaseNode),
    #{node_id := BaseNodeId} = BaseNode,
    Refs = parse_references(BaseNodeId, get_value([<<"References">>], Content), Meta),
    Node = ClassNode#{references => Refs},
    {Meta2, [Node | Nodes]};
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

parse_node_class({<<"UAObject">>, _, _} = Elem, Meta, Node) ->
    {collect(Node, Elem, Meta, [
        {set, node_class, object},
        {required, attrib, parent_node_id, <<"ParentNodeId">>, node_id, undefined},
        {required, attrib, event_notifier, <<"EventNotifier">>, string, undefined}
    ]), Meta};
parse_node_class({<<"UADataType">>, _, Content} = Elem, Meta, Node) ->
    Node2 = collect(Node, Elem, Meta, [
        {set, node_class, data_type},
        {required, attrib, is_abstract, <<"IsAbstract">>, boolean, false},
        {optional, attrib, purpose, <<"Purpose">>, string}
    ]),
    DefContent = get_in([<<"Definition">>], Content, undefined),
    parse_data_type_definition(Node2, DefContent, Meta);
parse_node_class({<<"UAReferenceType">>, _, _} = Elem, Meta, Node) ->
    {collect(Node, Elem, Meta, [
        {set, node_class, reference_type},
        {required, attrib, is_abstract, <<"IsAbstract">>, boolean, false},
        {required, attrib, symmetric, <<"Symmetric">>, boolean, false},
        {required, value, inverse_name, [<<"InverseName">>], string, undefined}
    ]), Meta};
parse_node_class({<<"UAObjectType">>, _, _} = Elem, Meta, Node) ->
    {collect(Node, Elem, Meta, [
        {set, node_class, object_type},
        {required, attrib, is_abstract, <<"IsAbstract">>, boolean, false}
    ]), Meta};
parse_node_class({<<"UAVariableType">>, _, Content} = Elem, Meta, Node) ->
    Node2 = collect(Node, Elem, Meta, [
        {set, node_class, variable_type},
        {required, attrib, is_abstract, <<"IsAbstract">>, boolean, false},
        {required, attrib, data_type, <<"DataType">>, node_id, undefined},
        {required, attrib, value_rank, <<"ValueRank">>, integer, -1},
        {required, attrib, array_dimensions, <<"ArrayDimensions">>, array_dimensions, []}
    ]),
    ValueContent = get_in([<<"Value">>], Content, undefined),
    parse_variable_value(Node2, ValueContent, Meta);
parse_node_class({<<"UAVariable">>, _, Content} = Elem, Meta, Node) ->
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
parse_node_class({<<"UAMethod">>, _, _} = Elem, Meta, Node) ->
    {collect(Node, Elem, Meta, [
        {set, node_class, method},
        {required, attrib, parent_node_id, <<"ParentNodeId">>, node_id, undefined},
        {required, attrib, executable, <<"Executable">>, boolean, true},
        {required, attrib, user_executable, <<"UserExecutable">>, boolean, true}
    ]), Meta}.

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

parse_data_type_definition(#{node_id := ?NNID(24)} = Node, undefined, Meta) ->
    % This is the base type
    {Node#{definition => undefined}, Meta};
parse_data_type_definition(Node, undefined, Meta) ->
    {Node#{partial_definition => undefined}, push_partial_type(Node, Meta)};
parse_data_type_definition(Node, {_Tag, _, Fields} = Elem, Meta) ->
    Def = collect(#{}, Elem, Meta, [
        {required, attrib, name, <<"Name">>, string},
        {required, attrib, is_union, <<"IsUnion">>, boolean, false},
        {required, attrib, is_option_set, <<"IsOptionSet">>, boolean, false}
    ]),
    ParsedFields = [parse_data_type_definition_field(F, Meta) || F <- Fields],
    Node2 = Node#{partial_definition => Def#{fields => ParsedFields}},
    {Node2, push_partial_type(Node, Meta)}.

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

parse_variable_value(Node, undefined, Meta) ->
    {Node#{value => undefined}, Meta};
parse_variable_value(Node, {<<"Value">>, _, [XMLValue]}, Meta) ->
    {Node#{value => XMLValue}, push_unparsed_value(Node, Meta)};
parse_variable_value(Node, Unexpected, Meta) ->
    io:format("Unexpected variable value: ~p~n", [Unexpected]),
    {Node, Meta}.

push_partial_type(#{node_id := NodeId}, #{partial_types := Acc} = Meta) ->
    Meta#{partial_types => [NodeId | Acc]}.

push_unparsed_value(#{node_id := NodeId}, #{unparsed_values := Acc} = Meta) ->
    Meta#{unparsed_values => [NodeId | Acc]}.

post_process_data_types(NodeMap, _RefIdx, []) -> NodeMap;
post_process_data_types(NodeMap, RefIdx, [NodeId | Rest]) ->
    NodeMap2 = post_process_data_type(NodeMap, RefIdx, NodeId),
    post_process_data_types(NodeMap2, RefIdx, Rest).

post_process_data_type(NodeMap, RefIdx, NodeId) ->
    case maps:find(NodeId, NodeMap) of
        {ok, #{definition := _Def}} -> NodeMap;
        {ok, #{partial_definition := _PartDef} = Node} ->
            EncNodeId = lookup_encoding_type(NodeMap, RefIdx, NodeId, <<"Default Binary">>),
            case refidx_supertype(RefIdx, NodeId) of
                undefined ->
                    Node2 = finalize_type_definition(Node, EncNodeId, undefined, RefIdx),
                    NodeMap#{NodeId => Node2};
                SuperNodeId ->
                    NodeMap2 = post_process_data_type(NodeMap, RefIdx, SuperNodeId),
                    {ok, SuperNode} = maps:find(SuperNodeId, NodeMap2),
                    Node2 = finalize_type_definition(Node, EncNodeId, SuperNode, RefIdx),
                    NodeMap2#{NodeId => Node2}
            end
    end.

finalize_type_definition(#{node_id := ?NNID(24), partial_definition := undefined} = Node,
                         _EncNodeId, _SuperType, _RefIdx) ->
    % We reached the base type
    Node2 = maps:remove(partial_definition, Node),
    Node2#{definition => undefined};
finalize_type_definition(#{node_id := NodeId, partial_definition := undefined} = Node,
                         _EncNodeId, _SuperType, RefIdx) ->
    Node2 = maps:remove(partial_definition, Node),
    case refidx_builtin_id(RefIdx, NodeId) of
        undefined -> Node2#{definition => undefined};
        BuiltinNodeId ->
            Def = #opcua_builtin{node_id = NodeId, builtin_node_id = BuiltinNodeId},
            Node2#{definition => Def}
    end;
finalize_type_definition(#{node_id := NodeId,
                           partial_definition := #{
                                name := Name,
                                is_option_set := IsOptionSet,
                                is_union := IsUnion} = PartDef} = Node,
                         EncNodeId, SuperType, RefIdx) ->
    BaseType = refidx_base_type(RefIdx, NodeId),
    FinalDef = finalize_type_definition(NodeId, Name, PartDef,
                    IsOptionSet, IsUnion, BaseType, EncNodeId, SuperType),
    Node2 = maps:remove(partial_definition, Node),
    Node2#{definition => FinalDef}.

finalize_type_definition(NodeId, _Name, PartDef, false, false, enum,
                         _EncNodeId, SuperType) ->
    finalize_enum(NodeId, PartDef, SuperType);
finalize_type_definition(NodeId, Name, PartDef, false, false, struct,
                         EncNodeId, SuperType) ->
    finalize_struct(NodeId, Name, PartDef, EncNodeId, SuperType);
finalize_type_definition(NodeId, _Name, PartDef, true, false, BaseType,
                         _EncNodeId, SuperType)
  when BaseType =:= byte; BaseType =:= uint16; BaseType =:= uint32 ->
    finalize_option_set(NodeId, PartDef, BaseType, SuperType);
finalize_type_definition(?NNID(12756) = NodeId, Name, PartDef, false, false, union,
                         EncNodeId, SuperType) ->
    % Special case for the base abstract uion type
    finalize_union(NodeId, Name, PartDef, EncNodeId, SuperType);
finalize_type_definition(NodeId, Name, PartDef, false, true, union,
                         EncNodeId, SuperType) ->
    finalize_union(NodeId, Name, PartDef, EncNodeId, SuperType).

finalize_enum(NodeId, #{fields := Fields}, _SuperNode) ->
    #opcua_enum{
        node_id = NodeId,
        fields = [
            #opcua_field{
                name = N,
                tag = opcua_util:convert_name(N),
                display_name = undefined,
                description = maps:get(description, F, undefined),
                value = V
            } || #{name := N, value := V} = F <- Fields
        ]
    }.

finalize_option_set(NodeId, #{fields := Fields}, BaseType, _SuperNode) ->
    #opcua_option_set{
        node_id = NodeId,
        mask_type = finalize_option_set_mask_type(BaseType),
        fields = [
            #opcua_field{
                name = N,
                tag = opcua_util:convert_name(N),
                display_name = undefined,
                description = maps:get(description, F, undefined),
                value = V
            } || #{name := N, value := V} = F <- Fields
        ]
    }.

finalize_option_set_mask_type(BaseType) ->
    case opcua_codec:builtin_type_name(opcua_node:id(BaseType)) of
        byte -> byte;
        uint16 -> uint16;
        uint32 -> uint32;
        uint64 -> uint64;
        InvalidType ->
            throw({invalid_option_set_mask_type, InvalidType})
    end.

finalize_struct(NodeId, Name, #{fields := Fields}, EncNodeId,
                #{node_id := BaseTypeId, is_abstract := true,
                  partial_definition := undefined}) ->
    finalize_struct(NodeId, Name, Fields, EncNodeId, BaseTypeId, [], false, false);
finalize_struct(NodeId, Name, #{fields := Fields}, EncNodeId,
                #{node_id := BaseTypeId, definition := SuperDef}) ->
    {SuperHasOpts, SuperAllowSubTypes, SuperFields}  = case SuperDef of
        #opcua_structure{with_options = O, allow_subtypes = S, fields = F} ->
            {O, S, F};
        #opcua_builtin{node_id = ?NNID(22), builtin_node_id = ?NNID(22)} ->
            {false, false, []}
    end,
    finalize_struct(NodeId, Name, Fields, EncNodeId, BaseTypeId,
                    SuperFields, SuperHasOpts, SuperAllowSubTypes).

finalize_struct(NodeId, Name, Fields, EncNodeId, BaseTypeId,
                SuperFields, SuperHasOpts, SuperAllowSubTypes) ->
    {RevFields, OptCount, SubTypeCount} = finalize_fields(Fields),
    {HasOpts, AllowSubTypes} =
        case {OptCount, SubTypeCount, SuperHasOpts, SuperAllowSubTypes} of
            {0, 0, false, false} -> {false, false};
            {_, 0, _, false} -> {true, false};
            {0, _, false, _} -> {false, true}
        end,
    % From the ref it seems it should be the parent type that may be anything:
    % https://reference.opcfoundation.org/Core/Part3/v105/docs/8.48
    #opcua_structure{
        node_id = NodeId,
        name = Name,
        base_type_id = BaseTypeId,
        default_encoding_id = EncNodeId,
        with_options = HasOpts,
        allow_subtypes = AllowSubTypes,
        fields = SuperFields ++ lists:reverse(RevFields)}.

finalize_union(NodeId, Name, #{fields := Fields}, EncNodeId,
              #{node_id := BaseTypeId, is_abstract := true,
                partial_definition := undefined}) ->
    finalize_union(NodeId, Name, Fields, EncNodeId, BaseTypeId, [], false);
finalize_union(NodeId, Name, #{fields := Fields}, EncNodeId,
              #{node_id := BaseTypeId, definition := SuperDef}) ->
    {SuperAllowSubTypes, SuperFields} = case SuperDef of
        #opcua_union{allow_subtypes = S, fields = F} ->
            {S, F};
        #opcua_builtin{node_id = ?NNID(22), builtin_node_id = ?NNID(22)} ->
            % The base type of unions is struct
            {false, []}
    end,
    finalize_union(NodeId, Name, Fields, EncNodeId, BaseTypeId, SuperFields,
                   SuperAllowSubTypes).

finalize_union(NodeId, Name, Fields, EncNodeId, BaseTypeId,
               SuperFields, SuperAllowSubTypes) ->
    {RevFields, OptCount, SubTypeCount} = finalize_fields(Fields),
    AllowSubTypes =
        case {OptCount, SubTypeCount, SuperAllowSubTypes} of
            {0, 0, false} -> false;
            {0, _, _} -> true
        end,
    % From the ref it seems it should be the parent type that may be anything:
    % https://reference.opcfoundation.org/Core/Part3/v105/docs/8.48
    #opcua_union{
        node_id = NodeId,
        name = Name,
        base_type_id = BaseTypeId,
        default_encoding_id = EncNodeId,
        allow_subtypes = AllowSubTypes,
        fields = SuperFields ++ lists:reverse(RevFields)}.

finalize_fields(Fields) ->
    lists:foldl(fun(F, {Acc, O, S}) ->
        Name = maps:get(name, F),
        {Value, IsOpt, O2} = case maps:get(is_optional, F, false) of
            true -> {O + 1, true, O + 1};
            false -> {undefined, false, O}
        end,
        {AllowSubTypes, S2} = case maps:get(allow_subtypes, F, false) of
            true -> {true, S + 1};
            false -> {false, S}
        end,
        Desc = maps:get(description, F, undefined),
        Rank = maps:get(value_rank, F, -1),
        DataType = maps:get(data_type, F, undefined),
        Field = #opcua_field{
            name = Name,
            tag = opcua_util:convert_name(Name),
            display_name = undefined,
            description = Desc,
            is_optional = IsOpt or AllowSubTypes,
            node_id = DataType,
            value_rank = Rank,
            value = Value
        },
        {[Field | Acc], O2, S2}
    end, {[], 0, 0}, Fields).

post_process_values(NodeRecMap, _Meta, []) -> NodeRecMap;
post_process_values(NodeRecMap, Meta, [NodeId | Rest]) ->
    #{NodeId := Node} = NodeRecMap,
    #opcua_node{
        node_class = #opcua_variable{
            data_type = TypeId,
            value_rank = ValueRank,
            value = Value
        } = NodeClass
    } = Node,
    {ok, #opcua_node{node_class = #opcua_data_type{data_type_definition = TypeDef}}} = maps:find(TypeId, NodeRecMap),
    DecoderOpts = #{space => {?MODULE, NodeRecMap}},
    Value2 = case TypeDef of
        #opcua_builtin{builtin_node_id = BuiltinNodeId} ->
            TypeName = opcua_codec:builtin_type_name(BuiltinNodeId),
            parse_value(DecoderOpts, TypeName, ValueRank, Value);
        #opcua_structure{} ->
            parse_struct(DecoderOpts, TypeId, ValueRank, Value);
        #opcua_enum{} ->
            parse_enum(DecoderOpts, TypeId, ValueRank, Value);
        #opcua_union{} ->
            throw(bad_not_implemented);
        #opcua_option_set{} ->
            throw(bad_not_implemented)
    end,
    Node2 = Node#opcua_node{node_class = NodeClass#opcua_variable{value = Value2}},
    NodeRecMap2 = NodeRecMap#{NodeId => Node2},
    post_process_values(NodeRecMap2, Meta, Rest).

parse_value(DecoderOpts, Type, -1, Data) ->
    Name = opcua_codec_xml_builtin:tag_name(-1, Type),
    case Data of
        {Name, _, Data2} -> decode_value(Type, Data2, DecoderOpts);
        _ -> throw(bad_decoding_error)
    end;
parse_value(DecoderOpts, Type, 1, Data) ->
    Name = opcua_codec_xml_builtin:tag_name(1, Type),
    case Data of
        {Name, _, Data2} ->
            [parse_value(DecoderOpts, Type, -1, I) || I <- Data2];
        _ ->
            throw(bad_decoding_error)
    end.

parse_enum(DecoderOpts, TypeId, -1, {<<"Int32">>, _, Data}) ->
    decode_value(TypeId, Data, DecoderOpts);
parse_enum(DecoderOpts, TypeId, 1, {<<"ListOfInt32">>, _, Data}) ->
    [parse_enum(DecoderOpts, TypeId, -1, I) || I <- Data].

parse_struct(DecoderOpts, TypeId, -1, {<<"ExtensionObject">>, _,
             [{<<"TypeId">>, _, XMLTypeDescId}, {<<"Body">>, _, Body}]}) ->
    % We already know the data is encoded as XML, and the original type,
    % so there is not much use for the type descriptor.
    %TODO: We could validate it is correct though, for now we assume it is correct
    _TypeDescId = decode_value(node_id, XMLTypeDescId, DecoderOpts),
    decode_value(TypeId, Body, DecoderOpts);
parse_struct(DecoderOpts, TypeId, 1,
            {<<"ListOfExtensionObject">>, _, Items}) ->
    [parse_struct(DecoderOpts, TypeId, -1, I) || I <- Items].

decode_value(Type, Data, DecoderOpts) ->
    opcua_codec_xml:decode(Type, Data, DecoderOpts).

finalize_nodes_and_refs(NodeMap) ->
    lists:foldl(fun(Node, {NodeRecMap, RefAcc}) ->
        #{node_id := NodeId, references := Refs} = Node,
        NodeRec = map_to_node(NodeMap, Node),
        {NodeRecMap#{NodeId => NodeRec}, Refs ++ RefAcc}
    end, {#{}, []}, maps:values(NodeMap)).

map_to_node(NodeMap, Node) ->
    #{
        node_id := NodeId,
        node_class := NodeClassType,
        browse_name := BrowseName,
        display_name := DisplayName
    } = Node,
    NodeClassRec = map_to_node_class(NodeMap, NodeClassType, Node),
    #opcua_node{
        node_id = NodeId,
        node_class = NodeClassRec,
        origin = standard,
        browse_name = BrowseName,
        display_name = DisplayName
    }.

map_to_node_class(_NodeMap, object, Node) ->
    #{
        event_notifier := EventNotifier
    } = Node,
    #opcua_object{
        event_notifier = EventNotifier
    };
map_to_node_class(NodeMap, variable, Node) ->
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
        access_level = unpack_option_set(NodeMap, ?NNID(15031), AccessLevel),
        user_access_level = unpack_option_set(NodeMap, ?NNID(15031), UserAccessLevel),
        minimum_sampling_interval = MinimumSamplingInterval,
        historizing = Historizing,
        access_level_ex =  unpack_option_set(NodeMap, ?NNID(15406), AccessLevelEx)
    };
map_to_node_class(_NodeMap, method, Node) ->
    #{
        executable := Executable,
        user_executable := UserExecutable
    } = Node,
    #opcua_method{
        executable = Executable,
        user_executable = UserExecutable
    };
map_to_node_class(_NodeMap, object_type, Node) ->
    #{
        is_abstract := IsAbstract
    } = Node,
    #opcua_object_type{
        is_abstract = IsAbstract
    };
map_to_node_class(_NodeMap, variable_type, Node) ->
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
map_to_node_class(_NodeMap, data_type, Node) ->
    #{
        is_abstract := IsAbstract,
        definition := Definition
    } = Node,
    #opcua_data_type{
        is_abstract = IsAbstract,
        data_type_definition = Definition
    };
map_to_node_class(_NodeMap, reference_type, Node) ->
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

unpack_option_set(NodeMap, TypeId, Value) ->
    {ok, #{definition := #opcua_option_set{} = Schema}} = maps:find(TypeId, NodeMap),
    opcua_codec:unpack_option_set(Schema, Value).

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

refidx_builtin_id(_Idx, #opcua_node_id{type = numeric, value = V} = NodeId)
  when ?IS_BUILTIN_TYPE_ID(V) ->
    NodeId;
refidx_builtin_id(Idx, NodeId) ->
    case refidx_supertype(Idx, NodeId) of
        undefined -> undefined;
        SuperNodeId -> refidx_builtin_id(Idx, SuperNodeId)
    end.

refidx_base_type(_Idx, undefined) -> undefined;
refidx_base_type(_Idx, ?NNID(3)) -> byte;
refidx_base_type(_Idx, ?NNID(5)) -> uint16;
refidx_base_type(_Idx, ?NNID(7)) -> uint32;
refidx_base_type(_Idx, ?NNID(22)) -> struct;
refidx_base_type(_Idx, ?NNID(29)) -> enum;
refidx_base_type(_Idx, ?NNID(12756)) -> union;
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
