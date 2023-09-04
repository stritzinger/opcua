-module(opcua_template).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API
-export([prepare/3]).
-export([ensure/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type template_alias() :: atom().
-type template_node_spec() :: opcua:node_spec()
                           | {server, non_neg_integer()}
                           | {server, binary()}
                           | {binary(), non_neg_integer()}
                           | {binary(), binary()}.

-type node_template() :: #{
    % Alias that can be used in later templates' references.
    alias => template_alias(),
    % If not specified, the namespace is assumed to be 0.
    % If set to 'server', the custom server namespace will be used.
    namespace => non_neg_integer() | atom() | binary(),
    % If not specified, a new identifier will be generated if the node is created.
    node_id => template_node_spec(),
    % if not specified, it is assumed to be object.
    node_class => opcua:node_class(),

    % List of required referencesa a list of tuple with the reference type spec
    % and either a node spec to an existing node, or a node template that may
    % need to be created.
    references => [
        {forward | inverse, template_node_spec(),
         template_alias() | template_node_spec() | node_template()}
    ],

    % The following are used only to create the node if it doesn't exit.
    browse_name := binary(),
    display_name => binary(),
    description => binary(),
    % Only for variables and variable types
    value => term(),
    data_type => opcua:builtin_type() | opcua:node_spec(),
    value_rank => opcua:value_rank(),
    array_dimensions => [non_neg_integer()],
    % Only for data types
    data_type_definition => term()
}.
-type template_result() :: {opcua:node_id(), [template_result()]}.
-type node_id_fun() :: fun(() -> opcua:node_id()).
-type options() :: #{
    node_id_fun := node_id_fun(),
    namespace_aliases => map(),
    node_aliases => map()
}.
-export_type([node_template/0]).
-export_type([template_result/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prepare(term(), [node_template()], options())
    -> {[template_result()], [opcua:node()], [opcua:refernce()]}.
prepare(Space, Templates, Opts) when is_list(Templates) ->
    NormOpts = normalize_options(Opts),
    prepare_templates(Space, NormOpts, Templates).

% @doc Ensure a template exists in the given space.
% All described nodes and references are checked and created if they do not exists.
% The validation is done template-per-template and depth-first.
% If the template do not use an explicit node identifier and do not specify
% any references to existing nodes by its identifier, a new set of
% nodes and references will be created every time.
% A template can specify an alias name that can be used in later templates to
% refer to it as if it was a node identifier.
% Returns the matching hierarchy of node records.
-spec ensure(term(), node_template() | [node_template()], options())
    -> template_result() | [template_result()].
ensure(Space, Templates, Opts) when is_list(Templates) ->
    {Result, Nodes, References} = prepare(Space, Templates, Opts),
    opcua_space:add_nodes(Space, Nodes),
    opcua_space:add_references(Space, References),
    Result;
ensure(Space, Template, Opts) when is_map(Template) ->
    [Result] = ensure(Space, [Template], Opts),
    Result.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

normalize_options(#{node_id_fun := Fun} = Opts) when is_function(Fun) ->
    maps:merge(#{
        namespace_aliases => #{},
        node_aliases => #{}
    }, Opts).

resolve_id(_Space, #{namespace_aliases := Aliases}, {Alias, Id})
  when is_atom(Alias), is_integer(Id); is_binary(Id) ->
    case maps:find(Alias, Aliases) of
        error -> throw({unknown_namespace_alias, Alias});
        {ok, NS} -> opcua_node:id({NS, Id})
    end;
resolve_id(_Space, _Opts, {NS, Id})
  when is_integer(NS), is_integer(Id);
       is_integer(NS), is_binary(Id) ->
    opcua_node:id({NS, Id});
resolve_id(Space, _Opts, {Uri, Id})
  when is_binary(Uri), is_integer(Id);
       is_binary(Uri), is_binary(Id) ->
    case opcua_space:namespace_id(Space, Uri) of
        undefined -> throw({unknown_namespace, Uri});
        NS -> opcua_node:id({NS, Id})
    end;
resolve_id(_Space, #{node_aliases := Aliases}, NodeSpec)
  when is_atom(NodeSpec); is_integer(NodeSpec); is_binary(NodeSpec) ->
    case maps:find(NodeSpec, Aliases) of
        error -> opcua_node:id(NodeSpec);
        {ok, NodeId} -> NodeId
    end;
resolve_id(_Space, _Opts, #opcua_node_id{} = NodeId) ->
    NodeId.

resolve_namespace(_Space, _Opts, undefined, undefined) ->
    {0, undefined};
resolve_namespace(Space, Opts, undefined, NodeSpec) ->
    NodeId = #opcua_node_id{ns = NS} = resolve_id(Space, Opts, NodeSpec),
    {NS, NodeId};
resolve_namespace(_Space, #{namespace_aliases := Aliases}, Alias, undefined)
  when is_atom(Alias) ->
    case maps:find(Alias, Aliases) of
        error -> throw({unknown_namespace_alias, Alias});
        {ok, NS} -> {NS, undefined}
    end;
resolve_namespace(Space, #{namespace_aliases := Aliases} = Opts, Alias, NodeSpec)
  when is_atom(Alias) ->
    case maps:find(Alias, Aliases) of
        error -> throw({unknown_namespace_alias, Alias});
        {ok, NS} ->
            case resolve_id(Space, Opts, NodeSpec) of
                #opcua_node_id{ns = NS} = ResolvedId ->
                    {NS, ResolvedId};
                #opcua_node_id{ns = OtherNS} ->
                    throw({incompatible_namespaces, NS, OtherNS})
            end
    end;
resolve_namespace(Space, _Opts, Uri, undefined)
  when is_binary(Uri) ->
    case opcua_space:namespace_id(Space, Uri) of
        undefined -> throw({unknown_namespace, Uri});
        NS -> {NS, undefined}
    end;
resolve_namespace(Space, Opts, Uri, NodeSpec)
  when is_binary(Uri) ->
    case opcua_space:namespace_id(Space, Uri) of
        undefined -> throw({unknown_namespace, Uri});
        NS ->
            case resolve_id(Space, Opts, NodeSpec) of
                #opcua_node_id{ns = NS} = ResolvedId ->
                    {NS, ResolvedId};
                #opcua_node_id{ns = OtherNS} ->
                    throw({incompatible_namespaces, NS, OtherNS})
            end
    end;
resolve_namespace(_Space, _Opts, NS, undefined)
  when is_integer(NS) ->
    {NS, undefined};
resolve_namespace(Space, Opts, NS, NodeSpec)
  when is_integer(NS) ->
    case resolve_id(Space, Opts, NodeSpec) of
        #opcua_node_id{ns = NS} = ResolvedId ->
            {NS, ResolvedId};
        #opcua_node_id{ns = OtherNS} ->
            throw({incompatible_namespaces, NS, OtherNS})
    end.

match_node({_NS, undefined, undefined}, _Node) -> false;
match_node({undefined, undefined, NodeId},
           #opcua_node{node_id = #opcua_node_id{} = NodeId}) -> true;
match_node({NS, undefined, NodeId},
           #opcua_node{node_id = #opcua_node_id{ns = NS} = NodeId}) -> true;
match_node({NS, Name, undefined},
           #opcua_node{node_id = #opcua_node_id{ns = NS}, browse_name = Name}) -> true;
match_node({undefined, Name, NodeId},
           #opcua_node{node_id = #opcua_node_id{} = NodeId, browse_name = Name}) -> true;
match_node({NS, Name, NodeId},
           #opcua_node{node_id = #opcua_node_id{ns = NS} = NodeId, browse_name = Name}) -> true;
match_node({_NS, Name1, #opcua_node_id{} = NodeId},
           #opcua_node{node_id = NodeId, browse_name = Name2}) ->
    throw({browse_name_conflict, NodeId, Name1, Name2});
match_node({_NS, _Name, _NodeId}, _Node) -> false.

prepare_templates(Space, Opts, Templates) when is_list(Templates) ->
    % First check for already existing nodes and relations.
    PrepTemplates = prepare_templates(Space, Opts, Templates, []),
    % Then create the missing nodes and relations, validate exisiting nodes
    % and prepare the result structure.
    apply_templates(Space, Opts, PrepTemplates, [], [], []).

prepare_templates(_Space, _Opts, [], Acc) ->
    lists:reverse(Acc);
prepare_templates(Space, Opts, [Template | Rest], Acc) ->
    PrepTemplate = prepare_template(Space, Opts, Template),
    prepare_templates(Space, Opts, Rest, [PrepTemplate | Acc]).

prepare_template(Space, Opts, Template) ->
    % First try figuring out if nodes and references needs to be created
    % by following references up from the bottom of the template structure,
    % propagating up the existing node identifiers.
    {_, Template2} = bottomup_prepare_node(Space, Opts, Template),
    % Then from top to bottom, check for exisiting nodes and relations.
    topdown_prepare_node(Space, Opts, Template2).

bottomup_prepare_node(Space, Opts, Template) when is_map(Template) ->
    #{browse_name := TemplateBrowseName} = Template,
    {Template2, TemplateNamespace, TemplateNodeId} = case Template of
        #{namespace := TNS, node_id := TId} = T ->
            {NS, Id} = resolve_namespace(Space, Opts, TNS, TId),
            {T#{namespace => NS, node_id => Id}, NS, Id};
        #{node_id := TId} = T ->
            {NS, Id} = resolve_namespace(Space, Opts, undefined, TId),
            {T#{namespace => NS, node_id => Id}, NS, Id};
        #{namespace := TNS} = T ->
            {NS, Id} = resolve_namespace(Space, Opts, TNS, undefined),
            {T#{namespace => NS, node_id => Id}, NS, Id}
    end,
    MatchSpec = {TemplateNamespace, TemplateBrowseName, TemplateNodeId},
    RefSpecs = maps:get(references, Template2, []),
    case bottomup_prepare_refs(Space, Opts, MatchSpec, RefSpecs) of
        {undefined, PreparedRefs} ->
            % No node found with any of the references
            {NodeRec, NodeId}
                = retrieve_node(Space, Opts, optional, TemplateNodeId),
            Template3 = Template2#{node => NodeRec, node_id => NodeId,
                                   references => PreparedRefs},
            {NodeRec, Template3};
        {NodeRec, PreparedRefs} ->
            % Found a match from the references, keep it for later validation
            Template3 = Template2#{node => NodeRec, references => PreparedRefs},
            {NodeRec, Template3}
    end;
bottomup_prepare_node(Space, Opts, NodeSpec) ->
    retrieve_node(Space, Opts, required, NodeSpec).

retrieve_node(Space, Opts, required, NodeSpec)
  when NodeSpec =/= undefined ->
    NodeId = resolve_id(Space, Opts, NodeSpec),
    case opcua_space:node(Space, NodeId) of
        undefined -> throw({node_not_found, NodeSpec});
        NodeRec -> {NodeRec, NodeId}
    end;
retrieve_node(_Space, _Opts, optional, undefined) ->
    {undefined, undefined};
retrieve_node(Space, Opts, optional, NodeSpec) ->
    NodeId = resolve_id(Space, Opts, NodeSpec),
    case opcua_space:node(Space, NodeId) of
        undefined -> {undefined, NodeId};
        NodeRec -> {NodeRec, NodeId}
    end.

bottomup_prepare_refs(Space, Opts, MatchSpec, RefSpecs) ->
    bottomup_prepare_refs(Space, Opts, MatchSpec, RefSpecs, undefined, []).


bottomup_prepare_refs(_Space, _Opts, _MatchSpec, [], NodeRec, Acc) ->
    {NodeRec, lists:reverse(Acc)};
bottomup_prepare_refs(Space, Opts, MatchSpec, [RefSpec | Rest], NodeRec, Acc) ->
    {PeerRec, PrepRefSpec}
        = bottomup_prepare_ref(Space, Opts, MatchSpec, RefSpec),
    case {NodeRec, PeerRec}  of
        {_, undefined} ->
            % No new matching reference found
            bottomup_prepare_refs(Space, Opts, MatchSpec, Rest,
                                  NodeRec, [PrepRefSpec | Acc]);
        {undefined, _} ->
            % No matching reference yet
            bottomup_prepare_refs(Space, Opts, MatchSpec, Rest,
                                  PeerRec, [PrepRefSpec | Acc]);
        {NodeRec, NodeRec} ->
            % Consistent reference to the same existing node
            bottomup_prepare_refs(Space, Opts, MatchSpec, Rest,
                                  NodeRec, [PrepRefSpec | Acc]);
        {#opcua_node{node_id = NodeId1}, #opcua_node{node_id = NodeId2}} ->
            % Found multiple nodes matching
            throw({node_id_conflict, NodeId1, NodeId2})
    end.

bottomup_prepare_ref(Space, Opts, MatchSpec, {Dir, RefTypeSpec, Template}) ->
    {PeerNode, Template2} = bottomup_prepare_node(Space, Opts, Template),
    RefTypeId = resolve_id(Space, Opts, RefTypeSpec),
    PrepRefSpec = {Dir, RefTypeId, PeerNode, Template2},
    case PeerNode of
        undefined ->
            % No sub-node found, no need to check references yet
            {undefined, PrepRefSpec};
        #opcua_node{node_id = PeerId} ->
            % Found peer node, check for a matching reverse reference
            RefOpts = #{direction => reverse(Dir),
                        type => RefTypeId,
                        include_subtypes => false},
            Refs = opcua_space:references(Space, PeerId, RefOpts),
            ParentIds = [select_side(top, Dir, Ref) || Ref <- Refs],
            ParentNodes = [opcua_space:node(Space, I) || I <- ParentIds],
            MatchingNodes = [N || N <- ParentNodes, match_node(MatchSpec, N)],
            case MatchingNodes of
                [] -> {undefined, PrepRefSpec};
                [ParentNode] -> {ParentNode, PrepRefSpec};
                OtherNodes ->
                    throw({multiple_match, MatchSpec,
                           [I || #opcua_node{node_id = I} <- OtherNodes]})
            end
    end.

topdown_prepare_node(Space, Opts,
                     #{node := Node, references := RefSpecs} = Template) ->
    RefSpecs2 = topdown_prepare_refs(Space, Opts, Node, RefSpecs),
    Template#{references := RefSpecs2}.

topdown_prepare_refs(Space, Opts, Node, RefSpecs) ->
    topdown_prepare_refs(Space, Opts, Node, RefSpecs, []).

topdown_prepare_refs(_Space, _Opts, _Node, [], Acc) ->
    lists:reverse(Acc);
topdown_prepare_refs(Space, Opts, Node, [RefSpec | Rest], Acc) ->
    RefSpec2 = topdown_prepare_ref(Space, Opts, Node, RefSpec),
    topdown_prepare_refs(Space, Opts, Node, Rest, [RefSpec2 | Acc]).

topdown_prepare_ref(_Space, _Opts, _ParentNode,
                    {_Dir, _RefTypeId, #opcua_node{} = _PeerNode,
                     #opcua_node_id{}} = RefSpec) ->
    % No top-down reference check required or possible and we reached a leaf
    RefSpec;
topdown_prepare_ref(Space, Opts, #opcua_node{} = _ParentNode,
                    {Dir, RefTypeId, #opcua_node{} = PeerNode, Template}) ->
    % No top-down reference check required, continue down
    Template2 = topdown_prepare_node(Space, Opts, Template),
    {Dir, RefTypeId, PeerNode, Template2};
topdown_prepare_ref(Space, Opts, undefined,
                    {Dir, RefTypeId, PeerNode, Template}) ->
    % No top-down reference check possible, continue down
    Template2 = topdown_prepare_node(Space, Opts, Template),
    {Dir, RefTypeId, PeerNode, Template2};
topdown_prepare_ref(Space, Opts, ParentNode,
                    {Dir, RefTypeId, undefined, Template}) ->
    % Check for references from the parent node
    #{namespace := NS, browse_name := BrowseName, node_id := NodeId} = Template,
    MatchSpec = {NS, BrowseName, NodeId},
    RefOpts = #{direction => Dir, type => RefTypeId, include_subtypes => false},
    #opcua_node{node_id = ParentId} = ParentNode,
    Refs = opcua_space:references(Space, ParentId, RefOpts),
    PeerIds = [select_side(bottom, Dir, Ref) || Ref <- Refs],
    PeerNodes = [opcua_space:node(Space, I) || I <- PeerIds],
    MatchingNodes = [N || N <- PeerNodes, match_node(MatchSpec, N)],
    case MatchingNodes of
        [] ->
            % No reference found, continue down
            Template2 = topdown_prepare_node(Space, Opts, Template),
            {Dir, RefTypeId, undefined, Template2};
        [PeerNode] ->
            % Found a peer node, update the ref spec and template and continue
            Template2 = Template#{node := PeerNode},
            Template3 = topdown_prepare_node(Space, Opts, Template2),
            {Dir, RefTypeId, PeerNode, Template3};
        OtherNodes ->
            throw({multiple_match, MatchSpec,
                   [I || #opcua_node{node_id = I} <- OtherNodes]})
    end.

reverse(forward) -> inverse;
reverse(inverse) -> forward.

select_side(top, forward, #opcua_reference{source_id = NodeId}) -> NodeId;
select_side(top, inverse, #opcua_reference{target_id = NodeId}) -> NodeId;
select_side(bottom, forward, #opcua_reference{target_id = NodeId}) -> NodeId;
select_side(bottom, inverse, #opcua_reference{source_id = NodeId}) -> NodeId.

apply_templates(_Space, _Opts, [], Refs, Nodes, Acc) ->
    {lists:reverse(Acc), Nodes, Refs};
apply_templates(Space, Opts, [Template | Rest], Refs, Nodes, Acc) ->
    {Result, Nodes2, Refs2}
        = collect_node(Space, Opts, Refs, Nodes, Template),
    apply_templates(Space, Opts, Rest, Refs2, Nodes2, [Result | Acc]).


collect_node(_Space, _Opts, Refs, Nodes, #opcua_node_id{} = NodeId) ->
    % Reached a leaf
    {{NodeId, []}, Nodes, Refs};
collect_node(Space, Opts, Refs, Nodes,
             #{node := #opcua_node{node_id = NodeId} = Node,
               references := RefSpecs} = Template) ->
    % Node was found, validate it matches the template
    case validate_node(Space, Opts, Template, Node) of
        %TODO: Uncomment when validate_node is implemented
        % false -> throw({node_validation_failed, Node#opcua_node.node_id});
        true ->
            {Results, Nodes2, Refs2}
                = collect_refs(Space, Opts, Refs, Nodes, true, Node, RefSpecs),
            {{NodeId, Results}, Nodes2, Refs2}
    end;
collect_node(Space, Opts, Refs, Nodes,
             #{node := undefined, references := RefSpecs} = Template) ->
    % Node not found, create it and continue down
    #opcua_node{node_id = NodeId} = Node = create_node(Space, Opts, Template),
    {Results, Nodes2, Refs2}
        = collect_refs(Space, Opts, Refs, [Node | Nodes], false, Node, RefSpecs),
    {{NodeId, Results}, Nodes2, Refs2}.

collect_refs(Space, Opts, Refs, Nodes, ParentExists, ParentNode, RefSpecs) ->
    collect_refs(Space, Opts, Refs, Nodes, ParentExists, ParentNode, RefSpecs, []).

collect_refs(_Space, _Opts, Refs, Nodes, _ParentExists, _ParentNode, [], Acc) ->
    {lists:reverse(Acc), Nodes, Refs};
collect_refs(Space, Opts, Refs, Nodes, ParentExists, ParentNode, [RefSpec | Rest], Acc) ->
    {Result, Nodes2, Refs2}
        = collect_ref(Space, Opts, Refs, Nodes, ParentExists, ParentNode, RefSpec),
    collect_refs(Space, Opts, Refs2, Nodes2, ParentExists, ParentNode, Rest, [Result | Acc]).

collect_ref(Space, Opts, Refs, Nodes, true, _ParentNode,
            {_Dir, _RefTypeId, #opcua_node{}, Template}) ->
    % The reference was already found, only continue collecting
    collect_node(Space, Opts, Refs, Nodes, Template);
collect_ref(Space, Opts, Refs, Nodes, _ParentExists, ParentNode,
            {Dir, RefTypeId, _PeerNode, Template}) ->
    % Reference wasn't found, create one and continue
    {{PeerId, _} = Result, Nodes2, Refs2}
        = collect_node(Space, Opts, Refs, Nodes, Template),
    Ref = create_reference(Space, Opts, RefTypeId, Dir, ParentNode, PeerId),
    {Result, Nodes2, [Ref | Refs2]}.

template_get(Key, Template) ->
    case maps:find(Key, Template) of
        error -> throw({missing_template_value, Key});
        {ok, Value} -> Value
    end.

template_get(Key, Template, Default) ->
    maps:get(Key, Template, Default).

create_node(Space, #{node_id_fun := Fun} = Opts, Template) ->
    NodeId = case Template of
        #{node_id := #opcua_node_id{} = I} -> I;
        _ -> Fun()
    end,
    ClassName = template_get(class, Template, object),
    ClassRec = create_node_class(Space, Opts, ClassName, Template),
    NodeRec = #opcua_node{
        node_id = NodeId,
        node_class = ClassRec,
        origin = local,
        browse_name = template_get(browse_name, Template),
        display_name = template_get(display_name, Template, undefined),
        description = template_get(description, Template, undefined)
    },
    NodeRec.

create_node_class(_Space, _Opts, object, _Template) ->
    #opcua_object{};
create_node_class(Space, Opts, variable, Template) ->
    DataType = resolve_id(Space, Opts, template_get(data_type, Template)),
    #opcua_variable{
        value = template_get(value, Template, undefined),
        data_type = DataType,
        value_rank = template_get(value, Template, -1),
        array_dimensions = template_get(array_dimensions, Template, [])
    };
create_node_class(_Space, _Opts, object_type, Template) ->
    #opcua_object_type{
        is_abstract = template_get(is_abstract, Template, false)
    };
create_node_class(Space, Opts, variable_type, Template) ->
    DataType = resolve_id(Space, Opts, template_get(data_type, Template)),
    #opcua_variable_type{
        value = template_get(value, Template, undefined),
        data_type = DataType,
        value_rank = template_get(value, Template, -1),
        array_dimensions = template_get(array_dimensions, Template, []),
        is_abstract = template_get(is_abstract, Template, false)
    };
create_node_class(_Space, _Opts, data_type, Template) ->
    #opcua_data_type{
        is_abstract = template_get(is_abstract, Template, false),
        data_type_definition = template_get(data_type_definition, Template)
    };
create_node_class(_Space, _Opts, reference_type, Template) ->
    #opcua_reference_type{
        is_abstract = template_get(is_abstract, Template, false),
        symmetric = template_get(symmetric, Template, false),
        inverse_name = template_get(inverse_name, Template, undefined)
    };
create_node_class(_Space, _Opts, NodeClass, _Template) ->
    throw({unsupported_node_class, NodeClass}).

create_reference(_Space, _Opts, RefTypeId, forward,
                 #opcua_node{node_id = SourceId, browse_name = _SourceName},
                 #opcua_node_id{} = TargetId) ->
    #opcua_reference{type_id = RefTypeId,
                     source_id = SourceId,
                     target_id = TargetId};
create_reference(_Space, _Opts, RefTypeId, inverse,
                 #opcua_node{node_id = TargetId, browse_name = _TargetName},
                 #opcua_node_id{} = SourceId) ->
    #opcua_reference{type_id = RefTypeId,
                     source_id = SourceId,
                     target_id = TargetId}.

validate_node(_Space, _Opts, _Template, _Node) ->
    %TODO: Implemente validation
    true.