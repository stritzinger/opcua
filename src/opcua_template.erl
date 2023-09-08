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
    % The type of object instance.
    % The references and sub-nodes will be created from the given object type.
    % If defined, the node_class MUST be object or undefined.
    instance_of => template_node_spec(),

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

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ENABLE_DEBUG, false).
-if(?ENABLE_DEBUG =:= true).
-define(DEBUG(F, A), io:format(F "~n", A)).
-define(TAG(ARG), fun
    (#opcua_node_id{} = __Id) ->
        iolist_to_binary(io_lib:format("~p", [opcua_node:spec(__Id)]));
    (#{browse_name := __N} = __T) ->
        __NS = maps:get(namespace, __T, 0),
        __Spec = opcua_node:spec(maps:get(node_id, __T, undefined)),
        __Alias = case maps:find(alias, __T) of
            error -> <<"">>;
            {ok, __R} when is_reference(__R) ->
                iolist_to_binary(io_lib:format("(~s)", [erlang:ref_to_list(__R)]));
            {ok, __A} when is_atom(__A) ->
                iolist_to_binary(io_lib:format("(~w)", [__A]))
        end,
        iolist_to_binary(io_lib:format("~p:~s/~p~s", [__NS, __N, __Spec, __Alias]));
    (#opcua_node{node_id = __Id, browse_name = __N}) ->
        iolist_to_binary(io_lib:format("~s/~p", [__N, opcua_node:spec(__Id)]));
    (#opcua_reference{type_id = __R, source_id = __S, target_id = __T}) ->
        iolist_to_binary(io_lib:format("~p=~p=>~p",
            [opcua_node:spec(__S), opcua_node:spec(__R),opcua_node:spec(__T)]));
    (__O) ->
        iolist_to_binary(io_lib:format("~p", [__O]))
end(ARG)).
-else.
-define(DEBUG(F, A), ok).
-endif.


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

resolve_id(_Space, _Opts, #opcua_node_id{} = NodeId) ->
    NodeId;
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
        {ok, #opcua_node_id{} = NodeId} -> NodeId;
        error -> opcua_node:id(NodeSpec)
    end.

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

update_alias(Aliases, #{alias := Alias} = Template) ->
    {Alias, Template, Aliases#{Alias => Template}};
update_alias(Aliases, Template) ->
    Alias = make_ref(),
    Template2 = Template#{alias => Alias},
    {Alias, Template2, Aliases#{Alias => Template2}}.

prepare_templates(Space, Opts, Templates) when is_list(Templates) ->
    % First check for already existing nodes and relations.
    ?DEBUG("Preparing ~w templates...", [length(Templates)]),
    {Order, Aliases} = prepare_templates(Space, Opts, Templates, #{}, []),
    % Then create the missing nodes and relations, validate exisiting nodes
    % and prepare the result structure.
    ?DEBUG("Applying ~w templates...", [length(Order)]),
    apply_templates(Space, Opts, Aliases, Order).

prepare_templates(_Space, _Opts, [], Aliases, Acc) ->
    {lists:reverse(Acc), Aliases};
prepare_templates(Space, Opts, [Template | Rest], Aliases, Acc) ->
    ?DEBUG("Preparing template ~s...", [?TAG(Template)]),
    {Alias, Aliases2} = prepare_template(Space, Opts, Aliases, Template),
    ?DEBUG("Preparing template ~s: DONE", [?TAG(Template)]),
    prepare_templates(Space, Opts, Rest, Aliases2, [Alias | Acc]).

prepare_template(Space, Opts, Aliases, Template) ->
    % First try figuring out if nodes and references needs to be created
    % by following references up from the bottom of the template structure,
    % propagating up the existing node identifiers.
    {_, Alias, Aliases2}
        = bottomup_prepare_node(Space, Opts, Aliases, Template),
    % Then from top to bottom, check for exisiting nodes and relations.
    Aliases3 = topdown_prepare_node(Space, Opts, Aliases2, Alias),
    {Alias, Aliases3}.

bottomup_prepare_node(Space, Opts, Aliases, Template) when is_map(Template) ->
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
    {_, Template3, Aliases2} = update_alias(Aliases, Template2),
    ?DEBUG("Bottom-Up preparation of ~s...", [?TAG(Template3)]),
    MatchSpec = {TemplateNamespace, TemplateBrowseName, TemplateNodeId},
    RefSpecs = maps:get(references, Template2, []),
    case bottomup_prepare_refs(Space, Opts, Aliases2, MatchSpec, RefSpecs) of
        {undefined, PreparedRefs, Aliases3} ->
            % No node found with any of the references,
            % try retriving the current node from node_id
            ?DEBUG("Bottom-Up preparation of template ~s: NODE NOT FOUND FROM REFERENCES",
                   [?TAG(Template3)]),
            {NodeRec, NodeId}
                = retrieve_node(Space, Opts, optional, TemplateNodeId),
            Template4 = Template3#{node => NodeRec, node_id => NodeId,
                                   references => PreparedRefs},
            {Alias, _, Aliases4} = update_alias(Aliases3, Template4),
            {NodeRec, Alias, Aliases4};
        {#opcua_node{node_id = NodeId} = NodeRec, PreparedRefs, Aliases3} ->
            ?DEBUG("Bottom-Up preparation of template ~s: FOUND ~s",
                   [?TAG(Template3), ?TAG(NodeRec)]),
            % Found a match from the references, keep it for later validation
            Template4 = Template3#{node => NodeRec, node_id => NodeId,
                                   references => PreparedRefs},
            {Alias, _, Aliases4} = update_alias(Aliases3, Template4),
            {NodeRec, Alias, Aliases4}
    end;
bottomup_prepare_node(Space, Opts, Aliases, NodeSpecOrAlias) ->
    case maps:find(NodeSpecOrAlias, Aliases) of
        error ->
            ?DEBUG("Bottom-Up preparation reached external ~s...",
                   [?TAG(NodeSpecOrAlias)]),
            {NodeRec, NodeId}
                = retrieve_node(Space, Opts, required, NodeSpecOrAlias),
            {NodeRec, NodeId, Aliases};
        {ok, #{node := NodeRec}} ->
            ?DEBUG("Bottom-Up preparation reached alias ~s...",
                   [?TAG(NodeSpecOrAlias)]),
            {NodeRec, NodeSpecOrAlias, Aliases}
    end.

retrieve_node(Space, Opts, required, NodeSpec)
  when NodeSpec =/= undefined ->
    NodeId = resolve_id(Space, Opts, NodeSpec),
    case opcua_space:node(Space, NodeId) of
        undefined -> throw({node_not_found, NodeSpec});
        NodeRec ->
            ?DEBUG("Found node ~s: ~s", [?TAG(NodeSpec), ?TAG(NodeRec)]),
            {NodeRec, NodeId}
    end;
retrieve_node(_Space, _Opts, optional, undefined) ->
    {undefined, undefined};
retrieve_node(Space, Opts, optional, NodeSpec) ->
    NodeId = resolve_id(Space, Opts, NodeSpec),
    case opcua_space:node(Space, NodeId) of
        undefined ->
            ?DEBUG("Node ~s not found", [?TAG(NodeSpec)]),
            {undefined, NodeId};
        NodeRec ->
            ?DEBUG("Found node ~s: ~s", [?TAG(NodeSpec), ?TAG(NodeRec)]),
            {NodeRec, NodeId}
    end.

bottomup_prepare_refs(Space, Opts, Aliases, MatchSpec, RefSpecs) ->
    ?DEBUG("Bottom-Up preparing ~w references...", [length(RefSpecs)]),
    bottomup_prepare_refs(Space, Opts, Aliases, MatchSpec,
                          RefSpecs, undefined, []).

bottomup_prepare_refs(_Space, _Opts, Aliases, _MatchSpec, [], NodeRec, Acc) ->
    {NodeRec, lists:reverse(Acc), Aliases};
bottomup_prepare_refs(Space, Opts, Aliases, MatchSpec,
                      [RefSpec | Rest], NodeRec, Acc) ->
    {PeerRec, PrepRefSpec, Aliases2}
        = bottomup_prepare_ref(Space, Opts, Aliases, MatchSpec, RefSpec),
    case {NodeRec, PeerRec}  of
        {_, undefined} ->
            % No new matching reference found
            bottomup_prepare_refs(Space, Opts, Aliases2, MatchSpec, Rest,
                                  NodeRec, [PrepRefSpec | Acc]);
        {undefined, _} ->
            % No matching reference yet
            bottomup_prepare_refs(Space, Opts, Aliases2, MatchSpec, Rest,
                                  PeerRec, [PrepRefSpec | Acc]);
        {NodeRec, NodeRec} ->
            % Consistent reference to the same existing node
            bottomup_prepare_refs(Space, Opts, Aliases2, MatchSpec, Rest,
                                  NodeRec, [PrepRefSpec | Acc]);
        {#opcua_node{node_id = NodeId1}, #opcua_node{node_id = NodeId2}} ->
            % Found multiple nodes matching
            throw({node_id_conflict, NodeId1, NodeId2})
    end.


bottomup_prepare_ref(Space, Opts, Aliases, MatchSpec,
                     {Dir, RefTypeSpec, Template}) ->
    ?DEBUG("Bottom-Up preparing ~s ~s reference to ~s...",
           [Dir, ?TAG(RefTypeSpec), ?TAG(Template)]),
    {PeerNode, TemplateId, Aliases2}
        = bottomup_prepare_node(Space, Opts, Aliases, Template),
    %TODO: Remove this assert
    case TemplateId of
        #opcua_node_id{} -> ok;
        _ -> {ok, _} = maps:find(TemplateId, Aliases2)
    end,
    %TODO
    {RefTypeId, RefTypeAlias} = case maps:find(RefTypeSpec, Aliases) of
        {ok, #{node_id := Id}} ->
            % Keep alias references for later
            {Id, RefTypeSpec};
        error ->
            % Resolve to a node id
            Id = resolve_id(Space, Opts, RefTypeSpec),
            {Id, Id}
    end,
    PrepRefSpec = {Dir, RefTypeAlias, PeerNode, TemplateId},
    case {RefTypeId, PeerNode} of
        {_, undefined} ->
            % No sub-node found, no need to check references yet
            ?DEBUG("Bottom-Up preparing ~s ~s reference to ~s: CAN'T DO",
                   [Dir, ?TAG(RefTypeSpec), ?TAG(Template)]),
            {undefined, PrepRefSpec, Aliases2};
        {#opcua_node_id{}, #opcua_node{node_id = PeerId} = _PeerNode} ->
            % Found peer node, and reference type, check for a matching reverse reference
            ?DEBUG("Bottom-Up preparing ~s ~s reference to ~s: FOUND PEER ~s",
                   [Dir, ?TAG(RefTypeSpec), ?TAG(Template), ?TAG(_PeerNode)]),
            RefOpts = #{direction => reverse(Dir),
                        type => RefTypeId,
                        include_subtypes => false},
            Refs = opcua_space:references(Space, PeerId, RefOpts),
            ?DEBUG("Bottom-Up preparing ~s ~s reference to ~s: FOUND ~w REFERENCES",
                   [Dir, ?TAG(RefTypeSpec), ?TAG(Template), length(Refs)]),
            ParentIds = [select_side(top, Dir, Ref) || Ref <- Refs],
            ParentNodes = [opcua_space:node(Space, I) || I <- ParentIds],
            MatchingNodes = [N || N <- ParentNodes, match_node(MatchSpec, N)],
            ?DEBUG("Bottom-Up preparing ~s ~s reference to ~s: FOUND ~w MATCHES",
                   [Dir, ?TAG(RefTypeSpec), ?TAG(Template), length(MatchingNodes)]),
            case MatchingNodes of
                [] -> {undefined, PrepRefSpec, Aliases2};
                [ParentNode] -> {ParentNode, PrepRefSpec, Aliases2};
                OtherNodes ->
                    throw({multiple_match, MatchSpec,
                           [I || #opcua_node{node_id = I} <- OtherNodes]})
            end;
        {_, _} ->
            % The reference type is not yet created
            {undefined, PrepRefSpec, Aliases2}
    end.

topdown_prepare_node(_Space, _Opts, Aliases, #opcua_node_id{} = _NodeId) ->
    % Reference to external node, nothing to do there
    ?DEBUG("Top-Down preparation reached leaf ~s", [?TAG(_NodeId)]),
    Aliases;
topdown_prepare_node(Space, Opts, Aliases, Alias) ->
    #{Alias := #{node := Node, references := RefSpecs} = Template} = Aliases,
    ?DEBUG("Top-Down preparation of ~s...", [?TAG(Template)]),
    {RefSpecs2, Aliases2}
        = topdown_prepare_refs(Space, Opts, Aliases, Node, RefSpecs),
    Template2 = Template#{references := RefSpecs2},
    {_, _, Aliases3} = update_alias(Aliases2, Template2),
    Aliases3.

topdown_prepare_refs(Space, Opts, Aliases, Node, RefSpecs) ->
    ?DEBUG("Top-Down preparation of ~w references...", [length(RefSpecs)]),
    topdown_prepare_refs(Space, Opts, Aliases, Node, RefSpecs, []).

topdown_prepare_refs(_Space, _Opts, Aliases, _Node, [], Acc) ->
    {lists:reverse(Acc), Aliases};
topdown_prepare_refs(Space, Opts, Aliases, Node, [RefSpec | Rest], Acc) ->
    {RefSpec2, Aliases2}
        = topdown_prepare_ref(Space, Opts, Aliases, Node, RefSpec),
    topdown_prepare_refs(Space, Opts, Aliases2, Node, Rest, [RefSpec2 | Acc]).

topdown_prepare_ref(_Space, _Opts, Aliases, _ParentNode,
                    {_Dir, _RefTypeId, #opcua_node{} = _PeerNode,
                     #opcua_node_id{} = _Id} = RefSpec) ->
    % No top-down reference check required or possible and we reached a leaf
    ?DEBUG("Top-Down preparing ~s ~s reference to ~s => ~s: REACHED LEAF",
           [_Dir, ?TAG(_RefTypeId), ?TAG(_Id), ?TAG(_PeerNode)]),
    {RefSpec, Aliases};
topdown_prepare_ref(Space, Opts, Aliases, #opcua_node{} = _ParentNode,
                    {_Dir, _RefTypeId, #opcua_node{} = _PeerNode,
                     Alias} = RefSpec) ->
    % No top-down reference check required, continue down
    ?DEBUG("Top-Down preparing ~s ~s reference to ~s => ~s: NO NEED",
           [_Dir, ?TAG(_RefTypeId), ?TAG(Alias), ?TAG(_PeerNode)]),
    {RefSpec, topdown_prepare_node(Space, Opts, Aliases, Alias)};
topdown_prepare_ref(Space, Opts, Aliases, undefined,
                    {_Dir, _RefTypeId, _PeerNode, Alias} = RefSpec) ->
    % No top-down reference check possible, continue down
    ?DEBUG("Top-Down preparing ~s ~s reference to ~s => ~s: CAN'T DO",
           [_Dir, ?TAG(_RefTypeId), ?TAG(Alias), ?TAG(_PeerNode)]),
    {RefSpec, topdown_prepare_node(Space, Opts, Aliases, Alias)};
topdown_prepare_ref(Space, Opts, Aliases, ParentNode,
                    {Dir, #opcua_node_id{} = RefTypeId, undefined, Alias}) ->
    ?DEBUG("Top-Down preparing ~s ~s reference to ~s: CHECKING...",
           [Dir, ?TAG(RefTypeId), ?TAG(Alias)]),
    #{Alias := Template} = Aliases,
    PeerNode
        = topdown_check_refs(Space, Opts, ParentNode, Dir, RefTypeId, Template),
    {_, _, Aliases2} = update_alias(Aliases, Template#{node := PeerNode}),
    Aliases3 = topdown_prepare_node(Space, Opts, Aliases2, Alias),
    {{Dir, RefTypeId, PeerNode, Alias}, Aliases3};
topdown_prepare_ref(Space, Opts, Aliases, ParentNode,
                    {Dir, RefTypeAlias, undefined, Alias}) ->
    % The reference type is an alias an may not exist yet
    ?DEBUG("Top-Down preparing ~s ~s reference to ~s: CHECKING...",
           [Dir, ?TAG(RefTypeAlias), ?TAG(Alias)]),
    #{RefTypeAlias := #{node_id := RefTypeId}} = Aliases,
    #{Alias := Template} = Aliases,
    PeerNode
        = topdown_check_refs(Space, Opts, ParentNode, Dir, RefTypeId, Template),
    {_, _, Aliases2} = update_alias(Aliases, Template#{node := PeerNode}),
    Aliases3 = topdown_prepare_node(Space, Opts, Aliases2, Alias),
    {{Dir, RefTypeAlias, PeerNode, Alias}, Aliases3}.

topdown_check_refs(_Space, _Opts, _ParentNode, _Dir, undefined, _Template) ->
    undefined;
topdown_check_refs(Space, _Opts, ParentNode, Dir, RefTypeId, Template) ->
    % Check for references from the parent node
    #opcua_node{node_id = ParentId} = ParentNode,
    #{namespace := NS, browse_name := BrowseName, node_id := NodeId} = Template,
    MatchSpec = {NS, BrowseName, NodeId},
    RefOpts = #{direction => Dir, type => RefTypeId, include_subtypes => false},
    Refs = opcua_space:references(Space, ParentId, RefOpts),
    ?DEBUG("Top-Down preparing ~s ~s reference to ~s: ~w REFERENCES FOUND",
           [Dir, ?TAG(RefTypeId), ?TAG(Template), length(Refs)]),
    PeerIds = [select_side(bottom, Dir, Ref) || Ref <- Refs],
    PeerNodes = [opcua_space:node(Space, I) || I <- PeerIds],
    MatchingNodes = [N || N <- PeerNodes, match_node(MatchSpec, N)],
    ?DEBUG("Top-Down preparing ~s ~s reference to ~s: ~w MATCHES",
           [Dir, ?TAG(RefTypeId), ?TAG(Template), length(MatchingNodes)]),
    case MatchingNodes of
        [] -> undefined;
        [PeerNode] -> PeerNode;
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

apply_templates(Space, Opts, Aliases, Order) ->
    apply_templates(Space, Opts, Aliases, Order, [], #{}, []).

apply_templates(_Space, _Opts, _Aliases, [], Refs, Nodes, Acc) ->
    {lists:reverse(Acc), maps:values(Nodes), Refs};
apply_templates(Space, Opts, Aliases, [Alias | Rest], Refs, Nodes, Acc) ->
    {Result, Nodes2, Refs2, Aliases2}
        = collect_node(Space, Opts, Aliases, Refs, Nodes, Alias),
    apply_templates(Space, Opts, Aliases2, Rest, Refs2, Nodes2, [Result | Acc]).

collect_node(_Space, _Opts, Aliases, Refs, Nodes, #opcua_node_id{} = NodeId) ->
    % Reached a leaf
    {{NodeId, []}, Nodes, Refs, Aliases};
collect_node(Space, Opts, Aliases, Refs, Nodes, Alias) ->
    case maps:find(Alias, Nodes) of
        {ok, #opcua_node{node_id = NodeId} = _Node} ->
            ?DEBUG("Collecting from ~s: NODE ~s ALLREADY CREATED",
                   [?TAG(maps:get(Alias, Aliases)), ?TAG(_Node)]),
            {{NodeId, []}, Nodes, Refs, Aliases};
        error ->
            #{Alias := Template} = Aliases,
            #{node := NodeRec, references := RefSpecs} = Template,
            ?DEBUG("Collecting from ~s...", [?TAG(Template)]),
            case NodeRec of
                undefined ->
                    % Node not found, create it and continue down
                    {Node, Aliases2}
                        = create_node(Space, Opts, Aliases, Template),
                    ?DEBUG("Collecting from ~s: NODE ~s CREATED",
                           [?TAG(Template), ?TAG(Node)]),
                    #opcua_node{node_id = NodeId} = Node,
                    Nodes2 = Nodes#{Alias => Node},
                    {Results, Nodes3, Refs2, Aliases3}
                        = collect_refs(Space, Opts, Aliases2, Refs,
                                       Nodes2, false, Node, RefSpecs),
                    {{NodeId, Results}, Nodes3, Refs2, Aliases3};
                #opcua_node{node_id = NodeId} ->
                    % Node was found, validate it matches the template
                    ?DEBUG("Collecting from ~s: NODE ~s ALREADY EXISTS",
                           [?TAG(Template), ?TAG(NodeRec)]),
                    case validate_node(Space, Opts, Template, NodeRec) of
                        %TODO: Uncomment when validate_node is implemented
                        % false -> throw({node_validation_failed, Node#opcua_node.node_id});
                        true ->
                            {Results, Nodes2, Refs2, Aliases2}
                                = collect_refs(Space, Opts, Aliases, Refs,
                                               Nodes, true, NodeRec, RefSpecs),
                            {{NodeId, Results}, Nodes2, Refs2, Aliases2}
                    end
            end
    end.

collect_refs(Space, Opts, Aliases, Refs, Nodes,
             ParentExists, ParentNode, RefSpecs) ->
    ?DEBUG("Collecting from ~w references of ~s ~s...",
           [length(RefSpecs),
            if ParentExists -> "existing"; true -> "new" end,
            ?TAG(ParentNode)]),
    collect_refs(Space, Opts, Aliases, Refs, Nodes,
                 ParentExists, ParentNode, RefSpecs, []).

collect_refs(_Space, _Opts, Aliases, Refs, Nodes,
             _ParentExists, _ParentNode, [], Acc) ->
    {lists:reverse(Acc), Nodes, Refs, Aliases};
collect_refs(Space, Opts, Aliases, Refs, Nodes,
             ParentExists, ParentNode, [RefSpec | Rest], Acc) ->
    {Result, Nodes2, Refs2, Aliases2}
        = collect_ref(Space, Opts, Aliases, Refs, Nodes,
                      ParentExists, ParentNode, RefSpec),
    collect_refs(Space, Opts, Aliases2, Refs2, Nodes2,
                 ParentExists, ParentNode, Rest, [Result | Acc]).

collect_ref(Space, Opts, Aliases, Refs, Nodes, true, _ParentNode,
            {_Dir, _RefTypeId, #opcua_node{} = _PeerNode, Alias}) ->
    % The reference was already found, only continue collecting
    ?DEBUG("Collecting from ~s ~s reference to ~s => ~s : ALREADY EXISTS",
           [_Dir, ?TAG(_RefTypeId), ?TAG(Alias), ?TAG(_PeerNode)]),
    collect_node(Space, Opts, Aliases, Refs, Nodes, Alias);
collect_ref(Space, Opts, Aliases, Refs, Nodes, _ParentExists, ParentNode,
            {Dir, RefTypeId, _PeerNode, Alias}) ->
    % Reference wasn't found, create one and continue
    ?DEBUG("Collecting from ~s ~s reference to ~s => ~s...",
           [Dir, ?TAG(RefTypeId), ?TAG(Alias), ?TAG(_PeerNode)]),
    {{PeerId, _} = Result, Nodes2, Refs2, Aliases2}
        = collect_node(Space, Opts, Aliases, Refs, Nodes, Alias),
    Ref = create_reference(Space, Opts, Aliases2, RefTypeId,
                           Dir, ParentNode, PeerId),
    ?DEBUG("Collecting from ~s ~s reference to ~s => ~s : CREATED REFERENCE ~s",
           [Dir, ?TAG(RefTypeId), ?TAG(Alias), ?TAG(_PeerNode), ?TAG(Ref)]),
    {Result, Nodes2, [Ref | Refs2], Aliases2}.

template_get(Key, Template) ->
    case maps:find(Key, Template) of
        error -> throw({missing_template_value, Key});
        {ok, Value} -> Value
    end.

template_get(Key, Template, Default) ->
    maps:get(Key, Template, Default).

create_node(Space, #{node_id_fun := Fun} = Opts, Aliases, Template) ->
    {NodeId, Template2, Aliases2} = case Template of
        #{node_id := #opcua_node_id{} = I} = T -> {I, T, Aliases};
        T ->
            % No node id defined, generate a new one
            I = Fun(),
            T2 = T#{node_id => I},
            {_, T2, As2} = update_alias(Aliases, T2),
            {I, T2, As2}
    end,
    ClassName = template_get(class, Template2, object),
    ClassRec = create_node_class(Space, Opts, ClassName, Template2),
    NodeRec = #opcua_node{
        node_id = NodeId,
        node_class = ClassRec,
        origin = local,
        browse_name = template_get(browse_name, Template2),
        display_name = template_get(display_name, Template2, undefined),
        description = template_get(description, Template2, undefined)
    },
    {NodeRec, Aliases2}.

create_node_class(_Space, _Opts, object, _Template) ->
    #opcua_object{};
create_node_class(Space, Opts, variable, Template) ->
    DataType = resolve_id(Space, Opts, template_get(data_type, Template)),
    #opcua_variable{
        value = template_get(value, Template, undefined),
        data_type = DataType,
        value_rank = template_get(value_rank, Template, -1),
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

create_reference(Space, Opts, Aliases, RefTypeAlias, Dir, Source, Target)
  when is_atom(RefTypeAlias) ->
    % The reference type is an alias
    #{RefTypeAlias := #{node_id := RefTypeId}} = Aliases,
    create_reference(Space, Opts, Aliases, RefTypeId, Dir, Source, Target);
create_reference(_Space, _Opts, _Aliases, RefTypeId, forward,
                 #opcua_node{node_id = SourceId},
                 #opcua_node_id{} = TargetId) ->
    #opcua_reference{type_id = RefTypeId,
                     source_id = SourceId,
                     target_id = TargetId};
create_reference(_Space, _Opts, _Aliases, RefTypeId, inverse,
                 #opcua_node{node_id = TargetId},
                 #opcua_node_id{} = SourceId) ->
    #opcua_reference{type_id = RefTypeId,
                     source_id = SourceId,
                     target_id = TargetId}.

validate_node(_Space, _Opts, _Template, _Node) ->
    %TODO: Implemente validation
    true.