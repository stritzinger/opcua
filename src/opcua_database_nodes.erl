-module(opcua_database_nodes).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("xmerl/include/xmerl.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([setup/1]).
-export([parse/1]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("opcua_database.hrl").


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
    Nodes = load_nodes(Dir),
    References = load_references(Dir),
    opcua_address_space:add_nodes(Nodes),
    opcua_address_space:add_references(References).

parse(File) ->
    {XML, []} = xmerl_scan:file(File, [{space, normalize}]),
    Nodes = parse_node_set(xml_to_simple(XML)),
    References = extract_references(Nodes),
    Root = filename:rootname(File),
    write_nodes(Nodes, Root),
    write_references(References, Root),
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
    Node = #opcua_node{
        node_id = get_attr(<<"NodeId">>, Attrs, node_id, Meta),
        browse_name = maps:get(<<"BrowseName">>, Attrs, undefined),
        display_name = hd(get_value([<<"DisplayName">>], Content)),
        references = parse_references(get_value([<<"References">>], Content), Meta),
        node_class = parse_node_class(Elem, Meta)
    },
    {Meta, [Node|Nodes]};
parse_node(_Element, State) ->
    State.

parse_references(Refs, Meta) ->
    [parse_reference(R, Meta) || R <- Refs].

parse_reference({<<"Reference">>, Attrs, [Target]}, Meta) ->
    #opcua_reference{
        reference_type_id = get_attr(<<"ReferenceType">>, Attrs, node_id, Meta),
        is_forward = get_attr(<<"IsForward">>, Attrs, boolean, Meta, true),
        target_id = parse(node_id, Target, Meta)
    }.

parse_node_class({<<"UAObject">>, Attrs, _Content}, Meta) ->
    #opcua_object{
        event_notifier = get_attr(<<"EventNotifier">>, Attrs, integer, Meta, undefined)
    };
parse_node_class({<<"UADataType">>, Attrs, Content}, Meta) ->
    #opcua_data_type{
        is_abstract = get_attr(<<"IsAbstract">>, Attrs, boolean, Meta, false),
        data_type_definition = parse_data_type_definition(get_value([<<"Definition">>], Content, undefined), Meta)
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
parse_data_type_definition(Fields, Meta) ->
    maps:from_list([parse_data_type_definition_field(F, Meta) || F <- Fields]).

parse_data_type_definition_field({<<"Field">>, Attrs, _Content}, Meta) ->
    {maps:get(<<"Name">>, Attrs), maps:from_list(lists:foldl(
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
            {<<"ValueRank">>, value_rank, integer}
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

extract_references(Nodes) ->
    lists:flatten(extract_references(Nodes, [])).

extract_references([#opcua_node{node_id = NodeId, references = NR}|Nodes], Refs) ->
    All = [resolve_reference({NodeId, R}) || R <- NR],
    extract_references(Nodes, [All|Refs]);
extract_references([], Refs) ->
    Refs.

resolve_reference({Source, #opcua_reference{is_forward = false, target_id = Target} = Reference}) ->
    {Target, Reference#opcua_reference{target_id = Source, is_forward = true}};
resolve_reference(Ref) ->
    Ref.

load_nodes(Dir) -> load_terms(filename:join(Dir, "**/*.nodes.term")).

load_references(Dir) -> load_terms(filename:join(Dir, "**/*.references.term")).

load_terms(Pattern) ->
    [T ||
        F <- filelib:wildcard(Pattern),
        {ok, Terms} <- [file:consult(F)],
        T <- Terms
    ].

write_nodes(Nodes, Root) -> write_terms(Nodes, Root ++ ".nodes.term").

write_references(Nodes, Root) -> write_terms(Nodes, Root ++ ".references.term").

write_terms(Terms, Filename) ->
    {ok, File} = file:open(Filename, [write]),
    try
        [ok = file:write(File, io_lib:format("~p.~n", [T])) || T <- Terms]
    after
        ok = file:close(File)
    end.