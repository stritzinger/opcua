-module(opcua_server_registry).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/1]).
-export([next_node_id/0]).
-export([allocate_secure_channel/1]).
-export([release_secure_channel/1]).
-export([perform/2]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_continue/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).


%%% MACRO %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).
-define(MAX_SECURE_CHANNEL_ID, 4294967295).
-define(FIRST_CUSTOM_NODE_ID,  50000).
-define(DEFAULT_RESOLVER, opcua_server_default_resolver).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    next_secure_channel_id :: pos_integer(),
    next_node_id = ?FIRST_CUSTOM_NODE_ID :: pos_integer()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

next_node_id() ->
    gen_server:call(?SERVER, next_node_id).

allocate_secure_channel(Pid) ->
    gen_server:call(?SERVER, {allocate_secure_channel, Pid}).

release_secure_channel(ChannelId) ->
    gen_server:call(?SERVER, {release_secure_channel, ChannelId}).

perform(NodeSpec, Commands) ->
    case get_node(opcua_node:id(NodeSpec)) of
        undefined ->
            [{error, bad_node_id_unknown} || _ <- Commands];
        {Mode, #opcua_node{} = Node} ->
            [static_perform(Mode, Node, C) || C <- Commands]
    end.


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Opts) ->
    ?LOG_DEBUG("OPCUA registry process starting with options: ~p", [Opts]),
    ResMod = application:get_env(opcua, resolver, ?DEFAULT_RESOLVER),
    {ok, Vals, Nodes, Refs, ResState} = ResMod:init(),
    persistent_term:put({?MODULE, resolver}, {ResMod, ResState}),
    NextSecureChannelId = crypto:bytes_to_integer(crypto:strong_rand_bytes(4)),
    State = #state{next_secure_channel_id = NextSecureChannelId},
    {ok, State, {continue, {Vals, Nodes, Refs}}}.

handle_continue({Vals, Nodes, Refs}, State) ->
    setup_static_data(Vals, Nodes, Refs),
    {noreply, State}.

handle_call(next_node_id, _From, #state{next_node_id = NextId} = State) ->
    {reply, ?NNID(NextId), State#state{next_node_id = NextId + 1}};
handle_call({allocate_secure_channel, _Pid}, _From, State) ->
    {ChannelId, State2} = next_secure_channel_id(State),
    {reply, {ok, ChannelId}, State2};
handle_call({release_secure_channel, _ChannelId}, _From, State) ->
    {reply, ok, State};
handle_call(Req, From, State) ->
    ?LOG_WARNING("Unexpected gen_server call from ~p: ~p", [From, Req]),
    {reply, {error, unexpected_call}, State}.

handle_cast(Req, State) ->
    ?LOG_WARNING("Unexpected gen_server cast: ~p", [Req]),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_WARNING("Unexpected gen_server message: ~p", [Msg]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_DEBUG("OPCUA registry process terminating: ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_node(NodeId) ->
    case resolver_get_node(NodeId) of
        undefined ->
            case opcua_address_space:get_node(default, NodeId) of
                undefined -> undefined;
                #opcua_node{node_class =
                        #opcua_variable{data_type = T, value = V} = C} = N ->
                    case resolver_get_value(NodeId, T, V) of
                        undefined ->
                            {static, N};
                        V2 ->
                            C2 = C#opcua_variable{value = V2},
                            {dynamic, N#opcua_node{node_class = C2}}
                    end;
                #opcua_node{} = Node ->
                    {static, Node}
            end;
        #opcua_node{} = Node ->
            {dynamic, Node}
    end.

get_references(static, NodeId, Opts) ->
    opcua_address_space:get_references(default, NodeId, Opts);
get_references(dynamic, NodeId, Opts) ->
    resolver_get_references(NodeId, Opts).

next_secure_channel_id(#state{next_secure_channel_id = ?MAX_SECURE_CHANNEL_ID} = State) ->
    {1, State#state{next_secure_channel_id = 2}};

next_secure_channel_id(#state{next_secure_channel_id = Id} = State) ->
    {Id, State#state{next_secure_channel_id = Id + 1}}.

static_perform(Mode, Node, #opcua_browse_command{type = BaseId, direction = Dir, subtypes = SubTypes}) ->
    #opcua_node{node_id = NodeId} = Node,
    ?LOG_DEBUG("Browsing node ~s ~w references ~s~s...",
               [opcua_node:format(NodeId), Dir, opcua_node:format(BaseId),
                if SubTypes -> " and subtypes"; true -> "" end]),
    BaseOpts = #{type => BaseId, direction => Dir},
    Opts = case SubTypes of
        true -> BaseOpts#{include_subtypes => true};
        false -> BaseOpts
    end,
    Refs = get_references(Mode, NodeId, Opts),
    ResRefs = post_process_refs(NodeId, Refs),
    #{status => good, references => ResRefs};
static_perform(_Mode, Node, #opcua_read_command{attr = Attr, range = undefined} = _Command) ->
    ?LOG_DEBUG("Reading node ~s attribute ~w...",
               [opcua_node:format(Node), Attr]),
    Result = try {opcua_node:attribute_type(Attr, Node),
                  opcua_node:attribute(Attr, Node)} of
        {AttrType, AttrValue} ->
            try opcua_codec:pack_variant(AttrType, AttrValue) of
                Value -> #opcua_data_value{value = Value}
            catch
                _:Reason ->
                    ?LOG_ERROR("Error while packing node ~s attribute ~w with type ~s and value ~p: ~p",
                       [opcua_node:format(Node), Attr,
                        opcua_node:format(AttrType), AttrValue, Reason]),
                    #opcua_data_value{status = bad_internal_error}
            end
    catch
        _:bad_attribute_id_invalid = Reason ->
            ?LOG_DEBUG("Error while reading node ~s attribute ~w: ~p",
                       [opcua_node:format(Node), Attr, Reason]),
            #opcua_data_value{status = Reason};
        _:Reason ->
            Status = case opcua_nodeset_status:is_name(Reason) of
                true -> Reason;
                false -> bad_internal_error
            end,
            ?LOG_ERROR("Error while reading node ~s attribute ~w: ~p",
                       [opcua_node:format(Node), Attr, Reason]),
            #opcua_data_value{status = Status}
    end,
    Result;
static_perform(_Mode, _Node, _Command) ->
    {error, bad_not_implemented}.



post_process_refs(NodeId, Refs) ->
    post_process_refs(NodeId, Refs, []).

post_process_refs(_NodeId, [], Acc) ->
    lists:reverse(Acc);
post_process_refs(NodeId, [Ref | Refs], Acc) ->
    %TODO: Add support for symetric references
    %TODO: Add support for field mask
    %TODO: Add support for class mask
    {IsForward, TargetId, RefId} = case Ref of
        #opcua_reference{type_id = R, source_id = NodeId, target_id = Id} ->
            {true, Id, R};
        #opcua_reference{type_id = R, source_id = Id, target_id = NodeId} ->
            {false, Id, R}
    end,
    % Filtering out references to nodes we don't know about.
    % e.g. if the hard-coded server node is not defined by the OPCUA server
    case get_node(TargetId) of
        undefined ->
            post_process_refs(NodeId, Refs, Acc);
        {TargetMode, TargetNode} ->
            RefOpts = #{
                type => ?NNID(?REF_HAS_TYPE_DEFINITION),
                direction => forward,
                include_subtypes => true
            },
            TargetRefs = get_references(TargetMode, TargetId, RefOpts),
            TypeDef = case TargetRefs of
                [] -> ?UNDEF_EXT_NODE_ID;
                [#opcua_reference{target_id = TypeId} | _] -> ?XID(TypeId)
            end,
            BaseObj = #{
                type => RefId,
                is_forward => IsForward,
                type_definition => TypeDef
            },
            Fields = [node_id, browse_name, display_name, node_class],
            Ref2 = lists:foldl(fun(Key, Map) ->
                    Map#{Key => opcua_node:attribute(Key, TargetNode)}
            end, BaseObj, Fields),
            post_process_refs(NodeId, Refs, [Ref2 | Acc])
    end.

resolver_get_node(NodeId) ->
    {ResMod, ResState} = persistent_term:get({?MODULE, resolver}),
    ResMod:get_node(ResState, NodeId).

resolver_get_references(NodeId, Opts) ->
    {ResMod, ResState} = persistent_term:get({?MODULE, resolver}),
    ResMod:get_references(ResState, NodeId, Opts).

resolver_get_value(NodeId, DataType, CurrVal) ->
    {ResMod, ResState} = persistent_term:get({?MODULE, resolver}),
    ResMod:get_value(ResState, NodeId, DataType, CurrVal).


%-- HARDCODED MODEL ------------------------------------------------------------

setup_static_data(Vals, Nodes, Refs) ->
    lists:foreach(fun({K, V}) -> opcua_server:set_value(K, V) end, Vals),
    opcua_address_space:add_nodes(default, Nodes),
    opcua_address_space:add_references(default, Refs),
    ok.
