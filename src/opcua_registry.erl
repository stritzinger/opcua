-module(opcua_registry).


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
    case get_node(opcua_database:lookup_id(NodeSpec)) of
        undefined ->
            [{error, bad_node_id_unknown} || _ <- Commands];
        {Mode, #opcua_node{} = Node} ->
            [static_perform(Mode, Node, C) || C <- Commands]
    end.


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Opts) ->
    ?LOG_DEBUG("OPCUA registry process starting with options: ~p", [Opts]),
    NextSecureChannelId = crypto:bytes_to_integer(crypto:strong_rand_bytes(4)),
    State = #state{next_secure_channel_id = NextSecureChannelId},
    {ok, State, {continue, undefined}}.

handle_continue(_, State) ->
    setup_static_nodes(),
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

get_resolver_node(NodeId) ->
    case application:get_env(opcua, resolver) of
        undefined -> undefined;
        {ok, Mod} -> Mod:get_node(NodeId)
    end.

get_resolver_refs(NodeId, Opts) ->
    case application:get_env(opcua, resolver) of
        undefined -> [];
        {ok, Mod} -> Mod:get_references(NodeId, Opts)
    end.

get_node(NodeId) ->
    case get_resolver_node(NodeId) of
        undefined ->
            case opcua_address_space:get_node(NodeId) of
                undefined -> undefined;
                #opcua_node{} = Node -> {static, Node}
            end;
        #opcua_node{} = Node -> {dynamic, Node}
    end.

get_references(static, NodeId, Opts) ->
    opcua_address_space:get_references(NodeId, Opts);
get_references(dynamic, NodeId, Opts) ->
    get_resolver_refs(NodeId, Opts).

next_secure_channel_id(#state{next_secure_channel_id = ?MAX_SECURE_CHANNEL_ID} = State) ->
    {1, State#state{next_secure_channel_id = 2}};

next_secure_channel_id(#state{next_secure_channel_id = Id} = State) ->
    {Id, State#state{next_secure_channel_id = Id + 1}}.

static_perform(Mode, Node, #opcua_browse_command{type = BaseId, direction = Dir, subtypes = SubTypes}) ->
    #opcua_node{node_id = NodeId} = Node,
    ?LOG_DEBUG("Browsing node ~w's ~w and ~w references; with subtypes: ~w...",
               [NodeId#opcua_node_id.value, BaseId#opcua_node_id.value, Dir, SubTypes]),
    BaseOpts = #{type => BaseId, direction => Dir},
    Opts = case SubTypes of
        true -> BaseOpts#{include_subtypes => true};
        false -> BaseOpts
    end,
    Refs = get_references(Mode, NodeId, Opts),
    ResRefs = [post_process_ref(R) || R <- Refs],
    ?LOG_DEBUG("    -> ~p", [ResRefs]),
    #{status => good, references => ResRefs};
static_perform(_Mode, Node, #opcua_read_command{attr = Attr, range = undefined} = _Command) ->
    ?LOG_DEBUG("Reading node ~w's attribute ~w...",
               [(Node#opcua_node.node_id)#opcua_node_id.value, Attr]),
    Result = try {opcua_node:attribute_type(Attr, Node),
                  opcua_node:attribute(Attr, Node)} of
        {AttrType, AttrValue} ->
            Value = opcua_codec:pack_variant(AttrType, AttrValue),
            #opcua_data_value{value = Value}
    catch
        _:Reason ->
            ?LOG_ERROR("Error while reading attribute ~w from node ~w: ~p",
                       [Attr, (Node#opcua_node.node_id)#opcua_node_id.value, Reason]),
            #opcua_data_value{status = Reason}
    end,
    ?LOG_DEBUG("    -> ~p", [Result]),
    Result;
static_perform(_Mode, _Node, _Command) ->
    {error, bad_not_implemented}.

post_process_ref(Ref) ->
    #opcua_reference{
        type_id = RefId,
        target_id = TargetId
    } = Ref,
    {TargetMode, TargetNode} = get_node(TargetId),
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
        type_definition => TypeDef
    },
    Fields = [node_id, browse_name, display_name, node_class],
    lists:foldl(fun(Key, Map) ->
            Map#{Key => opcua_node:attribute(Key, TargetNode)}
    end, BaseObj, Fields).


%-- HARDCODED MODEL ------------------------------------------------------------

setup_static_nodes() ->
    case application:get_env(opcua, resolver) of
        undefined -> ok;
        {ok, Mod} ->
            {ok, Nodes, Refs} = Mod:init(),
            opcua_address_space:add_nodes(Nodes),
            opcua_address_space:add_references(Refs),
            ok
    end.
