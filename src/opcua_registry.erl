-module(opcua_registry).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/1]).
-export([allocate_secure_channel/1]).
-export([release_secure_channel/1]).
-export([perform/2]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).


%%% MACRO %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).
-define(MAX_SECURE_CHANNEL_ID, 4294967295).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    next_secure_channel_id :: pos_integer()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).


allocate_secure_channel(Pid) ->
    gen_server:call(?SERVER, {allocate_secure_channel, Pid}).


release_secure_channel(ChannelId) ->
    gen_server:call(?SERVER, {release_secure_channel, ChannelId}).

perform(NodeSpec, Commands) ->
    case opcua_address_space:get_node(opcua_database:lookup_id(NodeSpec)) of
        undefined -> {error, bad_node_id_unknown};
        #opcua_node{} = Node ->
            [static_perform(Node, C) || C <- Commands]
    end.


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Opts) ->
    ?LOG_DEBUG("OPCUA registry process starting with options: ~p", [Opts]),
    NextSecureChannelId = crypto:bytes_to_integer(crypto:strong_rand_bytes(4)),
    setup_static_nodes(),
    {ok, #state{next_secure_channel_id = NextSecureChannelId}}.

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

next_secure_channel_id(#state{next_secure_channel_id = ?MAX_SECURE_CHANNEL_ID} = State) ->
    {1, State#state{next_secure_channel_id = 2}};

next_secure_channel_id(#state{next_secure_channel_id = Id} = State) ->
    {Id, State#state{next_secure_channel_id = Id + 1}}.

static_perform(Node, #opcua_browse_command{type = BaseId, direction = Dir, subtypes = false}) ->
    ?LOG_DEBUG("Browsing node ~w's ~w ~w references...",
               [(Node#opcua_node.node_id)#opcua_node_id.value, BaseId#opcua_node_id.value, Dir]),
    #opcua_node{references = AllRefs} = Node,
    IsForward = Dir =:= forward orelse Dir =:= both,
    ResRefs = [post_process_ref(R)
               || R = #opcua_reference{reference_type_id = I,
                                       is_forward = F} <- AllRefs,
                  I =:= BaseId, F =:= IsForward],
    ?LOG_DEBUG("    -> ~p", [ResRefs]),
    #{status => good, references => ResRefs};
static_perform(Node, #opcua_browse_command{type = BaseId, direction = Dir, subtypes = true}) ->
    ?LOG_DEBUG("Browsing node ~w's ~w and subtypes ~w references...",
               [(Node#opcua_node.node_id)#opcua_node_id.value, BaseId#opcua_node_id.value, Dir]),
    #opcua_node{references = AllRefs} = Node,
    IsForward = Dir =:= forward orelse Dir =:= both,
    ResRefs = [post_process_ref(R)
               || R = #opcua_reference{reference_type_id = I,
                                       is_forward = F} <- AllRefs,
                  opcua_address_space:is_subtype(I, BaseId), F =:= IsForward],
    ?LOG_DEBUG("    -> ~p", [ResRefs]),
    #{status => good, references => ResRefs};
static_perform(Node, #opcua_read_command{attr = Attr, range = undefined} = _Command) ->
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
static_perform(_Node, _Command) ->
    {error, bad_not_implemented}.

post_process_ref(Ref) ->
    #opcua_reference{
        reference_type_id = RefId,
        target_id = TargetId
    } = Ref,
    TargetNode = opcua_address_space:get_node(TargetId),
    BaseObj = #{reference_type_id => RefId},
    Fields = [node_id, browse_name, display_name, node_class],
    lists:foldl(fun(Key, Map) ->
            Map#{Key => opcua_node:attribute(Key, TargetNode)}
    end, BaseObj, Fields).


%-- HARDCODED MODEL ------------------------------------------------------------

setup_static_nodes() ->
    A = #opcua_node{
        node_id = ?NNID(50000),
        browse_name = <<"Stritzinger">>,
        display_name = <<"Stritzinger">>,
        node_class = #opcua_object{},
        references = [
        ]
    },
    R2A = #opcua_reference{
        reference_type_id = ?NNID(35),
        is_forward = true,
        target_id = ?NNID(50000)
    },
    opcua_address_space:add_nodes([A]),
    opcua_address_space:add_references([{?NNID(84), R2A}]),
    ok.
