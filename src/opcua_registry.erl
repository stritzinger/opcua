-module(opcua_registry).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua_codec.hrl").
-include("opcua_protocol.hrl").
-include("opcua_node_command.hrl").


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
    case opcua_database:lookup_id(NodeSpec) of
        #node_id{ns = 0, type = numeric, value = NodeNum} ->
            [model_perform(NodeNum, C) || C <- Commands]
    end.


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Opts) ->
    ?LOG_DEBUG("OPCUA registry process starting with options: ~p", [Opts]),
    NextSecureChannelId = crypto:bytes_to_integer(crypto:strong_rand_bytes(4)),
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


%-- HARDCODED MODEL ------------------------------------------------------------

model_perform(NodeNum, #browse_command{type = #node_id{type = numeric, ns = 0, value = RefNum}} = Command) ->
    #browse_command{subtypes = SubTypes, direction = Direction} = Command,
    ?LOG_DEBUG("Browsing node ~w's ~w ~w references, subtypes: ~w...",
               [NodeNum, RefNum, Direction, SubTypes]),
    Result = case model_reference(NodeNum, RefNum, Direction, SubTypes) of
        {error, Reason} -> #{status => Reason, references => []};
        {ok, Refs} -> #{status => good, references => post_process_refs(Refs)}
    end,
    ?LOG_DEBUG("    -> ~p", [Result]),
    Result;
model_perform(NodeNum, #read_command{attr = Attr, range = Range}) ->
    ?LOG_DEBUG("Reading node ~w's attribute ~w [~w]...",
               [NodeNum, Attr, Range]),
    DataValue = case model_attribute(NodeNum, Attr, Range) of
        {error, Reason} -> #data_value{status = Reason};
        {TypeSpec, Value} ->
            #data_value{value = opcua_codec:pack_variant(TypeSpec, Value)}
    end,
    ?LOG_DEBUG("    -> ~p", [DataValue]),
    DataValue.

post_process_refs(Refs) ->
    [post_process_ref(R) || R <- Refs].

post_process_ref(#{node_id := NodeId} = Ref) ->
    lists:foldl(fun(Key, Map) ->
        case model_attribute(NodeId, Key, undefined) of
            {error, _Reason} -> Map;
            {_, Value} -> Map#{Key => Value}
        end
    end, Ref, [browse_name, display_name, node_class]).

model_attribute(NodeNum, node_id, _) -> {node_id, #node_id{value = NodeNum}};
model_attribute(84, display_name, _) -> {localized_text, #localized_text{text = <<"Root">>}};
model_attribute(84, browse_name, _) -> {qualified_name, #qualified_name{name = <<"Root">>}};
model_attribute(84, node_class, _) -> {257, object};
model_attribute(84, _, _) -> {error, bad_attribute_id_invalid};
model_attribute(85, display_name, _) -> {localized_text, #localized_text{text = <<"Objects">>}};
model_attribute(85, browse_name, _) -> {qualified_name, #qualified_name{name = <<"Objects">>}};
model_attribute(85, node_class, _) -> {257, object};
model_attribute(85, _, _) -> {error, bad_attribute_id_invalid};
model_attribute(50000, display_name, _) -> {localized_text, #localized_text{text = <<"Test Folder 1">>}};
model_attribute(50000, browse_name, _) -> {qualified_name, #qualified_name{name = <<"TestFolder1">>}};
model_attribute(50000, node_class, _) -> {257, object};
model_attribute(50000, _, _) -> {error, bad_attribute_id_invalid};
model_attribute(50001, display_name, _) -> {localized_text, #localized_text{text = <<"Test Folder 2">>}};
model_attribute(50001, browse_name, _) -> {qualified_name, #qualified_name{name = <<"TestFolder2">>}};
model_attribute(50001, node_class, _) -> {257, object};
model_attribute(50001, _, _) -> {error, bad_attribute_id_invalid};
model_attribute(_, _, _) -> {error, bad_node_id_unknown}.

model_reference(84, T, forward, true) when T =:= 33; T =:= 31 ->
    {ok, [#{reference_type_id => T, node_id => 85}]};
model_reference(84, _, _, _) ->
    {ok, []};
model_reference(85, T, forward, true) when T =:= 33; T =:= 31 ->
    {ok, [#{reference_type_id => T, node_id => 50000},
          #{reference_type_id => T, node_id => 50001}]};
model_reference(85, _, _, _) ->
    {ok, []};
model_reference(_, _, _, _) ->
    {error, bad_node_id_unknown}.
