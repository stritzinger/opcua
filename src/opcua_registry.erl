-module(opcua_registry).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").


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

perform(NodeId, Commands) ->
    ?LOG_DEBUG("Asking node ~p to perform ~p", [NodeId, Commands]),
    [{error, bad_not_implemented} || _ <- Commands].


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