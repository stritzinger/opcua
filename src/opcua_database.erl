-module(opcua_database).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua_codec.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/1]).
-export([lookup_schema/1]).
-export([lookup_id/1]).
-export([lookup_encoding/2]).
-export([resolve_encoding/1]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

-spec lookup_schema(node_spec()) -> opcua_schema().
lookup_schema(NodeSpec) ->
    opcua_codec_data_types:lookup(opcua_codec:node_id(NodeSpec)).

-spec lookup_id(node_spec()) -> node_id().
lookup_id(NodeSpec) ->
    case opcua_codec:node_id(NodeSpec) of
        #node_id{type = numeric} = NodeId -> NodeId;
        _ -> error(not_implemented)
    end.

%% The returned node id is canonical, meaning it is always numeric.
-spec lookup_encoding(node_spec(), opcua_encoding()) -> node_id().
% OpenSecureChannelResponse
lookup_encoding(#node_id{ns = 0, type = numeric, value = 447}, binary) ->
    #node_id{value = 449};
% CloseSecureChannelResponse
lookup_encoding(#node_id{ns = 0, type = numeric, value = 453}, binary) ->
    #node_id{value = 455};
lookup_encoding(#node_id{}, binary) ->
    error(not_implemented);
lookup_encoding(NodeSpec, binary) ->
    lookup_encoding(opcua_codec:node_id(NodeSpec), binary).

%% The returned node id is canonical, meaning it is always numeric.
-spec resolve_encoding(node_spec()) -> {node_id(), opcua_encoding()}.
% OpenSecureChannelRequest
resolve_encoding(#node_id{ns = 0, type = numeric, value = 446}) ->
    {#node_id{value = 444}, binary};
% CloseSecureChannelRequest
resolve_encoding(#node_id{ns = 0, type = numeric, value = 452}) ->
    {#node_id{value = 450}, binary};
resolve_encoding(#node_id{}) ->
    error(not_implemented);
resolve_encoding(NodeSpec) ->
    resolve_encoding(opcua_codec:node_id(NodeSpec)).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Opts) ->
    ?LOG_DEBUG("OPCUA database process starting with options: ~p", [Opts]),
    {ok, #state{}}.

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
    ?LOG_DEBUG("OPCUA database process terminating: ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
