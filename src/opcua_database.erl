-module(opcua_database).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


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

-spec lookup_schema(opcua:node_spec()) -> opcua:schema().
lookup_schema(NodeSpec) ->
    opcua_database_data_types:lookup(opcua_codec:node_id(NodeSpec)).

-spec lookup_id(opcua:node_spec()) -> opcua:node_id().
lookup_id(NodeSpec) ->
    case opcua_codec:node_id(NodeSpec) of
        #opcua_node_id{type = numeric} = NodeId -> NodeId;
        _ -> error(not_implemented)
    end.

%% The returned node id is canonical, meaning it is always numeric.
-spec lookup_encoding(opcua:node_spec(), opcua:stream_encoding())
    -> {opcua:node_id(), undefined | opcua:stream_encoding()}.
lookup_encoding(NodeId = #opcua_node_id{}, Encoding) ->
    opcua_database_encodings:lookup(NodeId, Encoding);
lookup_encoding(NodeSpec, Encoding) ->
    lookup_encoding(lookup_id(NodeSpec), Encoding).

%% The returned node id is canonical, meaning it is always numeric.
-spec resolve_encoding(opcua:node_spec())
    -> {opcua:node_id(), undefined | opcua:stream_encoding()}.
resolve_encoding(NodeId = #opcua_node_id{}) ->
    opcua_database_encodings:resolve(NodeId);
resolve_encoding(NodeSpec) ->
    resolve_encoding(lookup_id(NodeSpec)).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Opts) ->
    ?LOG_DEBUG("OPCUA database process starting with options: ~p", [Opts]),
    load_status_codes(),
    load_attributes(),
    load_nodesets(),
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


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_nodesets() ->
    PrivDir = code:priv_dir(opcua),
    NodeSetDir = filename:join([PrivDir, "nodesets"]),
    opcua_address_space:create(default),
    opcua_database_nodes:setup(NodeSetDir),
    ok.

load_status_codes() ->
    PrivDir = code:priv_dir(opcua),
    StatusCodeFileName = "StatusCode.csv",
    StatusCodeFilePath = filename:join([PrivDir, StatusCodeFileName]),
    opcua_database_status_codes:load(StatusCodeFilePath),
    ok.

load_attributes() ->
    PrivDir = code:priv_dir(opcua),
    AttributeIdsFileName = "AttributeIds.csv",
    AttributeIdsFilePath = filename:join([PrivDir, AttributeIdsFileName]),
    opcua_database_attributes:load(AttributeIdsFilePath),
    ok.
