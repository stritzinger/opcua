%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA Server database shared amongst all the clients.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_server_database).

-behavior(gen_server).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/0]).
-export([add_nodes/1]).
-export([del_nodes/1]).
-export([add_references/1]).
-export([del_references/1]).
-export([get_node/1]).
-export([get_references/1]).
-export([get_references/2]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_nodes(Nodes) ->
    gen_server:call(?MODULE, {add_nodes, Nodes}).

del_nodes(NodeIds) ->
    gen_server:call(?MODULE, {del_nodes, NodeIds}).

add_references(References) ->
    gen_server:call(?MODULE, {add_references, References}).

del_references(References) ->
    gen_server:call(?MODULE, {del_references, References}).

get_node(NodeId) ->
    opcua_space:get_node([?MODULE, nodeset], NodeId).

get_references(OriginNode) ->
    opcua_space:get_references([?MODULE, nodeset], OriginNode).

get_references(OriginNode, Opts) ->
    opcua_space:get_references([?MODULE, nodeset], OriginNode, Opts).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    {ok, [opcua_space:init(?MODULE), nodeset]}.

handle_call({add_nodes, Nodes}, _From, Spaces) ->
    opcua_space:add_nodes(Spaces, Nodes),
    {reply, ok, Spaces};
handle_call({del_nodes, NodeIds}, _From, Spaces) ->
    opcua_space:del_nodes(Spaces, NodeIds),
    {reply, ok, Spaces};
handle_call({add_references, References}, _From, Spaces) ->
    opcua_space:add_references(Spaces, References),
    {reply, ok, Spaces};
handle_call({del_references, References}, _From, Spaces) ->
    opcua_space:del_references(Spaces, References),
    {reply, ok, Spaces};
handle_call(stop, _From, Spaces) ->
    {stop, normal, ok, Spaces}.

handle_cast(Request, _Spaces) ->
    error({unknown_cast, Request}).

handle_info(Info, _Spaces) ->
    error({unknown_info, Info}).

terminate(_Reason, [Space | _]) ->
    opcua_space:terminate(Space),
    ok.
