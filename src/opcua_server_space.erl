%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA Server address space shared amongst all the clients.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_server_space).

-behavior(gen_server).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/0]).
-export([add_namespace/2]).
-export([add_nodes/1]).
-export([del_nodes/1]).
-export([add_references/1]).
-export([del_references/1]).
-export([node/1]).
-export([references/1, references/2]).
-export([data_type/1]).
-export([type_descriptor/2]).
-export([schema/1]).
-export([namespace_uri/1]).
-export([namespace_id/1]).
-export([namespaces/0]).

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

add_namespace(Id, Uri) ->
    gen_server:call(?MODULE, {add_namespace, Id, Uri}).

add_nodes(Nodes) ->
    gen_server:call(?MODULE, {add_nodes, Nodes}).

del_nodes(NodeIds) ->
    gen_server:call(?MODULE, {del_nodes, NodeIds}).

add_references(References) ->
    gen_server:call(?MODULE, {add_references, References}).

del_references(References) ->
    gen_server:call(?MODULE, {del_references, References}).

node(NodeId) ->
    opcua_space_backend:node([?MODULE, opcua_nodeset], NodeId).

references(OriginNode) ->
    opcua_space_backend:references([?MODULE, opcua_nodeset], OriginNode, #{}).

references(OriginNode, Opts) ->
    opcua_space_backend:references([?MODULE, opcua_nodeset], OriginNode, Opts).

data_type(TypeDescriptorSpec) ->
    opcua_space_backend:data_type([?MODULE, opcua_nodeset], TypeDescriptorSpec).

type_descriptor(NodeSpec, Encoding) ->
    opcua_space_backend:type_descriptor([?MODULE, opcua_nodeset], NodeSpec, Encoding).

schema(NodeSpec) ->
    opcua_space_backend:schema([?MODULE, opcua_nodeset], NodeSpec).

namespace_uri(Id) ->
    opcua_space_backend:namespace_uri([?MODULE, opcua_nodeset], Id).

namespace_id(Uri) ->
    opcua_space_backend:namespace_id([?MODULE, opcua_nodeset], Uri).

namespaces() ->
    opcua_space_backend:namespaces([?MODULE, opcua_nodeset]).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    {ok, opcua_space_backend:new(?MODULE, [opcua_nodeset])}.

handle_call({add_namespace, Id, Uri}, _From, Space) ->
    opcua_space:add_namespace(Space, Id, Uri),
    {reply, ok, Space};
handle_call({add_nodes, Nodes}, _From, Space) ->
    opcua_space:add_nodes(Space, Nodes),
    {reply, ok, Space};
handle_call({del_nodes, NodeIds}, _From, Space) ->
    opcua_space:del_nodes(Space, NodeIds),
    {reply, ok, Space};
handle_call({add_references, References}, _From, Space) ->
    opcua_space:add_references(Space, References),
    {reply, ok, Space};
handle_call({del_references, References}, _From, Space) ->
    opcua_space:del_references(Space, References),
    {reply, ok, Space};
handle_call(stop, _From, Space) ->
    {stop, normal, ok, Space}.

handle_cast(Request, _Spaces) ->
    error({unknown_cast, Request}).

handle_info(Info, _Spaces) ->
    error({unknown_info, Info}).

terminate(_Reason, Space) ->
    opcua_space_backend:terminate(Space),
    ok.
