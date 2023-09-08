%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc OPCUA Server address space shared amongst all the clients.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_server_space).

-behavior(gen_server).

-compile({no_auto_import,[node/1]}).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/0]).
-export([add_namespace/1]).
-export([add_nodes/1]).
-export([del_nodes/1]).
-export([add_references/1]).
-export([del_references/1]).
-export([browse_path/2]).
-export([node/1]).
-export([references/1, references/2]).
-export([data_type/1]).
-export([type_descriptor/2]).
-export([schema/1]).
-export([namespace_uri/1]).
-export([namespace_id/1]).
-export([namespaces/0]).
-export([is_subtype/2]).

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


%%% MACRO %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(INPROC_SPACE, {opcua_space_backend, [?MODULE, opcua_nodeset]}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_namespace(Uri) ->
    delegate({add_namespace, Uri}).

add_nodes(Nodes) ->
    delegate({add_nodes, Nodes}).

del_nodes(NodeIds) ->
    delegate({del_nodes, NodeIds}).

add_references(References) ->
    delegate({add_references, References}).

del_references(References) ->
    delegate({del_references, References}).

browse_path(Source, Path) ->
    opcua_space:browse_path(?INPROC_SPACE, Source, Path).

node(NodeId) ->
    opcua_space:node(?INPROC_SPACE, NodeId).

references(OriginNode) ->
    opcua_space:references(?INPROC_SPACE, OriginNode, #{}).

references(OriginNode, Opts) ->
    opcua_space:references(?INPROC_SPACE, OriginNode, Opts).

data_type(TypeDescriptorSpec) ->
    opcua_space:data_type(?INPROC_SPACE, TypeDescriptorSpec).

type_descriptor(NodeSpec, Encoding) ->
    opcua_space:type_descriptor(?INPROC_SPACE, NodeSpec, Encoding).

schema(TypeSpec) ->
    opcua_space:schema(?INPROC_SPACE, TypeSpec).

namespace_uri(Id) ->
    opcua_space:namespace_uri(?INPROC_SPACE, Id).

namespace_id(Uri) ->
    opcua_space:namespace_id(?INPROC_SPACE, Uri).

namespaces() ->
    opcua_space:namespaces(?INPROC_SPACE).

is_subtype(SubTypeSpec, SuperTypeSpec) ->
    opcua_space:is_subtype(?INPROC_SPACE, SubTypeSpec, SuperTypeSpec).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    process_flag(trap_exit, true), % Ensure we cleanup the space
    {ok, opcua_space_backend:new(?MODULE, [opcua_nodeset])}.

handle_call({add_namespace, Uri}, _From, Space) ->
    try opcua_space:add_namespace(Space, Uri) of
        Id -> {reply, {ok, Id}, Space}
    catch throw:Reason:Stack ->
        {reply, {raise, throw, Reason, Stack}, Space}
    end;
handle_call({add_nodes, Nodes}, _From, Space) ->
    try opcua_space:add_nodes(Space, Nodes) of
        _ -> {reply, ok, Space}
    catch throw:Reason:Stack ->
        {reply, {raise, throw, Reason, Stack}, Space}
    end;
handle_call({del_nodes, NodeIds}, _From, Space) ->
    try opcua_space:del_nodes(Space, NodeIds) of
        _ -> {reply, ok, Space}
    catch throw:Reason:Stack ->
        {reply, {raise, throw, Reason, Stack}, Space}
    end;
handle_call({add_references, References}, _From, Space) ->
    try opcua_space:add_references(Space, References) of
        _ -> {reply, ok, Space}
    catch throw:Reason:Stack ->
        {reply, {raise, throw, Reason, Stack}, Space}
    end;
handle_call({del_references, References}, _From, Space) ->
    try opcua_space:del_references(Space, References) of
        _ -> {reply, ok, Space}
    catch throw:Reason:Stack ->
        {reply, {raise, throw, Reason, Stack}, Space}
    end;
handle_call(stop, _From, Space) ->
    {stop, normal, ok, Space}.

handle_cast(Request, _Spaces) ->
    error({unknown_cast, Request}).

handle_info(Info, _Spaces) ->
    error({unknown_info, Info}).

terminate(_Reason, Space) ->
    opcua_space_backend:terminate(Space),
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

delegate(Message) ->
    case gen_server:call(?MODULE, Message) of
        ok -> ok;
        {ok, Result} -> Result;
        {raise, Class, Reason, Stack} -> erlang:raise(Class, Reason, Stack)
    end.
