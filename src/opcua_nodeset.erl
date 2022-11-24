-module(opcua_nodeset).

% Original NodeSet from https://github.com/OPCFoundation/UA-Nodeset/tree/v1.04/Schema

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Startup Functions
-export([start_link/1]).

%% API Functions
-export([attribute_name/1]).
-export([attribute_id/1]).
-export([status/1]).
-export([status_code/1]).
-export([status_name/1, status_name/2]).
-export([is_status/1]).
-export([data_type/1]).
-export([type_descriptor/2]).
-export([schema/1]).
-export([namespace_uri/1]).
-export([namespace_id/1]).
-export([namespaces/0]).
-export([node/1]).
-export([references/1, references/2]).

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


%%% STARTUP FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(BaseDir) ->
    gen_server:start_link(?MODULE, [BaseDir], []).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

attribute_name(Attr) ->
    {_, Name} = persistent_term:get({?MODULE, Attr}),
    Name.

attribute_id(Attr) ->
    {Id, _} = persistent_term:get({?MODULE, Attr}),
    Id.

status(Status) ->
    persistent_term:get({?MODULE, Status}).

status_name(Status) ->
    {_, Name, _} = persistent_term:get({?MODULE, Status}),
    Name.

status_name(Status, Default) ->
    {_, Name, _} = persistent_term:get({?MODULE, Status},
                                       {undefined, Default, undefined}),
    Name.

status_code(Status) ->
    {Code, _, _} = persistent_term:get({?MODULE, Status}),
    Code.

is_status(Status) ->
    try persistent_term:get({?MODULE, Status}) of
        {_, _, _} -> true
    catch  error:badarg -> false
    end.

data_type(TypeDescriptorNodeSpec) ->
    opcua_space_backend:data_type(?MODULE, TypeDescriptorNodeSpec).

type_descriptor(DataTypeNodeSpec, Encoding) ->
    opcua_space_backend:type_descriptor(?MODULE, DataTypeNodeSpec, Encoding).

schema(NodeSpec) ->
    opcua_space_backend:schema(?MODULE, NodeSpec).

namespace_uri(Id) ->
    opcua_space_backend:namespace_uri(?MODULE, Id).

namespace_id(Uri) ->
    opcua_space_backend:namespace_id(?MODULE, Uri).

namespaces() ->
    opcua_space_backend:namespaces(?MODULE).

node(NodeSpec) ->
    opcua_space_backend:node(?MODULE, NodeSpec).

references(OriginNodeSpec) ->
    opcua_space_backend:references(?MODULE, OriginNodeSpec, #{}).

references(OriginNodeSpec, Opts) ->
    opcua_space_backend:references(?MODULE, OriginNodeSpec, Opts).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(undefined) ->
    ?LOG_DEBUG("OPCUA nodeset process starting empty", []),
    opcua_space_backend:init(?MODULE),
    {ok, #state{}};
init(BaseDir) ->
    ?LOG_DEBUG("OPCUA nodeset process starting, loading nodeset from ~s", [BaseDir]),
    opcua_space_backend:init(?MODULE),
    ?LOG_INFO("Loading OPCUA attributes mapping..."),
    load_attributes(BaseDir),
    ?LOG_INFO("Loading OPCUA status code mapping..."),
    load_status(BaseDir),
    ?LOG_INFO("Loading OPCUA address space..."),
    %FIXME: use real space persistence instead of this temporary backward compatible hack
    load_namespaces(BaseDir),
    load_nodes(BaseDir),
    load_references(BaseDir),
    load_datatypes(BaseDir),
    load_encodings(BaseDir),
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
    ?LOG_DEBUG("OPCUA nodeset process terminating: ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_attributes(BaseDir) ->
    load_all_terms(BaseDir, "attributes", fun store_attribute/1).

load_status(BaseDir) ->
    load_all_terms(BaseDir, "status", fun store_status/1).

load_namespaces(BaseDir) ->
    load_all_terms(BaseDir, "namespaces", fun store_namespace/1).

load_nodes(BaseDir) ->
    load_all_terms(BaseDir, "nodes", fun store_node/1).

load_references(BaseDir) ->
    load_all_terms(BaseDir, "references", fun store_reference/1).

load_datatypes(BaseDir) ->
    load_all_terms(BaseDir, "datatypes", fun store_datatype/1).

load_encodings(BaseDir) ->
    load_all_terms(BaseDir, "encodings", fun store_encoding/1).

load_all_terms(BaseDir, Tag, Fun) ->
    Pattern = filename:join(BaseDir, "**/*." ++ Tag ++ ".bterm"),
    NoAccCB = fun(V, C) ->
        Fun(V),
        case C rem 500 of
            0 ->
                ?LOG_DEBUG("Loaded ~w ~s; memory: ~.3f MB",
                           [C, Tag, erlang:memory(total)/(1024*1024)]);
            _ -> ok
        end,
        C + 1
    end,
    NoAccFun = fun(F, C) -> opcua_util_bterm:fold(F, NoAccCB, C) end,
    Count = lists:foldl(NoAccFun, 0, filelib:wildcard(Pattern)),
    ?LOG_DEBUG("Loaded ~w ~s terms", [Count, Tag]),
    ok.

store_attribute({Id, Name} = Spec) when is_integer(Id), is_atom(Name) ->
    persistent_term:put({?MODULE, Id}, Spec),
    persistent_term:put({?MODULE, Name}, Spec),
    ok.

store_status({Code, Name, Desc} = Spec)
  when is_integer(Code), is_atom(Name), is_binary(Desc) ->
    persistent_term:put({?MODULE, Code}, Spec),
    persistent_term:put({?MODULE, Name}, Spec),
    ok.

store_datatype({Keys, DataType}) ->
    %FIXME: Temporary backward compatible hack
    KeyValuePairs = [{datatype, {Key, DataType}} || Key <- Keys],
    lists:foreach(fun(Item) ->
        opcua_space_backend:store(?MODULE, Item)
    end, KeyValuePairs),
    ok.

store_namespace({_Id, _Uri} = Spec) ->
    %FIXME: Temporary backward compatible hack
    opcua_space_backend:store(?MODULE, {namespace, Spec}),
    ok.

store_encoding({NodeId, {TargetNodeId, Encoding}}) ->
    %FIXME: Temporary backward compatible hack
    opcua_space_backend:store(?MODULE, {encoding, {NodeId, {TargetNodeId, Encoding}}}),
    opcua_space_backend:store(?MODULE, {encoding, {{TargetNodeId, Encoding}, NodeId}}),
    ok.

store_node(#opcua_node{} = Node) ->
    %FIXME: Temporary backward compatible hack
    opcua_space_backend:add_nodes(?MODULE, [Node]),
    ok.

store_reference(#opcua_reference{} = Reference) ->
    %FIXME: Temporary backward compatible hack
    opcua_space_backend:add_references(?MODULE, [Reference]),
    ok.
