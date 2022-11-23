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
-export([resolve_encoding/1]).
-export([lookup_encoding/2]).
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


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ENCODING_TABLE, opcua_nodeset_encodings).
-define(DATATYPE_TABLE, opcua_nodeset_datatypes).
-define(NAMESPACE_URI_TABLE, opcua_nodeset_namespaces_uri).
-define(NAMESPACE_ID_TABLE, opcua_nodeset_namespaces_id).


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

resolve_encoding(NodeSpec) ->
    NodeId = opcua_node:id(NodeSpec),
    case ets:lookup(?ENCODING_TABLE, NodeId) of
        []            -> {NodeId, undefined};
        [{_, Result}] -> Result
    end.

lookup_encoding(NodeSpec, Encoding) ->
    NodeId = opcua_node:id(NodeSpec),
    Key = {NodeId, Encoding},
    case ets:lookup(?ENCODING_TABLE, Key) of
        []            -> {NodeId, undefined};
        [{_, Result}] -> {Result, Encoding}
    end.

schema(NodeSpec) ->
    NodeId = opcua_node:id(NodeSpec),
    proplists:get_value(NodeId, ets:lookup(?DATATYPE_TABLE, NodeId)).

namespace_uri(Id) when is_integer(Id), Id > 0 ->
    case ets:lookup(?NAMESPACE_URI_TABLE, Id) of
        [{_, Uri}] -> Uri;
        [] -> undefined
    end.

namespace_id(Uri) when is_binary(Uri) ->
    case ets:lookup(?NAMESPACE_ID_TABLE, Uri) of
        [{Id, _}] -> Id;
        [] -> undefined
    end.

namespaces() ->
    maps:from_list(ets:tab2list(?NAMESPACE_URI_TABLE)).

node(NodeSpec) ->
    opcua_space:get_node(nodeset, NodeSpec).

references(OriginNodeSpec) ->
    opcua_space:get_references(nodeset, OriginNodeSpec, #{}).

references(OriginNodeSpec, Opts) ->
    opcua_space:get_references(nodeset, OriginNodeSpec, Opts).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(undefined) ->
    ?LOG_DEBUG("OPCUA nodeset process starting empty", []),
    setup_encodings(),
    setup_datatypes(),
    setup_namespaces(),
    setup_space(),
    {ok, #state{}};
init(BaseDir) ->
    ?LOG_DEBUG("OPCUA nodeset process starting, loading nodeset from ~s", [BaseDir]),
    setup_encodings(),
    setup_datatypes(),
    setup_namespaces(),
    setup_space(),
    ?LOG_INFO("Loading OPCUA attributes mapping..."),
    load_attributes(BaseDir),
    ?LOG_INFO("Loading OPCUA status code mapping..."),
    load_status(BaseDir),
    ?LOG_INFO("Loading OPCUA namespaces..."),
    load_namespaces(BaseDir),
    ?LOG_INFO("Loading OPCUA nodes..."),
    load_nodes(BaseDir),
    ?LOG_INFO("Loading OPCUA references..."),
    load_references(BaseDir),
    ?LOG_INFO("Loading OPCUA data type schemas..."),
    load_datatypes(BaseDir),
    ?LOG_INFO("Loading OPCUA encoding specifications..."),
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

setup_encodings() ->
    ets:new(?ENCODING_TABLE, [named_table, {read_concurrency, true}]),
    ok.

setup_datatypes() ->
    ets:new(?DATATYPE_TABLE, [named_table, {read_concurrency, true}]),
    ok.

setup_namespaces() ->
    ets:new(?NAMESPACE_ID_TABLE, [named_table, {read_concurrency, true}, {keypos, 2}]),
    ets:new(?NAMESPACE_URI_TABLE, [named_table, {read_concurrency, true}, {keypos, 1}]),
    ok.

setup_space() ->
    opcua_space:init(nodeset),
    ok.

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
    KeyValuePairs = [{Key, DataType} || Key <- Keys],
    ets:insert(?DATATYPE_TABLE, KeyValuePairs),
    ok.

store_namespace({_Id, _Uri} = Spec) ->
    ets:insert(?NAMESPACE_ID_TABLE, Spec),
    ets:insert(?NAMESPACE_URI_TABLE, Spec),
    ok.

store_encoding({NodeId, {TargetNodeId, Encoding}}) ->
    ets:insert(?ENCODING_TABLE, [
        {NodeId, {TargetNodeId, Encoding}},
        {{TargetNodeId, Encoding}, NodeId}
    ]),
    ok.

store_node(#opcua_node{} = Node) ->
    opcua_space:add_nodes(nodeset, [Node]),
    ok.

store_reference(#opcua_reference{} = Reference) ->
    opcua_space:add_references(nodeset, [Reference]),
    ok.
