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
-export([attributes/0]).
-export([attribute_name/1]).
-export([attribute_type/1]).
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
-export([is_subtype/2]).
-export([browse_path/2]).
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

attributes() ->
    maps:values(persistent_term:get({?MODULE, attributes})).

attribute_name(Attr) ->
    {_, Name} = persistent_term:get({?MODULE, attribute, Attr}),
    Name.

%TODO: Why would we load the names from the specs but we would hardcode the types ???
attribute_type(node_id) -> node_id;
attribute_type(node_class) -> ?NNID(257);
attribute_type(browse_name) -> qualified_name;
attribute_type(display_name) -> localized_text;
attribute_type(description) -> localized_text;
attribute_type(write_mask) -> ?NNID(347);
attribute_type(user_write_mask) -> ?NNID(347);
attribute_type(is_abstract) -> boolean;
attribute_type(symmetric) -> boolean;
attribute_type(inverse_name) -> localized_text;
attribute_type(contains_no_loops) -> boolean;
attribute_type(event_notifier) -> byte_string;
attribute_type(value) -> variant;
attribute_type(data_type) -> node_id;
attribute_type(value_rank) -> int32;
attribute_type(array_dimensions) -> uint32;
attribute_type(access_level) -> ?NNID(15031);
attribute_type(user_access_level) -> ?NNID(15031);
attribute_type(minimum_sampling_interval) -> double;
attribute_type(historizing) -> boolean;
attribute_type(executable) -> boolean;
attribute_type(user_executable) -> boolean;
attribute_type(data_type_definition) -> ?NNID(97);
attribute_type(role_permissions) -> ?NNID(96); % It is a list
attribute_type(user_role_permissions) -> ?NNID(96); % It is a list
attribute_type(access_restrictions) -> ?NNID(95); % It is a list
attribute_type(access_level_ex) -> ?NNID(95); % It is a list
attribute_type(_Attr) -> error(bad_attribute_id_invalid).

attribute_id(Attr) ->
    {Id, _} = persistent_term:get({?MODULE, attribute, Attr}),
    Id.

status(Status) ->
    persistent_term:get({?MODULE, status, Status}).

status_name(Status) ->
    {_, Name, _} = persistent_term:get({?MODULE, status, Status}),
    Name.

status_name(Status, Default) ->
    {_, Name, _} = persistent_term:get({?MODULE, status, Status},
                                       {undefined, Default, undefined}),
    Name.

status_code(Status) ->
    {Code, _, _} = persistent_term:get({?MODULE, status, Status}),
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

schema(TypeSpec) ->
    opcua_space_backend:schema(?MODULE, TypeSpec).

namespace_uri(Id) ->
    opcua_space_backend:namespace_uri(?MODULE, Id).

namespace_id(Uri) ->
    opcua_space_backend:namespace_id(?MODULE, Uri).

namespaces() ->
    opcua_space_backend:namespaces(?MODULE).

is_subtype(SubTypeSpec, SuperTypeSpec) ->
    opcua_space_backend:is_subtype(?MODULE, SubTypeSpec, SuperTypeSpec).

browse_path(Source, Path) ->
    opcua_space:browse_path({opcua_space_backend, ?MODULE}, Source, Path).

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
    load_nodesets(BaseDir),
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

load_nodesets(BaseDir) ->
    load_all_terms(BaseDir, "space", fun(Term) ->
        opcua_space_backend:store(?MODULE, Term)
    end).

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
    Attributes = persistent_term:get({?MODULE, attributes}, #{}),
    Attributes2 = Attributes#{Id => Name},
    persistent_term:put({?MODULE, attributes}, Attributes2),
    persistent_term:put({?MODULE, attribute, Id}, Spec),
    persistent_term:put({?MODULE, attribute, Name}, Spec),
    ok.

store_status({Code, Name, Desc} = Spec)
  when is_integer(Code), is_atom(Name), is_binary(Desc) ->
    persistent_term:put({?MODULE, status, Code}, Spec),
    persistent_term:put({?MODULE, status, Name}, Spec),
    ok.
