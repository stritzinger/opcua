-module(opcua_nodeset).

% Original NodeSet from https://github.com/OPCFoundation/UA-Nodeset/tree/v1.04/Schema

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([setup/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup(BaseDir) ->
    opcua_nodeset_attributes:setup(),
    opcua_nodeset_status:setup(),
    opcua_nodeset_encodings:setup(),
    opcua_nodeset_datatypes:setup(),
    opcua_nodeset_namespaces:setup(),
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
    load_encodings(BaseDir).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_attributes(BaseDir) ->
    load_all_terms(BaseDir, "attributes", fun opcua_nodeset_attributes:store/1).

load_status(BaseDir) ->
    load_all_terms(BaseDir, "status", fun opcua_nodeset_status:store/1).

load_namespaces(BaseDir) ->
    load_all_terms(BaseDir, "namespaces", fun opcua_nodeset_namespaces:store/1).

load_nodes(BaseDir) ->
    load_all_terms(BaseDir, "nodes", fun(Node) ->
        opcua_address_space:add_nodes(default, [Node])
    end).

load_references(BaseDir) ->
    load_all_terms(BaseDir, "references", fun(Reference) ->
        opcua_address_space:add_references(default, [Reference])
    end).

load_datatypes(BaseDir) ->
    load_all_terms(BaseDir, "datatypes", fun opcua_nodeset_datatypes:store/1).

load_encodings(BaseDir) ->
    load_all_terms(BaseDir, "encodings", fun opcua_nodeset_encodings:store/1).

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
