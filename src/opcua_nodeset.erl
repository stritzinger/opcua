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

setup(Dir) ->
    opcua_nodeset_encodings:setup(),
    opcua_nodeset_types:setup(),
    opcua_nodeset_namespaces:setup(),
    ?LOG_INFO("Loading OPCUA namespaces..."),
    load_namespaces(Dir),
    ?LOG_INFO("Loading OPCUA nodes..."),
    load_nodes(Dir),
    ?LOG_INFO("Loading OPCUA references..."),
    load_references(Dir),
    ?LOG_INFO("Loading OPCUA data type schemas..."),
    load_data_type_schemas(Dir),
    ?LOG_INFO("Loading OPCUA encoding specifications..."),
    load_encodings(Dir).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_namespaces(Dir) ->
    load_all_terms(Dir, "namespaces", fun({ID, URI}) ->
        opcua_nodeset_namespaces:store(ID, URI)
    end).

load_nodes(Dir) ->
    load_all_terms(Dir, "nodes", fun(Node) ->
        opcua_address_space:add_nodes(default, [Node])
    end).

load_references(Dir) ->
    load_all_terms(Dir, "references", fun(Reference) ->
        opcua_address_space:add_references(default, [Reference])
    end).

load_data_type_schemas(Dir) ->
    load_all_terms(Dir, "data_type_schemas", fun({Keys, DataType}) ->
        opcua_nodeset_types:store(Keys, DataType)
    end).

load_encodings(Dir) ->
    load_all_terms(Dir, "encodings", fun({NodeId, {TargetNodeId, Encoding}}) ->
        opcua_nodeset_encodings:store(NodeId, TargetNodeId, Encoding)
    end).

load_all_terms(DirPath, Tag, Fun) ->
    Pattern = filename:join(DirPath, "**/*." ++ Tag ++ ".bterm"),
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
