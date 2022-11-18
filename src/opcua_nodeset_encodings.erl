-module(opcua_nodeset_encodings).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([resolve/1]).
-export([lookup/2]).

%% Functions to be used only by opcua_nodeset
-export([setup/0]).
-export([store/1]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

resolve(NodeId) ->
    case ets:lookup(?MODULE, NodeId) of
        []            -> {NodeId, undefined};
        [{_, Result}] -> Result
    end.

lookup(NodeId, Encoding) ->
    Key = {NodeId, Encoding},
    case ets:lookup(?MODULE, Key) of
        []            -> {NodeId, undefined};
        [{_, Result}] -> {Result, Encoding}
    end.


%%% PROTECTED FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup() ->
    ets:new(?MODULE, [named_table]),
    ok.

store({NodeId, {TargetNodeId, Encoding}}) ->
    ets:insert(?MODULE, [
        {NodeId, {TargetNodeId, Encoding}},
        {{TargetNodeId, Encoding}, NodeId}
    ]),
    ok.
