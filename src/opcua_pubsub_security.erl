-module(opcua_pubsub_security).

-export([lock/1]).
-export([unlock/1]).

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
lock(Binary) ->
    Binary.

unlock(Binary) ->
    Binary.

%%% INTERNALS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
