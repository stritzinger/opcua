%%%-------------------------------------------------------------------
%% @doc opcua public API
%% @end
%%%-------------------------------------------------------------------

-module(opcua_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case opcua_sup:start_link() of
        {ok, Pid} ->
            opcua:start_listener(),
            opcua:load_information_models(),
            {ok, Pid};
        Other ->
            Other
    end.

stop(_State) ->
    opcua:stop_listener(),
    ok.

%% internal functions
