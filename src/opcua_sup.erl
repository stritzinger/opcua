%%%-------------------------------------------------------------------
%% @doc opcua top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(opcua_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
	supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
	SupFlags = #{strategy => one_for_all, intensity => 0, period => 1},
    RegistrySpec = #{
        id => opcua_registry,
        start => {opcua_registry, start_link, [[]]}
    },
	{ok, {SupFlags, [RegistrySpec]}}.

%% internal functions
