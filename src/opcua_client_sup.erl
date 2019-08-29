-module(opcua_client_sup).

-behaviour(supervisor).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([start_link/0]).
-export([start_client/1]).

%% Behaviour supervisor callback functions
-export([init/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_client(Endpoint) ->
    supervisor:start_child(?MODULE, [Endpoint]).


%%% BEHAVIOUR supervisor CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    {ok, {#{strategy => simple_one_for_one}, [#{
        id      => undefined,
        restart => permanent,
        start   => {opcua_client, start_link, []}
    }]}}.
