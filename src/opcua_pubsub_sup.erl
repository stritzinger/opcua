-module(opcua_pubsub_sup).

-behaviour(supervisor).

%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/0]).

%% Behaviour supervisor callback functions
-export([init/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%% BEHAVIOUR supervisor CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    Childs = [
        supervisor(opcua_pubsub_middleware_sup, []),
        worker(opcua_pubsub, [])],
    {ok, {#{strategy => one_for_all}, Childs}}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

worker(Module, Args) ->
    #{id => Module, start => {Module, start_link, Args}}.

supervisor(Module, Args) ->
    #{id => Module, type => supervisor, start => {Module, start_link, Args}}.
