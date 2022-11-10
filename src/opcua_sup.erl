-module(opcua_sup).

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
    KeychainOpts = application:get_env(opcua, keychain, #{}),
    Childs = [
        worker(opcua_keychain_default, [KeychainOpts]),
        supervisor(opcua_address_space_sup, []),
        worker(opcua_database, [#{}]),
        supervisor(opcua_client_sup, [])
    ],
    Childs2 = case application:get_env(start_server) of
        {ok, false} -> Childs;
        _ -> Childs ++ [supervisor(opcua_server_sup, [])]
    end,
    {ok, {#{strategy => one_for_one}, Childs2}}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

worker(Module, Args) ->
    #{id => Module, start => {Module, start_link, Args}}.

supervisor(Module, Args) ->
    #{id => Module, type => supervisor, start => {Module, start_link, Args}}.
