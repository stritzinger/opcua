-module(opcua_server_session_sup).

-behaviour(supervisor).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([start_link/0]).

%% Behaviour supervisor callback functions
-export([init/1]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


%%% BEHAVIOUR supervisor CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    SupFlags = #{strategy => one_for_all, intensity => 0, period => 1},
    SessionManagerSpec = #{
        id => opcua_server_session_manager,
        start => {opcua_server_session_manager, start_link, [#{}]}
    },
    SessionSupSpec = #{
        id => opcua_server_session_pool_sup,
        type => supervisor,
        start => {opcua_server_session_pool_sup, start_link, []}
    },
    {ok, {SupFlags, [SessionManagerSpec, SessionSupSpec]}}.
