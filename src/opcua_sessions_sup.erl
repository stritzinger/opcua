-module(opcua_sessions_sup).

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
        id => opcua_session_manager,
        start => {opcua_session_manager, start_link, [#{}]}
    },
    SessionSupSpec = #{
        id => opcua_sessions_sup,
        type => supervisor,
        start => {opcua_session_sup, start_link, []}
    },
    {ok, {SupFlags, [SessionManagerSpec, SessionSupSpec]}}.
