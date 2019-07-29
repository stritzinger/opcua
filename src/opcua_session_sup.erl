-module(opcua_session_sup).

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
    SupFlags = #{strategy => simple_one_for_one},
    SessionSpec = #{
        id => opcua_session,
        start => {opcua_session, start_link, [#{}]}
    },
    {ok, {SupFlags, [SessionSpec]}}.
