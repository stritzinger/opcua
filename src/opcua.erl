-module(opcua).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_listener/0]).
-export([stop_listener/0]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(REF, opcua).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_listener() ->
	TOpts = [{port, 4840}],
	case ranch:start_listener(?REF, ranch_tcp, TOpts, opcua_protocol, #{}) of
	    {error, _Reason} = Error -> Error;
		{ok, _} -> ok
	end.

stop_listener() ->
	ranch:stop_listener(?REF).
