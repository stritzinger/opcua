-module(opcua).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_listener/0]).
-export([stop_listener/0]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(REF, opcua).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type uint32() :: 0..4294967295.
-type optional(Type) :: undefined | Type.

-export_type([
    uint32/0,
    optional/1
]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_listener() ->
	TOpts = [{port, 4840}],
	case ranch:start_listener(?REF, ranch_tcp, TOpts, opcua_protocol, #{}) of
	    {error, _Reason} = Error -> Error;
		{ok, _} -> ok
	end.

stop_listener() ->
	ranch:stop_listener(?REF).
