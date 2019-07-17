-module(opcua_conn).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua_conn.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([opt/2, opt/3]).
-export([send/3]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

opt(Key, #opcua_conn{opts = Opts}) -> maps:get(Key, Opts).


opt(Key, #opcua_conn{opts = Opts}, Default) -> maps:get(Key, Opts, Default).


send(Type, Data, #opcua_conn{transport = T, socket = S} = Conn) ->
    DataSize = iolist_size(Data) + 8,
    Header = <<Type:3/binary, $F, DataSize:32/little-unsigned-integer>>,
    ?LOG_DEBUG("Sending ~p", [[Header, Data]]),
    case T:send(S, [Header, Data]) of
        {error, _Reason} = Error -> Error;
        ok -> {ok, Conn}
    end.
