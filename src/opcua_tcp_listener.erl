-module(opcua_tcp_listener).

-export([start/0, listen/1]).

start() ->
	{ok, LSock} = gen_tcp:listen(4840, [binary, {packet, 0}, {active, false}]),
	Pid = spawn(?MODULE, listen, [LSock]),
	{ok, Pid}.

listen(LSock) ->
	{ok, Sock} = gen_tcp:accept(LSock),
	{ok, Chunk} = gen_tcp:recv(Sock, 0),
	io:format("got ~p bytes: ~s~n~n", [size(Chunk), opcua_util:bin_to_hex(Chunk)]),

	{ok, #{message_type := <<"HEL">>} = Hello} = opcua_oacp:decode(Chunk),
	io:format("decoded HEL message: ~p~n", [Hello]),

	%% TODO for getting a session:
	%% 1. hello ACK
	%% 2. Open Secure Channel Request/Response
	%% 3. Create Session Request/Response
	%% 4. Activate Session Request/Response

	gen_tcp:close(Sock),
	gen_tcp:close(LSock).
