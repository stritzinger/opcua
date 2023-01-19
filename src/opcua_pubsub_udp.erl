-module(opcua_pubsub_udp).

-export([start_link/1]).

-export([send/1]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include_lib("kernel/include/logger.hrl").

-record(state, {
    connection_id,
    socket
}).

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

send(Data) ->
    gen_server:cast(?MODULE, {?FUNCTION_NAME, Data}).

init([#{
        connection_id := ConnectionId,
        uri := #{
            host := BinaryIP,
            port := Port
        }
        }]) ->
    MulticastGroup = parse_ip(BinaryIP),
    InterfaceIP = get_ip_of_valid_interface(),
    ?LOG_DEBUG("PubSub UDP using interface ~p",[InterfaceIP]),
    Opts = [
        binary,
        {active, true},
        {reuseaddr, true},
        {ip, MulticastGroup},
        {multicast_ttl, 10}
    ],
    case gen_udp:open(Port, Opts) of
        {ok, Socket} ->
            inet:setopts(Socket, [{add_membership,{MulticastGroup,InterfaceIP}}]),
            {ok, #state{
                connection_id = ConnectionId,
                socket = Socket
            }};
        {error, Reason} -> {error, Reason}
    end.

handle_call(_, _, State) ->
    {reply, ok, State}.

% handle_cast(disconnect, State) ->
%     gen_udp:close(State#state.socket),
%     {noreply, State#state{ socket = undefined}};
handle_cast({send, Data}, #state{socket = Socket} = State) ->
    gen_udp:send(Socket, Data),
    {noreply, State}.

handle_info({udp, Socket, _IP, _Port, Packet},
        #state{socket = Socket, connection_id = ConnectionId} = S) ->
    % io:format("~n~nFrom: ~p~nPort: ~p~nData: ~p~n",[_IP,_Port,Packet]),
    opcua_pubsub:new_network_message(ConnectionId, Packet),
    {noreply, S}.


% helpers %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
parse_ip(BinaryIP) ->
    [A,B,C,D] = [ binary_to_integer(N) || N <- string:split(BinaryIP, ".", all)],
    {A,B,C,D}.

get_ip_of_valid_interface() ->
    case get_valid_interfaces() of
        [ {_Name, Opts} | _] -> get_ipv4_from_opts(Opts);
        _ -> undefined
    end.

get_valid_interfaces() ->
    {ok, Interfaces} = inet:getifaddrs(),
    Selected = [ I
     || {_Name, [{flags, Flags} | Opts]} = I <- Interfaces,
        flags_are_ok(Flags),
        has_ipv4(Opts)
    ],
    HasLoopback = fun({_Name, [{flags, Flags} | _]}) ->
                        lists:member(loopback, Flags)
                  end,
    {LoopBack, Others} = lists:partition(HasLoopback, Selected),
    Others ++ LoopBack.

has_ipv4(Opts) ->
    get_ipv4_from_opts(Opts) =/= undefined.

flags_are_ok(Flags) ->
    lists:member(up, Flags) and
        lists:member(running, Flags).

get_ipv4_from_opts([]) ->
    undefined;
get_ipv4_from_opts([{addr, {_1, _2, _3, _4}} | _]) ->
    {_1, _2, _3, _4};
get_ipv4_from_opts([_ | TL]) ->
    get_ipv4_from_opts(TL).
