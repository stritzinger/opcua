-module(opcua_pubsub_udp).


-export([init/1, send/2, handle_info/2]).

-include_lib("kernel/include/logger.hrl").

-record(state, {
    socket,
    out_socket
}).


init(#{
        uri := #{
            host := BinaryIP,
            port := Port
        }
        }) ->
    MulticastGroup = parse_ip(BinaryIP),
    InterfaceIP = get_ip_of_valid_interface(),
    ?LOG_DEBUG("PubSub UDP using interface ~p",[InterfaceIP]),
    Opts = [
        binary,
        {active, true},
        {reuseaddr, true},
        {ip, MulticastGroup},
        {multicast_ttl, 10},
        {multicast_loop, false}
    ],
    case gen_udp:open(Port, Opts) of
        {ok, Socket} ->
            inet:setopts(Socket, [{add_membership,{MulticastGroup, InterfaceIP}}]),
            {ok, S} = gen_udp:open(0),
            {ok, #state{
                socket = Socket,
                out_socket = S
            }};
        {error, Reason} -> {error, Reason}
    end.

send(Data, #state{out_socket = Socket} = S) ->
    ok = gen_udp:send(Socket, {224,0,0,22}, 4840, Data),
    S.

handle_info({udp, Socket, _IP, _Port, Packet}, #state{socket = Socket} = S) ->
    Packet.

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
