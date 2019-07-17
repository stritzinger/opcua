-module(opcua_protocol).

-behavior(ranch_protocol).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua_conn.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Behaviour ranch_protocol Callback Functions
-export([start_link/4]).

%% System Callback Functions
-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

%% Internal Exported Functions
-export([connection_process/4]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    parent :: pid(),
    ref :: ranch:ref(),
    conn :: opcua_conn(),
    sub :: term()
}).


%%% BEHAVIOUR ranch_protocol CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Ref, _Socket, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, connection_process,
                              [self(), Ref, Transport, Opts]),
    {ok, Pid}.


%%% SYSTEM CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

system_continue(_, _, {State, Buff}) ->
    loop(State, Buff).


system_terminate(Reason, _, _, {State, _}) ->
    terminate({stop, {exit, Reason}, 'sys:terminate/2,3 was called.'}, State).


system_code_change(Misc, _, _, _) ->
    {ok, Misc}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

connection_process(Parent, Ref, Transport, Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    init(Parent, Ref, Socket, Transport, Opts).


init(Parent, Ref, Socket, Transport, Opts) ->
    ?LOG_DEBUG("Starting protocol handler"),
    PeerRes = Transport:peername(Socket),
    SockRes = Transport:sockname(Socket),
    case {PeerRes, SockRes} of
        {{ok, Peer}, {ok, Sock}} ->
            Conn = #opcua_conn{
                socket = Socket,
                transport = Transport,
                opts = Opts,
                peer = Peer,
                sock = Sock
            },
            State = #state{
                parent = Parent,
                ref = Ref,
                conn = Conn
            },
            case opcua_protocol_uacp:init(Conn) of
                {ok, Conn2, Sub} ->
                    loop(State#state{conn = Conn2, sub = Sub}, <<>>);
                {error, Reason} ->
                    terminate(State, {protocol_error, Reason,
                        'A protocol error occurred when initializing UACP.'})
            end;
        {{error, Reason}, _} ->
            terminate(undefined, {socket_error, Reason,
                'A socket error occurred when retrieving the peer name.'});
        {_, {error, Reason}} ->
            terminate(undefined, {socket_error, Reason,
                'A socket error occurred when retrieving the sock name.'})
    end.


loop(#state{parent = P, conn = Conn} = State, Buff) ->
    #opcua_conn{socket = S, transport = T} = Conn,
    {OK, Closed, Error} = T:messages(),
    T:setopts(S, [{active, once}]),
    receive
        {OK, S, Data} ->
            ?LOG_DEBUG("Protocol handler's socket received data: ~p", [Data]),
            parse(State, <<Buff/binary, Data/binary >>);
        {Closed, S} ->
            ?LOG_DEBUG("Protocol handler's socket closed"),
            terminate(State, {socket_error, closed, 'The socket has been closed.'});
        {Error, S, Reason} ->
            ?LOG_DEBUG("Protocol handler's socket error: ~p", [Reason]),
            terminate(State, {socket_error, Reason, 'An error has occurred on the socket.'});
        {'EXIT', P, Reason} ->
            ?LOG_DEBUG("Protocol handler's parent died"),
            terminate(State, {stop, {exit, Reason}, 'Parent process terminated.'});
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, P, ?MODULE, [], {State, Buff});
        Msg ->
            ?LOG_WARNING("Received unexpected message ~p", [Msg]),
            loop(State, Buff)
    end.


parse(#state{conn = Conn, sub = Sub} = State,
      <<MsgType:3/binary, ChunkType:1/binary,
        FullLen:32/unsigned-little-integer, Rest/binary>> = Data) ->
    MsgLen = FullLen - 8,
    case Rest of
        <<Msg:MsgLen/binary, Buff/binary>> ->
            case opcua_protocol_uacp:handle_message(MsgType, ChunkType, Msg, Conn, Sub) of
                {ok, Conn2, Sub2} ->
                    loop(State#state{conn = Conn2, sub = Sub2}, Buff);
                {error, Reason} ->
                    terminate(State, Reason)
            end;
        _ ->
            %%TODO: Check maximum buffer size
            loop(State, Data)
    end;

parse(State, Buff) ->
    loop(State, Buff).


terminate(undefined, Reason) ->
    exit({shutdown, Reason});

terminate(State, Reason) ->
    terminate_linger(State),
    exit({shutdown, Reason}).


terminate_linger(#state{conn = Conn} = State) ->
    #opcua_conn{socket = S, transport = T, opts = O} = Conn,
    case T:shutdown(S, write) of
        ok ->
            case maps:get(linger_timeout, O, 1000) of
                0 -> ok;
                infinity -> terminate_linger_loop(State, undefined);
                Timeout ->
                    TRef = erlang:start_timer(Timeout, self(), linger_timeout),
                    terminate_linger_loop(State, TRef)
            end;
        {error, _} ->
            ok
    end.


terminate_linger_loop(#state{conn = Conn} = State, TRef) ->
    #opcua_conn{socket = S, transport = T} = Conn,
    {OK, Closed, Error} = T:messages(),
    case T:setopts(S, [{active, once}]) of
        {error, _} -> ok;
        ok ->
            receive
                {Closed, S} -> ok;
                {Error, S, _} -> ok;
                {timeout, TRef, linger_timeout} -> ok;
                {OK, S, _} ->
                    terminate_linger_loop(State, TRef);
                _ ->
                    terminate_linger_loop(State, TRef)
            end
    end.
