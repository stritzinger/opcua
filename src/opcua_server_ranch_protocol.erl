-module(opcua_server_ranch_protocol).

-behavior(ranch_protocol).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Behaviour ranch_protocol Callback Functions
-export([start_link/4]).

%% System Callback Functions
-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

%% Internal Exported Functions
-export([connection_process/4]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER_VER, 0).
-define(DEFAULT_LINGER_TIMEOUT, 1000).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    parent          :: pid(),
    ref             :: ranch:ref(),
    conn            :: undefined | opcua:connection(),
    proto           :: term(),
    linger_timeout  :: pos_integer()
}).

-type state() :: #state{}.


%%% BEHAVIOUR ranch_protocol CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Ref, _Socket, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, connection_process,
                              [self(), Ref, Transport, Opts]),
    {ok, Pid}.


%%% SYSTEM CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

system_continue(_, _, State) ->
    loop(State).

-spec system_terminate(term(), term(), term(), term()) -> no_return().
system_terminate(Reason, _, _, State) ->
    terminate({stop, {exit, Reason}, 'sys:terminate/2,3 was called.'}, State).

system_code_change(Misc, _, _, _) ->
    {ok, Misc}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec connection_process(pid(), atom(), module(), term()) -> no_return().
connection_process(Parent, Ref, Transport, Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    init(Parent, Ref, Socket, Transport, Opts).

-spec init(pid(), atom(), inet:socket(), module(), term()) -> no_return().
init(Parent, Ref, Socket, Transport, Opts) ->
    ?LOG_DEBUG("Starting protocol handler"),
    {[LingerTimeout], ProtoOpts} = extract_options([
        {linger_timeout, ?DEFAULT_LINGER_TIMEOUT}
    ], Opts),
    PeerRes = Transport:peername(Socket),
    SockRes = Transport:sockname(Socket),
    UACPRes = opcua_server_uacp:new(ProtoOpts),
    case {PeerRes, SockRes, UACPRes} of
        {{ok, Peer}, {ok, Sock}, {ok, Proto}} ->
            Conn = #uacp_connection{
                pid = self(),
                socket = Socket,
                transport = Transport,
                peer = Peer,
                sock = Sock
            },
            State = #state{
                parent = Parent,
                ref = Ref,
                conn = Conn,
                proto = Proto,
                linger_timeout = LingerTimeout
            },
            loop(State);
        {{error, Reason}, _, _} ->
            handle_error(undefined, socket_error, Reason,
                'A socket error occurred when retrieving the peer name.');
        {_, {error, Reason}, _} ->
            handle_error(undefined, socket_error, Reason,
                'A socket error occurred when retrieving the sock name.');
        {_, _, {error, Reason}} ->
            handle_error(undefined, protocol_error, Reason,
                'A protocol error occurred when setting up UACP.')
    end.

loop(#state{conn = Conn} = State) ->
    #uacp_connection{socket = S, transport = T} = Conn,
    T:setopts(S, [{active, once}]),
    loop_consume(State).

loop_consume(#state{parent = P, conn = Conn, proto = Proto} = State) ->
    #uacp_connection{socket = S, transport = T} = Conn,
    {OK, Closed, Error} = T:messages(),
    Timeout = case opcua_server_uacp:can_produce(Conn, Proto) of
        true -> 3;
        false -> infinity
    end,
    receive
        {OK, S, Input} ->
            ?LOG_DEBUG("Received: ~p", [Input]),
            case opcua_server_uacp:handle_data(Input, Conn, Proto) of
                {ok, Proto2} ->
                    loop_produce(State#state{proto = Proto2}, fun loop/1);
                {stop, Reason, Proto2} ->
                    terminate(State#state{proto = Proto2}, Reason)
            end;
        {Closed, S} ->
            ?LOG_DEBUG("Socket closed"),
            handle_error(State, socket_error, closed, 'The socket has been closed.');
        {Error, S, Reason} ->
            ?LOG_DEBUG("Socket error: ~p", [Reason]),
            handle_error(State, socket_error, Reason, 'An error has occurred on the socket.');
        {'EXIT', P, Reason} ->
            ?LOG_DEBUG("Parent ~p died: ~p", [P, Reason]),
            handle_error(State, stop, {exit, Reason}, 'Parent process terminated.');
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, P, ?MODULE, [], State);
        Msg ->
            ?LOG_WARNING("Received unexpected message ~p", [Msg]),
            loop(State)
    after Timeout ->
        loop_produce(State, fun loop_consume/1)
    end.

-spec loop_produce(state(), function()) -> no_return().
loop_produce(#state{conn = Conn, proto = Proto} = State, Cont) ->
    #uacp_connection{socket = S, transport = T} = Conn,
    case opcua_server_uacp:produce(Conn, Proto) of
        {stop, Reason, Proto2} ->
            terminate(State#state{proto = Proto2}, Reason);
        {ok, Proto2} ->
            Cont(State#state{proto = Proto2});
        {ok, Output, Proto2} ->
            State2 = State#state{proto = Proto2},
            ?LOG_DEBUG("Sending:  ~p", [Output]),
            case T:send(S, Output) of
                ok -> Cont(State2);
                {error, Reason} ->
                    terminate(State2, Reason)
            end
    end.

handle_error(State, Kind, Reason, Msg) ->
    ?LOG_ERROR("Connection error ~w: ~s (~p)", [Kind, Msg, Reason]),
    terminate(State, {Kind, Reason, Msg}).

terminate(undefined, Reason) ->
    exit({shutdown, Reason});
terminate(State, Reason) ->
    State2 = terminate_produce(State),
    State3 = terminate_linger(State2),
    #state{conn = Conn, proto = Proto} = State3,
    opcua_server_uacp:terminate(Reason, Conn, Proto),
    exit({shutdown, Reason}).

terminate_produce(#state{conn = Conn, proto = Proto} = State) ->
    %TODO: we may want to do that for a maximum amount of time ?
    #uacp_connection{socket = S, transport = T} = Conn,
    case opcua_server_uacp:produce(Conn, Proto) of
        {ok, Proto2} -> #state{proto = Proto2};
        {ok, Output, Proto2} ->
            ?LOG_DEBUG("Sending:  ~p", [Output]),
            case T:send(S, Output) of
                ok -> terminate_produce(State#state{proto = Proto2});
                {error, Reason} ->
                    ?LOG_WARNING("Socket error while terminating the "
                                 "connection: ~p", [Reason]),
                    ok
            end
    end.

terminate_linger(#state{conn = Conn, linger_timeout = Timeout} = State) ->
    #uacp_connection{socket = S, transport = T} = Conn,
    case T:shutdown(S, write) of
        ok ->
            case Timeout of
                0 -> State;
                infinity -> terminate_linger_loop(State, undefined);
                Timeout ->
                    TRef = erlang:start_timer(Timeout, self(), linger_timeout),
                    terminate_linger_loop(State, TRef)
            end;
        {error, _} ->
            State
    end.

terminate_linger_loop(#state{conn = Conn} = State, TRef) ->
    #uacp_connection{socket = S, transport = T} = Conn,
    {OK, Closed, Error} = T:messages(),
    case T:setopts(S, [{active, once}]) of
        {error, _} -> State;
        ok ->
            receive
                {Closed, S} -> State;
                {Error, S, _} -> State;
                {timeout, TRef, linger_timeout} -> State;
                {OK, S, _} ->
                    %TODO: We may want to do something with this data ?
                    terminate_linger_loop(State, TRef);
                _ ->
                    terminate_linger_loop(State, TRef)
            end
    end.

extract_options(Spec, Opts) -> extract_options(Spec, Opts, []).

extract_options([], Opts, Acc) -> {lists:reverse(Acc), Opts};
extract_options([{Key, Def} | Rest], Opts, Acc) ->
    case maps:take(Key, Opts) of
        error -> extract_options(Rest, Opts, [Def | Acc]);
        {Val, Opts2} -> extract_options(Rest, Opts2, [Val | Acc])
    end.
