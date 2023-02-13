-module(opcua_server_ranch_protocol).

-behavior(ranch_protocol).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Behaviour ranch_protocol Callback Functions
-export([start_listener/0]).
-export([stop_listener/0]).
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
    transport       :: module(),
    socket          :: inet:socket(),
    conn            :: undefined | opcua:connection(),
    proto           :: term(),
    linger_timeout  :: infinity | non_neg_integer()
}).

-type state() :: #state{}.


%%% BEHAVIOUR ranch_protocol CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_listener() ->
    TOpts = [{port, 4840}],
    ranch:start_listener(?MODULE, ranch_tcp, TOpts, ?MODULE, #{}).

start_link(Ref, _Socket, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, connection_process,
                              [self(), Ref, Transport, Opts]),
    {ok, Pid}.

stop_listener() ->
    ranch:stop_listener(?MODULE).


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
    {[LingerTimeout, Ident, ParentKeychain], ProtoOpts} = extract_options([
        {linger_timeout, ?DEFAULT_LINGER_TIMEOUT},
        {identity, undefined},
        {keychain, default}
    ], Opts),
    PeerNameRes = Transport:peername(Socket),
    SockNameRes = Transport:sockname(Socket),
    UACPRes = opcua_server_uacp:init(ProtoOpts),
    {ok, Keychain} = opcua_keychain_ets:new(ParentKeychain),
    try
        ResolvedIdent = resolve_identity(Keychain, Ident),
        case {PeerNameRes, SockNameRes, UACPRes} of
            {{ok, PeerName}, {ok, {SockAddr, _} = SockName}, {ok, Proto}} ->
                Endpoint = opcua_util:parse_endpoint({SockAddr, 4840}),
                State = #state{
                    parent = Parent,
                    ref = Ref,
                    transport = Transport,
                    socket = Socket,
                    conn = opcua_connection:new(opcua_server_space, Keychain,
                        ResolvedIdent, Endpoint, PeerName, SockName),
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
        end
    catch
        throw:ErrorReason ->
            handle_error(undefined, config_error, ErrorReason,
                    'An error occured due to invalid configuration,')
    end.

resolve_identity(Keychain, undefined) ->
    case opcua_keychain:lookup(Keychain, alias, server) of
        not_found -> undefined;
        [Id | _] -> Id
    end;
resolve_identity(Keychain, Id) ->
    case opcua_keychain:info(Keychain, Id) of
        not_found -> throw(invalid_server_identity);
        #{id := Id} -> Id
    end.

loop(#state{transport = T, socket = S} = State) ->
    T:setopts(S, [{active, once}]),
    loop_consume(State).

loop_consume(State) ->
    #state{parent = P, transport = T, socket = S,
           conn = Conn, proto = Proto} = State,
    {OK, Closed, Error} = T:messages(),
    Timeout = case opcua_server_uacp:can_produce(Conn, Proto) of
        true -> 0;
        false -> infinity
    end,
    receive
        {OK, S, Input} ->
            ?DUMP("Received data: ~p", [Input]),
            case opcua_server_uacp:handle_data(Input, Conn, Proto) of
                {ok, Conn2, Proto2} ->
                    State2 = State#state{conn = Conn2, proto = Proto2},
                    loop_produce(State2, fun loop/1);
                {error, Reason, Proto2} ->
                    terminate(State#state{proto = Proto2}, Reason)
            end;
        {Closed, S} ->
            ?LOG_DEBUG("Socket closed"),
            terminate(State, normal);
        {Error, S, Reason} ->
            ?LOG_DEBUG("Socket error: ~p", [Reason]),
            handle_error(State, socket_error, Reason, 'An error has occurred on the socket.');
        {'EXIT', P, Reason} ->
            ?LOG_DEBUG("Parent ~p died: ~p", [P, Reason]),
            handle_error(State, stop, {exit, Reason}, 'Parent process terminated.');
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, P, ?MODULE, [], State);
        {opcua_connection, Notif} ->
            ?LOG_DEBUG("Server OPCUA connection: ~p", [Notif]),
            loop_consume(State);
        Msg ->
            ?LOG_WARNING("Received unexpected message ~p", [Msg]),
            loop(State)
    after Timeout ->
        loop_produce(State, fun loop_consume/1)
    end.

-spec loop_produce(state(), function()) -> no_return().
loop_produce(State, Cont) ->
    #state{transport = T, socket = S, conn = Conn, proto = Proto} = State,
    case opcua_server_uacp:produce(Conn, Proto) of
        {error, Reason, Proto2} ->
            terminate(State#state{proto = Proto2}, Reason);
        {ok, Conn2, Proto2} ->
            Cont(State#state{conn = Conn2, proto = Proto2});
        {ok, Output, Conn2, Proto2} ->
            State2 = State#state{conn = Conn2, proto = Proto2},
            ?DUMP("Sending data:  ~p", [Output]),
            case T:send(S, Output) of
                ok -> Cont(State2);
                {error, Reason} ->
                    terminate(State2, Reason)
            end
    end.

% Add spec to make dialyzer happy
-spec handle_error(state() | undefined, term(), term(), term()) -> no_return().
handle_error(State, Kind, Reason, Msg) ->
    ?LOG_ERROR("Connection error ~w: ~s (~p)", [Kind, Msg, Reason]),
    terminate(State, {Kind, Reason, Msg}).

terminate(undefined, Reason) ->
    ?LOG_WARNING("Connection terminated: ~w", [Reason]),
    exit({shutdown, Reason});
terminate(State, Reason) ->
    ?LOG_WARNING("Connection terminated: ~w", [Reason]),
    State2 = terminate_produce(State),
    State3 = terminate_linger(State2),
    #state{conn = Conn, proto = Proto} = State3,
    opcua_server_uacp:terminate(Reason, Conn, Proto),
    exit({shutdown, Reason}).

terminate_produce(State) ->
    %TODO: we may want to do that for a maximum amount of time ?
    #state{socket = S, transport = T, conn = Conn, proto = Proto} = State,
    case opcua_server_uacp:produce(Conn, Proto) of
        {ok, Conn2, Proto2} ->
            State#state{conn = Conn2, proto = Proto2};
        {ok, Output, Conn2, Proto2} ->
            ?LOG_DEBUG("Sending:  ~p", [Output]),
            State2 = State#state{conn = Conn2, proto = Proto2},
            case T:send(S, Output) of
                ok -> terminate_produce(State2);
                {error, Reason} ->
                    ?LOG_WARNING("Socket error while terminating the "
                                 "connection: ~p", [Reason]),
                    State2
            end
    end.

terminate_linger(State) ->
    #state{socket = S, transport = T, linger_timeout = Timeout} = State,
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

terminate_linger_loop(#state{socket = S, transport = T} = State, TRef) ->
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
