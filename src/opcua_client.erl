-module(opcua_client).

-behaviour(gen_statem).
% Insipired by: https://gist.github.com/ferd/c86f6b407cf220812f9d893a659da3b8


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API
-export([connect/1]).
-export([request/2]).
-export([start_link/1]).

%% Behaviour gen_statem callback functions
-export([init/1]).
-export([callback_mode/0]).
-export([handle_event/4]).
-export([terminate/3]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

connect(Endpoint) ->
    opcua_client_sup:start_client(Endpoint).

request(Client, Req) ->
    gen_statem:call(Client, {request, Req}).

start_link(Endpoint) ->
    gen_statem:start_link(?MODULE, {Endpoint}, []).


%%% BEHAVIOUR gen_statem CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Endpoint}) ->
    {ok, {disconnected, 0}, #{endpoint => Endpoint}}.

callback_mode() -> [handle_event_function, state_enter].

%% STATE: {disconnected, N}
handle_event(enter, _OldState, {disconnected, N} = State, Data) ->
    ?LOG_DEBUG("Client ~p entered ~p", [self(), State]),
    {next_state, {disconnected, N}, Data, [
        {state_timeout, timeout(State), retry}
    ]};
handle_event(state_timeout, retry, {disconnected, N}, Data) ->
    case conn_init(Data) of
        {ok, Socket}     -> {next_state, {connected, Socket}, Data};
        {error, _Reason} -> {next_state, {disconnected, N + 1}, Data}
    end;
handle_event({call, From}, {request, _}, {disconnected, N}, Data) ->
    {next_state, {disconnected, N}, Data, [
        {reply, From, {error, disconnected}}
    ]};
%% STATE: {connected, Socket}
handle_event(enter, _OldState, {connected, Socket} = State, Data) ->
    ?LOG_DEBUG("Client ~p entered ~p", [self(), State]),
    {next_state, {connected, Socket}, Data, [
        {timeout, timeout(State), to_idle}
    ]};
handle_event({call, From}, {request, Req}, {connected, Socket} = State, Data) ->
    conn_send(Socket, Req),
    case conn_receive(Socket, 5000) of
        {error, Reason} ->
            conn_close(Socket),
            {next_state, {disconnected, 0}, Data, [
                {reply, From, {error, Reason}}
            ]};
        {ok, Resp} ->
            {next_state, {connected, Socket}, Data, [
                {reply, From, {ok, Resp}},
                {timeout, timeout(State), to_idle}
            ]}
    end;
handle_event(timeout, to_idle, {connected, Socket}, Data) ->
    {next_state, {idle, Socket}, Data};
%% STATE: {idle, Socket}
handle_event(enter, _OldState, {idle, Socket} = State, Data) ->
    ?LOG_DEBUG("Client ~p entered ~p", [self(), State]),
    {next_state, {idle, Socket}, Data, [
        {state_timeout, timeout(State), disconnect}
    ]};
handle_event({call, _From}, {request, _}, {idle, Socket}, Data) ->
    {next_state, {connected, Socket}, Data, [postpone]};
handle_event(state_timeout, disconnect, {idle, Socket}, Data) ->
    conn_close(Socket),
    {next_state, disconnected, Data};
%% STATE: disconnected
handle_event(enter, _OldState, disconnected = State, Data) ->
    ?LOG_DEBUG("Client ~p entered ~p", [self(), State]),
    {next_state, disconnected, Data, [hibernate]};
handle_event({call, From}, {request, _}, disconnected, Data) ->
    {next_state, {disconnected, 0}, Data, [
        {reply, From, {error, waking_up}}
    ]}.

terminate(_, _, _) -> error(not_implemented).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Connection

conn_init(#{endpoint := Endpoint}) ->
    % TODO: Implement actual socket. Can we share implementation with server?
    ?LOG_DEBUG("Connect: ~p", [Endpoint]),
    {ok, undefined}.

conn_send(_Socket, _Req) ->
    ?LOG_DEBUG("Send request: ~p", [_Req]),
    ok.

conn_receive(_Socket, _Timeout) ->
    ?LOG_DEBUG("Receive with timeout: ~p", [_Timeout]),
    {ok, 'RESPONSE'}.

conn_close(_Conn) ->
    ?LOG_DEBUG("Connection closed"),
    ok.


%% Other

timeout({disconnected, 0})    -> 0;
timeout({disconnected, 1})    -> 500;
timeout({disconnected, 2})    -> 1000;
timeout({disconnected, 3})    -> 3000;
timeout({disconnected, _})    -> 10000;
timeout({connected, _Socket}) -> 1000;
timeout({idle, _Socket})      -> 60000.
