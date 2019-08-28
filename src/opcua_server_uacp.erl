-module(opcua_server_uacp).

-behaviour(opcua_uacp).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([new/1]).
-export([can_produce/2]).
-export([produce/2]).
-export([handle_data/3]).
-export([terminate/3]).

%% Behaviour opcua_uacp callback functions
-export([init/1]).
-export([channel_allocate/1]).
-export([channel_release/2]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    uacp                            :: term(),
    sess                            :: undefined | pid()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Opts) ->
    case opcua_uacp:new(server, ?MODULE, Opts) of
        {ok, Sub} -> {ok, #state{uacp = Sub}};
        Error -> Error
    end.

can_produce(Conn, #state{uacp = Sub}) ->
    opcua_uacp:has_chunk(Conn, Sub).

produce(Conn, #state{uacp = Sub} = State) ->
    case opcua_uacp:next_chunk(Conn, Sub) of
        {error, Reason, Sub2} -> {stop, Reason, State#state{uacp = Sub2}};
        {ok, Output, Sub2} -> {ok, Output, State#state{uacp = Sub2}};
        {ok, Sub2} -> {ok, State#state{uacp = Sub2}}
    end.

handle_data(Data, Conn, #state{uacp = Sub} = State) ->
    case opcua_uacp:handle_data(Data, Conn, Sub) of
        {error, Reason, Sub2} -> {stop, Reason, State#state{uacp = Sub2}};
        {ok, Msgs, Sub2} -> handle_requests(State#state{uacp = Sub2}, Conn, Msgs)
    end.

terminate(Reason, Conn, #state{uacp = Sub}) ->
    opcua_uacp:terminate(Reason, Conn, Sub).


%%% BEHAVIOUR opcua_uacp CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Opts) -> {ok, undefined}.

channel_allocate(State) ->
    case opcua_registry:allocate_secure_channel(self()) of
        {ok, ChannelId} -> {ok, ChannelId, State};
        {error, _Reason} = Error -> Error
    end.

channel_release(undefined, State) -> {ok, State};
channel_release(ChannelId, State) ->
    case opcua_registry:release_secure_channel(ChannelId) of
        {error, _Reason} = Error -> Error;
        ok -> {ok, State}
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_requests(State, _Conn, []) -> {ok, State};
handle_requests(State, Conn, [Msg | Rest]) ->
    ?LOG_DEBUG("Handling request ~p", [Msg]),
    case handle_request(State, Conn, Msg) of
        {ok, State2} -> handle_requests(State2, Conn, Rest);
        {stop, _Reason, _State2} = Error -> Error
    end.

handle_request(State, _Conn, #uacp_message{type = error, payload = Msg}) ->
    #{error := Error, reason := Reason} = Msg,
    ?LOG_ERROR("Received client error ~w: ~s", [Error, Reason]),
    {ok, State};
handle_request(#state{sess = Sess} = State, Conn,
               #uacp_message{type = channel_message} = Request) ->
    #uacp_message{node_id = NodeSpec} = Request,
    %TODO: figure a way to not hardcode the ids...
    NodeId = opcua_database:lookup_id(NodeSpec),
    Request2 = Request#uacp_message{node_id = NodeId},
    Result = case {Sess, NodeId} of
        {_, #opcua_node_id{value = 426}} -> %% GetEndpoints
            opcua_discovery:handle_request(Conn, Request2);
        {undefined, _} ->
            opcua_session_manager:handle_request(Conn, Request2);
        {Session, _} ->
            opcua_session:handle_request(Conn, Request2, Session)
    end,
    case Result of
        {error, Reason} ->
            {error, Reason, State};
        {created, Resp, _SessPid} ->
            handle_response(State, Resp);
        {bound, Resp, SessPid} ->
            handle_response(State#state{sess = SessPid}, Resp);
        {closed, Resp} ->
            handle_response(State#state{sess = undefined}, Resp);
        {reply, Resp} ->
            handle_response(State, Resp)
    end.

handle_response(#state{uacp = Sub} = State, Response) ->
    case opcua_uacp:send(Response, Sub) of
        {error, Reason, Sub2} -> {stop, Reason, State#state{uacp = Sub2}};
        {ok, Sub2} -> {ok, State#state{uacp = Sub2}}
    end.
