-module(opcua_server_channel).

-behaviour(opcua_channel).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([init/0]).
-export([handle_request/3]).
-export([terminate/3]).

%% Behaviour opcua_channel
-export([channel_id/1]).
-export([lock/3]).
-export([unlock/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    channel_id                      :: undefined | pos_integer(),
    req_id = 1                      :: pos_integer(),
    curr_sec                        :: term(),
    temp_sec                        :: term()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init() ->
    case opcua_server_registry:allocate_secure_channel(self()) of
        {error, _Reason} = Error -> Error;
        {ok, ChannelId} -> {ok, #state{channel_id = ChannelId}}
    end.

handle_request(#uacp_message{type = channel_open, sender = client,
                             node_id = ?NID_CHANNEL_OPEN_REQ,
                             payload = Payload} = Req, Conn, State) ->
    %TODO: validate that the payload match the current security
    ?LOG_DEBUG("Secure channel opened: ~p", [Payload]),
    #state{channel_id = ChannelId, curr_sec = CurrSec} = State,
    TokenId = opcua_security:token_id(CurrSec),
    Resp = opcua_connection:response(Conn, Req, ?NID_CHANNEL_OPEN_RES, #{
        server_protocol_version => 0,
        security_token => #{
            channel_id => ChannelId,
            token_id => TokenId,
            created_at => opcua_util:date_time(),
            revised_lifetime => 3600000
        },
        server_nonce => undefined
    }),
    ?LOG_DEBUG("Open Secure channel response: ~p", [Resp]),
    {ok, Resp, State};
handle_request(#uacp_message{type = channel_close, sender = client,
                             node_id = ?NID_CHANNEL_CLOSE_REQ,
                             payload = Payload} = Req, Conn, State) ->
    ?LOG_DEBUG("Secure channel closed: ~p", [Payload]),
    Resp = opcua_connection:response(Conn, Req, ?NID_CHANNEL_CLOSE_RES, #{}),
    {ok, Resp, State}.

terminate(_Reason, _Conn, #state{channel_id = ChannelId}) ->
    opcua_server_registry:release_secure_channel(ChannelId),
    ok.


%%% BEHAVIOUR opcua_channel FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

channel_id(#state{channel_id = ChannelId}) -> ChannelId.

unlock(#uacp_chunk{state = locked, chunk_type = final,
                   message_type = channel_open, channel_id = ChunkChannelId} = Chunk,
       _Conn, #state{channel_id = ServerChannelId, temp_sec = undefined} = State)
  when ChunkChannelId =:= 0; ChunkChannelId =:= ServerChannelId ->
    #uacp_chunk{security = #uacp_chunk_security{policy_uri = PolicyUri}} = Chunk,
    case security_init(State, PolicyUri) of
        {error, _Reason} = Error -> Error;
        {ok, State2} -> security_unlock(State2, Chunk)
    end;
unlock(#uacp_chunk{state = locked, channel_id = ChannelId} = Chunk,
       _Conn, #state{channel_id = ChannelId} = State) ->
    security_unlock(State, Chunk);
unlock(_Chunk, _Conn, _State) ->
    {error, bad_tcp_secure_channel_unknown}.

lock(#uacp_chunk{state = unlocked, channel_id = undefined} = Chunk, _Conn,
     #state{channel_id = ChannelId} = State) ->
    Chunk2 = Chunk#uacp_chunk{channel_id = ChannelId},
    case security_setup(State, Chunk2) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk3, State2} ->
            Chunk4 = opcua_uacp_codec:prepare_chunks(Chunk3),
            case security_prepare(State2, Chunk4) of
                {error, _Reason} = Error -> Error;
                {ok, Chunk5, State3} ->
                    Chunk6 = opcua_uacp_codec:freeze_chunks(Chunk5),
                    security_lock(State3, Chunk6)
            end
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%== Security Module Abstraction Functions ======================================

security_init(#state{curr_sec = CurrSec, temp_sec = undefined} = State, Policy) ->
    case opcua_security:init_server(Policy, CurrSec) of
        {error, _Reason} = Error -> Error;
        {ok, NewSec} -> {ok, State#state{curr_sec = NewSec, temp_sec = CurrSec}}
    end.

security_unlock(#state{curr_sec = CurrSec, temp_sec = undefined} = State, Chunk) ->
    case opcua_security:unlock(Chunk, CurrSec) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, CurrSec2} ->
            {ok, Chunk2, State#state{curr_sec = CurrSec2}};
        expired ->
            {error, bad_secure_channel_token_unknown}
    end;
security_unlock(State, #uacp_chunk{security = TokenId} = Chunk) ->
    #state{curr_sec = CurrSec, temp_sec = TempSec} = State,
    case {opcua_security:token_id(CurrSec), opcua_security:token_id(TempSec)} of
        {TokenId, _} ->
            State2 = State#state{temp_sec = undefined},
            case opcua_security:unlock(Chunk, CurrSec) of
                {error, _Reason} = Error -> Error;
                {ok, Chunk2, CurrSec2} ->
                    {ok, Chunk2, State2#state{curr_sec = CurrSec2}};
                expired ->
                    {error, bad_secure_channel_token_unknown}
            end;
        {_, TokenId} ->
            case opcua_security:unlock(Chunk, TempSec) of
                {error, _Reason} = Error -> Error;
                {ok, Chunk2, TempSec2} ->
                    {ok, Chunk2, State#state{temp_sec = TempSec2}};
                expired ->
                    {error, bad_secure_channel_token_unknown}
            end;
        _ ->
            {error, bad_secure_channel_token_unknown}
    end.

security_setup(#state{curr_sec = Sub} = State, Chunk) ->
    case opcua_security:setup(Chunk, Sub) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, Sub2} -> {ok, Chunk2, State#state{curr_sec = Sub2}}
    end.

security_prepare(#state{curr_sec = Sub} = State, Chunk) ->
    case opcua_security:prepare(Chunk, Sub) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, Sub2} -> {ok, Chunk2, State#state{curr_sec = Sub2}}
    end.

security_lock(#state{curr_sec = Sub} = State, Chunk) ->
    case opcua_security:lock(Chunk, Sub) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, Sub2} -> {ok, Chunk2, State#state{curr_sec = Sub2}}
    end.
