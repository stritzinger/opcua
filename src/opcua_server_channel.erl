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
        {ok, ChannelId} ->
            {ok, #state{channel_id = ChannelId}}
    end.

handle_request(#uacp_message{type = channel_open, sender = client,
                             node_id = ?NID_CHANNEL_OPEN_REQ,
                             payload = Payload} = Req, Conn, State) ->
    %TODO: validate that the payload security settings match a valid endpoint
    ?LOG_DEBUG("Secure channel opened: ~p", [Payload]),
    #{client_nonce := ClientNonce, security_mode := SecMode} = Payload,
    #state{channel_id = ChannelId, curr_sec = CurrSec} = State,
    NewSec1 = opcua_security:set_mode(CurrSec, SecMode),
    TokenId = opcua_security:token_id(NewSec1),
    ServerNonce = opcua_security:nonce(NewSec1),
    NewSec2 = opcua_security:derive_keys(ServerNonce, ClientNonce, NewSec1),
    Resp = opcua_connection:response(Conn, Req, ?NID_CHANNEL_OPEN_RES, #{
        server_protocol_version => 0,
        security_token => #{
            channel_id => ChannelId,
            token_id => TokenId,
            created_at => opcua_util:date_time(),
            revised_lifetime => 3600000
        },
        server_nonce => ServerNonce
    }),
    Conn2 = opcua_connection:set_security_mode(Conn, SecMode),
    ?LOG_DEBUG("Open Secure channel response: ~p", [Resp]),
    {ok, Resp, Conn2, State#state{curr_sec = NewSec2}};
handle_request(#uacp_message{type = channel_close, sender = client,
                             node_id = ?NID_CHANNEL_CLOSE_REQ,
                             payload = Payload} = Req, Conn, State) ->
    ?LOG_DEBUG("Secure channel closed: ~p", [Payload]),
    Resp = opcua_connection:response(Conn, Req, ?NID_CHANNEL_CLOSE_RES, #{}),
    {ok, Resp, Conn, State}.

terminate(_Reason, _Conn, #state{channel_id = ChannelId}) ->
    opcua_server_registry:release_secure_channel(ChannelId),
    ok.


%%% BEHAVIOUR opcua_channel FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

channel_id(#state{channel_id = ChannelId}) -> ChannelId.

unlock(#uacp_chunk{state = locked, chunk_type = final,
                   message_type = channel_open, channel_id = ChunkChannelId} = Chunk,
       Conn, #state{channel_id = ServerChannelId, curr_sec = CurrSec,
                                                  temp_sec = undefined} = State)
  when ChunkChannelId =:= 0; ChunkChannelId =:= ServerChannelId ->
    #uacp_chunk{security = ChunkSecurity} = Chunk,
    case security_init(State, Conn, ChunkSecurity) of
        {error, _Reason} = Error -> Error;
        {ok, Conn2, NewSec} ->
            State2 = State#state{curr_sec = NewSec, temp_sec = CurrSec},
            security_unlock(State2, Conn2, Chunk)
    end;
unlock(#uacp_chunk{state = locked, channel_id = ChannelId} = Chunk,
       Conn, #state{channel_id = ChannelId} = State) ->
    security_unlock(State, Conn, Chunk);
unlock(_Chunk, _Conn, _State) ->
    {error, bad_tcp_secure_channel_unknown}.

lock(#uacp_chunk{state = unlocked, channel_id = undefined} = Chunk, Conn,
     #state{channel_id = ChannelId} = State) ->
    Chunk2 = Chunk#uacp_chunk{channel_id = ChannelId},
    case security_setup(State, Conn, Chunk2) of
        % No error case yet, commented to make dialyzer happy
        % {error, _Reason} = Error -> Error;
        {ok, Chunk3, Conn2, State2} ->
            Chunk4 = opcua_uacp_codec:prepare_chunks(Conn, Chunk3),
            case security_prepare(State2, Conn2, Chunk4) of
                % No error case yet, commented to make dialyzer happy
                % {error, _Reason} = Error -> Error;
                {ok, Chunk5, Conn3, State3} ->
                    Chunk6 = opcua_uacp_codec:freeze_chunks(Conn, Chunk5),
                    security_lock(State3, Conn3, Chunk6)
            end
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%== Security Module Abstraction Functions ======================================

security_init(#state{curr_sec = CurrSec, temp_sec = undefined}, Conn, SecurityConfig) ->
    case opcua_security:init_server(SecurityConfig, Conn, CurrSec) of
        {error, _Reason} = Error -> Error;
        {ok, Conn2, NewSec} -> {ok, Conn2, NewSec}
    end.

security_unlock(#state{curr_sec = CurrSec, temp_sec = undefined} = State, Conn, Chunk) ->
    case opcua_security:unlock(Chunk, Conn, CurrSec) of
        {error, _Reason} = Error -> Error;
        % Not returning exipred yet, commented to make dialyzer happy
        % expired ->
        %     {error, bad_secure_channel_token_unknown};
        {ok, Chunk2, Conn2, CurrSec2} ->
            {ok, Chunk2, Conn2, State#state{curr_sec = CurrSec2}}
    end;
security_unlock(State, Conn, #uacp_chunk{security = TokenId} = Chunk) ->
    #state{curr_sec = CurrSec, temp_sec = TempSec} = State,
    case {opcua_security:token_id(CurrSec), opcua_security:token_id(TempSec)} of
        {TokenId, _} ->
            State2 = State#state{temp_sec = undefined},
            case opcua_security:unlock(Chunk, Conn, CurrSec) of
                {error, _Reason} = Error -> Error;
                % Not returning exipred yet, commented to make dialyzer happy
                % expired ->
                %     {error, bad_secure_channel_token_unknown};
                {ok, Chunk2, Conn2, CurrSec2} ->
                    {ok, Chunk2, Conn2, State2#state{curr_sec = CurrSec2}}
            end;
        {_, TokenId} ->
            case opcua_security:unlock(Chunk, Conn, TempSec) of
                {error, _Reason} = Error -> Error;
                % Not returning exipred yet, commented to make dialyzer happy
                % expired ->
                %     {error, bad_secure_channel_token_unknown};
                {ok, Chunk2, Conn2, TempSec2} ->
                    {ok, Chunk2, Conn2, State#state{temp_sec = TempSec2}}
            end;
        _ ->
            {error, bad_secure_channel_token_unknown}
    end.

security_setup(#state{curr_sec = Sub} = State, Conn, Chunk) ->
    case opcua_security:setup(Chunk, Conn, Sub) of
        % No error case yet, commented to make dialyzer happy
        % {error, _Reason} = Error -> Error;
        {ok, Chunk2, Conn2, Sub2} ->
            {ok, Chunk2, Conn2, State#state{curr_sec = Sub2}}
    end.

security_prepare(#state{curr_sec = Sub} = State, Conn, Chunk) ->
    case opcua_security:prepare(Chunk, Conn, Sub) of
        % No error case yet, commented to make dialyzer happy
        % {error, _Reason} = Error -> Error;
        {ok, Chunk2, Conn2, Sub2} ->
            {ok, Chunk2, Conn2, State#state{curr_sec = Sub2}}
    end.

security_lock(#state{curr_sec = Sub} = State, Conn, Chunk) ->
    case opcua_security:lock(Chunk, Conn, Sub) of
        % No error case yet, commented to make dialyzer happy
        % {error, _Reason} = Error -> Error;
        {ok, Chunk2, Conn2, Sub2} ->
            {ok, Chunk2, Conn2, State#state{curr_sec = Sub2}}
    end.
