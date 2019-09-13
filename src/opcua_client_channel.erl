-module(opcua_client_channel).

-behaviour(opcua_channel).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([init/1]).
-export([make_request/6]).
-export([open/2]).
-export([close/2]).
-export([handle_response/3]).
-export([terminate/3]).

%% Behaviour opcua_channel
-export([channel_id/1]).
-export([lock/3]).
-export([unlock/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    channel_id                      :: undefined | pos_integer(),
    security                        :: term(),
    req_id = 1                      :: pos_integer(),
    req_handle = 1                  :: pos_integer()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Conn) ->
    Policy = #uacp_security_policy{policy_url = ?POLICY_NONE},
    security_init(#state{}, Policy).

make_request(Type, NodeId, Payload, Sess, Conn, State)
  when Type =:= channel_open; Type =:= channel_close; Type =:= channel_message ->
    #state{req_id = NextId, req_handle = NextHandle} = State,
    FinalPayload = Payload#{
        request_header => #{
            authentication_token => opcua_client_session:auth_token(Sess),
            timestamp => opcua_util:date_time(),
            request_handle => NextHandle,
            return_diagnostics => 0,
            audit_entry_id => undefined,
            timeout_hint => 4000,
            additional_header => #opcua_extension_object{}
        }
    },
    State2 = State#state{req_id = NextId + 1,  req_handle = NextHandle + 1},
    Req = opcua_connection:request(Conn, Type, NextId, NodeId, FinalPayload),
    {ok, Req, State2}.

open(Conn, State) ->
    Payload = #{
        client_protocol_version => 0,
        request_type => issue,
        security_mode => none,
        client_nonce => undefined,
        requested_lifetime => 3600000
    },
    make_request(channel_open, ?NID_CHANNEL_OPEN_REQ, Payload,
                 undefined, Conn, State).

close(Conn, State) ->
        make_request(channel_open, ?NID_CHANNEL_CLOSE_REQ, #{},
                 undefined, Conn, State).

handle_response(#uacp_message{type = channel_open, sender = server,
                             node_id = ?NID_CHANNEL_OPEN_RES,
                             payload = Payload}, _Conn, State) ->
    %TODO: validate that the payload match the current security
    ?LOG_DEBUG("Secure channel opened: ~p", [Payload]),
    #{security_token := #{channel_id := ChannelId, token_id := TokenId}} = Payload,
    {open, security_token_id(State#state{channel_id = ChannelId}, TokenId)};
handle_response(#uacp_message{type = channel_close, sender = server,
                             node_id = ?NID_CHANNEL_CLOSE_RES,
                             payload = Payload}, _Conn, State) ->
    %TODO: Do some validation of the message to check it is for this channel ?
    ?LOG_DEBUG("Secure channel closed: ~p", [Payload]),
    {closed, State};
handle_response(#uacp_message{type = channel_message, sender = server} = Msg,
                _Conn, State) ->
    %TODO: Do some validation of the message to check it is for this channel ?
    {forward, Msg, State}.


terminate(_Reason, _Conn, _State) ->
    ok.


%%% BEHAVIOUR opcua_channel FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

channel_id(#state{channel_id = ChannelId}) -> ChannelId.

unlock(#uacp_chunk{state = locked, message_type = channel_open} = Chunk, _Conn, State) ->
    security_unlock(State, Chunk);
unlock(#uacp_chunk{state = locked, channel_id = ChannelId} = Chunk,
       _Conn, #state{channel_id = ChannelId} = State) ->
    security_unlock(State, Chunk);
unlock(_Chunk, _Conn, _State) ->
    {error, bad_tcp_secure_channel_unknown}.

lock(#uacp_chunk{state = unlocked, channel_id = undefined, message_type = channel_open} = Chunk,
     Conn, #state{channel_id = undefined} = State) ->
    Chunk2 = Chunk#uacp_chunk{channel_id = 0},
    lock_chunk(State, Conn, Chunk2);
lock(#uacp_chunk{state = unlocked, channel_id = undefined} = Chunk, Conn, State) ->
    #state{channel_id = ChannelId} = State,
    Chunk2 = Chunk#uacp_chunk{channel_id = ChannelId},
    lock_chunk(State, Conn, Chunk2).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

lock_chunk(State, _Conn, Chunk) ->
    case security_setup(State, Chunk) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, State2} ->
            Chunk3 = opcua_uacp_codec:prepare_chunks(Chunk2),
            case security_prepare(State2, Chunk3) of
                {error, _Reason} = Error -> Error;
                {ok, Chunk4, State3} ->
                    Chunk5 = opcua_uacp_codec:freeze_chunks(Chunk4),
                    security_lock(State3, Chunk5)
            end
    end.


%== Security Module Abstraction Functions ======================================

security_init(#state{security = undefined} = State, Policy) ->
    case opcua_security:init_client(Policy) of
        {error, _Reason} = Error -> Error;
        {ok, Sec} -> {ok, State#state{security = Sec}}
    end.

security_token_id(#state{security = Sec} = State, TokenId) ->
    Sec2 = opcua_security:token_id(TokenId, Sec),
    State#state{security = Sec2}.

security_unlock(#state{security = Sec} = State, Chunk)
  when Sec =/= undefined ->
    case opcua_security:unlock(Chunk, Sec) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, Sec2} ->
            {ok, Chunk2, State#state{security = Sec2}};
        expired ->
            {error, bad_secure_channel_token_unknown}
    end;
security_unlock(#state{security = Sec} = State,
                #uacp_chunk{security = TokenId} = Chunk)
  when Sec =/= undefined ->
    case opcua_security:token_id(Sec) of
        TokenId ->
            case opcua_security:unlock(Chunk, Sec) of
                {error, _Reason} = Error -> Error;
                {ok, Chunk2, Sec2} ->
                    {ok, Chunk2, State#state{security = Sec2}};
                expired ->
                    {error, bad_secure_channel_token_unknown}
            end;
        _ ->
            {error, bad_secure_channel_token_unknown}
    end.

security_setup(#state{security = Sec} = State, Chunk) ->
    case opcua_security:setup(Chunk, Sec) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, Sec2} -> {ok, Chunk2, State#state{security = Sec2}}
    end.

security_prepare(#state{security = Sec} = State, Chunk) ->
    case opcua_security:prepare(Chunk, Sec) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, Sec2} -> {ok, Chunk2, State#state{security = Sec2}}
    end.

security_lock(#state{security = Sec} = State, Chunk) ->
    case opcua_security:lock(Chunk, Sec) of
        {error, _Reason} = Error -> Error;
        {ok, Chunk2, Sec2} -> {ok, Chunk2, State#state{security = Sec2}}
    end.
