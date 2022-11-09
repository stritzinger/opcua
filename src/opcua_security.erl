-module(opcua_security).

%TODO: Implemente token expiration.

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([init_client/2]).
-export([init_server/2]).
-export([token_id/1, token_id/2]).
-export([unlock/3]).
-export([setup/3]).
-export([prepare/3]).
-export([lock/3]).

%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% -define(POLICY_MATCH, #uacp_security_policy{policy_uri = ?POLICY_NONE}).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    token_id :: undefined | pos_integer(),
    security_policy :: undefined | opcua:security_policy(),
    peer_security_data :: undefined | opcua:security_data(),
    self_security_data :: undefined | opcua:security_data(),
    peer_seq :: undefined | non_neg_integer(),
    self_seq :: undefined | non_neg_integer()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_client(none, none) ->
    Sec = #uacp_chunk_security{policy_uri = opcua_util:policy_uri(none)},
    {ok, #state{self_security_data = Sec}};
init_client(none, _PolicyType) ->
    {error, bad_security_policy_rejected}.

init_server(none, undefined) ->
    TokenId = generate_token_id([0]),
    Sec = #uacp_chunk_security{policy_uri = opcua_util:policy_uri(none)},
    {ok, #state{self_security_data = Sec, token_id = TokenId}};
init_server(none, #state{token_id = OldTokenId}) ->
    NewTokenId = generate_token_id([0, OldTokenId]),
    Sec = #uacp_chunk_security{policy_uri = opcua_util:policy_uri(none)},
    {ok, #state{self_security_data = Sec, token_id = NewTokenId}};
init_server(_PolicyType, _ParentState) ->
    {error, bad_security_policy_rejected}.

token_id(#state{token_id = TokenId}) -> TokenId.

token_id(TokenId, #state{token_id = undefined} = State) ->
    State#state{token_id = TokenId}.

unlock(#uacp_chunk{message_type = channel_open, security = SecData} = Chunk,
       Conn, #state{self_security_data = SecData} = State) ->
    validate_peer_sequence(State#state{peer_security_data = SecData}, Conn,
                           decode_sequence_header(Chunk));
unlock(#uacp_chunk{security = TokenId} = Chunk,
       Conn, #state{token_id = TokenId} = State) ->
    validate_peer_sequence(State, Conn, decode_sequence_header(Chunk));
unlock(_Chunk, _Conn, _State) ->
    {error, bad_security_checks_failed}.

setup(#uacp_chunk{state = unlocked, message_type = Type, security = undefined} = Chunk,
      Conn, #state{self_security_data = Policy, token_id = TokenId} = State) ->
    Security = case Type of
        channel_open -> Policy;
        channel_message -> TokenId;
        channel_close -> TokenId
    end,
    {ok, Chunk#uacp_chunk{security = Security}, Conn, State}.

prepare(#uacp_chunk{state = unlocked, security = Security, header_size = HSize,
                    unlocked_size = USize, request_id = ReqId, body = Body} = Chunk,
        Conn, #state{self_security_data = Policy, token_id = TokenId} = State)
  when (Security =:= TokenId orelse Security =:= Policy),
       HSize =/= undefined, USize =/= undefined,
       ReqId =/= undefined, Body =/= undefined ->
    ?assertEqual(USize, iolist_size(Body)),
    {SeqNum, State2} = next_sequence(State),
    Chunk2 = Chunk#uacp_chunk{
        sequence_num = SeqNum,
        locked_size = USize + 8
    },
    {ok, Chunk2, Conn, State2}.

lock(#uacp_chunk{state = unlocked, security = Security, sequence_num = SeqNum,
                 request_id = ReqId, body = Body} = Chunk, Conn,
     #state{self_security_data = Policy, token_id = TokenId} = State)
  when (Security =:= Policy orelse Security =:= TokenId),
       SeqNum =/= undefined, ReqId =/= undefined, Body =/= undefined ->
    SeqHeader = opcua_uacp_codec:encode_sequence_header(SeqNum, ReqId),
    {ok, Chunk#uacp_chunk{
        state = locked,
        body = [SeqHeader, Body]
    }, Conn, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_sequence_header(#uacp_chunk{body = Body} = Chunk) ->
    {{SeqNum, ReqId}, RemBody} =
        opcua_uacp_codec:decode_sequence_header(Body),
    Chunk#uacp_chunk{
        sequence_num = SeqNum,
        request_id = ReqId,
        body = RemBody
    }.

validate_peer_sequence(#state{peer_seq = undefined} = State, Conn,
                       #uacp_chunk{sequence_num = NewNum} = Chunk) ->
    {ok, Chunk, Conn, State#state{peer_seq = NewNum}};
validate_peer_sequence(#state{peer_seq = LastNum} = State, Conn,
                       #uacp_chunk{sequence_num = NewNum} = Chunk)
  when LastNum < NewNum ->
    {ok, Chunk, Conn, State#state{peer_seq = NewNum}};
validate_peer_sequence(#state{peer_seq = LastNum} = State, Conn,
                       #uacp_chunk{sequence_num = NewNum} = Chunk)
  when LastNum > 4294966271, NewNum < 1024 ->
    {ok, Chunk, Conn, State#state{peer_seq = NewNum}};
validate_peer_sequence(_State, _Conn, _Chunk) ->
    {error, bad_sequence_number_invalid}.

next_sequence(#state{self_seq = undefined} = State) ->
    {1, State#state{self_seq = 1}};
next_sequence(#state{self_seq = LastNum} = State)
  when LastNum > 4294966783 ->
    {512, State#state{self_seq = 512}};
next_sequence(#state{self_seq = LastNum} = State) ->
    {LastNum + 1, State#state{self_seq = LastNum + 1}}.

generate_token_id(Except) when is_list(Except) ->
    Token = crypto:bytes_to_integer(crypto:strong_rand_bytes(4)),
    case lists:member(Token, Except) of
        true -> generate_token_id(Except);
        false -> Token
    end.
