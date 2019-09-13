-module(opcua_security).

%TODO: Implemente token expiration.

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([init_client/1]).
-export([init_server/2]).
-export([token_id/1, token_id/2]).
-export([unlock/2]).
-export([setup/2]).
-export([prepare/2]).
-export([lock/2]).

%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(POLICY_MATCH, #uacp_security_policy{policy_url = ?POLICY_NONE}).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    token_id :: pos_integer(),
    peer_policy :: undefined | opcua:security_policy(),
    self_policy :: undefined | opcua:security_policy(),
    peer_seq :: undefined | non_neg_integer(),
    self_seq :: undefined | non_neg_integer()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_client(?POLICY_MATCH = Policy) ->
    {ok, #state{self_policy = Policy}};
init_client(_Policy) ->
    {error, bad_security_policy_rejected}.

init_server(?POLICY_MATCH = Policy, undefined) ->
    TokenId = generate_token_id([0]),
    {ok, #state{self_policy = Policy, token_id = TokenId}};
init_server(?POLICY_MATCH = Policy,
    #state{token_id = OldTokenId}) ->
    NewTokenId = generate_token_id([0, OldTokenId]),
    {ok, #state{self_policy = Policy, token_id = NewTokenId}};
init_server(_Policy, _ParentState) ->
    {error, bad_security_policy_rejected}.

token_id(#state{token_id = TokenId}) -> TokenId.

token_id(TokenId, #state{token_id = undefined} = State) ->
    State#state{token_id = TokenId}.

unlock(#uacp_chunk{message_type = channel_open, security = ?POLICY_MATCH = Policy} = Chunk, State) ->
    validate_peer_sequence(State#state{peer_policy = Policy}, decode_sequence_header(Chunk));
unlock(#uacp_chunk{security = TokenId} = Chunk, #state{token_id = TokenId} = State) ->
    validate_peer_sequence(State, decode_sequence_header(Chunk));
unlock(_Chunk, _State) ->
    {error, bad_security_checks_failed}.

setup(#uacp_chunk{state = unlocked, message_type = Type, security = undefined} = Chunk,
           #state{self_policy = Policy, token_id = TokenId} = State) ->
    Security = case Type of
        channel_open -> Policy;
        channel_message -> TokenId;
        channel_close -> TokenId
    end,
    {ok, Chunk#uacp_chunk{security = Security}, State}.

prepare(#uacp_chunk{state = unlocked, security = Security, header_size = HSize,
                    unlocked_size = USize, request_id = ReqId, body = Body} = Chunk,
        #state{self_policy = Policy, token_id = TokenId} = State)
  when (Security =:= TokenId orelse Security =:= Policy),
       HSize =/= undefined, USize =/= undefined,
       ReqId =/= undefined, Body =/= undefined ->
    ?assertEqual(USize, iolist_size(Body)),
    {SeqNum, State2} = next_sequence(State),
    Chunk2 = Chunk#uacp_chunk{
        sequence_num = SeqNum,
        locked_size = USize + 8
    },
    {ok, Chunk2, State2}.

lock(#uacp_chunk{state = unlocked, security = Security, sequence_num = SeqNum,
                 request_id = ReqId, body = Body} = Chunk,
     #state{self_policy = Policy, token_id = TokenId} = State)
  when (Security =:= Policy orelse Security =:= TokenId),
       SeqNum =/= undefined, ReqId =/= undefined, Body =/= undefined ->
    SeqHeader = opcua_protocol_codec:encode_sequence_header(SeqNum, ReqId),
    {ok, Chunk#uacp_chunk{
        state = locked,
        body = [SeqHeader, Body]
    }, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_sequence_header(#uacp_chunk{body = Body} = Chunk) ->
    {{SeqNum, ReqId}, RemBody} =
        opcua_protocol_codec:decode_sequence_header(Body),
    Chunk#uacp_chunk{
        sequence_num = SeqNum,
        request_id = ReqId,
        body = RemBody
    }.

validate_peer_sequence(#state{peer_seq = undefined} = State,
                       #uacp_chunk{sequence_num = NewNum} = Chunk) ->
    {ok, Chunk, State#state{peer_seq = NewNum}};
validate_peer_sequence(#state{peer_seq = LastNum} = State,
                       #uacp_chunk{sequence_num = NewNum} = Chunk)
  when LastNum < NewNum ->
    {ok, Chunk, State#state{peer_seq = NewNum}};
validate_peer_sequence(#state{peer_seq = LastNum} = State,
                       #uacp_chunk{sequence_num = NewNum} = Chunk)
  when LastNum > 4294966271, NewNum < 1024 ->
    {ok, Chunk, State#state{peer_seq = NewNum}};
validate_peer_sequence(_State, _Chunk) ->
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
