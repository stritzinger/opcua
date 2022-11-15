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

-record(state, {
    token_id :: undefined | pos_integer(),
    policy :: undefined | binary(),
    peer_seq :: undefined | non_neg_integer(),
    self_seq :: undefined | non_neg_integer()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_client(none, none) ->
    {ok, #state{policy = opcua_util:policy_uri(none)}};
init_client(none, _PolicyType) ->
    {error, bad_security_policy_rejected}.

init_server(none, undefined) ->
    TokenId = generate_token_id([0]),
    {ok, #state{policy = opcua_util:policy_uri(none), token_id = TokenId}};
init_server(none, #state{token_id = OldTokenId}) ->
    NewTokenId = generate_token_id([0, OldTokenId]),
    {ok, #state{policy = opcua_util:policy_uri(none), token_id = NewTokenId}};
init_server(_PolicyType, _ParentState) ->
    {error, bad_security_policy_rejected}.

token_id(#state{token_id = TokenId}) -> TokenId.

token_id(TokenId, #state{token_id = undefined} = State) ->
    State#state{token_id = TokenId}.

unlock(#uacp_chunk{message_type = channel_open,
                   security = #uacp_chunk_security{policy_uri = PolicyUri} = Sec} = Chunk,
       Conn, #state{policy = PolicyUri} = State) ->
    case validate_security(Conn, Sec) of
        {error, _Reason} = Error -> Error;
        {ok, Conn2} ->
            Chunk2 = decode_sequence_header(Chunk),
            validate_peer_sequence(State, Conn2, Chunk2)
    end;
unlock(#uacp_chunk{security = TokenId} = Chunk,
       Conn, #state{token_id = TokenId} = State) ->
    Chunk2 = decode_sequence_header(Chunk),
    validate_peer_sequence(State, Conn, Chunk2);
unlock(_Chunk, _Conn, _State) ->
    {error, bad_security_checks_failed}.

setup(#uacp_chunk{state = unlocked, message_type = Type, security = undefined} = Chunk,
      Conn, #state{policy = PolicyUri, token_id = TokenId} = State) ->
    Security = case Type of
        channel_open -> #uacp_chunk_security{
            policy_uri = PolicyUri
            %sender_cert = opcua_connection:self_certificate(Conn),
            %receiver_thumbprint = opcua_connection:peer_thumbprint(Conn)
        };
        channel_message -> TokenId;
        channel_close -> TokenId
    end,
    {ok, Chunk#uacp_chunk{security = Security}, Conn, State}.

prepare(#uacp_chunk{state = unlocked, security = Security, header_size = HSize,
                    unlocked_size = USize, request_id = ReqId, body = Body} = Chunk,
        Conn, #state{policy = PolicyUri, token_id = TokenId} = State)
  when Security =:= TokenId,
       HSize =/= undefined, USize =/= undefined,
       ReqId =/= undefined, Body =/= undefined;
       is_record(Security, uacp_chunk_security),
       Security#uacp_chunk_security.policy_uri =:= PolicyUri,
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
     #state{policy = PolicyUri, token_id = TokenId} = State)
  when Security =:= TokenId,
       SeqNum =/= undefined, ReqId =/= undefined, Body =/= undefined;
       is_record(Security, uacp_chunk_security),
       Security#uacp_chunk_security.policy_uri =:= PolicyUri,
       SeqNum =/= undefined, ReqId =/= undefined, Body =/= undefined ->
    SeqHeader = opcua_uacp_codec:encode_sequence_header(SeqNum, ReqId),
    {ok, Chunk#uacp_chunk{
        state = locked,
        body = [SeqHeader, Body]
    }, Conn, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

validate_security(Conn, #uacp_chunk_security{policy_uri = ?POLICY_NONE,
                                             sender_cert = undefined}) ->
    % We may have a defined peer certificate, but without any security, peer do
    % have to give a certificate, so we can't enforce peer validation.
    {ok, Conn};
validate_security(Conn, #uacp_chunk_security{} = Sec) ->
    #uacp_chunk_security{sender_cert = Cert, receiver_thumbprint = Thumb} = Sec,
    case opcua_connection:validate_peer(Conn, Cert) of
        {error, _Reason} -> {error, invalid_peer_certificate};
        {ok, Conn2} ->
            %TODO: Figure out when we should really check the thumbprint....
            case {opcua_connection:self_thumbprint(Conn2), Thumb} of
                {_, undefined} -> {ok, Conn2};
                {Same, Same} -> {ok, Conn2};
                {_, _} -> {error, receiver_thumbprint_mismatch}
            end
    end.

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
