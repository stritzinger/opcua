-module(opcua_security).

%TODO: Implemente token expiration.

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua_codec.hrl").
-include("opcua_protocol.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([init/3]).
-export([unlock/2]).
-export([open/3]).
-export([setup_asym/2]).
-export([setup_sym/2]).
-export([prepare/2]).
-export([lock/2]).
-export([close/3]).
-export([cleanup/1]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(POLICY_NONE, <<"http://opcfoundation.org/UA/SecurityPolicy#None">>).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    channel_id :: pos_integer(),
    client_policy :: undefined | opcua_protocol:security_policy(),
    server_policy :: undefined | opcua_protocol:security_policy(),
    token_id :: pos_integer(),
    last_client_seq :: undefined | non_neg_integer(),
    last_server_seq :: undefined | non_neg_integer()
}).

-type state() :: #state{}.


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init(ChannelId, SecurityPolicy, undefined | state())
    -> {ok, TokenId, state()}
  when ChannelId :: opcua_protocol:channel_id(),
       SecurityPolicy :: opcua_protocol:security_policy(),
       TokenId :: opcua_protocol:token_id().
init(ChannelId, #uacp_security_policy{policy_url = ?POLICY_NONE} = Policy, undefined) ->
    TokenId = generate_token_id([0]),
    State = #state{channel_id = ChannelId, client_policy = Policy, token_id = TokenId},
    {ok, TokenId, State};
init(ChannelId, #uacp_security_policy{policy_url = ?POLICY_NONE} = Policy,
     #state{channel_id = ChannelId, token_id = OldTokenId}) ->
    NewTokenId = generate_token_id([0, OldTokenId]),
    State = #state{channel_id = ChannelId, client_policy = Policy, token_id = NewTokenId},
    {ok, NewTokenId, State};
init(_ChannelId, _SecurityPolicy, _ParentState) ->
    {error, bad_security_policy_rejected}.

-spec unlock(opcua_protocol:chunk(), state())
    -> {ok, opcua_protocol:chunk(), state()} | {error, term()}.
unlock(#uacp_chunk{security = Security} = Chunk,
       #state{client_policy = ClientPolicy, token_id = TokenId} = State)
  when Security =:= ClientPolicy; Security =:= TokenId ->
    validate_client_sequence(State, decode_sequence_header(Chunk));
unlock(_Chunk, _State) ->
    {error, bad_security_checks_failed}.

-spec open(opcua_protocol:connection(), opcua_protocol:message(), state())
    -> {ok, opcua_protocol:message(), state()} | {error, term()}.
open(Conn, #uacp_message{type = channel_open,
                         node_id = #opcua_node_id{ns = 0, value = 444},
                         payload = Msg} = Req, State) ->
    ?LOG_DEBUG("Secure channel opened: ~p", [Msg]),
    #state{channel_id = ChannelId, token_id = TokenId} = State,
    ServerPolicy = #uacp_security_policy{policy_url = ?POLICY_NONE},
    Resp = opcua_connection:req2res(Conn, Req, 447, #{
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
    {ok, Resp, State#state{server_policy = ServerPolicy}}.

-spec setup_asym(opcua_protocol:chunk(), state())
    -> {ok, opcua_protocol:chunk(), state()} | {error, term()}.
setup_asym(#uacp_chunk{state = undefined, security = undefined} = Chunk,
           #state{server_policy = ServerPolicy} = State) ->
    {ok, Chunk#uacp_chunk{state = unlocked, security = ServerPolicy}, State}.

-spec setup_sym(opcua_protocol:chunk(), state())
    -> {ok, opcua_protocol:chunk(), state()} | {error, term()}.
setup_sym(#uacp_chunk{state = undefined, security = undefined} = Chunk,
         #state{token_id = TokenId} = State) ->
    {ok, Chunk#uacp_chunk{state = unlocked, security = TokenId}, State}.

-spec prepare(opcua_protocol:chunk(), state())
    -> {ok, opcua_protocol:chunk(), state()} | {error, term()}.
prepare(#uacp_chunk{state = unlocked, security = Security, header_size = HSize,
                    unlocked_size = USize, request_id = ReqId, body = Body} = Chunk,
        #state{server_policy = ServerPolicy, token_id = TokenId} = State)
  when (Security =:= TokenId orelse Security =:= ServerPolicy),
       HSize =/= undefined, USize =/= undefined,
       ReqId =/= undefined, Body =/= undefined ->
    ?assertEqual(USize, iolist_size(Body)),
    {SeqNum, State2} = next_server_sequence(State),
    Chunk2 = Chunk#uacp_chunk{
        sequence_num = SeqNum,
        locked_size = USize + 8
    },
    {ok, Chunk2, State2}.

-spec lock(opcua_protocol:chunk(), state())
    -> {ok, opcua_protocol:chunk(), state()} | {error, term()}.
lock(#uacp_chunk{state = unlocked, security = Security, sequence_num = SeqNum,
                 request_id = ReqId, body = Body} = Chunk,
     #state{server_policy = Policy, token_id = TokenId} = State)
  when (Security =:= Policy orelse Security =:= TokenId),
       SeqNum =/= undefined, ReqId =/= undefined, Body =/= undefined ->
    SeqHeader = opcua_protocol_codec:encode_sequence_header(SeqNum, ReqId),
    {ok, Chunk#uacp_chunk{
        state = locked,
        body = [SeqHeader, Body]
    }, State}.

-spec close(opcua_protocol:connection(), opcua_protocol:message(), state())
    -> {ok, opcua_protocol:message(), state()} | {error, term()}.
close(Conn, #uacp_message{type = channel_close,
                          node_id = #opcua_node_id{ns = 0, value = 450},
                          payload = Msg} = Req, State) ->
    ?LOG_DEBUG("Secure channel closed: ~p", [Msg]),
    Resp = opcua_connection:req2res(Conn, Req, 453, #{}),
    {ok, Resp, State}.

-spec cleanup(state()) -> ok.
cleanup(_State) -> ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_sequence_header(#uacp_chunk{body = Body} = Chunk) ->
    {{SeqNum, ReqId}, RemBody} =
        opcua_protocol_codec:decode_sequence_header(Body),
    Chunk#uacp_chunk{
        sequence_num = SeqNum,
        request_id = ReqId,
        body = RemBody
    }.

validate_client_sequence(#state{last_client_seq = undefined} = State,
                         #uacp_chunk{sequence_num = NewNum} = Chunk) ->
    {ok, Chunk, State#state{last_client_seq = NewNum}};
validate_client_sequence(#state{last_client_seq = LastNum} = State,
                         #uacp_chunk{sequence_num = NewNum} = Chunk)
  when LastNum < NewNum ->
    {ok, Chunk, State#state{last_client_seq = NewNum}};
validate_client_sequence(#state{last_client_seq = LastNum} = State,
                         #uacp_chunk{sequence_num = NewNum} = Chunk)
  when LastNum > 4294966271, NewNum < 1024 ->
    {ok, Chunk, State#state{last_client_seq = NewNum}};
validate_client_sequence(_State, _Chunk) ->
    {error, bad_sequence_number_invalid}.

next_server_sequence(#state{last_server_seq = undefined} = State) ->
    {1, State#state{last_server_seq = 1}};
next_server_sequence(#state{last_server_seq = LastNum} = State)
  when LastNum > 4294966783 ->
    {512, State#state{last_server_seq = 512}};
next_server_sequence(#state{last_server_seq = LastNum} = State) ->
    {LastNum + 1, State#state{last_server_seq = LastNum + 1}}.

generate_token_id(Except) when is_list(Except) ->
    Token = crypto:bytes_to_integer(crypto:strong_rand_bytes(4)),
    case lists:member(Token, Except) of
        true -> generate_token_id(Except);
        false -> Token
    end.
