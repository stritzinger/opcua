-module(opcua_security).

%TODO: Implemente token expiration.

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([supported_endpoints/1]).
-export([init_client/1]).
-export([init_server/3]).
-export([set_mode/2]).
-export([token_id/1, token_id/2]).
-export([nonce/1]).
-export([derive_keys/3]).
-export([session_signature/4]).
-export([encrypt_user_password/4]).
-export([decrypt_user_password/3]).
-export([unlock/3]).
-export([setup/3]).
-export([prepare/3]).
-export([lock/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    token_id            :: undefined | pos_integer(),
    symmetric_keys      :: undefined | opcua:symmetric_keys(),
    security_policy     :: undefined | opcua:security_policy(),
    security_mode       :: undefined | opcua:security_mode(),
    peer_seq            :: undefined | non_neg_integer(),
    self_seq            :: undefined | non_neg_integer()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

supported_endpoints(EndpointUrl) ->
    [ServerIdent] = opcua_keychain:lookup(alias, server),
    ServerChain = opcua_keychain:chain(ServerIdent, der),
    DerBlob = iolist_to_binary(ServerChain),
    SupportedCombos =
        [{0, none, none, [anonymous, user_name]},
         {65, sign, basic256sha256, [anonymous, user_name]},
         {115, sign_and_encrypt, basic256sha256, [anonymous, user_name]},
         {60, sign, aes128_sha256_RsaOaep, [anonymous, user_name]},
         {110, sign_and_encrypt, aes128_sha256_RsaOaep, [anonymous, user_name]}],
    [{EndpointUrl, DerBlob, L, M, P, T} || {L, M, P, T} <- SupportedCombos].

init_client(Conn) ->
    SecMode = opcua_connection:security_mode(Conn),
    PolicyType = opcua_connection:security_policy(Conn),
    SecurityPolicy = opcua_util:security_policy(PolicyType),
    ServerCert = opcua_connection:peer_certificate(Conn),
    case {SecMode, SecurityPolicy, ServerCert} of
        {_, {error, E}, _} -> {error, E};
        {none, {ok, P}, _} ->
            {ok, Conn, #state{security_mode = none, security_policy = P}};
        { _, {ok, _P}, undefined} ->
            {error, certificate_undefined};
        {_, {ok, P}, _} ->
            {ok, Conn, #state{security_mode = SecMode, security_policy = P}}
    end.

init_server(#uacp_chunk_security{
            policy_uri = Uri,
            sender_cert = undefined,
            receiver_thumbprint = undefined}, Conn, OldState) ->
    PolicyType = opcua_util:policy_type(Uri),
    case opcua_util:security_policy(PolicyType) of
        {ok, P} ->
            State = #state{
                security_mode = none,
                security_policy = P,
                token_id = generate_token_id(OldState)
            },
            Conn2 = opcua_connection:set_security_policy(Conn, PolicyType),
            {ok, Conn2, State};
        Error -> Error
    end;
init_server(#uacp_chunk_security{policy_uri = Uri, sender_cert = SenderCert},
            Conn,
            OldState) ->
    [ServerIdent] = opcua_keychain:lookup(alias, server),
    Conn2 = Conn#uacp_connection{self_ident = ServerIdent},
    State = #state{
        security_mode = undefined,
        token_id = generate_token_id(OldState)},
    PolicyType = opcua_util:policy_type(Uri),
    case opcua_util:security_policy(PolicyType) of
        {ok, P} ->
            S2 = State#state{security_policy = P},
            Conn3 = opcua_connection:set_security_policy(Conn2, PolicyType),
            case opcua_keychain:validate(Conn3, SenderCert) of
                {ok, Conn4, PeerIdentity} ->
                    case opcua_connection:lock_peer(Conn4, PeerIdentity) of
                        {ok, Conn5} ->
                            {ok, Conn5, S2};
                        Error -> Error
                    end;
                Error -> Error
            end;
        Error -> Error
    end.

set_mode(S, NewMode) ->
    S#state{security_mode = NewMode}.

token_id(#state{token_id = TokenId}) -> TokenId.

token_id(TokenId, #state{token_id = undefined} = State) ->
    State#state{token_id = TokenId}.

nonce(#state{security_mode = none}) -> <<>>;
nonce(#state{security_policy = SecurityPolicy}) ->
    L = SecurityPolicy#uacp_security_policy.secureChannelNonceLength,
    crypto:strong_rand_bytes(L).

derive_keys(_, _, #state{security_mode = none} = S) ->
    S;
derive_keys(LocalNonce, PeerNonce, S) ->
    LocalKeys = derive_keyset(PeerNonce, LocalNonce, S),
    PeerKeys = derive_keyset(LocalNonce, PeerNonce, S),
    % io:format("~p~n",[LocalKeys]),
    % io:format("~p~n",[PeerKeys]),
    S#state{symmetric_keys = #uacp_symmetric_keys{
        local = LocalKeys,
        peer = PeerKeys
    }}.

session_signature(PolicyType, PrivateKey, DerCert, Nonce) ->
    Payload = <<DerCert/binary, Nonce/binary>>,
    {ok, Policy} = opcua_util:security_policy(PolicyType),
    #{
        algorithm => Policy#uacp_security_policy.certificate_signature_algorithm,
        signature => public_key:sign(Payload, sha256, PrivateKey,
                                            [{rsa_padding, rsa_pkcs1_padding}])
    }.

encrypt_user_password(Conn, Password, ServerNonce, #uacp_security_policy{
        asymmetric_encryption_algorithm = {_Algo, Padding}}) ->
    PublicKey = opcua_connection:peer_public_key(Conn),
    CleatTextSize = byte_size(Password) + byte_size(ServerNonce),
    ClearText = <<CleatTextSize:32/little, Password/binary, ServerNonce/binary>>,
    public_key:encrypt_public(ClearText, PublicKey, [{rsa_padding, Padding}]).

decrypt_user_password(Conn, Secret, {_RsaAlg, Padding}) ->
    {ok, Policy} = opcua_util:security_policy(opcua_connection:security_policy(Conn)),
    #uacp_security_policy{secureChannelNonceLength = NL} = Policy,
    PrivateKey = opcua_connection:self_private_key(Conn),
    Result = public_key:decrypt_private(Secret, PrivateKey, [{rsa_padding, Padding}]),
    <<CleatTextSize:32/little, Rest/binary>> = Result,
    <<Password:(CleatTextSize - NL)/binary,_Nonce/binary>> = Rest,
    Password.

unlock(#uacp_chunk{security = #uacp_chunk_security{policy_uri = PolicyUri}} = Chunk,
       Conn,
       #state{security_policy = #uacp_security_policy{policy_uri = PolicyUri},
           security_mode = none} = State) ->
    validate_peer_sequence(State, Conn, decode_sequence_header(Conn, Chunk));
unlock(#uacp_chunk{security = TokenId} = Chunk,
        Conn,
       #state{token_id = TokenId, security_mode = none} = State) ->
    validate_peer_sequence(State, Conn, decode_sequence_header(Conn, Chunk));
unlock(#uacp_chunk{message_type = channel_open,
                   security = #uacp_chunk_security{policy_uri = PolicyUri} = ChunkSec,
                   header = [_MsgHeader, _SecHeader] = Headers,
                   body = EncryptedBody} = Chunk,
       Conn,
       #state{
            security_policy = #uacp_security_policy{
                    policy_uri = PolicyUri,
                    asymmetric_signature_algorithm = {_Alg, _Pad}}
            } = State) ->
    ok = validate_thumbprint(Conn, ChunkSec),
    PublicKey = opcua_connection:peer_public_key(Conn),
    Pub_L = opcua_crypto:rsa_keysize(PublicKey) div 8,
    PrivateKey = opcua_connection:self_private_key(Conn),
    Priv_L = opcua_crypto:rsa_keysize(PrivateKey),

    ClearData = asymmetric_decrypt(EncryptedBody, Conn, State),

    <<BodyWithPadding:(byte_size(ClearData) - Pub_L)/binary,
      Signature:Pub_L/binary>> = ClearData,

    SignedPayload = iolist_to_binary([ Headers, BodyWithPadding]),
    asymmetric_verify(SignedPayload, Signature, Conn, State),
    ClearBody = remove_footer(sign_and_encrypt, BodyWithPadding, Priv_L),
    Chunk2 = decode_sequence_header(Conn, Chunk#uacp_chunk{body = ClearBody}),
    validate_peer_sequence(State, Conn, Chunk2);
unlock(#uacp_chunk{security = TokenId,
                   header = [_MsgHeader, _SecHeader] = Headers,
                   body = Body} = Chunk,
        Conn,
       #state{token_id = TokenId,
            security_mode = SecurityMode,
            security_policy = #uacp_security_policy{
                    symmetric_encryption_algorithm = {_, KeySize, _},
                    derived_signature_keyLength = KL}} = State) ->
    Decrypted = symmetric_decrypt(Body, State),
    % io:format("Decrypted ~p~n",[Decrypted]),
    SigLen = KL div 8,
    <<PaddedBody:(byte_size(Decrypted) - SigLen)/binary, Signature:SigLen/binary>> = Decrypted,
    SignedPayload = iolist_to_binary([ Headers, PaddedBody]),
    symmetric_verify(SignedPayload, Signature, State),
    ClearBody = remove_footer(SecurityMode, PaddedBody, KeySize),
    Chunk2 = Chunk#uacp_chunk{body = ClearBody},
    validate_peer_sequence(State, Conn, decode_sequence_header(Conn, Chunk2));
unlock(_Chunk, _Conn, _State) ->
    {error, bad_security_checks_failed}.

% Client and server have different init steps
% the setup code is the same for both
setup(#uacp_chunk{state = unlocked, message_type = Type, security = undefined} = Chunk,
      Conn, #state{security_policy = Policy, token_id = TokenId,
                   security_mode = none} = State) ->
    ChunkSecurity = case Type of
        channel_open ->
            #uacp_chunk_security{
                policy_uri = Policy#uacp_security_policy.policy_uri
            };
        channel_message -> TokenId;
        channel_close -> TokenId
    end,
    {ok, Chunk#uacp_chunk{security = ChunkSecurity}, Conn, State};
setup(#uacp_chunk{state = unlocked, message_type = Type, security = undefined} = Chunk,
      Conn, #state{security_policy = Policy, token_id = TokenId} = State) ->
    SelfID = opcua_connection:self_identity(Conn),
    Chain = opcua_keychain:chain(Conn, SelfID, der),
    DerBlob = iolist_to_binary(Chain),
    PeerThumbprint = opcua_connection:peer_thumbprint(Conn),
    ChunkSecurity = case Type of
        channel_open ->
            #uacp_chunk_security{
                policy_uri = Policy#uacp_security_policy.policy_uri,
                sender_cert = DerBlob,
                receiver_thumbprint = PeerThumbprint
            };
        channel_message -> TokenId;
        channel_close -> TokenId
    end,
    {ok, Chunk#uacp_chunk{security = ChunkSecurity}, Conn, State}.

prepare(#uacp_chunk{state = unlocked,
                    header_size = HSize, unlocked_size = USize,
                    request_id = ReqId, body = Body} = Chunk,
        Conn,
        #state{security_mode = none} = State)
  when HSize =/= undefined, USize =/= undefined,
       ReqId =/= undefined, Body =/= undefined
    ->
    {SeqNum, State2} = next_sequence(State),
    Chunk2 = Chunk#uacp_chunk{
        sequence_num = SeqNum,
        locked_size = USize + 8
    },
    {ok, Chunk2, Conn, State2};
prepare(#uacp_chunk{state = unlocked,
                    security = #uacp_chunk_security{policy_uri = PolicyUri},
                    message_type = channel_open, header_size = HSize,
                    unlocked_size = USize, request_id = ReqId, body = Body} = Chunk,
        Conn,
        #state{security_policy = #uacp_security_policy{
                    asymmetric_encryption_algorithm = { _Alg, Padding},
                    policy_uri = PolicyUri}}
            = State)
  when
        HSize =/= undefined, USize =/= undefined,
        ReqId =/= undefined, Body =/= undefined
    ->
    ?assertEqual(USize, iolist_size(Body)),
    PrivateKey = opcua_connection:self_private_key(Conn),
    PublicKey = opcua_connection:peer_public_key(Conn),
    {SeqNum, State2} = next_sequence(State),
    SignatureSize = opcua_crypto:rsa_keysize(PrivateKey) div 8,
    EncryptedBlockSize = opcua_crypto:rsa_keysize(PublicKey) div 8,
    UnlockedSize = 8 + USize + SignatureSize,
    MaxBlockSize = EncryptedBlockSize - opcua_crypto:rsa_padding_offset(Padding),
    N_Blocks = fun(S, B) -> S div B + case S rem B of 0 -> 0; _ -> 1 end end,
    Blocks = N_Blocks(UnlockedSize, MaxBlockSize),
    FooterSize = byte_size(gen_footer(Chunk, Conn, State)),
    LockedSize = UnlockedSize + FooterSize +
                 opcua_crypto:rsa_padding_offset(Padding) * Blocks,
    % io:format("Estimated Asymmetric locked size: ~p~n",[LockedSize]),
    Chunk2 = Chunk#uacp_chunk{
        sequence_num = SeqNum,
        locked_size = LockedSize
    },
    {ok, Chunk2, Conn, State2};
prepare(#uacp_chunk{state = unlocked, security = ChunkSecurity, header_size = HSize,
                    unlocked_size = USize, request_id = ReqId, body = Body} = Chunk,
        Conn,
        #state{token_id = TokenId,
               security_mode = SecurityMode,
               security_policy = #uacp_security_policy{
                    derived_signature_keyLength = KL,
                    symmetric_encryption_algorithm = {_,_,BlockSize}}} = State)
  when ChunkSecurity =:= TokenId,
       HSize =/= undefined, USize =/= undefined,
       ReqId =/= undefined, Body =/= undefined ->
    ?assertEqual(USize, iolist_size(Body)),
    {SeqNum, State2} = next_sequence(State),
    SeqHeaderBodySize = USize + 8,
    Paddingsize = case SecurityMode of
        sign -> 0;
        sign_and_encrypt -> (BlockSize - SeqHeaderBodySize rem BlockSize)
    end,
    SignatureSize = KL div 8,
    LockedSize = SeqHeaderBodySize + Paddingsize + SignatureSize,
    % io:format("Estimated Symmetric Locked size: ~p~n",[LockedSize]),
    Chunk2 = Chunk#uacp_chunk{
        sequence_num = SeqNum,
        locked_size = LockedSize
    },
    {ok, Chunk2, Conn, State2}.

lock(#uacp_chunk{state = unlocked, message_type = channel_open,
                 security = #uacp_chunk_security{policy_uri = PolicyUri},
                 header = [MsgHeader, SecHeader],
                 sequence_num = SeqNum, request_id = ReqId, body = Body} = Chunk,
     Conn,
     #state{
        security_policy = #uacp_security_policy{policy_uri = PolicyUri},
        security_mode = SecMode} = State)
  when
       SeqNum =/= undefined,
       ReqId =/= undefined,
       Body =/= undefined ->
    % SIGN PACKET and apply ASIMMETRIC ENCRYPTION
    SeqHeader = opcua_uacp_codec:encode_sequence_header(Conn, SeqNum, ReqId),
    Footer = case SecMode of
        none -> <<>>;
        _ -> gen_footer(Chunk, Conn, State)
    end,
    ToBeSigned = iolist_to_binary([
        MsgHeader,
        SecHeader,
        SeqHeader,
        Body,
        Footer]),
    Signature = asymmetric_sign(ToBeSigned, Conn, State),
    ToBeEncrypted = iolist_to_binary([SeqHeader, Body, Footer, Signature]),
    % Asymmetric encryption with RSA
    Crypted = asymmetric_encrypt(ToBeEncrypted, Conn, State),
    % io:format("EncryptedSize: ~p\n",[byte_size(Crypted)]),
    {ok, Chunk#uacp_chunk{
        state = locked,
        body = Crypted
    }, Conn, State};
lock(#uacp_chunk{state = unlocked, security = Security, sequence_num = SeqNum,
        request_id = ReqId, header = [MsgHeader, SecHeader], body = Body} = Chunk,
     Conn,
     #state{security_mode = SecMode, token_id = TokenId} = State)
  when (Security =:= TokenId),
       SeqNum =/= undefined, ReqId =/= undefined, Body =/= undefined ->
    % SIGN PACKET and apply SIMMETRIC ENCRYPTION
    SeqHeader = opcua_uacp_codec:encode_sequence_header(Conn, SeqNum, ReqId),
    Footer = case SecMode of
        sign_and_encrypt -> gen_footer(Chunk, Conn, State);
        _ -> <<>>
    end,
    ToBeSigned = iolist_to_binary([
        MsgHeader,
        SecHeader,
        SeqHeader,
        Body,
        Footer]),
    Signature = symmetric_sign(ToBeSigned, State),
    % io:format("generated signature ~p~n",[Signature]),
    ToBeEncrypted = iolist_to_binary([
        SeqHeader,
        Body,
        Footer,
        Signature]),
    Crypted = symmetric_encrypt(ToBeEncrypted, State),
    % io:format("EncryptedSize: ~p\n",[byte_size(Crypted)]),
    {ok, Chunk#uacp_chunk{
        state = locked,
        body = Crypted
    }, Conn, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

validate_thumbprint(Conn, #uacp_chunk_security{receiver_thumbprint = T}) ->
    case {opcua_connection:self_thumbprint(Conn), T} of
        {_, undefined} -> ok;
        {Same, Same} -> ok;
        {_A, _B} -> {error, receiver_thumbprint_mismatch}
    end.

decode_sequence_header(Space, #uacp_chunk{body = Body} = Chunk) ->
    {{SeqNum, ReqId}, RemBody} =
        opcua_uacp_codec:decode_sequence_header(Space, Body),
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

generate_token_id(undefined) ->
    Token = crypto:bytes_to_integer(crypto:strong_rand_bytes(4)),
    case lists:member(Token, [0]) of
        true -> generate_token_id(undefined);
        false -> Token
    end;
generate_token_id(#state{token_id = TokenId} = OldState) ->
    Token = crypto:bytes_to_integer(crypto:strong_rand_bytes(4)),
    case lists:member(Token, [0, TokenId]) of
        true -> generate_token_id(OldState);
        false -> Token
    end.



asymmetric_sign(_, _, #state{security_mode = none}) ->
    <<>>;
asymmetric_sign(Binary, Conn,
                #state{
                    security_policy = #uacp_security_policy{
                    asymmetric_signature_algorithm = {Alg, Pad},
                    min_asymmetric_keyLength = MinKL,
                    max_asymmetric_keyLength = MaxKL}}) ->
    PrivateKey = opcua_connection:self_private_key(Conn),
    KeySize = opcua_crypto:rsa_keysize(PrivateKey),
    ?assert((KeySize >= MinKL) andalso (KeySize =< MaxKL)),
    public_key:sign(Binary, Alg, PrivateKey, [{rsa_padding, Pad}]).


asymmetric_verify(_, _, _, #state{security_mode = none}) ->
    ok;
asymmetric_verify(SignedPayload, Signature, Conn,
                    #state{
                        security_policy = #uacp_security_policy{
                            asymmetric_signature_algorithm = {Alg, Pad}}}) ->
    PublicKey = opcua_connection:peer_public_key(Conn),
    ?assert(public_key:verify(SignedPayload, Alg, Signature, PublicKey,
                                                        [{rsa_padding, Pad}])).

% Available padding opts
% rsa_pkcs1_padding | rsa_pkcs1_oaep_padding |
% rsa_sslv23_padding | rsa_x931_padding | rsa_no_padding
asymmetric_encrypt(Binary, _Conn, #state{security_mode = none}) ->
    Binary;
asymmetric_encrypt(Binary, Conn,
                   #state{security_policy = #uacp_security_policy{
                            asymmetric_encryption_algorithm = {_Alg, Pad}}}) ->
    PublicKey = opcua_connection:peer_public_key(Conn),
    opcua_crypto:rsa_encrypt(Binary, PublicKey, Pad).

asymmetric_decrypt(Binary, _Conn, #state{security_mode = none}) ->
    Binary;
asymmetric_decrypt(Binary, Conn,
                   #state{
                    security_policy = #uacp_security_policy{
                    asymmetric_encryption_algorithm = {_Alg, Pad}}}) ->
    PrivateKey = opcua_connection:self_private_key(Conn),
    opcua_crypto:rsa_decrypt(Binary, PrivateKey, Pad).

symmetric_sign(_,#state{security_mode = none}) ->
    <<>>;
symmetric_sign(Binary, #state{
        symmetric_keys = #uacp_symmetric_keys{local = #uacp_keyset{
        signing = SigningKey}},
        security_policy = #uacp_security_policy{
                                symmetric_signature_algorithm = Alg}}) ->
    crypto:mac(hmac, Alg, SigningKey, Binary).

symmetric_verify(_, _, #state{security_mode = none}) ->
    ok;
symmetric_verify(Binary, Signature, #state{
    symmetric_keys = #uacp_symmetric_keys{peer = #uacp_keyset{
    signing = SigningKey}},
    security_policy = #uacp_security_policy{
                            symmetric_signature_algorithm = Alg}}) ->
    ?assertMatch(Signature, crypto:mac(hmac, Alg, SigningKey, Binary)).


symmetric_encrypt(Binary, #state{security_mode = sign_and_encrypt,
                        symmetric_keys = #uacp_symmetric_keys{local = #uacp_keyset{
                            encryption = EncryptionKey,
                            iv = InitVector}},
                        security_policy = #uacp_security_policy{
                            symmetric_encryption_algorithm = {Alg, _, _}}}) ->
    % io:format("Key size ~p~n",[byte_size(EncryptionKey)]),
    % io:format("InitVector size: ~p~n",[byte_size(InitVector)]),
    % io:format("Binary size: ~p~n",[byte_size(Binary)]),
    crypto:crypto_one_time(Alg, EncryptionKey, InitVector, Binary, [{encrypt, true}]);
symmetric_encrypt( Binary, _S) ->
    Binary.

% Not sure why using the peer keys works here,
% the peer should encrypt with our Key,
% but i have to use the peer keyset to have a successful decryption.
symmetric_decrypt(Binary, #state{security_mode = sign_and_encrypt,
                    symmetric_keys = #uacp_symmetric_keys{peer = #uacp_keyset{
                        encryption = EncryptionKey,
                        iv = InitVector}},
                    security_policy = #uacp_security_policy{
                        symmetric_encryption_algorithm = {Alg, _, _}}}) ->
    crypto:crypto_one_time(Alg, EncryptionKey, InitVector, Binary, [{encrypt, false}]);
symmetric_decrypt( Binary, _S) ->
    Binary.

% footer is only needed for the encryption
gen_footer(#uacp_chunk{message_type = channel_open, unlocked_size = UnlockedSize},
           Conn,
           #state{security_policy = #uacp_security_policy{
                    asymmetric_encryption_algorithm = { _Alg, Padding}}}) ->
    PublicKey = opcua_connection:peer_public_key(Conn),
    PrivateKey = opcua_connection:self_private_key(Conn),
    SignatureKeySize = opcua_crypto:rsa_keysize(PrivateKey),
    SignatureSize = SignatureKeySize div 8,
    EncryptionKeySize = opcua_crypto:rsa_keysize(PublicKey),
    EncryptedBlockSize = EncryptionKeySize div 8,
    MaxBlockSize = EncryptedBlockSize - opcua_crypto:rsa_padding_offset(Padding),
    PayloadSize = (UnlockedSize + 8 + SignatureSize),
    PadSize = MaxBlockSize - PayloadSize rem MaxBlockSize,
    assemble_footer(PadSize, EncryptionKeySize);
gen_footer(#uacp_chunk{unlocked_size = UnlockedSize}, _Conn,
            #state{security_policy = #uacp_security_policy{
                symmetric_encryption_algorithm = {_, KeySize, BlockSize}}}) ->
    PadSize =  BlockSize - (UnlockedSize + 8) rem BlockSize,
    assemble_footer(PadSize, KeySize).

assemble_footer(FooterSize, KeySize) when KeySize =< 2048 ->
    PadSizeValue = FooterSize - 1,
    Padding = << <<PadSizeValue:8>> || _ <- lists:seq(1,PadSizeValue)>>,
    <<PadSizeValue:8, Padding/binary>>;
assemble_footer(FooterSize, KeySize) when KeySize > 2048 ->
    PadSizeValue = FooterSize - 2,
    Padding = << <<PadSizeValue:8>> || _ <- lists:seq(1,PadSizeValue) >>,
    <<MostSigByte:8,_:8>> = <<PadSizeValue:16>>,
    ExtraPaddingSizeByte = <<MostSigByte:8>>,
    <<PadSizeValue:8, Padding/binary, ExtraPaddingSizeByte/binary>>.

remove_footer(sign_and_encrypt, Binary, KeySize) ->
    do_remove_footer(Binary, KeySize);
remove_footer(_, Binary, _) ->
    Binary.

do_remove_footer(Binary, KeySize) ->
    Size = byte_size(Binary),
    PaddingSize = case KeySize =< 2048 of
        true ->
            <<_Rest:(Size - 1)/binary, PaddingSize_:8>> = Binary,
            PaddingSize_;
        false ->
            <<_Rest:(Size - 2)/binary, LeastSigByte:8, MostSigByte:8>> = Binary,
            binary_to_integer(<<MostSigByte:8,LeastSigByte:8>>)
    end,
    BodySize = Size - PaddingSize - 1,
    <<Body:BodySize/binary,
      _PaddingSizeByte:8,
      _PaddingBytes:PaddingSize/binary>> = Binary,
    Body.

derive_keyset(Secret, Seed, #state{security_policy =
        #uacp_security_policy{
            key_derivation_algorithm = KeyDerivationAlgorithm,
            derived_signature_keyLength = DerivedSignatureKeyLength,
            symmetric_encryption_algorithm =
                {_Alg, EncryptingKeyLength, EncryptingBlockLength}}}) ->
    DSKL_bytes = DerivedSignatureKeyLength div 8,
    EKL_bytes =  EncryptingKeyLength div 8,
    Length = DSKL_bytes + EKL_bytes + EncryptingBlockLength,
    Result = opcua_crypto:p_hash_rfc5246(KeyDerivationAlgorithm, Secret, Seed, Length),
    #uacp_keyset{
        signing = erlang:binary_part(Result, 0, DSKL_bytes),
        encryption = erlang:binary_part(Result,
                                        DSKL_bytes,
                                        EKL_bytes),
        iv = erlang:binary_part(Result,
                                DSKL_bytes + EKL_bytes,
                                EncryptingBlockLength)
    }.


