-module(opcua_crypto).

-export([
    rsa_encrypt/3,
    rsa_padding_offset/1,
    rsa_keysize/1,
    rsa_decrypt/3,
    p_hash_rfc5246/4
]).

-include_lib("public_key/include/OTP-PUB-KEY.hrl").

rsa_encrypt(Binary, PubKey, Padding) ->
    RSAopts = [{rsa_padding, Padding}],
    BlockSize = rsa_keysize(PubKey) div 8 - rsa_padding_offset(Padding),
    rsa_encrypt_block(Binary, BlockSize, PubKey, RSAopts, <<>>).

rsa_encrypt_block(<<>>, _, _, _, Acc) ->
    Acc;
rsa_encrypt_block(Binary, BlockSize, PubKey, RSAopts, Acc) ->
    <<BinaryBlock:BlockSize/binary, Rest/binary>> = Binary,
    EncryptedBlock = public_key:encrypt_public(BinaryBlock, PubKey, RSAopts),
    rsa_encrypt_block(Rest, BlockSize, PubKey, RSAopts,
                                        <<Acc/binary,EncryptedBlock/binary>>).

rsa_padding_offset(rsa_pkcs1_oaep_padding) -> 42;
rsa_padding_offset(_) -> error(unhandled_padding).

% Size is in bits
rsa_keysize(#'RSAPrivateKey'{} = Key) ->
    byte_size(integer_to_binary(Key#'RSAPrivateKey'.modulus, 16)) * 4;
rsa_keysize(#'RSAPublicKey'{} = Key) ->
    byte_size(integer_to_binary(Key#'RSAPublicKey'.modulus, 16)) * 4.

rsa_decrypt(Binary, PrivKey, Padding) ->
    RSAopts = [{rsa_padding, Padding}],
    BlockSize = rsa_keysize(PrivKey) div 8,
    rsa_decrypt_block(Binary, BlockSize, PrivKey, RSAopts, <<>>).

rsa_decrypt_block(<<>>, _, _, _, Acc) ->
    Acc;
rsa_decrypt_block(Binary, BlockSize, PrivKey, RSAopts, Acc) ->
    <<Encrypted:BlockSize/binary, Rest/binary>> = Binary,
    ClearBinary = public_key:decrypt_private(Encrypted, PrivKey, RSAopts),
    rsa_decrypt_block(Rest, BlockSize, PrivKey, RSAopts,
                                            <<Acc/binary,ClearBinary/binary>>).

% p_hash algoritm used by TLS and pointed by the Basic256Sha256 policy:
% https://tools.ietf.org/html/rfc5246
p_hash_rfc5246(Algo = sha256, Secret, Seed, Length) ->
    A = fun A(0) -> Seed;
            A(I) -> crypto:mac(hmac, Algo, Secret, A(I-1))
        end,
        p_hash_rfc5246(Algo, Secret, Seed, A, 1, Length, <<>>).
% P_HASH(secret, seed) =
% HMAC_HASH(secret, A(1) + seed) +
% HMAC_HASH(secret, A(2) + seed) +
% HMAC_HASH(secret, A(3) + seed) +
%
% + indicates that the results are appended to previous results.
%
% A(n) is defined as:
% A(0) = seed
% A(n) = HMAC_HASH(secret, A(n-1))
p_hash_rfc5246(Algo, Secret, Seed, A, I, Length, Acc) ->
    Step = crypto:mac(hmac, Algo, Secret, <<(A(I))/binary, Seed/binary>>),
    Result = <<Acc/binary, Step/binary>>,
    case byte_size(Result) >= Length of
        true -> erlang:binary_part(Result, 0, Length);
        false ->
            p_hash_rfc5246(Algo, Secret, Seed, A, I+1, Length, Result)
    end.

