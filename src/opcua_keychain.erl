-module(opcua_keychain).

%%% BEHAVIOUR opcua_keychain DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-callback init(Args) -> {ok, State} | {error, Reason}
    when Args :: term(), State :: term(), Reason :: term().

-callback info(State, Ident) -> Info | not_found
    when State :: term(), Ident :: ident(), Info :: ident_info().

-callback certificate(State, Ident, Format) -> {ok, Cert} | not_found
    when State :: term(), Ident :: ident(),
         Format :: format(), Cert :: certificate().

-callback private_key(State, Ident, Format) -> {ok, Key} | not_found
    when State :: term(), Ident :: ident(),
         Format :: format(), Key :: private_key().

-callback public_key(State, Ident, Format) -> {ok, Key} | not_found
    when State :: term(), Ident :: ident(),
         Format :: format(), Key :: public_key().

-callback add_certificate(State, Data) ->
        {ok, Info, State} | {error, Reason}
    when State :: term(), Data :: binary(), Info :: ident_info(),
         Reason :: term().

-callback add_private(State, Data) ->
        {ok, Info, State} | {error, Reason}
    when State :: term(), Data :: binary(), Info :: ident_info(),
         Reason :: certificate_not_found | term().

-callback trust(State, Ident) -> {ok, State} | {error, Reason}
    when State :: term(), Ident :: ident(), Reason :: term().


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("public_key/include/public_key.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([new/2, new/3]).
-export([info/1, info/2]).
-export([certificate/2, certificate/3]).
-export([public_key/2, public_key/3]).
-export([private_key/2, private_key/3]).
-export([load_pem/2, load_pem/3]).
-export([load_certificate/1, load_certificate/2]).
-export([load_private/1, load_private/2]).
-export([add_pem/2, add_pem/3]).
-export([add_certificate/1, add_certificate/2]).
-export([add_private/1, add_private/2]).
-export([trust/1, trust/2]).
-export([chain/2, chain/3]).
-export([validate/1, validate/2, validate/3]).

% Helper functions for keychain handlers
-export([certificate_id/1]).
-export([public_key_id/1]).
-export([private_key_id/1]).
-export([certificate_thumbprint/1]).
-export([certificate_capabilities/1]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type state() :: term().
-type ident() :: binary().
-type filename() :: binary().
-type format() :: der | rec.
-type certificate() :: binary() | #'OTPCertificate'{}.
-type private_key() :: binary() | public_key:private_key().
-type public_key() :: binary() | public_key:public_key().
-type capability() :: decrypt | encrypt | sign | authenticate | ca.
-type ident_info() :: #{
    ident := binary(),
    thumbprint := binary(),
    capabilities := [capability()],
    is_trusted := boolean(),
    has_private := boolean()
}.

-export_type([ident/0, format/0, capability/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Mod, Args) ->
    new(default, Mod, Args).

new(Parent, Mod, Args) ->
    case Mod:init(Args) of
        {ok, Sub} -> {ok, {Mod, Sub, Parent}};
        {error, _Reason} = Error -> Error
    end.

-spec info(ident()) -> not_found | ident_info().
info(Ident) ->
    opcua_keychain_default:info(Ident).

-spec info(state(), ident()) -> not_found | ident_info().
info(undefined, _Ident) -> not_found;
info(default, Ident) -> info(Ident);
info({Mod, Sub, Parent}, Ident) ->
    case Mod:info(Sub, Ident) of
        not_found -> info(Parent, Ident);
        Result -> Result
    end.

-spec certificate(ident(), format()) -> certificate() | not_found.
certificate(Ident, Format) ->
    opcua_keychain_default:certificate(Ident, Format).

-spec certificate(state(), ident(), format()) -> certificate() | not_found.
certificate(undefined, _Ident, _Format) -> not_found;
certificate(default, Ident, Format) ->
    certificate(Ident, Format);
certificate({Mod, Sub, Parent}, Ident, Format) ->
    case Mod:certificate(Sub, Ident, Format) of
        not_found -> certificate(Parent, Ident, Format);
        Result -> Result
    end.

-spec public_key(ident(), format()) -> public_key() | not_found.
public_key(Ident, Format) ->
    opcua_keychain_default:public_key(Ident, Format).

-spec public_key(state(), ident(), format()) -> public_key() | not_found.
public_key(undefined, _Ident, _Format) -> not_found;
public_key(default, Ident, Format) ->
    public_key(Ident, Format);
public_key({Mod, Sub, Parent}, Ident, Format) ->
    case Mod:public_key(Sub, Ident, Format) of
        not_found -> public_key(Parent, Ident, Format);
        Result -> Result
    end.

-spec private_key(ident(), format()) -> private_key() | not_found.
private_key(Ident, Format) ->
    opcua_keychain_default:private_key(Ident, Format).

-spec private_key(state(), ident(), format()) -> public_key() | not_found.
private_key(undefined, _Ident, _Format) -> not_found;
private_key(default, Ident, Format) ->
    private_key(Ident, Format);
private_key({Mod, Sub, Parent}, Ident, Format) ->
    case Mod:private_key(Sub, Ident, Format) of
        not_found -> private_key(Parent, Ident, Format);
        Result -> Result
    end.

-spec load_pem(filename(), undefined | binary()) ->
    {ok, [ident_info()]} | {error, Reason :: term()}.
load_pem(Filename, Password) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_pem(Data, Password)
    end.

-spec load_pem(state(), filename(), undefined | binary()) ->
    {ok, [ident_info()], state()} | {error, Reason :: term()}.
load_pem(undefined, _Filename, _Password) -> {error, undefined_keychain};
load_pem(default, Filename, Password) ->
    case load_pem(Filename, Password) of
        {ok, Infos} -> {ok, Infos, default};
        Result -> Result
    end;
load_pem(State, Filename, Password) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} ->
            case add_pem(State, Data, Password) of
                {ok, Infos, State2} -> {ok, Infos, State2};
                Result -> Result
            end
    end.

-spec load_certificate(filename()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
load_certificate(Filename) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_certificate(Data)
    end.

-spec load_certificate(state(), filename()) ->
    {ok, ident_info(), state()} | {error, Reason :: term()}.
load_certificate(undefined, _Filename) -> {error, undefined_keychain};
load_certificate(default, Filename) ->
    case load_certificate(Filename) of
        {ok, Info} -> {ok, Info, default};
        Result -> Result
    end;
load_certificate({Mod, Sub, Parent}, Filename) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} ->
            case Mod:add_certificate(Sub, Data) of
                {ok, Info, Sub2} -> {ok, Info, {Mod, Sub2, Parent}};
                Result -> Result
            end
    end.

-spec load_private(filename()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
load_private(Filename) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_private(Data)
    end.

-spec load_private(state(), filename()) ->
    {ok, ident_info(), state()} | {error, Reason :: term()}.
load_private(undefined, _Filename) -> {error, undefined_keychain};
load_private(default, Filename) ->
    opcua_keychain_default:load_private(Filename);
load_private({Mod, Sub, Parent}, Filename) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} ->
            case Mod:add_private(Sub, Data) of
                {ok, Info, Sub2} -> {ok, Info, {Mod, Sub2, Parent}};
                Result -> Result
            end
    end.

-spec add_pem(binary(), undefined | binary()) ->
    {ok, [ident_info()]} | {error, Reason :: term()}.
add_pem(Data, Password) ->
    case decode_pem(Data, Password, undefined, fun
        (cert, undefined, Der) ->
            case opcua_keychain_default:add_certificate(Der) of
                {ok, Info} -> {ok, Info, undefined};
                Result -> Result
            end;
        (priv, undefined, Der) ->
            case opcua_keychain_default:add_private(Der) of
                {ok, Info} -> {ok, Info, undefined};
                Result -> Result
            end
    end) of
        {ok, Infos, undefined} -> {ok, Infos};
        Result -> Result
    end.

-spec add_pem(state(), binary(), undefined | binary()) ->
    {ok, [ident_info()], state()} | {error, Reason :: term()}.
add_pem(undefined, Data, Password) -> {error, undefined_keychain};
add_pem(default, Data, Password) ->
    case add_pem(Data, Password) of
        {ok, Infos} -> {ok, Infos, default};
        Result -> Result
    end;
add_pem({Mod, Sub, Parent}, Data, Password) ->
    case decode_pem(Data, Password, Sub, fun
        (cert, State, Der) -> Mod:add_certificate(State, Der);
        (priv, State, Der) -> Mod:add_private(State, Der)
    end) of
        {ok, Infos, Sub2} -> {ok, Infos, {Mod, Sub2, Parent}};
        Result -> Result
    end.

-spec add_certificate(binary()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
add_certificate(Data) ->
    opcua_keychain_default:add_certificate(Data).

-spec add_certificate(state(), binary()) ->
    {ok, ident_info(), state()} | {error, Reason :: term()}.
add_certificate(undefined, _Data) -> {error, undefined_keychain};
add_certificate(default, Data) ->
    case add_certificate(Data) of
        {ok, Info} -> {ok, Info, default};
        Result -> Result
    end;
add_certificate({Mod, Sub, Parent}, Data) ->
    case Mod:add_certificate(Sub, Data) of
        {ok, Info, Sub2} -> {ok, Info, {Mod, Sub2, Parent}};
        Result -> Result
    end.

-spec add_private(binary()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
add_private(Data) ->
    opcua_keychain_default:add_private(Data).

-spec add_private(state(), binary()) ->
    {ok, ident_info(), state()} | {error, Reason :: term()}.
add_private(undefined, _Data) -> {error, undefined_keychain};
add_private(default, Data) ->
    case add_private(Data) of
        {ok, Info} -> {ok, Info, default};
        Result -> Result
    end;
add_private({Mod, Sub, Parent}, Data) ->
    case Mod:add_private(Sub, Data) of
        {ok, Info, Sub2} -> {ok, Info, {Mod, Sub2, Parent}};
        Result -> Result
    end.

-spec trust(ident()) -> ok | not_found.
trust(Ident) ->
    opcua_keychain_default:trust(Ident).

-spec trust(state(), ident()) -> {ok, state()} | not_found.
trust(undefined, _Ident) -> {error, undefined_keychain};
trust(default, Ident) ->
    case trust(Ident) of
        ok -> {ok, default};
        Result -> Result
    end;
trust({Mod, Sub, Parent}, Ident) ->
    case Mod:trust(Sub, Ident) of
        {ok, Sub2} -> {ok, {Mod, Sub2, Parent}};
        not_found ->
            case trust(Parent, Ident) of
                {ok, Parent2} -> {ok, {Mod, Sub, Parent2}};
                Result -> Result
            end;
        Result -> Result
    end.

-spec chain(ident(), format()) -> [certificate()] | not_found.
chain(_Ident, _Format) ->
    %TODO: use othre functions to recursively retrieve the certification chain,
    not_implemented.

-spec chain(state(), ident(), format()) -> [certificate()] | not_found.
chain(_State, _Ident, _Format) ->
    %TODO: use othre functions to recursively retrieve the certification chain,
    not_implemented.

-spec validate(ident()) ->
    ok | {error, Reason :: term()}.
validate(_Ident) ->
    %TODO: implemente chain validation
    ok.

-spec validate(state() | [certificate()], ident() | format()) ->
    {ok, state()} | {error, Reason :: term()}.
validate(Chain, _Format) when is_list(Chain) ->
    %TODO: implemente chain validation
    ok;
validate(State, _Ident) ->
    %TODO: implemente chain validation
    {ok, State}.

-spec validate(state(), [certificate()], format()) ->
    {ok, state()} | {error, Reason :: term()}.
validate(State, _Chain, _Format) ->
    %TODO: implemente chain validation
    {ok, State}.


%%% HELPER FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

certificate_id(#'OTPCertificate'{} = CertRec) ->
    KeyRec = CertRec#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subjectPublicKeyInfo#'OTPSubjectPublicKeyInfo'.subjectPublicKey,
    public_key_id(KeyRec).

public_key_id(#'RSAPublicKey'{} = KeyRec) ->
    %TODO: Add support for EC
    crypto:hash(sha, integer_to_binary(KeyRec#'RSAPublicKey'.modulus)).

private_key_id(#'RSAPrivateKey'{} = KeyRec) ->
    %TODO: Add support for EC
    crypto:hash(sha, integer_to_binary(KeyRec#'RSAPrivateKey'.modulus)).

certificate_thumbprint(CertDer) ->
    crypto:hash(sha, CertDer).

certificate_capabilities(_CertRec) ->
    %TODO: implement
    [].


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_pem(Data, Password, State, Fun) ->
    %TODO: Add suport for EC keys
    %TODO: Handle the private key being before the certificate
    %TODO: Maybe keep the certificate order ?
    decode_pem_entries(public_key:pem_decode(Data), Password, State, Fun, #{}).

decode_pem_entries([], _Password, State, _Fun, Acc) ->
    {ok, maps:values(Acc), State};
decode_pem_entries([{Type, _EncDer, Algo} = Entry | Rest], Password, State, Fun, Acc)
  when Algo =/= not_encrypted ->
    %TODO: Do some type checking ? See public_key:decode_pem_entries/2
    DecDer = pubkey_pem:decipher(Entry, Password),
    decode_pem_entries([{Type, DecDer, not_encrypted} | Rest], Password, State, Fun, Acc);
decode_pem_entries([{'Certificate', CertDer, not_encrypted} | Rest], Password, State, Fun, Acc) ->
    case Fun(cert, State, CertDer) of
        {ok, #{id := Id} = Info, State2} ->
            decode_pem_entries(Rest, Password, State2, Fun, Acc#{Id => Info});
        Result -> Result
    end;
decode_pem_entries([{'RSAPrivateKey', KeyDer, not_encrypted} | Rest], Password, State, Fun, Acc) ->
    case Fun(priv, State, KeyDer) of
        {ok, #{id := Id} = Info, State2} ->
            decode_pem_entries(Rest, Password, State2, Fun, Acc#{Id => Info});
        Result -> Result
    end;
decode_pem_entries([_Other | Rest], Password, State, Fun, Acc) ->
    decode_pem_entries(Rest, Password, State, Fun, Acc).
