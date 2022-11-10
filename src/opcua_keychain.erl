%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Keychain managment abstraction module.
%%%
%%% Interface to all keychain backends.
%%% The identity is defined by a binary that is the sha1 of the key modulo so
%%% it is the same for the certificate and for the privagte key.
%%%
%%% TODO:
%%%  - Implement certificate lookup functions
%%%  - Implement certification chain retrieval
%%%  - Implement certification chain validation
%%%  - Optionaly use system CA certificatges
%%%  - Support giving away a keychain to another process
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_keychain).

%%% BEHAVIOUR opcua_keychain DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-callback init(Args) -> {ok, State} | {error, Reason}
    when Args :: term(), State :: term(), Reason :: term().

-callback shareable(State) -> State
    when State :: term().

-callback lookup(State, Method, Params) -> [Ident] | not_found
    when State :: term(), Method :: lookup_method(), Params :: lookup_params(),
         Ident :: ident().

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

-callback add_certificate(State, Data, Opts) ->
        {ok, Info, State} | {error, Reason}
    when State :: term(), Data :: binary(), Info :: ident_info(),
         Opts :: ident_options(), Reason :: read_only | term().

-callback add_private(State, Data) ->
        {ok, Info, State} | {error, Reason}
    when State :: term(), Data :: binary(), Info :: ident_info(),
         Reason :: read_only | certificate_not_found | term().

-callback trust(State, Ident) -> {ok, State} | {error, Reason}
    when State :: term(), Ident :: ident(), Reason :: read_only | term().


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("public_key/include/public_key.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([new/2, new/3]).
-export([shareable/1]).
-export([lookup/2, lookup/3]).
-export([info/1, info/2]).
-export([certificate/2, certificate/3]).
-export([public_key/2, public_key/3]).
-export([private_key/2, private_key/3]).
-export([load_pem/1, load_pem/2, load_pem/3]).
-export([load_certificate/1, load_certificate/2, load_certificate/3]).
-export([load_private/1, load_private/2]).
-export([add_pem/1, add_pem/2, add_pem/3]).
-export([add_certificate/1, add_certificate/2, add_certificate/3]).
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

-type state() :: default | term().
-type ident() :: binary().
-type lookup_method() :: alias.
-type lookup_params() :: term().
-type filename() :: binary().
-type format() :: der | rec.
-type certificate() :: binary() | #'OTPCertificate'{}.
-type private_key() :: binary() | public_key:private_key().
-type public_key() :: binary() | public_key:public_key().
-type ident_options() :: #{
    alias => atom(),
    password => binary(),
    is_trusted => boolean()
}.
-type capability() :: decrypt | encrypt | sign | authenticate | ca.
-type ident_info() :: #{
    id := binary(),
    alias := undefined | atom(),
    thumbprint := binary(),
    capabilities := [capability()],
    is_trusted := boolean(),
    has_private := boolean()
}.

-export_type([state/0, ident/0, format/0, capability/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc Creates a new keychain with given callback module.
new(Mod, Args) ->
    new(default, Mod, Args).

% @doc Creates a new keychain with given callback module and parent keychain.
new(Parent, Mod, Args) ->
    case Mod:init(Args) of
        {ok, Sub} -> {ok, {Mod, Sub, Parent}};
        {error, _Reason} = Error -> Error
    end.

% @doc Makes the keychain shareable with other processes.
% The result must be a read-only keychain that can be passed to onther
% processes without conflicts.
-spec shareable(state()) -> state().
shareable(default) -> default;
shareable({Mod, Sub, Parent}) ->
    {Mod, Mod:shareable(Sub), shareable(Parent)}.

% @doc Lookups an identity in default keychain.
-spec lookup(lookup_method(), lookup_params()) -> [ident()] | not_found.
lookup(Method, Params) ->
    opcua_keychain_default:lookup(Method, Params).

% @doc Lookups an identity in given keychain.
-spec lookup(state(), lookup_method(), lookup_params()) -> [ident()] | not_found.
lookup(undefined, _Method, _Params) -> not_found;
lookup(default, Method, Params) -> lookup(Method, Params);
lookup({Mod, Sub, Parent}, Method, Params) ->
    case Mod:lookup(Sub, Method, Params) of
        not_found -> lookup(Parent, Method, Params);
        Result -> Result
    end.

% @doc Returns information about an identity from the default keychain.
-spec info(ident()) -> not_found | ident_info().
info(Ident) ->
    opcua_keychain_default:info(Ident).

% @doc Returns information about an identity from the given keychain.
-spec info(state(), ident()) -> not_found | ident_info().
info(undefined, _Ident) -> not_found;
info(default, Ident) -> info(Ident);
info({Mod, Sub, Parent}, Ident) ->
    case Mod:info(Sub, Ident) of
        not_found -> info(Parent, Ident);
        Result -> Result
    end.

% @doc Returns a certificate in either DER or OTP record for an
% identity identifier from the default keychain.
-spec certificate(ident(), format()) -> certificate() | not_found.
certificate(Ident, Format) ->
    opcua_keychain_default:certificate(Ident, Format).

% @doc Returns a certificate in either DER or OTPCertificate record for an
% identity identifier from the given keychain.
-spec certificate(state(), ident(), format()) -> certificate() | not_found.
certificate(undefined, _Ident, _Format) -> not_found;
certificate(default, Ident, Format) ->
    certificate(Ident, Format);
certificate({Mod, Sub, Parent}, Ident, Format) ->
    case Mod:certificate(Sub, Ident, Format) of
        not_found -> certificate(Parent, Ident, Format);
        Result -> Result
    end.

% @doc Returns a public key in either DER or OTP record for an
% identity identifier from the default keychain.
-spec public_key(ident(), format()) -> public_key() | not_found.
public_key(Ident, Format) ->
    opcua_keychain_default:public_key(Ident, Format).

% @doc Returns a public key in either DER or OTP record for an
% identity identifier from the given keychain.
-spec public_key(state(), ident(), format()) -> public_key() | not_found.
public_key(undefined, _Ident, _Format) -> not_found;
public_key(default, Ident, Format) ->
    public_key(Ident, Format);
public_key({Mod, Sub, Parent}, Ident, Format) ->
    case Mod:public_key(Sub, Ident, Format) of
        not_found -> public_key(Parent, Ident, Format);
        Result -> Result
    end.

% @doc Returns a private key in either DER or OTP record for an
% identity identifier from the default keychain.
-spec private_key(ident(), format()) -> private_key() | not_found.
private_key(Ident, Format) ->
    opcua_keychain_default:private_key(Ident, Format).

% @doc Returns a private key in either DER or OTP record for an
% identity identifier from the given keychain.
-spec private_key(state(), ident(), format()) -> public_key() | not_found.
private_key(undefined, _Ident, _Format) -> not_found;
private_key(default, Ident, Format) ->
    private_key(Ident, Format);
private_key({Mod, Sub, Parent}, Ident, Format) ->
    case Mod:private_key(Sub, Ident, Format) of
        not_found -> private_key(Parent, Ident, Format);
        Result -> Result
    end.

% @doc Loads certificates and keys from given PEM file into the default
% keychain without ny options.
-spec load_pem(filename()) ->
    {ok, [ident_info()]} | {error, Reason :: term()}.
load_pem(Filename) ->
    load_pem(Filename, #{}).

% @doc Loads certificates and keys from given PEM file into the default keychain
% with given options, or into given keychain without any options.
-spec load_pem(state() | filename(), filename() | ident_options()) ->
    {ok, [ident_info()]} | {ok, [ident_info()], state()} | {error, Reason :: term()}.
load_pem(NamedState, Filename) when is_atom(NamedState) ->
    load_pem(NamedState, Filename, #{});
load_pem({_Mod, _Sub, _Parent} = State, Filename) ->
    load_pem(State, Filename, #{});
load_pem(Filename, Opts) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_pem(Data, Opts)
    end.

% @doc Loads certificates and keys from given PEM file into the given keychain.
% Could fail if the keystore is read-only after being made shareable.
-spec load_pem(state(), filename(), ident_options()) ->
    {ok, [ident_info()], state()} | {error, Reason :: term()}.
load_pem(undefined, _Filename, _Opts) -> {error, undefined_keychain};
load_pem(default, Filename, Opts) ->
    case load_pem(Filename, Opts) of
        {ok, Infos} -> {ok, Infos, default};
        Result -> Result
    end;
load_pem(State, Filename, Opts) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_pem(State, Data, Opts)
    end.

% @doc Loads a certificate from given DER file into the default keychain
% without any options.
-spec load_certificate(filename()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
load_certificate(Filename) ->
    load_certificate(Filename, #{}).

% @doc Loads a certificate from given DER file into the given keychain without
% any options, or in the default keychain with given options.
-spec load_certificate(state() | filename(), filename() | ident_options()) ->
    {ok, ident_info()} | {ok, ident_info(), state()} | {error, Reason :: term()}.
load_certificate(NameState, Filename) when is_atom(NameState) ->
    load_certificate(NameState, Filename, #{});
load_certificate({_Mod, _Sub, _Parent} = State, Filename) ->
    load_certificate(State, Filename, #{});
load_certificate(Filename, Opts) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_certificate(Data, Opts)
    end.

% @doc Loads a certificate from given DER file into the given keychain with
% given options.
% Could fail if the keystore is read-only after being made shareable.
-spec load_certificate(state(), filename(), ident_options()) ->
    {ok, ident_info(), state()} | {error, Reason :: term()}.
load_certificate(undefined, _Filename, _Opts) -> {error, undefined_keychain};
load_certificate(default, Filename, Opts) ->
    case load_certificate(Filename, Opts) of
        {ok, Info} -> {ok, Info, default};
        Result -> Result
    end;
load_certificate(State, Filename, Opts) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_certificate(State, Data, Opts)
    end.

% @doc Loads a privagte key from given DER file into the default keychain.
-spec load_private(filename()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
load_private(Filename) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_private(Data)
    end.

% @doc Loads a private key from given DER file into the given keychain.
% Could fail if the keystore is read-only after being made shareable.
-spec load_private(state(), filename()) ->
    {ok, ident_info(), state()} | {error, Reason :: term()}.
load_private(undefined, _Filename) -> {error, undefined_keychain};
load_private(default, Filename) ->
    load_private(Filename);
load_private({Mod, Sub, Parent}, Filename) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} ->
            case Mod:add_private(Sub, Data) of
                {ok, Info, Sub2} -> {ok, Info, {Mod, Sub2, Parent}};
                Result -> Result
            end
    end.

% @doc Adds certificates and keys from given PEM data into the default keychain
% without any options.
-spec add_pem(binary()) ->
    {ok, [ident_info()]} | {error, Reason :: term()}.
add_pem(Data) ->
    add_pem(Data, #{}).

% @doc Adds certificates and keys from given PEM data into the default keychain
% with given options, or into the given keychain without any options.
-spec add_pem(state() | binary(), binary() | ident_options()) ->
    {ok, [ident_info()], state()} | {ok, [ident_info()]} | {error, Reason :: term()}.
add_pem(NamedState, Data) when is_atom(NamedState) ->
    add_pem(NamedState, Data, #{});
add_pem({_Mod, _Sub, _Parent} = State, Data) ->
    add_pem(State, Data, #{});
add_pem(Data, Opts) ->
    case decode_pem(Data, Opts, undefined, fun
        (cert, undefined, Der) ->
            case opcua_keychain_default:add_certificate(Der, Opts) of
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

% @doc Adds certificates and keys from given PEM data into the given keychain
% with given options.
% Could fail if the keystore is read-only after being made shareable.
-spec add_pem(state(), binary(), ident_options()) ->
    {ok, [ident_info()], state()} | {error, Reason :: term()}.
add_pem(undefined, _Data, _Opts) -> {error, undefined_keychain};
add_pem(default, Data, Opts) ->
    case add_pem(Data, Opts) of
        {ok, Infos} -> {ok, Infos, default};
        Result -> Result
    end;
add_pem({Mod, Sub, Parent}, Data, Opts) ->
    case decode_pem(Data, Opts, Sub, fun
        (cert, State, Der) -> Mod:add_certificate(State, Der, Opts);
        (priv, State, Der) -> Mod:add_private(State, Der)
    end) of
        {ok, Infos, Sub2} -> {ok, Infos, {Mod, Sub2, Parent}};
        Result -> Result
    end.

% @doc Adds a certificate from given DER data into the default keychain
% without any options.
-spec add_certificate(binary()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
add_certificate(Data) ->
    add_certificate(Data, #{}).

% @doc Adds a certificate from given DER data into the given keychain without
% any options, or in the default keychain with given options.
% Could fail if the keystore is read-only after being made shareable.
-spec add_certificate(state() | binary(), binary() | ident_options()) ->
    {ok, ident_info(), state()} | {ok, ident_info()} | {error, Reason :: term()}.
add_certificate(NamedState, Data) when is_atom(NamedState) ->
    add_certificate(NamedState, Data, #{});
add_certificate({_Mod, _Sub, _Parent} = State, Data) ->
    add_certificate(State, Data, #{});
add_certificate(Data, Opts) ->
    opcua_keychain_default:add_certificate(Data, Opts).

% @doc Adds a certificate from given DER data into the given keychain with
% given options.
% Could fail if the keystore is read-only after being made shareable.
-spec add_certificate(state(), binary(), ident_options()) ->
    {ok, ident_info(), state()} | {error, Reason :: term()}.
add_certificate(undefined, _Data, _Opts) -> {error, undefined_keychain};
add_certificate(default, Data, Opts) ->
    case add_certificate(Data, Opts) of
        {ok, Info} -> {ok, Info, default};
        Result -> Result
    end;
add_certificate({Mod, Sub, Parent}, Data, Opts) ->
    case Mod:add_certificate(Sub, Data, Opts) of
        {ok, Info, Sub2} -> {ok, Info, {Mod, Sub2, Parent}};
        Result -> Result
    end.

% @doc Adds a private key from given DER data into the default keychain.
-spec add_private(binary()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
add_private(Data) ->
    opcua_keychain_default:add_private(Data).

% @doc Adds a private key from given DER data into the given keychain.
% Could fail if the keystore is read-only after being made shareable.
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

% @doc Marks a certificate as trusted in the default keychain.
-spec trust(ident()) -> ok | not_found.
trust(Ident) ->
    opcua_keychain_default:trust(Ident).

% @doc Marks a certificate as trusted in the given keychain.
% Could fail if the keystore is read-only after being made shareable.
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

% @doc Retrieves the certification chain of an identity from the default keychain.
-spec chain(ident(), format()) -> [certificate()] | not_found.
chain(_Ident, _Format) ->
    %TODO: use othre functions to recursively retrieve the certification chain,
    not_found.

% @doc Retrieves the certification chain of an identity from the given keychain.
-spec chain(state(), ident(), format()) -> [certificate()] | not_found.
chain(_State, _Ident, _Format) ->
    %TODO: use othre functions to recursively retrieve the certification chain,
    not_found.

% @doc Validates the certification chain for an identity in default keychain.
-spec validate(ident()) ->
    ok | {error, Reason :: term()}.
validate(_Ident) ->
    %TODO: implemente chain validation
    ok.

% @doc Validates the certification chain for an identity in given keychain or
% validates the given chain of certificate in either DER or record agains the
% default keychain.
-spec validate(state() | [certificate()], dummy | ident() | format()) ->
    ok | {error, Reason :: term()}.
validate(_Chain, dummy) ->
    % Did this dummy implementation to make dialyzer happy and behave like it
    % can return an error
    {error, dummy};
validate(Chain, _Format) when is_list(Chain) ->
    %TODO: implemente chain validation
    ok;
validate(_State, _Ident) ->
    %TODO: implemente chain validation
    ok.

% @doc Validates a given certification chain against the given keychain.
-spec validate(state(), [certificate()], dummy | format()) ->
    ok | {error, Reason :: term()}.
validate(_State, _Chain, dummy) ->
    % Did this dummy implementation to make dialyzer happy and behave like it
    % can return an error
    {error, dummy};
validate(_State, _Chain, _Format) ->
    %TODO: implemente chain validation
    ok.


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

decode_pem(Data, Opts, State, Fun) ->
    %TODO: Add suport for EC keys
    %TODO: Handle the private key being before the certificate
    %TODO: Maybe keep the certificate order ?
    decode_pem_entries(public_key:pem_decode(Data), Opts, State, Fun, #{}).

decode_pem_entries([], _Opts, State, _Fun, Acc) ->
    {ok, maps:values(Acc), State};
decode_pem_entries([{Type, _EncDer, Algo} = Entry | Rest], Opts, State, Fun, Acc)
  when Algo =/= not_encrypted ->
    %TODO: Do some type checking ? See public_key:decode_pem_entries/2
    DecDer = pubkey_pem:decipher(Entry, maps:get(password, Opts, undefined)),
    decode_pem_entries([{Type, DecDer, not_encrypted} | Rest], Opts, State, Fun, Acc);
decode_pem_entries([{'Certificate', CertDer, not_encrypted} | Rest], Opts, State, Fun, Acc) ->
    case Fun(cert, State, CertDer) of
        {ok, #{id := Id} = Info, State2} ->
            decode_pem_entries(Rest, Opts, State2, Fun, Acc#{Id => Info});
        Result -> Result
    end;
decode_pem_entries([{'RSAPrivateKey', KeyDer, not_encrypted} | Rest], Opts, State, Fun, Acc) ->
    case Fun(priv, State, KeyDer) of
        {ok, #{id := Id} = Info, State2} ->
            decode_pem_entries(Rest, Opts, State2, Fun, Acc#{Id => Info});
        Result -> Result
    end;
decode_pem_entries([_Other | Rest], Opts, State, Fun, Acc) ->
    decode_pem_entries(Rest, Opts, State, Fun, Acc).
