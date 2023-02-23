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
%%%  - Implement CertificateValidationOptions
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
        {ok, Info, State} | {already_exists, Id} | {error, Reason}
    when State :: term(), Data :: binary(), Info :: ident_info(), Id :: binary(),
         Opts :: callback_ident_options(), Reason :: read_only | term().

-callback add_private(State, Data) ->
        {ok, Info, State} | {error, Reason}
    when State :: term(), Data :: binary(), Info :: ident_info(),
         Reason :: read_only | certificate_not_found | term().

-callback trust(State, Ident) -> {ok, State} | {error, Reason}
    when State :: term(), Ident :: ident(), Reason :: read_only | term().

-callback add_alias(State, Ident, Alias) -> {ok, State} | not_found
    when State :: term(), Ident :: ident(), Alias :: atom().

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("opcua/include/opcua.hrl").
-include("opcua_internal.hrl").

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
-export([add_alias/2, add_alias/3]).
-export([chain/2, chain/3]).
-export([validate/1, validate/2]).

% Helper functions for keychain handlers
-export([certificate_id/1]).
-export([public_key_id/1]).
-export([private_key_id/1]).
-export([certificate_issuer/1]).
-export([certificate_subject/1]).
-export([certificate_thumbprint/1]).
-export([certificate_capabilities/1]).
-export([certificate_validity/1]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type state() :: default | term().
-type ident() :: binary().
-type lookup_method() :: alias | subject.
-type lookup_params() :: term().
-type filename() :: binary().
-type format() :: der | rec.
-type certificate() :: binary() | #'OTPCertificate'{}.
-type private_key() :: binary() | public_key:private_key().
-type public_key() :: binary() | public_key:public_key().
-type ident_options() :: #{
    alias => atom() | [atom()],
    password => binary(),
    is_trusted => boolean()
}.
-type callback_ident_options() :: #{
    aliases := [atom()],
    is_trusted := boolean()
}.
-type capability() :: decrypt | encrypt | sign | authenticate | ca.
-type ident_info() :: #{
    id := binary(),
    aliases := [atom()],
    issuer := binary(),
    subject := term(),
    validity := term(),
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
-spec shareable(state() | opcua:connection()) -> state() | ocpua:connection().
shareable(default) -> default;
shareable(#uacp_connection{keychain = K} = C) ->
    C#uacp_connection{keychain = shareable(K)};
shareable({Mod, Sub, Parent}) ->
    {Mod, Mod:shareable(Sub), shareable(Parent)}.

% @doc Lookups an identity in default keychain.
-spec lookup(lookup_method(), lookup_params()) -> [ident()] | not_found.
lookup(Method, Params) ->
    opcua_keychain_default:lookup(Method, Params).

% @doc Lookups an identity in given keychain.
-spec lookup(state() | opcua:connection(), lookup_method(), lookup_params()) ->
    [ident()] | not_found.
lookup(undefined, _Method, _Params) -> not_found;
lookup(#uacp_connection{keychain = K}, Method, Params) -> lookup(K, Method, Params);
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
-spec info(state() | opcua:connection(), ident()) -> not_found | ident_info().
info(undefined, _Ident) -> not_found;
info(#uacp_connection{keychain = K}, Ident) -> info(K, Ident);
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
-spec certificate(state() | opcua:connection(), ident(), format()) ->
    certificate() | not_found.
certificate(undefined, _Ident, _Format) -> not_found;
certificate(#uacp_connection{keychain = K}, Ident, Format) ->
    certificate(K, Ident, Format);
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
-spec public_key(state() | opcua:connection(), ident(), format()) ->
    public_key() | not_found.
public_key(undefined, _Ident, _Format) -> not_found;
public_key(#uacp_connection{keychain = K}, Ident, Format) ->
    public_key(K, Ident, Format);
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
-spec private_key(state() | opcua:connection(), ident(), format()) ->
    private_key() | not_found.
private_key(undefined, _Ident, _Format) -> not_found;
private_key(#uacp_connection{keychain = K}, Ident, Format) ->
    private_key(K, Ident, Format);
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
-spec load_pem(state() | filename()| opcua:connection(), filename() | ident_options()) ->
    {ok, [ident_info()]} |
    {ok, [ident_info()], state()| opcua:connection()} |
    {error, Reason :: term()}.
load_pem(NamedState, Filename) when is_atom(NamedState) ->
    load_pem(NamedState, Filename, #{});
load_pem({_Mod, _Sub, _Parent} = State, Filename) ->
    load_pem(State, Filename, #{});
load_pem(#uacp_connection{} = C, Filename) ->
    load_pem(C, Filename, #{});
load_pem(Filename, Opts) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_pem(Data, Opts)
    end.

% @doc Loads certificates and keys from given PEM file into the given keychain.
% Could fail if the keystore is read-only after being made shareable.
-spec load_pem(state() | opcua:connection(), filename(), ident_options()) ->
    {ok, [ident_info()], state() | opcua:connection()} |
    {error, Reason :: term()}.
load_pem(undefined, _Filename, _Opts) -> {error, undefined_keychain};

load_pem(default, Filename, Opts) ->
    case load_pem(Filename, Opts) of
        {ok, Infos, _} -> {ok, Infos, default};
        Result -> Result
    end;
load_pem(StateOrConnection, Filename, Opts) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_pem(StateOrConnection, Data, Opts)
    end.

% @doc Loads a certificate from given DER file into the default keychain
% without any options.
-spec load_certificate(filename()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
load_certificate(Filename) ->
    load_certificate(Filename, #{}).

% @doc Loads a certificate from given DER file into the given keychain without
% any options, or in the default keychain with given options.
-spec load_certificate(state() | opcua:connection() | filename(), filename() | ident_options()) ->
    {ok, ident_info()} |
    {ok, ident_info(), state() | opcua:connection()} |
    {error, Reason :: term()}.
load_certificate(NameState, Filename) when is_atom(NameState) ->
    load_certificate(NameState, Filename, #{});
load_certificate({_Mod, _Sub, _Parent} = State, Filename) ->
    load_certificate(State, Filename, #{});
load_certificate(#uacp_connection{} = C, Filename) ->
    load_certificate(C, Filename, #{});
load_certificate(Filename, Opts) ->
    case file:read_file(Filename) of
        {error, _Reason} = Error -> Error;
        {ok, Data} -> add_certificate(Data, Opts)
    end.

% @doc Loads a certificate from given DER file into the given keychain with
% given options.
% Could fail if the keystore is read-only after being made shareable.
-spec load_certificate(state() , filename(), ident_options()) ->
    {ok, ident_info(), state() | opcua:connection()} |
    {error, Reason :: term()}.
load_certificate(undefined, _Filename, _Opts) -> {error, undefined_keychain};
load_certificate(#uacp_connection{keychain = K} = C, Filename, Opts) ->
    case load_certificate(K, Filename, Opts) of
        {ok, Info, K2} -> {ok, Info, C#uacp_connection{keychain = K2}};
        Error -> Error
    end;
load_certificate(default, Filename, Opts) ->
    case load_certificate(Filename, Opts) of
        {ok, Info, _} -> {ok, Info, default};
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
-spec load_private(state() | opcua:connection(), filename()) ->
    {ok, ident_info(), state() | opcua:connection()} |
    {error, Reason :: term()}.
load_private(undefined, _Filename) -> {error, undefined_keychain};
load_private(#uacp_connection{keychain = K} = C, Filename) ->
    case load_private(K, Filename) of
        {ok, Info, K2} -> {ok, Info, C#uacp_connection{keychain = K2}};
        Error -> Error
    end;
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
-spec add_pem(state() | opcua:connection() | binary(), binary() | ident_options()) ->
    {ok, [ident_info()], state() | opcua:connection()} |
    {error, Reason :: term()}.
add_pem(NamedState, Data) when is_atom(NamedState) ->
    add_pem(NamedState, Data, #{});
add_pem({_Mod, _Sub, _Parent} = State, Data) ->
    add_pem(State, Data, #{});
add_pem(#uacp_connection{} = C, Data) ->
    add_pem(C, Data, #{});
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
-spec add_pem(state() | opcua:connection(), binary(), ident_options()) ->
    {ok, [ident_info()], state() | opcua:connection()} |
    {error, Reason :: term()}.
add_pem(undefined, _Data, _Opts) -> {error, undefined_keychain};
add_pem(#uacp_connection{keychain = K} = C, Data, Opts) ->
    case add_pem(K, Data, Opts) of
        {ok, Infos, K2} -> {ok, Infos, C#uacp_connection{keychain = K2}};
        Error -> Error
    end;
add_pem(default, Data, Opts) ->
    case add_pem(Data, Opts) of
        {ok, Infos, _} -> {ok, Infos, default};
        Result -> Result
    end;
add_pem({Mod, Sub, Parent}, Data, Opts) ->
    case decode_pem(Data, Opts, Sub, fun
        (cert, State, Der) ->
            Mod:add_certificate(State, Der, normalize_ident_opts(Opts));
        (priv, State, {Type, Der}) ->
            Mod:add_private(State, {Type, Der})
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
-spec add_certificate(state() | binary() | opcua:connection(), binary() | ident_options()) ->
    {ok, ident_info(), state() | opcua:codec_schema()} |
    {error, Reason :: term()}.
add_certificate(NamedState, Data) when is_atom(NamedState) ->
    add_certificate(NamedState, Data, #{});
add_certificate({_Mod, _Sub, _Parent} = State, Data) ->
    add_certificate(State, Data, #{});
add_certificate(#uacp_connection{} = Conn, Data) ->
    add_certificate(Conn, Data, #{});
add_certificate(Data, Opts) ->
    opcua_keychain_default:add_certificate(Data, Opts).

% @doc Adds a certificate from given DER data into the given keychain with
% given options.
% Could fail if the keystore is read-only after being made shareable.
-spec add_certificate(state(), binary(), ident_options()) ->
    {ok, ident_info(), state() | opcua:connection()} |
    {already_exists, Ident :: binary()} |
    {error, Reason :: term()}.
add_certificate(undefined, _Data, _Opts) -> {error, not_found};
add_certificate(#uacp_connection{keychain = K} = C, Data, Opts) ->
    case add_certificate(K, Data, Opts) of
        {ok, Info, K2} -> {ok, Info, C#uacp_connection{keychain = K2}};
        Error -> Error
    end;
add_certificate(default, Data, Opts) ->
    case add_certificate(Data, Opts) of
        {ok, Info, _} -> {ok, Info, default};
        Result -> Result
    end;
add_certificate({Mod, Sub, Parent}, Data, Opts) ->
    try public_key:pkix_decode_cert(Data, otp) of
        CertRec ->
            Id = certificate_id(CertRec),
            case certificate(Id, der) of
                Data -> {already_exists, Id};
                not_found ->
                    case Mod:add_certificate(Sub, Data, normalize_ident_opts(Opts)) of
                        {ok, Info, Sub2} -> {ok, Info, {Mod, Sub2, Parent}};
                        Result -> Result
                    end;
                _ -> {error, cert_identity_clash}
            end
    catch
        error:{badmatch, _} ->
            {error, decoding_error}
    end.

% @doc Adds a private key from given DER data into the default keychain.
-spec add_private(binary()) ->
    {ok, ident_info()} | {error, Reason :: term()}.
add_private(Data) ->
    opcua_keychain_default:add_private(Data).

% @doc Adds a private key from given DER data into the given keychain.
% Could fail if the keystore is read-only after being made shareable.
-spec add_private(state() | opcua:connection(), binary()) ->
    {ok, ident_info(), state() | opcua:connection()} |
    {error, Reason :: term()}.
add_private(undefined, _Data) -> {error, not_found};
add_private(#uacp_connection{keychain = K} = C, Data) ->
    case add_private(K, Data) of
        {ok, Info, K2} -> {ok, Info, C#uacp_connection{keychain = K2}};
        Error -> Error
    end;
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
-spec trust(state() | opcua:connection(), ident()) ->
    {ok, state() | opcua:connection()} | not_found.
trust(undefined, _Ident) -> {error, not_found};
trust(#uacp_connection{keychain = K} = C, Ident) ->
    case trust(K, Ident) of
        {ok, K2} -> {ok, C#uacp_connection{keychain = K2}};
        Error -> Error
    end;
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

% @doc Add an alias to an existing identity in default keychain.
-spec add_alias(ident(), atom()) -> ok | not_found.
add_alias(Ident, Alias) ->
    opcua_keychain_default:add_alias(Ident, Alias).

% @doc Add an alias to an existing identity in given keychain.
% Could fail if the keystore is read-only after being made shareable.
-spec add_alias(state() | opcua:connection(), ident(), atom()) ->
    {ok, state() | ocpua:connection()} | not_found.
add_alias(undefined, _Ident, _Alias) -> not_found;
add_alias(#uacp_connection{keychain = K} = C, Ident, Alias) ->
    case add_alias(K, Ident, Alias) of
        {ok, K2} -> {ok, C#uacp_connection{keychain = K2}};
        Error -> Error
    end;
add_alias(default, Ident, Alias) ->
    case add_alias(Ident, Alias) of
        ok -> {ok, default};
        Result -> Result
    end;
add_alias({Mod, Sub, Parent}, Ident, Alias) ->
    case Mod:add_alias(Sub, Ident, Alias) of
        {ok, Sub2} -> {ok, {Mod, Sub2, Parent}};
        not_found ->
            case add_alias(Parent, Ident, Alias) of
                {ok, Parent2} -> {ok, {Mod, Sub, Parent2}};
                Result -> Result
            end;
        Result -> Result
    end.

% @doc Retrieves the certification chain of an identity from the default keychain.
-spec chain(ident(), format()) -> [certificate()] | not_found.
chain(Ident, Format) ->
    opcua_keychain_default:chain(Ident, Format).

% @doc Retrieves the certification chain of an identity from the given keychain.
-spec chain(state() | opcua:connection(), ident(), format()) ->
    [certificate()] | {error, atom()}.
chain(#uacp_connection{keychain = K}, Ident, Alias) ->
    chain(K, Ident, Alias);
chain(Keychain, Ident, Format) ->
    try try_build_chain(Keychain, Ident, Format)
    catch
        throw:E -> {error, E}
    end.

% @doc Validates the certification chain for an identity in default keychain.
-spec validate(ident()) ->
    ok | {error, Reason :: term()}.
validate(Ident) ->
    opcua_keychain_default:validate(Ident).

% @doc Validates the peer certificate for a given keychain connection
% Validates a certification chain for an identity in a given keychain
-spec validate(state() | opcua:connection(), ident() | binary()) ->
    {ok, opcua:connection(), ident() | undefined} |
     ok |
    {error, Reason :: term()}.
validate(#uacp_connection{peer_ident = undefined} = Conn, undefined) ->
    {ok, Conn, undefined};
validate(#uacp_connection{keychain = Keychain} = Conn, DerData) ->
    try
        {KeyChain2, [LeafIdent|_]} = add_certificates(Keychain,
                                        unpack_der_sequence(DerData), []),
        case opcua_keychain:validate(KeyChain2, LeafIdent) of
            ok -> {ok, Conn#uacp_connection{keychain = KeyChain2}, LeafIdent};
            {error, E} -> {error, E}
        end
    catch
        throw:Error -> {error, Error}
    end;
validate(Keychain, Ident) ->
    case chain(Keychain, Ident, rec) of
        {error, E} -> {error, E};
        Chain ->
            [Root | _] = RevChain = lists:reverse(Chain),
            RootID = certificate_id(Root),
            case info(Keychain, RootID) of
                #{is_trusted := true} ->
                    % Just basic x509 cert validation with default verify_fun
                    % Unknown critical extension will cause failure
                    case public_key:pkix_path_validation(Root, RevChain, []) of
                        {ok, _} -> ok;
                        Error -> Error
                    end;
                _ -> check_self_signed_settings()
            end
    end.


%%% HELPER FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

certificate_id(#'OTPCertificate'{} = CertRec) ->
    KeyRec = CertRec#'OTPCertificate'.tbsCertificate
                        #'OTPTBSCertificate'.subjectPublicKeyInfo
                            #'OTPSubjectPublicKeyInfo'.subjectPublicKey,
    public_key_id(KeyRec).

public_key_id(#'RSAPublicKey'{} = KeyRec) ->
    %TODO: Add support for EC
    crypto:hash(sha, integer_to_binary(KeyRec#'RSAPublicKey'.modulus)).

private_key_id(#'RSAPrivateKey'{} = KeyRec) ->
    %TODO: Add support for EC
    crypto:hash(sha, integer_to_binary(KeyRec#'RSAPrivateKey'.modulus)).

certificate_issuer(CertRec) ->
    CertRec#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.issuer.

certificate_subject(CertRec) ->
    CertRec#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject.

certificate_thumbprint(CertDer) ->
    crypto:hash(sha, CertDer).

certificate_capabilities(_CertRec) ->
    %TODO: implement
    [].

certificate_validity(CertRec) ->
    CertRec#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.validity.

%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

normalize_ident_opts(Opts) ->
    Aliases = case maps:get(alias, Opts, []) of
        L when is_list(L) -> L;
        A when is_atom(A) -> [A]
    end,
    IsTrusted = maps:get(is_trusted, Opts, false),
    #{aliases => Aliases, is_trusted => IsTrusted}.

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
decode_pem_entries([{Type, KeyDer, not_encrypted} | Rest], Opts, State, Fun, Acc)
when Type == 'RSAPrivateKey' orelse Type == 'PrivateKeyInfo' ->
    %TODO: add support for EC keys
    case Fun(priv, State, {Type, KeyDer}) of
        {ok, #{id := Id} = Info, State2} ->
            decode_pem_entries(Rest, Opts, State2, Fun, Acc#{Id => Info});
        Result -> Result
    end;
decode_pem_entries([_Other | Rest], Opts, State, Fun, Acc) ->
    decode_pem_entries(Rest, Opts, State, Fun, Acc).



unpack_der_sequence(undefined) -> throw(empty_certificate);
unpack_der_sequence(DerBlob) ->
    try unpack_der_sequence(DerBlob, []) catch
        error:_ -> throw(bad_certificate_chain)
    end.

unpack_der_sequence(<<>>, Result) -> lists:reverse(Result);
unpack_der_sequence(DerBlob, Binaries) ->
    <<16#30, 16#82, L:16, Rest/binary>> = DerBlob,
    <<CertBody:L/binary, Others/binary>> = Rest,
    Cert =  <<16#30, 16#82, L:16, CertBody/binary>>,
    unpack_der_sequence(Others, [Cert | Binaries]).

add_certificates(Keychain, [], Identities) ->
    {Keychain, lists:reverse(Identities)};
add_certificates(Keychain, [Cert|Rest], Identities) ->
    case add_certificate(Keychain, Cert, #{}) of
        {already_exists, Id} -> add_certificates(Keychain, Rest, [Id|Identities]);
        {ok, #{id := Id}, Keychain2} -> add_certificates(Keychain2, Rest, [Id|Identities]);
        {error, _Reason} -> throw(bad_certificate_in_chain)
    end.

try_build_chain(Keychain, Ident, Format) ->
    lists:reverse(try_build_chain(Keychain, Ident, Format, [])).

try_build_chain(Keychain, Ident, Format, Chain) ->
    Cert = case certificate(Keychain, Ident, Format) of
        not_found -> throw(cert_not_found);
        Cert_ -> Cert_
    end,
    #{issuer := Issuer} =
    case info(Keychain, Ident) of
        not_found -> throw(info_not_found);
        Info -> Info
    end,
    case lookup(Keychain, subject, Issuer) of
        not_found -> throw(issuer_not_found);
        [Ident] -> [Cert| Chain];
        [Other] -> try_build_chain(Keychain, Other, Format, [Cert | Chain])
    end.

check_self_signed_settings() ->
    KeychainOpts = application:get_env(opcua, keychain, #{}),
    case maps:get(trust_self_signed, KeychainOpts, false) of
        true ->
            ?LOG_WARNING("Allowing untrusted self-signed certificate!"),
            ok;
        false -> {error, self_signed}
    end.
