-module(opcua_keychain_ets).

-behavior(opcua_keychain).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("public_key/include/public_key.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% API functions
-export([new/0, new/1]).

% Behaviour opcua_keychain callback functions
-export([init/1]).
-export([shareable/1]).
-export([lookup/3]).
-export([info/2]).
-export([certificate/3]).
-export([public_key/3]).
-export([private_key/3]).
-export([add_certificate/3]).
-export([add_private/2]).
-export([trust/2]).
-export([add_alias/3]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    table :: ets:table(),
    aliases :: ets:table(),
    read_only = false
}).

-record(entry, {
    id :: binary(),
    aliases :: [atom()],
    thumbprint :: binary(),
    capabilities :: [opcua_keychain:capabilities()],
    cert_der :: binary(),
    cert_rec :: term(),
    key_der :: undefined | binary(),
    key_rec :: undefined | term(),
    is_trusted = false :: boolean()

}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new() ->
    opcua_keychain:new(?MODULE, []).

new(Parent) ->
    opcua_keychain:new(Parent, ?MODULE, []).


%%% BEHAVIOUR opcua_keychain CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Args) ->
    Table = ets:new(keychain_data, [set, protected, {keypos, #entry.id}]),
    Aliases = ets:new(keychain_alias_lookup, [bag, protected, {keypos, 1}]),
    {ok, #state{table = Table, aliases = Aliases}}.

shareable(#state{read_only = true} = State) -> State;
shareable(State) -> State#state{read_only = true}.

lookup(#state{aliases = Aliases}, alias, Alias) ->
    [I || {_, I} <- ets:lookup(Aliases, Alias)].

info(#state{table = Table}, Ident) ->
    case ets:lookup(Table, Ident) of
        [] -> not_found;
        [Entry] -> get_info(Entry)
    end.

certificate(_State, _Ident, pem) ->
    not_implemented;
certificate(#state{table = Table}, Ident, der) ->
    case ets:lookup(Table, Ident) of
        [] -> not_found;
        [#entry{cert_der = CertDer}] -> CertDer
    end;
certificate(#state{table = Table}, Ident, rec) ->
    case ets:lookup(Table, Ident) of
        [] -> not_found;
        [#entry{cert_rec = CertRec}] -> CertRec
    end.

public_key(#state{table = Table}, Ident, rec) ->
    case ets:lookup(Table, Ident) of
        [] -> not_found;
        [#entry{cert_rec = CertRec}] ->
            CertRec#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subjectPublicKeyInfo#'OTPSubjectPublicKeyInfo'.subjectPublicKey
    end;
public_key(State, Ident, der) ->
    %TODO: Add support for EC ?
    case public_key(State, Ident, rec) of
        not_found -> not_found;
        #'RSAPublicKey'{} = KeyRec ->
            public_key:der_encode('RSAPublicKey', KeyRec)
    end.

private_key(#state{table = Table}, Ident, der) ->
    case ets:lookup(Table, Ident) of
        [] -> not_found;
        [#entry{key_der = undefined}] -> not_found;
        [#entry{key_der = KeyDer}] -> KeyDer
    end;
private_key(#state{table = Table}, Ident, rec) ->
    case ets:lookup(Table, Ident) of
        [] -> not_found;
        [#entry{key_rec = undefined}] -> not_found;
        [#entry{key_rec = KeyRec}] -> KeyRec
    end.

add_certificate(#state{read_only = true}, _CertDer, _Opts) ->
    {error, read_only};
add_certificate(#state{table = Table} = State, CertDer, Opts) ->
    #{aliases := Aliases, is_trusted := IsTrusted} = Opts,
    try public_key:pkix_decode_cert(CertDer, otp) of
        CertRec ->
            Id = opcua_keychain:certificate_id(CertRec),
            CertThumbprint = opcua_keychain:certificate_thumbprint(CertDer),
            CertCaps = opcua_keychain:certificate_capabilities(CertRec),
            case ets:lookup(Table, Id) of
                [] ->
                    Entry = #entry{
                        id = Id,
                        aliases = Aliases,
                        thumbprint = CertThumbprint,
                        capabilities = CertCaps,
                        cert_der = CertDer,
                        cert_rec = CertRec,
                        is_trusted = IsTrusted
                    },
                    ets:insert(Table, Entry),
                    update_lookups(State, Entry),
                    {ok, get_info(Entry), State};
                _ ->
                    {error, already_exists}
            end
    catch
        error:{badmatch, _} ->
            {error, decoding_error}
    end.

add_private(#state{read_only = true}, _KeyDer) -> {error, read_only};
add_private(#state{table = Table} = State, KeyDer) ->
    %TODO: add support for EC keys
    try public_key:der_decode('RSAPrivateKey', KeyDer) of
        KeyRec ->
            Id = opcua_keychain:private_key_id(KeyRec),
            case ets:lookup(Table, Id) of
                [] -> {error, certificate_not_found};
                [Entry] ->
                    Entry2 = Entry#entry{key_der = KeyDer, key_rec = KeyRec},
                    ets:insert(Table, Entry2),
                    {ok, get_info(Entry2), State}
            end
    catch
        error:{badmatch, _} ->
            {error, decoding_error}
    end.

trust(#state{read_only = true}, _Ident) -> {error, read_only};
trust(#state{table = Table} = State, Ident) ->
    case ets:lookup(Table, Ident) of
        [] -> {error, certificate_not_found};
        [Entry] ->
            Entry2 = Entry#entry{is_trusted = true},
            ets:insert(Table, Entry2),
            {ok, State}
    end.

add_alias(#state{read_only = true}, _Ident, _Alias) -> {error, read_only};
add_alias(#state{table = Table} = State, Ident, Alias) ->
    case ets:lookup(Table, Ident) of
        [] -> not_found;
        [#entry{aliases = Aliases} = Entry] ->
            case lists:member(Alias, Aliases) of
                true -> {ok, State};
                false ->
                    Entry2 = Entry#entry{aliases = [Alias | Aliases]},
                    ets:insert(Table, Entry2),
                    {ok, State}
            end
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_info(#entry{id = Id, aliases = Aliases, thumbprint = Thumb,
                capabilities = Caps, key_der = undefined,
                is_trusted = IsTrusted}) ->
    #{id => Id, aliases => Aliases, thumbprint => Thumb, capabilities => Caps,
      is_trusted => IsTrusted, has_private => false};
get_info(#entry{id = Id, aliases = Aliases, thumbprint = Thumb,
                capabilities = Caps, is_trusted = IsTrusted}) ->
    #{id => Id, aliases => Aliases, thumbprint => Thumb, capabilities => Caps,
      is_trusted => IsTrusted, has_private => true}.

update_lookups(#state{aliases = Table}, #entry{id = Id, aliases = Aliases}) ->
    update_alias_lookups(Table, Aliases, Id),
    ok.

update_alias_lookups(_Table, [], _Id) -> ok;
update_alias_lookups(Table, [Alias | Aliases], Id) ->
    ets:insert(Table, {Alias, Id}),
    update_alias_lookups(Table, Aliases, Id).
