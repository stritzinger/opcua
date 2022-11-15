-module(opcua_keychain_default).

-behavior(gen_server).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

 -include_lib("kernel/include/logger.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([start_link/1]).

%% Keychain functions
-export([lookup/2]).
-export([info/1]).
-export([certificate/2]).
-export([public_key/2]).
-export([private_key/2]).
-export([add_certificate/2]).
-export([add_private/1]).
-export([trust/1]).
-export([add_alias/2]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Opts}, []).


%%% KEYCHAIN FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

lookup(Method, Params) ->
    gen_server:call(?MODULE, {lookup, Method, Params}).

info(Ident) ->
    gen_server:call(?MODULE, {info, Ident}).

certificate(Ident, Format) ->
    gen_server:call(?MODULE, {certificate, Ident, Format}).

private_key(Ident, Format) ->
    gen_server:call(?MODULE, {private_key, Ident, Format}).

public_key(Ident, Format) ->
    gen_server:call(?MODULE, {public_key, Ident, Format}).

add_certificate(Data, Opts) ->
    gen_server:call(?MODULE, {add_certificate, Data, Opts}).

add_private(Data) ->
    gen_server:call(?MODULE, {add_private, Data}).

trust(Ident) ->
    gen_server:call(?MODULE, {trust, Ident}).

add_alias(Ident, Alias) ->
    gen_server:call(?MODULE, {add_alias, Ident, Alias}).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Opts}) ->
    ?LOG_INFO("Starting default keychain process...", []),
    Mod = maps:get(backend_module, Opts, opcua_keychain_ets),
    Args = maps:get(backend_options, Opts, #{}),
    Entries = maps:get(entries, Opts, []),
    case opcua_keychain:new(undefined, Mod, Args) of
        {error, _Reason} = Error -> Error;
        {ok, Keychain} ->
            try {ok, add_entries(Keychain, Entries)}
            catch throw:Reason -> {stop, Reason}
            end
    end.

handle_call({lookup, Method, Params}, _From, State) ->
    {reply, opcua_keychain:lookup(State, Method, Params), State};
handle_call({info, Ident}, _From, State) ->
    {reply, opcua_keychain:info(State, Ident), State};
handle_call({certificate, Ident, Format}, _From, State) ->
    {reply, opcua_keychain:certificate(State, Ident, Format), State};
handle_call({public_key, Ident, Format}, _From, State) ->
    {reply, opcua_keychain:public_key(State, Ident, Format), State};
handle_call({private_key, Ident, Format}, _From, State) ->
    {reply, opcua_keychain:private_key(State, Ident, Format), State};
handle_call({add_certificate, Data, Opts}, _From, State) ->
    case opcua_keychain:add_certificate(State, Data, Opts) of
        {ok, Info, State2} -> {reply, {ok, Info}, State2};
        Other -> {reply, Other, State}
    end;
handle_call({add_private, Data}, _From, State) ->
    case opcua_keychain:add_private(State, Data) of
        {ok, Info, State2} -> {reply, {ok, Info}, State2};
        Other -> {reply, Other, State}
    end;
handle_call({trust, Ident}, _From, State) ->
    case opcua_keychain:trust(State, Ident) of
        {ok, State2} -> {reply, ok, State2};
        Other -> {reply, Other, State}
    end;
handle_call({add_alias, Ident, Alias}, _From, State) ->
    case opcua_keychain:add_alias(State, Ident, Alias) of
        {ok, State2} -> {reply, ok, State2};
        Other -> {reply, Other, State}
    end;
handle_call(Request, From, State) ->
    ?LOG_WARNING("Unexpected call to default keychain process from ~p: ~p",
                 [From, Request]),
    {reply, {error, unexpected_call}, State}.

handle_cast(Request, State) ->
    ?LOG_WARNING("Unexpected cast to default keychain process: ~p", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG_WARNING("Unexpected message to default keychain process: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_entries(Keychain, Entries) ->
    lists:foldl(fun add_entry/2, Keychain, Entries).

add_entry(Entry, Keychain) ->
    IdentOpts = #{
        alias => maps:get(alias, Entry, undefined),
        password => maps:get(password, Entry, undefined),
        is_trusted => maps:get(is_trusted, Entry, false)
    },
    add_key(add_cert(Keychain, Entry, IdentOpts), Entry, IdentOpts).

add_cert(Keychain, #{cert := FileSpec} = Entry, Opts) ->
    Path = resolve_path(FileSpec),
    case opcua_keychain:load_pem(Keychain, Path, Opts) of
        {ok, [], Keychain2} ->
            case opcua_keychain:load_certificate(Keychain2, Path, Opts) of
                {ok, _Info, Keychain3} -> Keychain3;
                {error, Reason} -> maybe_fail(Keychain2, Entry, Reason)
            end;
        {ok, _, Keychain2} ->
            Keychain2;
        {error, Reason} ->
            maybe_fail(Keychain, Entry, Reason)
    end;
add_cert(Keychain, _Entry, _Opts) ->
    Keychain.

add_key(Keychain, #{key := FileSpec} = Entry, Opts) ->
    Path = resolve_path(FileSpec),
    case opcua_keychain:load_pem(Keychain, Path, Opts) of
        {ok, [], Keychain2} ->
            case opcua_keychain:load_private(Keychain2, Path) of
                {ok, _Info, Keychain3} -> Keychain3;
                {error, Reason} -> maybe_fail(Keychain2, Entry, Reason)
            end;
        {ok, _, Keychain2} ->
            %TODO: We should maybe check a private key actually got loaded ?
            Keychain2;
        {error, Reason} ->
            maybe_fail(Keychain, Entry, Reason)
    end;
add_key(Keychain, _Entry, _Opts) ->
    Keychain.

maybe_fail(_Keychain, #{is_required := true} = Entry, Reason) ->
    ?LOG_ERROR("Failed loading keychain entry ~p: ~p", [Entry, Reason]),
    throw(missing_required_entry);
maybe_fail(Keychain, Entry, Reason) ->
    ?LOG_WARNING("Failed loading keychain entry ~p: ~p", [Entry, Reason]),
    Keychain.

resolve_path({priv, AppName, RelPath} = Spec) ->
    case code:priv_dir(AppName) of
        {error, bad_name} -> throw({bad_filename_spec, Spec});
        PrivPath -> filename:join(PrivPath, RelPath)
    end;
resolve_path(Filename) when is_list(Filename) ->
    list_to_binary(Filename);
resolve_path(Filename) when is_binary(Filename) ->
    Filename.
