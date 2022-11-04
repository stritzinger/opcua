-module(opcua_keychain_default).

-behavior(gen_server).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

 -include_lib("kernel/include/logger.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([start_link/2]).

%% Keychain functions
-export([info/1]).
-export([certificate/2]).
-export([public_key/2]).
-export([private_key/2]).
-export([add_certificate/1]).
-export([add_private/1]).
-export([trust/1]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Mod, Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Mod, Args}, []).


%%% KEYCHAIN FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

info(Ident) ->
    gen_server:call(?MODULE, {info, Ident}).

certificate(Ident, Format) ->
    gen_server:call(?MODULE, {certificate, Ident, Format}).

private_key(Ident, Format) ->
    gen_server:call(?MODULE, {private_key, Ident, Format}).

public_key(Ident, Format) ->
    gen_server:call(?MODULE, {public_key, Ident, Format}).

add_certificate(Data) ->
    gen_server:call(?MODULE, {add_certificate, Data}).

add_private(Data) ->
    gen_server:call(?MODULE, {add_private, Data}).

trust(Ident) ->
    gen_server:call(?MODULE, {trust, Ident}).


%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Mod, Args}) ->
    ?LOG_INFO("Starting default keychain process...", []),
    opcua_keychain:new(undefined, Mod, Args).

handle_call({info, Ident}, _From, State) ->
    {reply, opcua_keychain:info(State, Ident), State};
handle_call({certificate, Ident, Format}, _From, State) ->
    {reply, opcua_keychain:certificate(State, Ident, Format), State};
handle_call({public_key, Ident, Format}, _From, State) ->
    {reply, opcua_keychain:public_key(State, Ident, Format), State};
handle_call({private_key, Ident, Format}, _From, State) ->
    {reply, opcua_keychain:private_key(State, Ident, Format), State};
handle_call({add_certificate, Data}, _From, State) ->
    case opcua_keychain:add_certificate(State, Data) of
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
