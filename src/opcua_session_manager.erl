-module(opcua_session_manager).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua_codec.hrl").
-include("opcua_protocol.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/1]).
-export([handle_request/2]).

%% Behaviour gen_server callback functions
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).


%%% MACRO %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER, ?MODULE).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(session, {
    pid :: pid(),
    auth :: binary(),
    ref :: reference()
}).

-record(state, {
    next_session_id = 1 :: pos_integer(),
    sessions = #{} :: #{pid() => #session{}},
    auth_lookup = #{} :: #{binary() => pid()}
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

handle_request(Conn, #uacp_message{node_id = #opcua_node_id{value = 459}} = Req) ->
    gen_server:call(?SERVER, {create_session, Conn, Req});
handle_request(Conn, #uacp_message{} = Req) ->
    gen_server:call(?SERVER, {forward_request, Conn, Req}).

%%% BEHAVIOUR gen_server CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Opts) ->
    ?LOG_DEBUG("OPCUA session manager process starting with options: ~p", [Opts]),
    {ok, #state{}}.

handle_call({create_session, Conn, Req}, _From, State) ->
    case create_session(State, Conn, Req) of
        {error, _Reason} = Error -> {reply, Error, State};
        {Result, State2} -> {reply, Result, State2}
    end;
handle_call({forward_request, Conn, Req}, _From, State) ->
    case forward_request(State, Conn, Req) of
        {error, _Reason} = Error -> {reply, Error, State};
        {Result, State2} -> {reply, Result, State2}
    end;
handle_call(Req, From, State) ->
    ?LOG_WARNING("Unexpected gen_server call from ~p: ~p", [From, Req]),
    {reply, {error, unexpected_call}, State}.

handle_cast(Req, State) ->
    ?LOG_WARNING("Unexpected gen_server cast: ~p", [Req]),
    {noreply, State}.

handle_info({'DOWN', MonRef, process, SessPid, _Info}, State) ->
    {noreply, session_del(State, SessPid, MonRef)};
handle_info(Msg, State) ->
    ?LOG_WARNING("Unexpected gen_server message: ~p", [Msg]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_DEBUG("OPCUA registry process terminating: ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

generate_session_auth_token() ->
    #opcua_node_id{type = opaque, value = crypto:strong_rand_bytes(32)}.

next_session_node_id(#state{next_session_id = Id} = State) ->
    {#opcua_node_id{ns = 232, value = Id}, State#state{next_session_id = Id + 1}}.

create_session(State, Conn, Req) ->
    %TODO: Probably check the request header...
    {SessNodeId, State2} = next_session_node_id(State),
    AuthToken = generate_session_auth_token(),
    case opcua_session_sup:start_session(SessNodeId, AuthToken) of
        {error, _Reason} = Error -> Error;
        {ok, SessPid} ->
            case opcua_session:handle_request(Conn, Req, SessPid) of
                {error, _Reason} = Error -> Error;
                {created, Resp} ->
                    State3 = session_add(State2, SessPid, AuthToken),
                    {{created, Resp, SessPid}, State3}
            end
    end.

forward_request(State, Conn, #uacp_message{payload = Msg} = Req) ->
    #{request_header := #{authentication_token := AuthToken}} = Msg,
    case session_find_by_auth(State, AuthToken) of
        error -> {error, bad_session_id_invalid};
        {ok, #session{pid = SessPid}} ->
            case opcua_session:handle_request(Conn, Req, SessPid) of
                ok -> {{ok, SessPid}, State};
                {error, _Reason} = Error -> Error;
                {Tag, Resp} when is_atom(Tag) -> {{Tag, Resp, SessPid}, State}
            end
    end.

session_find_by_auth(State, Auth) ->
    #state{sessions = Sessions, auth_lookup = AuthLookup} = State,
    case maps:find(Auth, AuthLookup) of
        error -> error;
        {ok, SessPid} -> maps:find(SessPid, Sessions)
    end.

session_add(State, Pid, Auth) ->
    #state{sessions = Sessions, auth_lookup = AuthLookup} = State,
    ?assert(not maps:is_key(Pid, Sessions)),
    ?assert(not maps:is_key(Auth, AuthLookup)),
    MonRef = erlang:monitor(process, Pid),
    SessRec = #session{pid = Pid, auth = Auth, ref = MonRef},
    State#state{
        sessions = Sessions#{Pid => SessRec},
        auth_lookup = AuthLookup#{Auth => Pid}
    }.

session_del(State, Pid, MonRef) ->
    #state{sessions = Sessions, auth_lookup = AuthLookup} = State,
    case maps:take(Pid, Sessions) of
        error -> State;
        {#session{pid = Pid, ref = MonRef, auth = Auth}, Sessions2} ->
            State#state{
                sessions = Sessions2,
                auth_lookup = maps:remove(Auth, AuthLookup)
            };
        {#session{pid = Pid, ref = OtherMonRef, auth = Auth}, Sessions2} ->
            erlang:demonitor(OtherMonRef, [flush]),
            State#state{
                sessions = Sessions2,
                auth_lookup = maps:remove(Auth, AuthLookup)
            }
    end.
