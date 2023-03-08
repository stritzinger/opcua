-module(opcua_client).

-behaviour(gen_statem).
% Inspired by: https://gist.github.com/ferd/c86f6b407cf220812f9d893a659da3b8

% When adding a state handler, remember to always add a timeout by calling
% enter_timeouts or event_timeouts so the state machine keep consuming data
% from the protocol. When adding a completly new state, remember to update
% enter_timeouts and event_timeouts themselves to handle the new state name.


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([connect/1, connect/2]).
-export([close/1]).
-export([browse/2, browse/3]).
-export([read/2, read/3, read/4]).
-export([write/2, write/3, write/4, write/5]).
-export([get/2, get/3]).
-export([put/3]).
-export([del/3]).
-export([put_refs/3]).
-export([del_refs/3]).

%% Startup functions
-export([start_link/1]).

%% Behaviour gen_statem callback functions
-export([init/1]).
-export([callback_mode/0]).
-export([handle_event/4]).
-export([terminate/3]).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type client_auth_spec() :: anonymous | {user_name, binary(), binary()} | {certificate, opcua_keychain:ident()}.
-type endpoint_selector() :: fun((Conn :: opcua:connection(), Endpoints :: [map()]) ->
     {ok, Endpoint :: term(), TokenPolicyId :: binary(),
          AuthMethod :: client_auth_spec()}
   | {error, not_found}).

-type connect_options() :: #{
    % The parent keychain to use for the connection, if not defined it will use
    % the default keychain.
    keychain => opcua_keychain:state(),
    % The number of time the client will retry connecting. Default: 3
    connect_retry => non_neg_integer(),
    % The connection timeout. Default: infinty.
    connect_timeout => infinity | non_neg_integer(),
    % The keychain manager to use, if not specfied it uses the default one.
    keychain => term(),
    % If the client should lookup the server endpoints first.
    % The default is false if mode is none, true otherwise.
    endpoint_lookup => boolean(),
    % The security to use for endpoint lookup, if not specified,
    % mode and policy will be none.
    % Note that to be able to establish a secure connection, the client
    % needs to know the server certificate/identity, this is what the endpoint
    % lookup is usually used for.
    endpoint_lookup_security => #{
        mode => opcua:security_mode(),
        policy => opcua:security_policy_type()
    },
    % The endpoint selector function to use if endpoint lookup is enabled.
    % If not specified, the first endpoint and token type that match the then
    % required options mode and auth will be selected.
    endpoint_selector => endpoint_selector(),
    % The security mode to use if the endpoint_selector is not defined or
    % the endpoint selection is not enabled. By default it uses none.
    mode => opcua:security_mode(),
    % The security policy to use if the endpoint_selector is not defined or
    % the endpoint selection is not enabled. By default it uses none.
    policy => opcua:security_policy_type(),
    % The client identity, must be defined if the policy type is not none.
    identity => undefined | opcua_keychain:ident(),
    % The server identity, if not defined, the identity will be found by
    % looking up the server endpoints. This could be specified to force the
    % use of a specific endpoint, or do a secured endpoint lookup (if that
    % ever make sense).
    server_identity => undefined | opcua_keychain:ident(),
    % The authentication method to use if the endpoint_selector is not defined
    % or the endpoint selection is not enabled. By default it uses anonymous.
    auth => client_auth_spec()
}.

-type browse_options() :: opcua:references_options().

-type read_options() :: #{
}.

-type write_options() :: #{
}.

-type get_options() :: #{
    % If the retrieved node should be stored in the client space.
    % For now, the space is not checked before retieving the remote data,
    % so it is not really a cache yet, but this could be used to retrieve
    % remote types manually.
    cache => boolean()
}.

-type put_options() :: #{
    % The default parent node for the added node. If not specified, the standard
    % node 'objects' will be used as default parent node.
    parent => opcua:node_spec(),
    % The default reference type for the reference beween the parent node and
    % the added node. If not specified, the reference type will be `organizes`.
    ref_type => opcua:node_spec()
}.

-type del_options() :: #{
    % If the reference having the deleted node as target should be deleted.
    % If not specified, they will not be deleted.
    delete_target_references => boolean()
}.

-type put_ref_options() :: #{
    % Add the reverse reference automatically, possibly changing the reference
    % type if the reference is not symmetric (contains/contained_in).
    % If not specified, no inverse reference will be added.
    bidirectional => boolean()
}.

-type del_ref_options() :: #{
    % If the opposite direction reference should be deleted too.
    % If not specified, they will not be deleted.
    delete_bidirectional => boolean()
}.

-type browse_spec() :: opcua:node_spec() | {opcua:node_spec(), browse_options()}.
-type attrib_spec_range() :: {non_neg_integer(), non_neg_integer()}.
-type attrib_spec_index_level() :: non_neg_integer() | attrib_spec_range().
-type attrib_spec_index() :: attrib_spec_index_level() | [attrib_spec_index_level()].
-type attrib_spec() :: atom() | {atom(), attrib_spec_index()}.
-type read_spec() :: {opcua:node_spec(), attrib_spec() | [attrib_spec()]}.
-type write_spec() :: {opcua:node_spec(), {attrib_spec(), term()} | [{attrib_spec(), term()}]}.
-type ref_desc() :: map().

-record(data, {
    opts                        :: undefined | map(),
    socket                      :: undefined | inet:socket(),
    conn                        :: undefined | opcua:connection(),
    proto                       :: undefined | term(),
    continuations = #{}         :: #{term() => {term(), cont_fun()}}
}).

-type cont_fun() :: fun((#data{}, ok | error, ResultOrReason  :: term(),
                         Params :: term()) -> #data{}).

-export_type([connect_options/0, browse_options/0,
              read_options/0, write_options/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

connect(EndpointUrl) ->
    connect(EndpointUrl, #{}).

close(Pid) ->
    gen_statem:call(Pid, close).

-spec connect(EndpointUrl :: binary(), Opts :: connect_options()) ->
    {ok, ClientPid :: pid()} | {error, Reason :: term()}.
connect(EndpointUrl, Opts) ->
    Pid = opcua_client_pool_sup:start_client(#{}),
    EndpointRec = opcua_util:parse_endpoint(EndpointUrl),
    try prepare_connect_options(Opts) of
        FullOpts ->
            Msg = {connect, EndpointRec, FullOpts},
            case gen_statem:call(Pid, Msg, infinity) of
                {error, _Reason} = Error -> Error;
                ok -> {ok, Pid}
            end
    catch throw:Reason -> {error, Reason}
    end.

-spec browse(pid(), opcua:node_spec() | [browse_spec()]) ->
    [ref_desc()] | [[ref_desc()]].
browse(Pid, BrowseSpec) when is_list(BrowseSpec)  ->
    browse(Pid, BrowseSpec, #{});
browse(Pid, NodeSpec) ->
    [Result] = browse(Pid, [NodeSpec], #{}),
    Result.

-spec browse(pid(), opcua:node_spec() | [browse_spec()], browse_options()) ->
    [ref_desc()] | [[ref_desc()]].
browse(Pid, BrowseSpec, DefaultOpts) when is_list(BrowseSpec) ->
    {BrowseSpec2, DefaultOpts2} = prepare_browse(BrowseSpec, DefaultOpts),
    Command = {browse, BrowseSpec2, DefaultOpts2},
    case gen_statem:call(Pid, Command) of
        {ok, Result} ->  Result;
        {error, Reason} ->
            erlang:error(Reason)
    end;
browse(Pid, NodeSpec, DefaultOpts) ->
    [Result] = browse(Pid, [NodeSpec], DefaultOpts),
    Result.

-spec read(pid(), [read_spec()]) -> [map()].
read(Pid, ReadSpec) ->
    read(Pid, ReadSpec, #{}).

-spec read(pid(), opcua:node_spec() | [read_spec()],
           attrib_spec() | [attrib_spec()] | read_options()) ->
    map() | [map()].
read(Pid, ReadSpec, Opts) when is_list(ReadSpec) ->
    PreparedSpec = prepare_read_spec(ReadSpec, []),
    Command = {read, PreparedSpec, Opts},
    case gen_statem:call(Pid, Command) of
        {ok, Result} -> Result;
        {error, Reason} ->
            erlang:error(Reason)
    end;
read(Pid, NodeSpec, AttribSpecs) ->
    read(Pid, NodeSpec, AttribSpecs, #{}).

read(Pid, NodeSpec, AttribSpecs, Opts) when is_list(AttribSpecs) ->
    [Result] = read(Pid, [{NodeSpec, AttribSpecs}], Opts),
    Result;
read(Pid, NodeSpec, AttribSpec, Opts) ->
    Key = result_key(AttribSpec),
    case read(Pid, [{NodeSpec, [AttribSpec]}], Opts) of
        [#{Key := #opcua_error{status = Status}}] -> erlang:error(Status);
        [#{Key := Result}] -> Result
    end.

-spec write(pid(), [write_spec()]) -> [map()].
write(Pid, WriteSpec) ->
    write(Pid, WriteSpec, #{}).

-spec write(pid(), opcua:node_spec() | [write_spec()],
            [{attrib_spec(), term()}] | write_options()) ->
    map() | [map()].
write(Pid, WriteSpec, Opts) when is_list(WriteSpec) ->
    PreparedSpec = prepare_write_spec(WriteSpec, []),
    Command = {write, PreparedSpec, Opts},
    case gen_statem:call(Pid, Command) of
        {ok, Result} -> Result;
        {error, Reason} ->
            erlang:error(Reason)
    end;
write(Pid, NodeSpec, AttribValuePairs) ->
    write(Pid, NodeSpec, AttribValuePairs, #{}).

write(Pid, NodeSpec, AttribValuePairs, Opts) when is_list(AttribValuePairs) ->
    [Result] = write(Pid, [{NodeSpec, AttribValuePairs}], Opts),
    Result;
write(Pid, NodeSpec, AttribSpec, Value) ->
    write(Pid, NodeSpec, AttribSpec, Value, #{}).

write(Pid, NodeSpec, AttribSpec, Value, Opts) ->
    Key = result_key(AttribSpec),
    case write(Pid, NodeSpec, [{AttribSpec, Value}], Opts) of
        #{Key := #opcua_error{status = Status}} -> erlang:error(Status);
        #{Key := Result} -> Result
    end.

-spec get(pid(), NodeSpec | [NodeSpec]) -> Node | [Node | Error]
  when NodeSpec :: opcua:node_spec(),
       Node :: opcua:node_rec(), Error :: opcua:error().
get(Pid, NodeSpec) ->
    get(Pid, NodeSpec, #{}).

-spec get(pid(), NodeSpec | [NodeSpec], Opts) -> Node | [Node | Error]
  when NodeSpec :: opcua:node_spec(), Opts :: get_options(),
       Node :: opcua:node_rec(), Error :: opcua:error().
get(Pid, NodeSpecs, Opts) when is_list(NodeSpecs) ->
    NodeIds = [opcua_node:id(Spec) || Spec <- NodeSpecs],
    case gen_statem:call(Pid, {get, NodeIds, Opts}) of
        {ok, Nodes} -> Nodes;
        {error, Reason} -> erlang:error(Reason)
    end;
get(Pid, NodeSpec, Opts) ->
    case get(Pid, [NodeSpec], Opts) of
        [#opcua_error{status = Status}] -> erlang:error(Status);
        [Node] -> Node
    end.

-spec put(pid(), [NodeDef], Opts) -> [Node | Error]
  when NodeDef :: Node | {ParentSpec, Node} | {ParentSpec, RefTypeSpec, Node},
       ParentSpec :: opcua:node_spec(), RefTypeSpec :: opcua:node_spec(),
       Node :: opcua:node_rec(), Error :: opcua:error(),
       Opts :: put_options().
put(_Pid, _NodeDefs, _Opts) ->
    erlang:error(not_implemented).

-spec del(pid(), [NodeSpec], Opts) -> [Status]
  when NodeSpec :: opcua:node_spec(), Opts :: del_options(),
       Status :: opcua:status().
del(_Pid, _NodeSpecs, _Opts) ->
    erlang:error(not_implemented).

-spec put_refs(pid(), [RefDef], Opts) -> [Status]
  when RefDef :: {SourceSpec, RefTypeSpec, TargetSpec} | opcua:node_ref(),
       SourceSpec :: opcua:node_spec(), RefTypeSpec :: opcua:node_spec(),
       TargetSpec :: opcua:node_spec(), Opts :: put_ref_options(),
       Status :: opcua:status().
put_refs(_Pid, _RefDefs, _Opts) ->
    erlang:error(not_implemented).

-spec del_refs(pid(), [RefDef], Opts) -> [Status]
  when RefDef :: {SourceSpec, RefTypeSpec, TargetSpec} | opcua:node_ref(),
       SourceSpec :: opcua:node_spec(), RefTypeSpec :: opcua:node_spec(),
       TargetSpec :: opcua:node_spec(), Opts :: del_ref_options(),
       Status :: opcua:status().
del_refs(_Pid, _RefDefs, _Opts) ->
    erlang:error(not_implemented).


%%% STARTUP FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Opts) ->
    gen_statem:start_link(?MODULE, Opts, []).


%%% BEHAVIOUR gen_statem CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Opts) ->
    ?LOG_DEBUG("OPCUA client process starting", []),
    {ok, disconnected, #data{}}.

callback_mode() -> [handle_event_function, state_enter].

%% STATE: disconnected
handle_event({call, From}, {connect, EndpointRec, Opts}, disconnected = State,
             #data{conn = undefined} = Data) ->
    Data2 = Data#data{opts = Opts},
    {ProtoMode, ProtoOpts} = proto_initial_mode(Data2),
    case opcua_client_uacp:init(ProtoMode, ProtoOpts) of
        % No error use-case yet, disabling to make dialyzer happy
        % {error, Reason} ->
        %     {stop, normal, do_abort_all(Data2, Reason)};
        {ok, Proto} ->
            Data3 = delay_response(Data2#data{proto = Proto}, on_ready, From),
            {next_state, {connecting, 0, ProtoMode, EndpointRec}, Data3,
                         event_timeouts(State, Data2)}
    end;
%% STATE: {connecting, N, EndpointRec}
handle_event(enter, _OldState, {connecting, N, _, _} = State, Data) ->
    ?LOG_DEBUG("Client ~p entered ~p", [self(), State]),
    #data{opts = #{connect_retry := MaxRetry}} = Data,
    case MaxRetry =:= infinity orelse N =< MaxRetry of
        true  ->
            {keep_state, Data, enter_timeouts(State, Data)};
        false ->
            {stop, normal, do_abort_all(Data, retry_exhausted)}
    end;
handle_event(state_timeout, retry,
            {connecting, N, ProtoMode, EndpointRec} = State, Data) ->
    case conn_init(Data, ProtoMode, EndpointRec) of
        {ok, Data2} ->
            {next_state, handshaking, Data2, event_timeouts(State, Data2)};
        {error, _Reason} ->
            {next_state, {connecting, N + 1, ProtoMode, EndpointRec}, Data,
                event_timeouts(State, Data)}
    end;
%% STATE: handshaking
handle_event(enter, _OldState, handshaking = State, Data) ->
    ?LOG_DEBUG("Client ~p entered ~p", [self(), State]),
    case proto_handshake(Data) of
        % No error use-case yet, diabling to make dialyzer happy
        % {error, Reason, Data2} ->
        %     {stop, normal, do_abort_all(Data2, Reason)};
        {ok, Data2} ->
            {keep_state, Data2, enter_timeouts(State, Data2)}
    end;
handle_event(info, {opcua_connection, {reconnect, {Peer, Url}}},
             handshaking = State, Data) ->
    ?LOG_INFO("Reconnecting to endpoint ~s", [Url]),
    {next_state, {reconnecting, {Peer, Url}}, Data, event_timeouts(State, Data)};
handle_event(info, {opcua_connection, ready}, handshaking = State, Data) ->
    {next_state, connected, Data, event_timeouts(State, Data)};
handle_event(info, {opcua_connection, _}, handshaking, Data) ->
    {stop, normal, do_abort_all(Data, opcua_handshaking_failed)};
handle_event(state_timeout, abort, handshaking, Data) ->
    {stop, normal, do_abort_all(Data, handshake_timeout)};
%% STATE: connected
handle_event(enter, _OldState, connected = State, Data) ->
    ?LOG_DEBUG("Client ~p entered connected", [self()]),
    Data2 = do_continue(Data, on_ready, ok),
    {keep_state, Data2, enter_timeouts(State, Data)};
handle_event({call, From}, close, connected = State, Data) ->
    Data2 = delay_response(Data, on_closed, From),
    {next_state, closing, Data2, event_timeouts(State, Data)};
handle_event({call, From}, {browse, BrowseSpec, Opts}, connected = State, Data) ->
    Data2 = do_browse(Data, BrowseSpec, Opts, From, fun contfun_reply/4),
    {keep_state, Data2, enter_timeouts(State, Data2)};
handle_event({call, From}, {read, ReadSpec, Opts},
             connected = State, Data) ->
    Data2 = do_read(Data, ReadSpec, Opts, From, fun contfun_reply/4),
    {keep_state, Data2, enter_timeouts(State, Data2)};
handle_event({call, From}, {write, WriteSpec, Opts},
             connected = State, Data) ->
    Data2 = do_write(Data, WriteSpec, Opts, From, fun contfun_reply/4),
    {keep_state, Data2, enter_timeouts(State, Data2)};
handle_event({call, From}, {get, NodeIds, Opts}, connected = State, Data) ->
    Data2 = do_get(Data, NodeIds, Opts, From, fun contfun_reply/4),
    {keep_state, Data2, enter_timeouts(State, Data2)};
%% STATE: {reconnecting, Endpoint}
handle_event(enter, _OldState, {reconnecting, _Conn} = State, Data) ->
    ?LOG_DEBUG("Client ~p entered reconnecting", [self()]),
    case proto_close(Data) of
        % No error use-case yet, diabling to make dialyzer happy
        % {error, Reason, Data2} ->
        %     Data3 = do_continue(Data2, on_closed, error, Reason),
        %     {stop, Reason, do_abort_all(Data3, closed)};
        {ok, Data2} ->
            {keep_state, Data2, enter_timeouts(State, Data2)}
    end;
handle_event(info, {opcua_connection, closed},
             {reconnecting, PeerInfo} = State, Data) ->
    reconnect(Data, State, PeerInfo);
handle_event(state_timeout, abort, {reconnecting, PeerInfo} = State, Data) ->
    reconnect(Data, State, PeerInfo);
handle_event(info, {tcp_closed, Sock}, {reconnecting, PeerInfo} = State,
             #data{socket = Sock} = Data) ->
    %% When closing the server may close the socket at any time
    reconnect(Data, State, PeerInfo);
%% STATE: closing
handle_event(enter, _OldState, closing = State, Data) ->
    ?LOG_DEBUG("Client ~p entered closing", [self()]),
    case proto_close(Data) of
        % No error use-case yet, diabling to make dialyzer happy
        % {error, Reason, Data2} ->
        %     Data3 = do_continue(Data2, on_closed, error, Reason),
        %     {stop, Reason, do_abort_all(Data3, closed)};
        {ok, Data2} ->
            {keep_state, Data2, enter_timeouts(State, Data2)}
    end;
handle_event(info, {opcua_connection, closed}, closing, Data) ->
    Data2 = do_continue(Data, on_closed, ok),
    Data3 = do_abort_all(Data2, closed),
    {stop, normal, Data3};
handle_event(state_timeout, abort, closing, Data) ->
    {stop, normal, do_abort_all(Data, close_timeout)};
handle_event(info, {tcp_closed, Sock}, closing, #data{socket = Sock} = Data) ->
    %% When closing the server may close the socket at any time
    Data2 = do_continue(Data, on_closed, ok),
    Data3 = do_abort_all(Data2, closed),
    {stop, normal, Data3};
%% STATE: handshaking, connected, reconnecting and closing
handle_event(timeout, produce, State, Data) ->
    case proto_produce(Data) of
        {ok, Data2} ->
            {keep_state, Data2, event_timeouts(State, Data2)};
        {ok, Output, Data2} ->
            case conn_send(Data2, Output) of
                ok -> {keep_state, Data2, event_timeouts(State, Data2)};
                {error, Reason} -> {stop, Reason, do_abort_all(Data2, Reason)}
            end;
        {error, Reason, Data2} -> {stop, Reason, do_abort_all(Data2, Reason)}
    end;
handle_event(info, {tcp, Sock, Input}, State, #data{socket = Sock} = Data) ->
    ?DUMP("Received Data: ~p", [Input]),
    case do_handle_result(proto_handle_data(Data, Input)) of
        {ok, Data2} -> {keep_state, Data2, event_timeouts(State, Data2)};
        {error, Reason, Data2} -> {stop, Reason, Data2}
    end;
handle_event(info, {tcp_passive, Sock}, State, #data{socket = Sock} = Data) ->
    case conn_activate(Data) of
        ok ->
            {keep_state, Data, event_timeouts(State, Data)};
        {error, Reason} ->
            {stop, Reason, do_abort_all(Data, socket_error)}
    end;
handle_event(info, {tcp_closed, Sock}, _State, #data{socket = Sock} = Data) ->
    {stop, normal, do_abort_all(Data, socket_closed)};
handle_event(info, {tcp_error, Sock}, _State, #data{socket = Sock} = Data) ->
    {stop, tcp_error, do_abort_all(Data, socket_error)};
%% GENERIC STATE HANDLERS
handle_event(enter, _OldState, NewState, Data) ->
    ?LOG_DEBUG("Client ~p entered ~p", [self(), NewState]),
    {keep_state, Data, enter_timeouts(NewState, Data)};
handle_event(call, _, _, Data) ->
    {stop, unexpected_call, do_abort_all(Data, unexpected_call)};
handle_event(cast, _, _, Data) ->
    %TODO: Should be changed to not crash the client later on
    {stop, unexpected_cast, do_abort_all(Data, unexpected_cast)};
handle_event(info, _, _, Data) ->
    %TODO: Should be changed to not crash the client later on
    {stop, unexpected_message, do_abort_all(Data, unexpected_message)}.

terminate(Reason, State, Data) ->
    ?LOG_DEBUG("OPCUA client process terminated in state ~w: ~p", [State, Reason]),
    proto_terminate(Data, Reason),
    conn_close(Data).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-- INTERNAL ASYNC API FUNCTIONS -----------------------------------------------

do_browse(Data, BrowseSpec, Opts, ContParams, ContFun) ->
    case proto_browse(Data, BrowseSpec, Opts) of
        {async, Handle, Data2} ->
            UnpackParams = {BrowseSpec, ContParams, ContFun},
            schedule_continuation(Data2, Handle, UnpackParams,
                                  fun contfun_unpack_browse/4)
    end.

do_read(Data, ReadSpec, Opts, ContParams, ContFun) ->
    case proto_read(Data, ReadSpec, Opts) of
        {async, Handle, Data2} ->
            UnpackParams = {ReadSpec, ContParams, ContFun},
            schedule_continuation(Data2, Handle, UnpackParams,
                                  fun contfun_unpack_read/4)
    end.

do_write(Data, WriteSpec, Opts, ContParams, ContFun) ->
    case proto_write(Data, WriteSpec, Opts) of
        {async, Handle, Data2} ->
            UnpackParams = {WriteSpec, ContParams, ContFun},
            schedule_continuation(Data2, Handle, UnpackParams,
                                  fun contfun_unpack_write/4)
    end.

do_get(Data, NodeIds, Opts, ContParams, ContFun) ->
    Attribs = [{{A, undefined}, #{}} || A <- opcua_nodeset:attributes()],
    ReadSpec = [{NID, Attribs} || NID <- NodeIds],
    UnpackParams = {ContParams, ContFun, Opts},
    do_read(Data, ReadSpec, #{}, UnpackParams, fun contfun_unpack_get/4).

do_retrieve_datatype(Data, NodeIds, ContParams, ContFun) ->
    retrieve_datatype_browse(Data, NodeIds, sets:from_list(NodeIds), sets:new(),
                             {ContParams, ContFun}).

schedule_continuation(#data{continuations = ContMap} = Data, Key, Params, ContFun) ->
    ?assertNot(maps:is_key(Key, ContMap)),
    Data#data{continuations = ContMap#{Key => {Params, ContFun}}}.

do_handle_result(#data{} = Data) ->
    {ok, Data};
do_handle_result({deferred, Action, ProtoCont, #data{} = Data}) ->
    do_handle_result(take_action(Data, Action, ProtoCont));
do_handle_result({ok, Responses, #data{} = Data}) ->
    {ok, do_continue(Data, Responses)};
do_handle_result({error, Reason, #data{} = Data}) ->
    {error, Reason, do_abort_all(Data, Reason)}.

take_action(Data, {cache_datatype, Schemas}, ProtoCont) ->
    ?LOG_DEBUG("Trying to retrieve remote data types ~s",
              [lists:join(", ", [opcua_node:format(I) || I <- Schemas])]),
    do_retrieve_datatype(Data, Schemas, ProtoCont,
                         fun contfun_datatype_retrieved/4).

% These are for this module internal continuations after any do_XXX calls,
% they are different from the protocol continuations returned with the
% deferred result. We should probably do some renaming...
do_continue(#data{} = Data, []) -> Data;
do_continue(#data{continuations = ContMap} = Data,
            [{Key, Outcome, ResultOrReason} | Rest]) ->
    case maps:take(Key, ContMap) of
        error ->
            ?LOG_WARNING("Continuation for ~w not found", [Key]),
            do_continue(Data, Rest);
        {{Params, ContFun}, ContMap2} ->
            Data2 = Data#data{continuations = ContMap2},
            do_continue(ContFun(Data2, Outcome, ResultOrReason, Params), Rest)
    end;
do_continue({error, Reason, #data{} = Data}, Responses) ->
    ?LOG_WARNING("Error while processing client responses: ~p", [Reason]),
    do_continue(Data, Responses);
do_continue({ok, #data{} = Data}, Responses) ->
    do_continue(Data, Responses);
do_continue({ok, MoreResponses, #data{} = Data}, Responses) ->
    do_continue(Data, Responses ++ MoreResponses).

do_continue(Data, Key, Outcome) ->
    do_continue(Data, Key, Outcome, '_NO_RESULT_').

do_continue(Data, Key, Outcome, Result) ->
    do_continue(Data, [{Key, Outcome, Result}]).

do_abort_all(#data{continuations = ContMap} = Data, Reason) ->
    do_continue(Data, [{K, error, Reason} || K <- maps:keys(ContMap)]).

contfun_unpack_browse(Data, error, Reason,
                    {_BrowseSpec, SubParams, SubContFun}) ->
    SubContFun(Data, error, Reason, SubParams);
contfun_unpack_browse(#data{conn = Conn} = Data, ok, Results,
                    {BrowseSpec, SubParams, SubContFun}) ->
    UnpackedResult = unpack_browse_result(Conn, BrowseSpec, Results),
    SubContFun(Data, ok, UnpackedResult, SubParams).

contfun_unpack_read(Data, error, Reason,
                    {_ReadSpec, SubParams, SubContFun}) ->
    SubContFun(Data, error, Reason, SubParams);
contfun_unpack_read(#data{conn = Conn} = Data, ok, Results,
                    {ReadSpec, SubParams, SubContFun}) ->
    UnpackedResult = unpack_read_result(Conn, ReadSpec, Results),
    SubContFun(Data, ok, UnpackedResult, SubParams).

contfun_unpack_write(Data, error, Reason,
                     {_WriteSpec, SubParams, SubContFun}) ->
    SubContFun(Data, error, Reason, SubParams);
contfun_unpack_write(Data, ok, Results,
                     {WriteSpec, SubParams, SubContFun}) ->
    UnpackedResult = unpack_write_result(WriteSpec, Results),
    SubContFun(Data, ok, UnpackedResult, SubParams).

contfun_unpack_get(Data, error, Reason,
                   {Params, ContFun, _Opts}) ->
    ContFun(Data, error, Reason, Params);
contfun_unpack_get(#data{conn = Conn} = Data, ok, Result,
                   {Params, ContFun, Opts}) ->
    Nodes = [opcua_node:from_attributes(A) || A <- Result],
    case maps:get(cache, Opts, false) of
        true -> opcua_space:add_nodes(Conn, Nodes);
        false -> ok
    end,
    ContFun(Data, ok, Nodes, Params).

contfun_reply(Data, Outcome, '_NO_RESULT_', From) ->
    gen_statem:reply(From, Outcome),
    Data;
contfun_reply(Data, Outcome, ResultOrReason, From) ->
    gen_statem:reply(From, {Outcome, ResultOrReason}),
    Data.

contfun_retrieve_datatype(Data, error, Reason, {_, {ContParams, ContFun}}) ->
    ContFun(Data, error, Reason, ContParams);
contfun_retrieve_datatype(#data{conn = Conn} = Data, ok, Result,
                          {{browsing, NodeIdsSet, RefSet}, ContInfo}) ->
    %TODO: Refactor this function to make it more clear.
    % If any browsed command failed, we only issue a warning, it could be
    % some inconsistency that wouldn't prevent decoding. If this inconsistency
    % would prevent decoding, the maximum number of decoding atempts will 
    % be reached.
    % Concatenate all the references, filter the HasSubType,
    % HasProperty, HasEncoding and HasTypeDefinition, check if there is any new
    % nodes referenced (Not is the space and not already browsed), if there is
    % keep the references and keep browsing until there is no more new nodes.
    % If there isn't any new nodes, get all of them and we are done.
    RefGroups = lists:foldl(fun
        (#opcua_error{node_id = NodeId, status = Reason}, Acc) ->
            ?LOG_WARNING("Error while browsing data type information node ~s: ~p",
                         [opcua_node:format(NodeId), Reason]),
            Acc;
        (Refs, Acc) ->
            [Refs | Acc]
    end, [], Result),
    {NewNodeIds, NodeIdsSet2, RefSet2} = lists:foldl(fun
        (#opcua_reference{type_id = T, source_id = S, target_id = D} = Ref,
         {Acc0, Ns0, Rs0})
          when T =:= ?NID_HAS_ENCODING; T =:= ?NID_HAS_SUBTYPE;
               T =:= ?NID_HAS_TYPE_DEFINITION; T =:= ?NID_HAS_PROPERTY ->
            Rs1 = sets:add_element(Ref, Rs0),
            lists:foldl(fun(NodeId, {Acc2, Ns2, Rs2}) ->
                case sets:is_element(NodeId, Ns2) of
                    true ->
                        {Acc2, Ns2, Rs2};
                    false ->
                        case opcua_space:node(Conn, NodeId) of
                            #opcua_node{} ->
                                {Acc2, Ns2, Rs2};
                            undefined ->
                                Ns3 = sets:add_element(NodeId, Ns2),
                                {[NodeId | Acc2], Ns3, Rs2}
                        end
                end
            end, {Acc0, Ns0, Rs1}, [S, D]);
        (_Ref, Acc) ->
            Acc
    end, {[], NodeIdsSet, RefSet},
         [R || #{reference := R} <- lists:append(RefGroups)]),
    case NewNodeIds of
        [] ->
            % No more new nodes, get them all
            NodeIdList = sets:to_list(NodeIdsSet2),
            Refs = sets:to_list(RefSet2),
            retrieve_datatype_get(Data, NodeIdList, Refs, ContInfo);
        NewNodeIds ->
            retrieve_datatype_browse(Data, NewNodeIds, NodeIdsSet2, RefSet2, ContInfo)
    end;
contfun_retrieve_datatype(Data, ok, Result,
                          {{getting, Refs}, {ContParams, ContFun}}) ->
    % Same as for browse commands, we ignore the errors.
    Nodes = lists:foldl(fun
        (#opcua_error{node_id = NodeId, status = Reason}, Acc) ->
            ?LOG_WARNING("Error while retrieving data type information node ~s: ~p",
                         [opcua_node:format(NodeId), Reason]),
            Acc;
        (Node, Acc) ->
            [Node | Acc]
    end, [], Result),
    ContFun(Data, ok, {Nodes, Refs}, ContParams).

retrieve_datatype_browse(Data, NodeIds, NodeIdsSet, RefSet, ContInfo) ->
    {BrowseSpec, BrowseOpts} = prepare_browse(NodeIds, #{direction => both}),
    Params = {{browsing, NodeIdsSet, RefSet}, ContInfo},
    do_browse(Data, BrowseSpec, BrowseOpts, Params, fun contfun_retrieve_datatype/4).

retrieve_datatype_get(Data, NodeIds, Refs, ContInfo) ->
    Params = {{getting, Refs}, ContInfo},
    do_get(Data, NodeIds, #{}, Params, fun contfun_retrieve_datatype/4).


contfun_datatype_retrieved(Data, error, Reason, ProtoCont) ->
    proto_abort(Data, ProtoCont, Reason);
contfun_datatype_retrieved(#data{conn = Conn} = Data, ok,
                           {Nodes, References}, ProtoCont) ->

    DataTypesStr = [opcua_node:format(opcua_node:spec(I))
                    || #opcua_node{node_id = I, node_class = #opcua_data_type{}}
                    <- Nodes],
    ?LOG_INFO("Caching remote data type(s) ~s",
              [iolist_to_binary(lists:join(", ", DataTypesStr))]),
    opcua_space:add_nodes(Conn, Nodes),
    opcua_space:add_references(Conn, References),
    continue_protocol(Data, ProtoCont).

delay_response(Data, Tag, From) ->
    schedule_continuation(Data, Tag, From, fun contfun_reply/4).

% This is for the continuation returned by the protocol when it requires the
% client to do something (take action).
continue_protocol(Data, ProtoCont) ->
    do_handle_result(proto_continue(Data, ProtoCont)).


%-- OTHER INTERNAL FUNCTIONS ---------------------------------------------------

result_key(Attrib) when is_atom(Attrib) -> Attrib;
result_key({Attrib, undefined}) when is_atom(Attrib) -> Attrib;
result_key({Attrib, IndexRange}) when is_atom(Attrib) -> {Attrib, IndexRange}.

prepare_connect_options(Opts) ->
    EndpointLookupSec = case maps:get(endpoint_lookup_security, Opts, #{}) of
        undefined -> #{};
        Value -> Value
    end,
    Merged = maps:merge(#{
        connect_retry => 3,
        connect_timeout => infinity,
        keychain => default,
        endpoint_lookup => undefined,
        endpoint_lookup_security =>
            maps:merge(#{
                mode => none,
                policy => none
            }, EndpointLookupSec),
        mode => none,
        policy => none,
        identity => undefined,
        server_identity => undefined,
        auth => anonymous
    }, Opts),
    prepare_keychain(
        prepare_lookup(
            prepare_server_identity(
                prepare_client_identity(
                    prepare_selector(Merged))))).

default_browse_opts() ->
    #{type => undefined, direction => forward, include_subtypes => false}.

prepare_browse(BrowseSpec, Opts) ->
    DefaultOpts = default_browse_opts(),
    PreparedOpts = maps:merge(DefaultOpts, prepare_browse_opts(Opts)),
    {prepare_browse_spec(DefaultOpts, BrowseSpec, []), PreparedOpts}.

prepare_browse_spec(_DefaultOpts, [], Acc) ->
    lists:reverse(Acc);
prepare_browse_spec(DefaultOpts, [{NodeSpec, undefined} | Rest], Acc) ->
    Acc2 = [{opcua_node:id(NodeSpec), undefined} | Acc],
    prepare_browse_spec(DefaultOpts, Rest, Acc2);
prepare_browse_spec(DefaultOpts, [{NodeSpec, Opts} | Rest], Acc)
  when is_map(Opts) ->
    FixedOpts = maps:merge(DefaultOpts, prepare_browse_opts(Opts)),
    Acc2 = [{opcua_node:id(NodeSpec), FixedOpts} | Acc],
    prepare_browse_spec(DefaultOpts, Rest, Acc2);
prepare_browse_spec(DefaultOpts, [NodeSpec | Rest], Acc)->
    Acc2 = [{opcua_node:id(NodeSpec), undefined} | Acc],
    prepare_browse_spec(DefaultOpts, Rest, Acc2).

prepare_browse_opts(#{type := NodeSpec} = Opts) ->
    Opts#{type => opcua_node:id(NodeSpec)};
prepare_browse_opts(Opts) ->
    Opts.

prepare_read_spec([], Acc) ->
    lists:reverse(Acc);
prepare_read_spec([{NodeSpec, Attribs} | Rest], Acc)
  when is_list(Attribs) ->
    FixedAttribs = prepare_read_spec_attr(Attribs, []),
    Acc2 = [{opcua_node:id(NodeSpec), FixedAttribs} | Acc],
    prepare_read_spec(Rest, Acc2);
prepare_read_spec([{NodeSpec, Attrib} | Rest], Acc)->
    FixedAttribs = prepare_read_spec_attr([Attrib], []),
    Acc2 = [{opcua_node:id(NodeSpec), FixedAttribs} | Acc],
    prepare_read_spec(Rest, Acc2).

prepare_read_spec_attr([], Acc) ->
    lists:reverse(Acc);
prepare_read_spec_attr([Attr | Rest], Acc) when is_atom(Attr) ->
    prepare_read_spec_attr(Rest, [{{Attr, undefined}, undefined} | Acc]);
prepare_read_spec_attr([{Attr, IndexRange} | Rest], Acc) when is_atom(Attr) ->
    prepare_read_spec_attr(Rest, [{{Attr, IndexRange}, undefined} | Acc]).

prepare_write_spec([], Acc) ->
    lists:reverse(Acc);
prepare_write_spec([{NodeSpec, Attribs} | Rest], Acc)
  when is_list(Attribs) ->
    FixedAttribs = prepare_write_spec_attr(Attribs, []),
    Acc2 = [{opcua_node:id(NodeSpec), FixedAttribs} | Acc],
    prepare_write_spec(Rest, Acc2);
prepare_write_spec([{NodeSpec, Attrib, Value} | Rest], Acc)->
    FixedAttribs = prepare_write_spec_attr([{Attrib, Value}], []),
    Acc2 = [{opcua_node:id(NodeSpec), FixedAttribs} | Acc],
    prepare_write_spec(Rest, Acc2).

prepare_write_spec_attr([], Acc) ->
    lists:reverse(Acc);
prepare_write_spec_attr([{Attr, Val} | Rest], Acc) when is_atom(Attr) ->
    prepare_write_spec_attr(Rest, [{{Attr, undefined}, Val, undefined} | Acc]);
prepare_write_spec_attr([{{Attr, IndexRange}, Val} | Rest], Acc) when is_atom(Attr) ->
    prepare_write_spec_attr(Rest, [{{Attr, IndexRange}, Val, undefined} | Acc]).

prepare_keychain(#{keychain := Keychain} = Opts) ->
    % Make sure the keychain can be shared with other processes
    Opts#{keychain := opcua_keychain:shareable(Keychain)}.

prepare_lookup(#{mode := none, endpoint_lookup := undefined} = Opts) ->
    Opts#{endpoint_lookup := false};
prepare_lookup(#{endpoint_lookup := undefined} = Opts) ->
    Opts#{endpoint_lookup := true};
prepare_lookup(Opts) ->
    Opts.

prepare_selector(#{endpoint_selector := Selector} = Opts)
  when Selector =/= undefined -> Opts;
prepare_selector(#{mode := Mode, policy := Policy, auth := AuthSpec} = Opts) ->
    DefaultSelector = fun(Conn, Endpoints) ->
        select_endpoint(Conn, Mode, Policy, AuthSpec, Endpoints)
    end,
    Opts#{endpoint_selector => DefaultSelector}.

prepare_client_identity(#{keychain := Keychain, identity := undefined} = Opts) ->
    case opcua_keychain:lookup(Keychain, alias, client) of
        not_found -> Opts;
        [Id | _] -> Opts#{identity => Id}
    end;
prepare_client_identity(#{keychain := Keychain, identity := Id} = Opts) ->
    case opcua_keychain:info(Keychain, Id) of
        not_found -> throw(client_identity_not_found);
        #{id := Id} -> Opts
    end.

prepare_server_identity(#{keychain := Keychain, server_identity := Id} = Opts)
  when Id =/= undefined ->
    case opcua_keychain:info(Keychain, Id) of
        not_found -> throw(server_identity_not_found);
        #{id := Id} -> Opts
    end;
prepare_server_identity(Opts) ->
    Opts.

proto_initial_mode(#data{opts = #{endpoint_lookup := true} = Opts}) ->
    #{endpoint_selector := Selector, endpoint_lookup_security := SubOpts} = Opts,
    #{mode := Mode, policy := Policy} = SubOpts,
    ProtoOpts = #{endpoint_selector => Selector, mode => Mode, policy => Policy},
    {lookup_endpoint, ProtoOpts};
proto_initial_mode(#data{opts = #{endpoint_lookup := false} = Opts}) ->
    #{endpoint_selector := Selector, mode := Mode, policy := Policy} = Opts,
    ProtoOpts = #{endpoint_selector => Selector, mode => Mode, policy => Policy},
    {open_session, ProtoOpts}.

reconnect(#data{opts = Opts, conn = #uacp_connection{keychain = Keychain}} = Data,
        State, {PeerIdent, EndpointUrl}) ->
    Data2 = conn_close(Data),
    #{endpoint_selector := Selector} = Opts,
    ProtoOpts = #{endpoint_selector => Selector},
    case opcua_client_uacp:init(open_session, ProtoOpts) of
        % No error use-case yet, disabling to make dialyzer happy
        % {error, Reason} ->
        %     stop_and_reply_all(internal_error, Data2, {error, Reason});
        {ok, Proto} ->
            Data3 = Data2#data{proto = Proto,
                               opts = maps:merge(Opts, #{
                                    server_identity => PeerIdent,
                                    keychain => Keychain
                               })},
            {next_state,
                {connecting, 0, open_session, opcua_util:parse_endpoint(EndpointUrl)},
                Data3,
                event_timeouts(State, Data3)}
    end.

select_endpoint(Conn, Mode, Policy, AuthSpec, Endpoints) ->
    ServerCert = opcua_connection:peer_certificate(Conn),
    PolicyUri = opcua_util:policy_uri(Policy),
    AuthType = auth_type(AuthSpec),
    FilteredBySecurity = filter_by_security(Endpoints, Mode, PolicyUri, ServerCert),
    FilteredByTokens = filter_by_token_type(FilteredBySecurity, AuthType),
    case take_first_who_validates(Conn, Mode, FilteredByTokens) of
        not_found -> {error, not_found};
        {Conn3, PeerIdentity, #{user_identity_tokens:= Tokens} = Endpoint} ->
            [#{policy_id := PolicyId} | _] = filter_tokens(Tokens, AuthType),
            {ok, Conn3, PeerIdentity, Endpoint, PolicyId, AuthSpec}
    end.

take_first_who_validates(_, _, []) ->
    not_found;
take_first_who_validates(Conn,  Mode, [#{server_certificate := undefined}| Rest])
when Mode =/= none ->
    take_first_who_validates(Conn,  Mode, Rest);
take_first_who_validates(Conn,  Mode, [CandidateEndpoint| Rest]) ->
    #{server_certificate := C} = CandidateEndpoint,
    case opcua_keychain:validate(Conn, C) of
        {ok, Conn2, PeerIdentity} -> {Conn2, PeerIdentity, CandidateEndpoint};
        {error, _Reason} ->
            take_first_who_validates(Conn,  Mode, Rest)
    end.

filter_by_security(Endpoints, Mode, PolicyUri, ServerDerCert) ->
    [E || E = #{security_mode := M,
                security_policy_uri := P,
                server_certificate := CertOrChain} <- Endpoints,
            M =:= Mode, P =:= PolicyUri,
            ServerDerCert =:= undefined orelse
            cert_head_of_chain(CertOrChain, ServerDerCert)].

cert_head_of_chain(DerChain, DerCert) ->
    Size = byte_size(DerCert),
    case DerChain of
        <<DerCert:Size/binary, _/binary>> -> true;
        _ -> false
    end.

filter_by_token_type(Endpoints, AuthType) ->
    [ E || #{user_identity_tokens := Tokens} = E <- Endpoints,
        length(filter_tokens(Tokens, AuthType)) > 0].

filter_tokens(Tokens, AuthType) ->
    [I || I = #{token_type := T} <- Tokens, T =:= AuthType].

auth_type(anonymous) -> anonymous;
auth_type({user_name, _, _}) -> user_name.

unpack_browse_result(Space, BrowseSpec, BrowseResult) ->
    unpack_browse_result(Space, BrowseSpec, BrowseResult, []).

unpack_browse_result(_Space, [], [], Acc) ->
    lists:reverse(Acc);
unpack_browse_result(Space, [{NodeId, _} | Spec],
                     [#opcua_error{} = Error | Results], Acc) ->
    Error2 = Error#opcua_error{node_id = NodeId},
    unpack_browse_result(Space, Spec, Results, [Error2 | Acc]);
unpack_browse_result(Space, [{NodeId, _} | Spec], [BrowseResult | Results], Acc) ->
    BrowseResult2 = [unpack_browse_result(NodeId, R) || R <- BrowseResult],
    unpack_browse_result(Space, Spec, Results, [BrowseResult2 | Acc]).

unpack_browse_result(SourceId, #{is_forward := true,
                                 reference_type_id := TypeId,
                                 node_id := ?XID(TargetId)} = Result) ->
    Ref = #opcua_reference{type_id = TypeId,
                           source_id = SourceId,
                           target_id = TargetId},
    Result#{reference => Ref};
unpack_browse_result(TargetId, #{is_forward := false,
                                 reference_type_id := TypeId,
                                 node_id := ?XID(SourceId)} = Result) ->
    Ref = #opcua_reference{type_id = TypeId,
                           source_id = SourceId,
                           target_id = TargetId},
    Result#{reference => Ref}.

unpack_read_result(Space, ReadSpec, ReadResult) ->
    unpack_read_result(Space, ReadSpec, ReadResult, #{}, []).

unpack_read_result(_Space, [], [], _, Acc) ->
    lists:reverse(Acc);
unpack_read_result(Space, [{_NodeId, []} | MoreNodes], ReadResult, Map, Acc) ->
    unpack_read_result(Space, MoreNodes, ReadResult, #{}, [Map | Acc]);
unpack_read_result(Space, [{NodeId, [{AttribSpec, _} | MoreAttribs]} | MoreNodes],
                   [#opcua_error{} = Error | MoreResults], Map, Acc) ->
    % In case of error, we add the node identifier to simplify caller's life
    ResultKey = result_key(AttribSpec),
    unpack_read_result(Space, [{NodeId, MoreAttribs} | MoreNodes], MoreResults,
                       Map#{ResultKey => Error#opcua_error{node_id = NodeId}}, Acc);
unpack_read_result(Space, [{NodeId, [{AttribSpec, _} | MoreAttribs]} | MoreNodes],
                   [Result | MoreResults], Map, Acc) ->
    ResultKey = result_key(AttribSpec),
    AttribType = opcua_nodeset:attribute_type(ResultKey),
    UnpackedResult = unpack_attribute_value(Space, ResultKey, AttribType, Result),
    unpack_read_result(Space, [{NodeId, MoreAttribs} | MoreNodes], MoreResults,
                       Map#{ResultKey => UnpackedResult}, Acc).

unpack_attribute_value(_Space, _, variant, undefined) -> undefined;
unpack_attribute_value(_Space, _, variant, #opcua_variant{} = Value) -> Value;
unpack_attribute_value(_Space, _, Type, #opcua_variant{type = Type, value = Value}) -> Value;
unpack_attribute_value(_Space, _, byte_string, #opcua_variant{type = byte, value = Byte}) ->
    <<Byte:8>>;
unpack_attribute_value(Space, _, #opcua_node_id{} = Type, #opcua_variant{value = Value}) ->
    opcua_codec:unpack_type(Space, Type, Value);
unpack_attribute_value(_Space, Key, Type, Value) ->
    ?LOG_ERROR("Unexpected attribute ~s value with expected type ~p: ~p",
               [Key, Type, Value]),
    %TODO: Should we just crash if we receive unexpected attribute data ?
    Value.

unpack_write_result(WriteSpec, WriteResult) ->
    unpack_write_result(WriteSpec, WriteResult, #{}, []).

unpack_write_result([], [], _, Acc) ->
    lists:reverse(Acc);
unpack_write_result([{_NodeId, []} | MoreNodes], WriteResult, Map, Acc) ->
    unpack_write_result(MoreNodes, WriteResult, #{}, [Map | Acc]);
unpack_write_result([{NodeId, [{AttribSpec, _, _} | MoreAttribs]} | MoreNodes],
                   [#opcua_error{} = Error | MoreResults], Map, Acc) ->
    % In case of error, we add the node identifier to simplify caller's life
    ResultKey = result_key(AttribSpec),
    unpack_write_result([{NodeId, MoreAttribs} | MoreNodes], MoreResults,
                       Map#{ResultKey => Error#opcua_error{node_id = NodeId}}, Acc);
unpack_write_result([{NodeId, [{AttribSpec, _, _} | MoreAttribs]} | MoreNodes],
                   [Result | MoreResults], Map, Acc) ->
    ResultKey = result_key(AttribSpec),
    unpack_write_result([{NodeId, MoreAttribs} | MoreNodes], MoreResults,
                       Map#{ResultKey => Result}, Acc).


%== Protocol Module Abstraction Functions ======================================

proto_produce(#data{conn = Conn, proto = Proto} = Data) ->
    case opcua_client_uacp:produce(Conn, Proto) of
        {ok, Conn2, Proto2} ->
            {ok, Data#data{conn = Conn2, proto = Proto2}};
        {ok, Output, Conn2, Proto2} ->
            {ok, Output, Data#data{conn = Conn2, proto = Proto2}};
        {error, Reason, Proto2} ->
            {error, Reason, Data#data{proto = Proto2}}
    end.

proto_handle_data(#data{conn = Conn, proto = Proto} = Data, Input) ->
    case opcua_client_uacp:handle_data(Input, Conn, Proto) of
        {deferred, Action, Cont, Conn2, Proto2} ->
            {deferred, Action, Cont, Data#data{conn = Conn2, proto = Proto2}};
        {ok, Responses, Conn2, Proto2} ->
            {ok, Responses, Data#data{conn = Conn2, proto = Proto2}};
        {error, Reason, Proto2} ->
            {error, Reason, Data#data{proto = Proto2}}
    end.

proto_continue(#data{conn = Conn, proto = Proto} = Data, Cont) ->
    case opcua_client_uacp:continue(Cont, Conn, Proto) of
        {deferred, Action, Cont2, Conn2, Proto2} ->
            {deferred, Action, Cont2, Data#data{conn = Conn2, proto = Proto2}};
        {ok, Responses, Conn2, Proto2} ->
            {ok, Responses, Data#data{conn = Conn2, proto = Proto2}};
        {error, Reason, Proto2} ->
            {error, Reason, Data#data{proto = Proto2}}
    end.

proto_abort(#data{conn = Conn, proto = Proto} = Data, Cont, Reason) ->
    case opcua_client_uacp:abort(Reason, Cont, Conn, Proto) of
        {ok, Responses, Conn2, Proto2} ->
            {ok, Responses, Data#data{conn = Conn2, proto = Proto2}};
        {error, Reason, Proto2} ->
            {error, Reason, Data#data{proto = Proto2}}
    end.

proto_handshake(#data{conn = Conn, proto = Proto} = Data) ->
    case opcua_client_uacp:handshake(Conn, Proto) of
        % No error use-case yet, diabling to make dialyzer happy
        % {error, Reason, Proto2} ->
        %     {error, Reason, Data#data{proto = Proto2}};
        {ok, Conn2, Proto2} ->
            {ok, Data#data{conn = Conn2, proto = Proto2}}
    end.

proto_browse(#data{conn = Conn, proto = Proto} = Data, NodeId, Opts) ->
    case opcua_client_uacp:browse(NodeId, Opts, Conn, Proto) of
        % No error use-case yet, diabling to make dialyzer happy
        % {error, Reason, Proto2} ->
        %     {error, Reason, Data#data{proto = Proto2}};
        {async, Handle, Conn2, Proto2} ->
            {async, Handle, Data#data{conn = Conn2, proto = Proto2}}
    end.

proto_read(#data{conn = Conn, proto = Proto} = Data, ReadSpecs, Opts) ->
    case opcua_client_uacp:read(ReadSpecs, Opts, Conn, Proto) of
        % No error use-case yet, diabling to make dialyzer happy
        % {error, Reason, Proto2} ->
        %     {error, Reason, Data#data{proto = Proto2}};
        {async, Handle, Conn2, Proto2} ->
            {async, Handle, Data#data{conn = Conn2, proto = Proto2}}
    end.

proto_write(#data{conn = Conn, proto = Proto} = Data, WriteSpec, Opts) ->
    case opcua_client_uacp:write(WriteSpec, Opts, Conn, Proto) of
        % No error use-case yet, diabling to make dialyzer happy
        % {error, Reason, Proto2} ->
        %     {error, Reason, Data#data{proto = Proto2}};
        {async, Handle, Conn2, Proto2} ->
            {async, Handle, Data#data{conn = Conn2, proto = Proto2}}
    end.

proto_close(#data{conn = Conn, proto = Proto} = Data) ->
    case opcua_client_uacp:close(Conn, Proto) of
        % No error use-case yet, diabling to make dialyzer happy
        % {error, Reason, Proto2} ->
        %     {error, Reason, Data#data{proto = Proto2}};
        {ok, Conn2, Proto2} ->
            {ok, Data#data{conn = Conn2, proto = Proto2}}
    end.

proto_terminate(#data{conn = Conn, proto = Proto}, Reason) ->
    opcua_client_uacp:terminate(Reason, Conn, Proto).


%== Connection Managment =======================================================

conn_init(#data{opts = CliOpts, socket = undefined} = Data, ProtoMode, EndpointUrl) ->
    #{keychain := ParentKeychain,
      identity := Identity,
      connect_timeout := Timeout
    } = CliOpts,
    {SecurityMode, SecurityPolicy} = case ProtoMode of
        lookup_endpoint ->
            #{mode := M,
              policy := P} = maps:get(endpoint_lookup_security, CliOpts),
            {M,P};
        open_session ->
            M = maps:get(mode, CliOpts, none),
            P = maps:get(policy, CliOpts, none),
            {M,P}
    end,
    #opcua_endpoint_url{host = Host, port = Port, url = Url} = EndpointUrl,
    ?LOG_DEBUG("Connecting to ~s", [Url]),
    Opts = [binary, {active, false}, {packet, raw}],
    case gen_tcp:connect(Host, Port, Opts, Timeout) of
        {error, _Reason} = Error -> Error;
        {ok, Socket} ->
            PeerNameRes = inet:peername(Socket),
            SockNameRes = inet:sockname(Socket),
            case {PeerNameRes, SockNameRes} of
                {{error, _Reason} = Error, _} -> Error;
                {_, {error, _Reason} = Error} -> Error;
                {{ok, PeerName}, {ok, SockName}} ->
                    {ok, Keychain} = opcua_keychain_ets:new(ParentKeychain),
                    ClientSpace = opcua_space_backend:new([opcua_nodeset]),
                    Conn = opcua_connection:set_security_policy(
                                opcua_connection:set_security_mode(
                                    opcua_connection:new(ClientSpace, Keychain,
                                                         Identity, EndpointUrl,
                                                         PeerName, SockName),
                                    SecurityMode),
                                SecurityPolicy),
                    Data2 = Data#data{socket = Socket, conn = Conn},
                    case conn_lock_peer(Data2) of
                        {error, _Reason} = Error -> Error;
                        {ok, Data3} ->
                            case conn_activate(Data3) of
                                {error, _Reason} = Error -> Error;
                                ok -> {ok, Data3}
                            end
                    end
            end
    end.

conn_lock_peer(#data{opts = #{server_identity := undefined}} = Data) ->
    {ok, Data};
conn_lock_peer(#data{opts = #{server_identity := Ident}, conn = Conn} = Data) ->
    % We don't have a server certificate, but an expected server identity,
    % the certificate MUST already be in the keychain.
    case opcua_connection:lock_peer(Conn, Ident) of
        {error, _Reason} = Error -> Error;
        {ok, Conn2} -> {ok, Data#data{conn = Conn2}}
    end.

conn_activate(#data{socket = Socket}) ->
    inet:setopts(Socket, [{active, 5}]).

conn_send(#data{socket = Socket}, Packet) ->
    ?DUMP("Sending Data: ~p", [Packet]),
    gen_tcp:send(Socket, Packet).

conn_close(#data{socket = undefined}) -> ok;
conn_close(#data{socket = Socket} = Data) ->
    ?LOG_DEBUG("Closing connection"),
    gen_tcp:close(Socket),
    Data#data{socket = undefined}.


%== Timeouts ===================================================================

enter_timeouts({connecting, 0, _, _} = State, Data) ->
    [{state_timeout, 0, retry} | event_timeouts(State, Data)];
enter_timeouts({connecting, 1, _, _} = State, Data) ->
    [{state_timeout, 500, retry} | event_timeouts(State, Data)];
enter_timeouts({connecting, 2, _, _} = State, Data) ->
    [{state_timeout, 1000, retry} | event_timeouts(State, Data)];
enter_timeouts({connecting, 3, _, _} = State, Data) ->
    [{state_timeout, 3000, retry} | event_timeouts(State, Data)];
enter_timeouts({connecting, _, _, _} = State, Data) ->
    [{state_timeout, 10000, retry} | event_timeouts(State, Data)];
enter_timeouts(handshaking = State, Data) ->
    [{state_timeout, 3000, abort} | event_timeouts(State, Data)];
enter_timeouts({reconnecting, _} = State, Data) ->
    [{state_timeout, 3000, abort} | event_timeouts(State, Data)];
enter_timeouts(closing = State, Data) ->
    [{state_timeout, 4000, abort} | event_timeouts(State, Data)];
enter_timeouts(State, Data) ->
    event_timeouts(State, Data).

event_timeouts({reconnecting, _}, Data) ->
    event_timeouts(reconnecting, Data);
event_timeouts(State, Data)
  when State =:= handshaking; State =:= connected;
       State =:= reconnecting; State =:= closing ->
    #data{conn = Conn, proto = Proto} = Data,
    case opcua_client_uacp:can_produce(Conn, Proto) of
        true -> [{timeout, 0, produce}];
        false -> []
    end;
event_timeouts(_State, _Data) ->
    [].
