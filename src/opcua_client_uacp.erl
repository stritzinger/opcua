-module(opcua_client_uacp).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([init/2]).
-export([handshake/2]).
-export([browse/4]).
-export([read/4]).
-export([write/4]).
-export([close/2]).
-export([can_produce/2]).
-export([produce/2]).
-export([handle_data/3]).
-export([continue/3]).
-export([abort/4]).
-export([terminate/3]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    mode                            :: lookup_endpoint | open_session,
    introspection                   :: boolean(),
    server_ver                      :: undefined | pos_integer(),
    channel                         :: term(),
    proto                           :: term(),
    sess                            :: term(),
    endpoint_selector               :: function()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Mode, Opts) ->
    AllOpts = maps:merge(default_options(), Opts),
    case opcua_uacp:init(client, opcua_client_channel) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, _Reason} = Error -> Error;
        {ok, Proto} ->
            State = #state{
                mode = Mode,
                introspection = maps:get(introspection, AllOpts),
                endpoint_selector = maps:get(endpoint_selector, AllOpts),
                proto = Proto
            },
            {ok, State}
    end.

handshake(Conn, #state{proto = Proto} = State) ->
    Payload = #{
        ver => opcua_uacp:version(Proto),
        max_res_chunk_size => opcua_uacp:limit(max_res_chunk_size, Proto),
        max_req_chunk_size => opcua_uacp:limit(max_req_chunk_size, Proto),
        max_msg_size => opcua_uacp:limit(max_msg_size, Proto),
        max_chunk_count => opcua_uacp:limit(max_chunk_count, Proto),
        endpoint_url => opcua_connection:endpoint_url(Conn)
    },
    Request = opcua_connection:request(Conn, hello, undefined, undefined, Payload),
    proto_consume(State, Conn, Request).

browse(BrowseSpec, Opts, Conn, State) ->
    maybe_consume(async, session_browse(State, Conn, BrowseSpec, Opts)).

read(ReadSpecs, Opts, Conn, State) ->
    maybe_consume(async, session_read(State, Conn, ReadSpecs, Opts)).

write(WriteSpec, Opts, Conn, State) ->
    maybe_consume(async, session_write(State, Conn, WriteSpec, Opts)).

close(Conn, #state{sess = undefined} = State) ->
    maybe_consume(ok, channel_close(State, Conn));
close(Conn, State) ->
    maybe_consume(ok, session_close(State, Conn)).

can_produce(Conn, State) ->
    proto_can_produce(State, Conn).

produce(Conn, State) ->
    proto_produce(State, Conn).

handle_data(Data, Conn, State) ->
    case proto_handle_data(State, Conn, Data) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Msgs, Issues, Conn2, State2} ->
            case handle_issues(State2, Conn2, Msgs, Issues) of
                {error, _Reason, _State3} = Error -> Error;
                {deferred, _Action, _Cont, _Conn3, _State3} = Result -> Result;
                {ok, Results, Msgs2, Conn3, State3} ->
                    handle_responses(State3, Conn3, Msgs2, Results)
            end
    end.

continue({datatype_cached, PartialMsg, ProtoCont}, Conn, State) ->
    case handle_proto_continue(ProtoCont, Conn, State) of
        {ok, _Results, _Conn2, _State2} = Result -> Result;
        {deferred, _Action, _Cont, _Conn2, _State2} = Result -> Result;
        {error, Reason, State2} ->
            ?LOG_ERROR("Failed to resolve missing datatype information: ~p",
                       [Reason]),
            session_abort_response(State2, Conn, PartialMsg, missing_typedata)
    end.

abort(Reason, {datatype_cached, PartialMsg, ProtoCont}, Conn, State) ->
    ?LOG_ERROR("Failed to resolve missing datatype information", []),
    {ok, AbortResults, Conn3, State3} =
        case handle_proto_abort(Reason, ProtoCont, Conn, State) of
            {ok, _Results, _Conn2, _State2} = Result1 -> Result1;
            {error, Reason, State2} ->
                ?LOG_ERROR("Error while aborting action: ~p", [Reason]),
                {ok, [], Conn, State2}
        end,
    case session_abort_response(State3, Conn3, PartialMsg, missing_typedata) of
        {error, _Reason2, _State4} = Result2 ->
            % What about the results we got from aborting ?
            % Does it even make sense for aborting to return a result ?!?
            Result2;
        {ok, Results, Conn4, State4} ->
            {ok, AbortResults ++ Results , Conn4, State4}
    end.

terminate(Reason, Conn, State) ->
    channel_terminate(State, Conn, Reason).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

default_options() ->
    #{mode => none, policy => none, auth => anonymous, introspection => true}.

handle_proto_continue(ProtoCont, Conn, State) ->
    case proto_continue(State, Conn, ProtoCont) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Msgs, Issues, Conn2, State2} ->
            case handle_issues(State2, Conn2, Msgs, Issues) of
                {error, _Reason, _State3} = Error -> Error;
                {deferred, _Action, _Cont, _Conn3, _State3} = Result -> Result;
                {ok, Results, Msgs2, Conn3, State3} ->
                    handle_responses(State3, Conn3, Msgs2, Results)
            end
    end.

handle_proto_abort(Reason, ProtoCont, Conn, State) ->
    case proto_abort(State, Conn, ProtoCont, Reason) of
        % Disabled to make dialyzer happy for now
        % {error, _Reason, _State2} = Error -> Error;
        {ok, Msgs, Issues, Conn2, State2} ->
            case handle_issues(State2, Conn2, Msgs, Issues) of
                {error, _Reason, _State3} = Error -> Error;
                {deferred, _Action, _Cont, _Conn3, _State3} = Result -> Result;
                {ok, Results, Msgs2, Conn3, State3} ->
                    handle_responses(State3, Conn3, Msgs2, Results)
            end
    end.

handle_issues(State, Conn, Msgs, Issues) ->
    handle_issues(State, Conn, [Msgs], Issues, []).

handle_issues(State, Conn, Msgs, [], Results) ->
    {ok, lists:append(Results), lists:append(Msgs), Conn, State};
handle_issues(State, Conn, Msgs, [Issue | Rest], Results) ->
    case handle_issue(State, Conn, Issue) of
        {error, _Reason, _State} = Error -> Error;
        {deferred, _Action, _Cont, _Conn3, _State3} = Result -> Result;
        % Disabled to make dialyzer happy for now
        % {ok, Conn2, State2} ->
        %     handle_issues(State2, Conn2, Msgs, Rest, Results);
        {ok, NewResults, Conn2, State2} ->
            handle_issues(State2, Conn2, Msgs, Rest, [NewResults | Results])
        % {ok, NewResults, NewMsgs, NewIssues, Conn2, State2} ->
        %     handle_issues(State2, Conn2, [NewMsgs | Msgs], NewIssues ++ Rest,
        %                   [NewResults | Results])
    end.

handle_issue(#state{sess = undefined} = State, _Conn,
             {missing_typedata, _PartialMsg, TypeIds, _ProtoCont}) ->
    ?LOG_WARNING("Failed to handle response, datatype information not found: ~s",
                 [opcua_node:format(TypeIds)]),
    {error, bad_decoding_error, State};
handle_issue(#state{introspection = false} = State, Conn,
             {missing_typedata, PartialMsg, TypeIds, _ProtoCont}) ->
    ?LOG_WARNING("Failed to handle response, type information not found: ~s",
                 [opcua_node:format(TypeIds)]),
    session_abort_response(State, Conn, PartialMsg, missing_typedata);
handle_issue(#state{introspection = true} = State, Conn,
             {missing_typedata, PartialMsg, TypeIds, ProtoCont}) ->
    ?LOG_WARNING("Failed to handle response, trying introspection to retrieve type information: ~s",
                 [opcua_node:format(TypeIds)]),
    Cont = {datatype_cached, PartialMsg, ProtoCont},
    {deferred, {cache_datatype, TypeIds}, Cont, Conn, State}.

handle_responses(State, Conn, Msgs, Results) ->
    handle_responses_loop(State, Conn, Msgs, [Results]).

handle_responses_loop(State, Conn, [], Acc) ->
    {ok, lists:append(Acc), Conn, State};
handle_responses_loop(State, Conn, [Msg | Rest], Acc) ->
    ?DUMP("Receiving message: ~p", [Msg]),
    case handle_response(State, Conn, Msg) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, Conn2, State2} ->
            handle_responses_loop(State2, Conn2, Rest, Acc);
        {ok, Results, Requests, Conn2, State2} ->
            case proto_consume(State2, Conn2, Requests) of
                % No error use-case yet, disabled to make dialyzer happy
                % {error, _Reason, _State2} = Error -> Error;
                {ok, Conn3, State3} ->
                    handle_responses_loop(State3, Conn3, Rest, [Results | Acc])
            end
    end.

handle_response(State, Conn, #uacp_message{type = acknowledge, payload = Payload}) ->
    ClientVersion = proto_version(State),
    #{
        ver := ServerVersion,
        max_res_chunk_size := MaxResChunkSize,
        max_req_chunk_size := MaxReqChunkSize,
        max_msg_size := MaxMsgSize,
        max_chunk_count := MaxChunkCount
    } = Payload,
    ?LOG_INFO("Negociated protocol configuration: client_ver=~w, server_ver=~w, "
              "max_res_chunk_size=~w, max_req_chunk_size=~w, max_msg_size=~w, "
              "max_chunk_count=~w" ,
              [ClientVersion, ServerVersion, MaxResChunkSize, MaxReqChunkSize,
               MaxMsgSize, MaxChunkCount]),
    State2 = proto_limit(State, max_res_chunk_size, MaxResChunkSize),
    State3 = proto_limit(State2, max_req_chunk_size, MaxReqChunkSize),
    State4 = proto_limit(State3, max_msg_size, MaxMsgSize),
    State5 = proto_limit(State4, max_chunk_count, MaxChunkCount),
    State6 = State5#state{server_ver = ServerVersion},
    no_result(channel_open(State6, Conn));
handle_response(#state{channel = undefined} = State, Conn, _Response) ->
    %% Called when closed and receiving a message, not sure what we should be doing
    {ok, Conn, State};
handle_response(#state{mode = lookup_endpoint} = State, Conn, Response) ->
    case channel_handle_response(State, Conn, Response) of
        {error, _Reason, _State2} = Error -> Error;
        {open, Conn2, State2} ->
            no_result(channel_get_endpoints(State2, Conn2));
        {endpoints, Endpoints, Conn2, State2} ->
            case select_endpoint(State2, Conn2, Endpoints) of
                {error, Reason} ->
                    {error, Reason, State2};
                {ok, Conn3, PeerInfo} ->
                    opcua_connection:notify(Conn3, {reconnect, PeerInfo}),
                    {ok, Conn3, State2}
            end;
        {closed, Conn2, State2} ->
            opcua_connection:notify(Conn, closed),
            {ok, Conn2, State2#state{channel = undefined}}
    end;
handle_response(#state{mode = open_session} = State, Conn, Response) ->
    case channel_handle_response(State, Conn, Response) of
        {error, _Reason, _State2} = Error -> Error;
        {open, Conn2, #state{endpoint_selector = Selector} = State2} ->
            no_result(session_create(State2, Conn2, Selector));
        {closed, Conn2, State2} ->
            opcua_connection:notify(Conn2, closed),
            {ok, Conn2, State2#state{channel = undefined}};
        {forward, Response2, Conn2, State2} ->
            case session_handle_response(State2, Conn2, Response2) of
                {error, _Reason, _State3} = Error -> Error;
                {ok, _Results, _Requests, _Conn3, _State3} = Result -> Result;
                {closed, Conn3, State3} ->
                    no_result(channel_close(State3#state{sess = undefined}, Conn3))
            end
    end.

select_endpoint(#state{endpoint_selector = Selector}, Conn, Endpoints) ->
    case Selector(Conn, Endpoints) of
        {error, not_found} -> {error, no_compatible_server_endpoint};
        {ok, Conn2, PeerIdent,
            #{endpoint_url := Url} = _Endpoint, _AuthPolicyId, _AuthSpec} ->
            {ok, Conn2, {PeerIdent, Url}}
    end.


%== Utility Functions ==========================================================

% No error use-case yet, disabled to make dialyzer happy
% maybe_consume(_Tag, {error, _Reason, _State} = Error) -> Error;
maybe_consume(Tag, {ok, Requests, Conn, State}) ->
    case proto_consume(State, Conn, Requests) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, _Reason, _State2} = Error -> Error;
        {ok, Conn2, State2} -> {Tag, Conn2, State2}
    end;
maybe_consume(Tag, {ok, Result, Requests, Conn, State}) ->
    case proto_consume(State, Conn, Requests) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, _Reason, _State2} = Error -> Error;
        {ok, Conn2, State2} -> {Tag, Result, Conn2, State2}
    end.

no_result({error, _Reason, _State} = Error) -> Error;
no_result({ok, Req, Conn, State}) -> {ok, [], Req, Conn, State}.


%== Session Module Abstraction Functions =======================================

session_create(#state{channel = Channel, sess = undefined} = State, Conn, EndpointSelector) ->
    Sess = opcua_client_session:new(EndpointSelector),
    case opcua_client_session:create(Conn, Channel, Sess) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, Reason} -> {error, Reason, State};
        {ok, Req, Conn2, Channel2, Sess2} ->
            State2 = State#state{channel = Channel2, sess = Sess2},
            {ok, Req, Conn2, State2}
    end.

session_browse(#state{channel = Channel, sess = Sess} = State, Conn, BrowseSpec, Opts) ->
    case opcua_client_session:browse(BrowseSpec, Opts, Conn, Channel, Sess) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, Reason} -> {error, Reason, State};
        {ok, Handle, Requests, Conn2, Channel2, Sess2} ->
            State2 = State#state{channel = Channel2, sess = Sess2},
            {ok, Handle, Requests, Conn2, State2}
    end.

session_read(#state{channel = Channel, sess = Sess} = State, Conn, ReadSpecs, Opts) ->
    case opcua_client_session:read(ReadSpecs, Opts, Conn, Channel, Sess) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, Reason} -> {error, Reason, State};
        {ok, Handle, Requests, Conn2, Channel2, Sess2} ->
            State2 = State#state{channel = Channel2, sess = Sess2},
            {ok, Handle, Requests, Conn2, State2}
    end.

session_write(#state{channel = Channel, sess = Sess} = State, Conn, WriteSpec, Opts) ->
    case opcua_client_session:write(WriteSpec, Opts, Conn,
                                    Channel, Sess) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, Reason} -> {error, Reason, State};
        {ok, Handle, Requests, Conn2, Channel2, Sess2} ->
            State2 = State#state{channel = Channel2, sess = Sess2},
            {ok, Handle, Requests, Conn2, State2}
    end.

session_close(#state{channel = Channel, sess = Sess} = State, Conn) ->
    case opcua_client_session:close(Conn, Channel, Sess) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, Reason} -> {error, Reason, State};
        {ok, Requests, Conn2, Channel2, Sess2} ->
            State2 = State#state{channel = Channel2, sess = Sess2},
            {ok, Requests, Conn2, State2}
    end.

session_handle_response(#state{channel = Channel, sess = Sess} = State, Conn, Response) ->
    case opcua_client_session:handle_response(Response, Conn, Channel, Sess) of
        {error, Reason} -> {error, Reason, State};
        {closed, Conn2, Channel2, Sess2} ->
            {closed, Conn2, State#state{channel = Channel2, sess = Sess2}};
        {ok, Results, Requests, Conn2, Channel2, Sess2} ->
            State2 = State#state{channel = Channel2, sess = Sess2},
            {ok, Results, Requests, Conn2, State2}
    end.

session_abort_response(#state{channel = Channel, sess = Sess} = State, Conn, Msg, Reason) ->
    case opcua_client_session:abort_response(Msg, Reason, Conn, Channel, Sess) of
        {error, Reason2} -> {error, Reason2, State};
        {ok, Results, Conn2, Channel2, Sess2} ->
            State2 = State#state{channel = Channel2, sess = Sess2},
            {ok, Results, Conn2, State2}
    end.

%== Channel Module Abstraction Functions =======================================

channel_open(#state{channel = undefined} = State, Conn) ->
    case opcua_client_channel:init(Conn) of
        {error, Reason} ->
            ?LOG_WARNING("Error: ~p", [Reason]),
            {error, Reason, State};
        {ok, Conn2, Channel} ->
            case opcua_client_channel:open(Conn2, Channel) of
                % No error use-case yet, disabled to make dialyzer happy
                % {error, _Reason} = Error -> Error;
                {ok, Req, Conn3, Channel2} ->
                    {ok, Req, Conn3, State#state{channel = Channel2}}
            end
    end.

channel_get_endpoints(#state{channel = Channel} = State, Conn) ->
    case opcua_client_channel:get_endpoints(Conn, Channel) of
        {ok, Req, Conn2, Channel2} ->
            {ok, Req, Conn2, State#state{channel = Channel2}}
    end.

channel_close(#state{channel = Channel} = State, Conn) ->
    case opcua_client_channel:close(Conn, Channel) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, Reason} ->
        %     {error, Reason, State};
        {ok, Req, Conn2, Channel2} ->
            {ok, Req, Conn2, State#state{channel = Channel2}}
    end.

channel_terminate(#state{channel = Channel}, Conn, Reason) ->
    opcua_client_channel:terminate(Reason, Conn, Channel).

channel_handle_response(#state{channel = Channel} = State, Conn, Req) ->
    case opcua_client_channel:handle_response(Req, Conn, Channel) of
        {error, Reason} ->
            {error, Reason, State};
        {Tag, Conn2, Channel2} ->
            {Tag, Conn2, State#state{channel = Channel2}};
        {Tag, Resp, Conn2, Channel2} ->
            {Tag, Resp, Conn2, State#state{channel = Channel2}}
    end.


%== Protocol Module Abstraction Functions ======================================

proto_version(#state{proto = Proto}) ->
    opcua_uacp:version(Proto).

proto_limit(#state{proto = Proto} = State, Name, Value) ->
    Proto2 = opcua_uacp:limit(Name, Value, Proto),
    State#state{proto = Proto2}.

proto_can_produce(#state{proto = Proto}, Conn) ->
    opcua_uacp:can_produce(Conn, Proto).

proto_produce(#state{channel = Channel, proto = Proto} = State, Conn) ->
    case opcua_uacp:produce(Conn, Channel, Proto) of
        {error, Reason, Channel2, Proto2} ->
            {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Conn2, Channel2, Proto2} ->
            {ok, Conn2, State#state{channel = Channel2, proto = Proto2}};
        {ok, Output, Conn2, Channel2, Proto2} ->
            {ok, Output, Conn2, State#state{channel = Channel2, proto = Proto2}}
    end.

proto_handle_data(#state{channel = Channel, proto = Proto} = State, Conn, Data) ->
    case opcua_uacp:handle_data(Data, Conn, Channel, Proto) of
        {error, Reason, Channel2, Proto2} ->
            {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Messages, Issues, Conn2, Channel2, Proto2} ->
            State2 = State#state{channel = Channel2, proto = Proto2},
            {ok, Messages, Issues, Conn2, State2}
    end.

proto_consume(#state{channel = Channel, proto = Proto} = State, Conn, Msgs) ->
    case proto_consume_loop(Proto, Channel, Conn, Msgs) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, Reason, Channel2, Proto2} ->
        %     {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Conn2, Channel2, Proto2} ->
            {ok, Conn2, State#state{channel = Channel2, proto = Proto2}}
    end.

proto_consume_loop(Proto, Channel, Conn, []) -> {ok, Conn, Channel, Proto};
proto_consume_loop(Proto, Channel, Conn, [Msg | Rest]) ->
    case proto_consume_loop(Proto, Channel, Conn, Msg) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, _Reason, _Channel2, _Proto2} = Result -> Result;
        {ok, Conn2, Channel2, Proto2} ->
            proto_consume_loop(Proto2, Channel2, Conn2, Rest)
    end;
proto_consume_loop(Proto, Channel, Conn, Msg) ->
    ?DUMP("Sending message: ~p", [Msg]),
    opcua_uacp:consume(Msg, Conn, Channel, Proto).

proto_continue(#state{channel = Channel, proto = Proto} = State, Conn, ContData) ->
    case opcua_uacp:continue(ContData, Conn, Channel, Proto) of
        {error, Reason, Channel2, Proto2} ->
            {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Messages, Issues, Conn2, Channel2, Proto2} ->
            State2 = State#state{channel = Channel2, proto = Proto2},
            {ok, Messages, Issues, Conn2, State2}
    end.

proto_abort(#state{channel = Channel, proto = Proto} = State, Conn, ContData, Reason) ->
    case opcua_uacp:abort(Reason, ContData, Conn, Channel, Proto) of
        % No error use-case yet, disabled to make dialyzer happy
        % {error, Reason, Channel2, Proto2} ->
        %     {error, Reason, State#state{channel = Channel2, proto = Proto2}};
        {ok, Messages, Issues, Conn2, Channel2, Proto2} ->
            State2 = State#state{channel = Channel2, proto = Proto2},
            {ok, Messages, Issues, Conn2, State2}
    end.
