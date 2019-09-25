-module(opcua_server_session).

-behaviour(gen_statem).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([start_link/2]).
-export([handle_request/3]).

%% Behaviour gen_statem callback functions
-export([init/1]).
-export([callback_mode/0]).
-export([terminate/3]).

%% Behaviour gen_statem state functions
-export([started/3]).
-export([created/3]).
-export([bound/3]).


%%% MACRO %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(EPSILON, 1.0e-5).
-define(MAX_INT32, 4294967296).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(data, {
    session_id :: opcua:node_id(),
    auth_token :: opcua:node_id(),
    session_name :: undefined | binary(),
    client_certificate :: undefined | binary(),
    client_description :: undefined | binary(),
    client_nonce :: undefined | binary(),
    server_nonce :: undefined | binary(),
    max_response_message_size :: undefined | non_neg_integer(),
    requested_session_timeout :: undefined | non_neg_integer(),
    ident :: undefined | binary(),
    local_ids :: undefined | [binary()],
    conn :: undefined | opcua_protocol:connection(),
    mon_ref :: undefined | reference()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(SessId, AuthToken) ->
    gen_statem:start_link(?MODULE, [SessId, AuthToken], []).

handle_request(Conn, #uacp_message{} = Req, SessPid) ->
    gen_statem:call(SessPid, {request, Conn, Req}).


%%% BEHAVIOUR gen_statem CALLBACK FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([SessId, AuthToken]) ->
    ?LOG_DEBUG("OPCUA session ~w process starting", [SessId]),
    Data = #data{session_id = SessId, auth_token = AuthToken},
    {ok, started, Data, [{state_timeout, 1000, terminate}]}.

callback_mode() -> state_functions.

terminate(Reason, _State, _Data) ->
    ?LOG_DEBUG("OPCUA session process terminating: ~p", [Reason]),
    ok.

%%% BEHAVIOUR gen_statem STATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

started(state_timeout, terminate, _Data) -> {stop, timeout};
started({call, From}, {request, Conn, Req}, Data) ->
    dispatch_request(Data, started, From, Conn, Req, [], #{
        459 => {next_state, created, fun session_create_command/3}
    }, bad_request_not_allowed, [{state_timeout, 10000, terminate}]).

created(state_timeout, terminate, _Data) -> {stop, timeout};
created({call, From}, {request, Conn, Req}, Data) ->
    dispatch_request(Data, created, From, Conn, Req, [fun validate_auth/2], #{
        465 => {next_state, bound, fun session_activate_command/3}
    }, bad_session_not_activated, []).

bound(info, {'DOWN', MonRef, process, _Pid, _Info},
      #data{mon_ref = MonRef} = Data) ->
    ?LOG_DEBUG("Session disconnected"),
    {next_state, created, session_deactivate(Data),
     [{state_timeout, 600000, terminate}]};
bound({call, From}, {request, Conn, Req}, Data) ->
    dispatch_request(Data, bound, From, Conn, Req, [fun validate_auth/2], #{
        471 => {stop, normal, fun session_close_command/3},
        525 => {keep_state, fun view_browse_command/3},
        629 => {keep_state, fun attribute_read_command/3}
    }, bad_request_not_allowed, []).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dispatch_request(Data, State, From, Conn, Req, Validators, HandlerMap, Reason, Extra) ->
    case validate_request(Data, Req, Validators) of
        {error, _Reason} = Error ->
            {keep_state, Data, [{reply, From, Error} | Extra]};
        ok ->
            #uacp_message{node_id = NodeSpec} = Req,
            #opcua_node_id{value = Num} = opcua_database:lookup_id(NodeSpec),
            case maps:find(Num, HandlerMap) of
                error ->
                    ?LOG_ERROR("Unexpected request ~w in state ~w: ~p",
                               [Num, State, Req]),
                    {keep_state, Data, [{reply, From, {error, Reason}} | Extra]};
                {ok, Spec} ->
                    dispatch_request(Data, Spec, From, Conn, Req, Extra)
            end
    end.

dispatch_request(Data, {keep_state, Fun}, From, Conn, Req, Extra) ->
    try Fun(Data, Conn, Req) of
        {error, _Reason} = Error ->
            {keep_state, Data, [{reply, From, Error} | Extra]};
        {Result, Data2} ->
            {keep_state, Data2, [{reply, From, Result} | Extra]}
    catch
        Reason ->
            Error = {error, Reason},
            {keep_state, Data, [{reply, From, Error} | Extra]}
    end;
dispatch_request(Data, {next_state, NextState, Fun}, From, Conn, Req, Extra) ->
    try Fun(Data, Conn, Req) of
        {error, _Reason} = Error ->
            {keep_state, Data, [{reply, From, Error} | Extra]};
        {Result, Data2} ->
            {next_state, NextState, Data2,
             [{reply, From, Result} | Extra]}
    catch
        Reason ->
            Error = {error, Reason},
            {keep_state, Data, [{reply, From, Error} | Extra]}
    end;
dispatch_request(Data, {stop, Reason, Fun}, From, Conn, Req, Extra) ->
    try Fun(Data, Conn, Req) of
        {error, _Reason} = Error ->
            {keep_state, Data, [{reply, From, Error} | Extra]};
        {Result, Data2} ->
            {stop_and_reply, Reason, [{reply, From, Result}], Data2}
    catch
        Reason ->
            Error = {error, Reason},
            {keep_state, Data, [{reply, From, Error} | Extra]}
    end.


validate_request(_Data, _Req, []) -> ok;
validate_request(Data, Req, [Fun | Rest]) ->
    try Fun(Data, Req) of
        {error, _Reason} = Error -> Error;
        ok -> validate_request(Data, Req, Rest)
    catch
        Reason -> {error, Reason}
    end.

validate_auth(#data{auth_token = AuthToken},
              #uacp_message{payload = #{
                request_header := #{authentication_token := AuthToken}}}) ->
    ok;
validate_auth(_State, _Headers) ->
    {error, bad_session_id_invalid}.


%-- SESSION SERVICE SET --------------------------------------------------------

session_create_command(Data, Conn, #uacp_message{payload = Msg} = Req) ->
    %TODO: Probably check the request header...
    %TODO: We need some imformation about the security channel from the protocol
    %      like the transport uri, channel id, the policy URL...
    %      For now it is all hardcoded.
    #{
        client_certificate := ClientCertificate,
        client_description := ClientDescription,
        client_nonce := ClientNonce,
        endpoint_url := EndpointURL,
        max_response_message_size := MaxResponseMessageSize,
        requested_session_timeout := RequestedSessionTimeout,
        session_name := SessionName
    } = Msg,
    %TODO: Validate that the given endpoint match the connection one
    ServerNonce = opcua_util:nonce(),
    #data{
        session_id = SessId,
        auth_token = AuthToken
    } = Data,
    Data2 = Data#data{
        client_certificate = ClientCertificate,
        client_description = ClientDescription,
        client_nonce = ClientNonce,
        server_nonce = ServerNonce,
        max_response_message_size = MaxResponseMessageSize,
        requested_session_timeout = RequestedSessionTimeout,
        session_name = SessionName
    },
    Resp = opcua_connection:response(Conn, Req, 462, #{
        session_id => SessId,
        authentication_token => AuthToken,
        revised_session_timeout => RequestedSessionTimeout,
        server_nonce => ServerNonce,
        server_certificate => undefined,
        server_endpoints => [ %% Duplicated in opcua_server_discovery
            #{
                endpoint_url => EndpointURL,
                server => #{
                    application_uri => <<"urn:stritzinger:opcua:erlang:server">>,
                    product_uri => <<"urn:stritzinger.com:opcua:erlang:server">>,
                    application_name => <<"Stritzinger GmbH OPCUA Server">>,
                    application_type => server,
                    gateway_server_uri => undefined,
                    discovery_profile_uri => undefined,
                    discovery_urls => [
                        <<"opc.tcp://0.0.0.0:4840">>
                    ]
                },
                server_certificate => undefined,
                security_mode => none,
                security_policy_uri => <<"http://opcfoundation.org/UA/SecurityPolicy#None">>,
                user_identity_tokens => [
                    #{
                        policy_id => <<"anonymous">>,
                        token_type => anonymous,
                        issued_token_type => undefined,
                        issuer_endpoint_url => undefined,
                        security_policy_uri => undefined
                    }
                ],
                transport_profile_uri => <<"http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary">>,
                security_level => 0
            }
        ],
        server_software_certificates => [],
        server_signature => #{
            algorithm => <<"http://www.w3.org/2000/09/xmldsig#rsa-sha1">>,
            signature => undefined
        },
        max_request_message_size => 65536
    }),
    {{created, Resp}, Data2}.

session_activate_command(Data, Conn, #uacp_message{payload = Msg} = Req) ->
    #{
        locale_ids := LocalIds,
        user_identity_token := IdentTokenExtObj
    } = Msg,
    case check_identity(IdentTokenExtObj) of
        {error, _Reason} = Error -> Error;
        {ok, Ident} ->
            MonRef = opcua_connection:monitor(Conn),
            ServerNonce = opcua_util:nonce(),
            Data2 = Data#data{
                server_nonce = ServerNonce,
                ident = Ident,
                local_ids = LocalIds,
                conn = Conn,
                mon_ref = MonRef
            },
            Resp = opcua_connection:response(Conn, Req, 468, #{
                server_nonce => ServerNonce,
                results => [],
                diagnostic_infos => []
            }),
            {{bound, Resp}, Data2}
    end.

session_deactivate(Data) ->
    Data#data{conn = undefined, mon_ref = undefined}.

session_close_command(Data, Conn, #uacp_message{payload = _Msg} = Req) ->
    #data{mon_ref = MonRef} = Data,
    opcua_connection:demonitor(Conn, MonRef),
    Data2 = Data#data{conn = undefined, mon_ref = undefined},
    Resp = opcua_connection:response(Conn, Req, 474, #{}),
    {{reply, Resp}, Data2}.

check_identity(ExtObj) ->
    #opcua_extension_object{type_id = NodeSpec, body = Body} = ExtObj,
    case opcua_database:lookup_id(NodeSpec) of
        #opcua_node_id{value = 319} -> %% AnonymousIdentityToken
            #{policy_id := PolicyId} = Body,
            {ok, PolicyId};
        _ ->
            {error, bad_user_access_denied}
    end.


%-- ATTRIBUTE SERVICE SET ------------------------------------------------------

attribute_read_command(Data, Conn, #uacp_message{payload = Msg} = Req) ->
    #{
        max_age := MaxAge,
        timestamps_to_return := TimestampsToReturn,
        nodes_to_read := NodesToRead
    } = Msg,

    ReadOpts = #{
        max_age => parse_max_age(MaxAge),
        timestamp_type => TimestampsToReturn
    },
    Results = attribute_read(Data, ReadOpts, NodesToRead),
    ?assertEqual(length(NodesToRead), length(Results)),
    Resp = opcua_connection:response(Conn, Req, 632, #{
        results => Results,
        diagnostic_infos => []
    }),
    {{reply, Resp}, Data}.

attribute_read(Data, ReadOpts, ReadIds) ->
    attribute_read(Data, ReadOpts, ReadIds, []).

attribute_read(_Data, _ReadOpts, [], Acc) -> lists:reverse(Acc);
attribute_read(Data, ReadOpts, [ReadId | Rest], Acc) ->
    #{
        node_id := NodeId,
        attribute_id := AttributeId,
        index_range := RangeStr,
        data_encoding := DataEncoding
    } = ReadId,
    case DataEncoding of
        #opcua_qualified_name{ns = 0, name = Name}
          when Name =:= <<"Default Binary">>; Name =:= undefined ->
            Command = #opcua_read_command{
                attr = opcua_database_attributes:name(AttributeId),
                range = opcua_util:parse_range(RangeStr),
                opts = ReadOpts
            },
            case opcua_server_registry:perform(NodeId, [Command]) of
                [{error, Reason}] ->
                    Status = opcua_database_status_codes:name(Reason, bad_internal_error),
                    Result = #opcua_data_value{status = Status},
                    attribute_read(Data, ReadOpts, Rest, [Result | Acc]);
                [#opcua_data_value{} = Result] ->
                    attribute_read(Data, ReadOpts, Rest, [Result | Acc])
            end;
        _ ->
            Result = #opcua_data_value{status = bad_data_encoding_unsupported},
            attribute_read(Data, ReadOpts, Rest, [Result | Acc])
    end.

parse_max_age(Age) when Age >= 0, Age < ?EPSILON -> newest;
parse_max_age(Age) when Age >= ?MAX_INT32 -> cached;
parse_max_age(Age) when Age >= 0 -> trunc(Age);
parse_max_age(_Other) -> throw(bad_max_age_invalid).


%-- VIEW SERVICE SET -----------------------------------------------------------

view_browse_command(Data, Conn, #uacp_message{payload = Msg} = Req) ->
    #{
        requested_max_references_per_node := MaxRefs,
        nodes_to_browse := NodesToBrowse
    } = Msg,
    BrowseOpts = #{max_refs => MaxRefs},
    Results = view_browse(Data, BrowseOpts, NodesToBrowse),
    ?assertEqual(length(NodesToBrowse), length(Results)),
    Resp = opcua_connection:response(Conn, Req, 528, #{
        results => Results,
        diagnostic_infos => []
    }),
    {{reply, Resp}, Data}.

view_browse(Data, BrowseOpts, NodesToBrowse) ->
    view_browse(Data, BrowseOpts, NodesToBrowse, []).

view_browse(_Data, _BrowseOpts, [], Acc) -> lists:reverse(Acc);
view_browse(Data, BrowseOpts, [BrowseSpec | Rest], Acc) ->
    #{
        node_id := NodeId,
        reference_type_id := RefType,
        include_subtypes := SubTypes,
        browse_direction := Direction
    } = BrowseSpec,
    Command = #opcua_browse_command{
        type = RefType,
        subtypes = SubTypes,
        direction = Direction,
        opts = BrowseOpts
    },
    case opcua_server_registry:perform(NodeId, [Command]) of
        [{error, Reason}] ->
            Status = opcua_database_status_codes:name(Reason, bad_internal_error),
            BrowseResult = #{
                status_code => Status,
                continuation_point => undefined,
                references => []
            },
            view_browse(Data, BrowseOpts, Rest, [BrowseResult | Acc]);
        [CommandResult] ->
            BrowseResult = #{
                status_code => maps:get(status, CommandResult, good),
                continuation_point => undefined,
                references => [#{
                    reference_type_id => maps:get(type, Ref, ?UNDEF_NODE_ID),
                    is_forward => maps:get(is_forward, Ref, true),
                    node_id => maps:get(node_id, Ref),
                    browse_name => maps:get(browse_name, Ref, #opcua_qualified_name{}),
                    display_name => maps:get(display_name, Ref, #opcua_localized_text{}),
                    node_class => maps:get(node_class, Ref, unspecified),
                    type_definition => maps:get(type_definition, Ref, ?UNDEF_EXT_NODE_ID)
                } || Ref <- maps:get(references, CommandResult, [])]
            },
            view_browse(Data, BrowseOpts, Rest, [BrowseResult | Acc])
    end.