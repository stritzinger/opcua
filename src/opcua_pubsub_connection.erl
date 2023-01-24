-module(opcua_pubsub_connection).

-export([start_link/2]).
-export([send/2]).
-export([create/2]).
-export([add_reader_group/2]).
-export([add_data_set_reader/3]).
-export([create_target_variables/4]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
    id,
    config,
    middleware :: {module(), term()},
    reader_groups = #{},
    writer_groups = #{}
}).

-include("opcua_pubsub.hrl").

% CONFIGURATION API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% These help to build the Connection process state
% which holds the settings of all pubsub sub-entities

create(Url, Config) ->
    Uri = uri_string:parse(Url),
    Config2 = maps:merge(default_config(), Config),
    Config3 = maps:put(uri, Uri, Config2),
    {ok, #state{config = Config3}}.

add_reader_group(ReaderGroupCfg, #state{reader_groups = RG} = S) ->
    RG_id = uuid:get_v4(),
    {ok, ReaderGroup} = opcua_pubsub_reader_group:new(ReaderGroupCfg),
    RG2 = maps:put(RG_id, ReaderGroup, RG),
    {ok, RG_id, S#state{reader_groups = RG2}}.

add_data_set_reader(RG_id, DSR_cfg, #state{reader_groups = RGs} = S) ->
    RG = maps:get(RG_id, RGs),
    {ok, DSR_id, NewRG} = opcua_pubsub_reader_group:add_data_set_reader(DSR_cfg, RG),
    NewGroups = maps:put(RG_id, NewRG, RGs),
    {ok, DSR_id, S#state{reader_groups = NewGroups}}.

create_target_variables(RG_id, DSR_id, Config, #state{reader_groups = RGs} = S) ->
    RG = maps:get(RG_id, RGs),
    {ok, NewRG} = opcua_pubsub_reader_group:create_target_variables(DSR_id, Config, RG),
    NewGroups = maps:put(RG_id, NewRG, RGs),
    {ok, S#state{reader_groups = NewGroups}}.


%%% GEN_SERVER API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(ID, ConfiguredState) ->
    gen_server:start_link(?MODULE, [ID, ConfiguredState], []).

send(Pid, Data) ->
    gen_server:cast(Pid, {?FUNCTION_NAME, Data}).

%%% GEN_SERVER CALLBACKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([ID, #state{config = Config} = ConfiguredState]) ->
    case start_transport(Config) of
        {ok, Module, State} ->
            opcua_pubsub:register_connection(ID),
            {ok, ConfiguredState#state{
                id = ID,
                middleware = {Module, State}
            }};
        {error, E} -> error(E)
    end.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast({send, Data}, #state{middleware = {M,S}} = State) ->
    M:send(Data, S),
    {noreply, State}.

handle_info(Info, #state{middleware = {M, S}} = State) ->
    {ok, NewS} = handle_network_message(M:handle_info(Info, S), State),
    {noreply, NewS}.

%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_transport(#{uri := #{scheme := <<"opc.udp">>}} = Config) ->
    {ok, Transport} = opcua_pubsub_udp:init(Config),
    {ok, opcua_pubsub_udp, Transport};
start_transport(_Config) ->
    {error, unsupported_transport}.

default_config() -> #{
        publisher_id_type => ?UA_PUBLISHERIDTYPE_UINT16,
        publisher_id => 1111,
        name => "Unnamed"
    }.

handle_network_message(Binary, #state{reader_groups = RGs} = S) ->
    {Headers, Payload} = opcua_pubsub_uadp:decode_network_message_headers(Binary),
    InterestedReaders =
        [begin
            DSR_ids = opcua_pubsub_reader_group:filter_readers(Headers,RG),
            {RG_id, RG, DSR_ids}
        end
        || { RG_id, RG} <- maps:to_list(RGs)],
    ReadersCount = lists:sum([length(DSR_ids)
                                    || {_, _, DSR_ids} <- InterestedReaders]),
    case ReadersCount > 0 of
        false -> io:format("Skipped NetMsg\n"),{ok, S};
        true ->
            % we can procede with the security step if needed:
            % opcua_pubsub_security: ... not_implemented yet
            % Then we decode all messages
            DataSetMessages = opcua_pubsub_uadp:decode_payload(Headers, Payload),
            #{payload_header := #{data_set_writer_ids := DSW_ids}} = Headers,
            BundledMessages = lists:zip(DSW_ids, DataSetMessages),
            % After processing, the DSRs could change state.
            % All groups must be updated
            RG_list = dispatchMessages(BundledMessages, InterestedReaders),
            NewRGs = lists:foldl(fun
                    ({RG_id, NewRG}, Map) -> maps:put(RG_id, NewRG, Map)
                end, RGs, RG_list),
            {ok, S#state{reader_groups = NewRGs}}
    end.

dispatchMessages(BundledMessages, InterestedReaders) ->
    [begin
        NewRG = opcua_pubsub_reader_group:dispatch_messages(BundledMessages,
                                                                 DSR_ids, RG),
        {RG_id, NewRG}
     end || {RG_id, RG, DSR_ids} <- InterestedReaders].

