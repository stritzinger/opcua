-module(opcua_pubsub_connection).

-export([create/3]).
-export([add_reader_group/2]).
-export([add_dataset_reader/3]).
-export([create_target_variables/4]).

-export([add_writer_group/2]).
-export([add_dataset_writer/4]).

-export([start_link/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include("opcua_pubsub.hrl").

-record(state, {
    id,
    uri,
    transport_config,
    publisher_id,
    publisher_id_type,
    middleware,
    reader_groups = #{},
    writer_groups = #{}
}).


% CONFIGURATION API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% These help to build the initial Connection process state
% which holds the settings of all pubsub sub-entities

create(Url,
       #connection_config{publisher_id = PublisherId,
                          publisher_id_type = PublisherIdType},
       TransportOpts) ->
    Uri = uri_string:parse(Url),
    Config2 = maps:merge(default_config(), TransportOpts),
    Config3 = maps:merge(Config2, #{uri => Uri}),
    {ok, #state{uri = Uri,
                transport_config = Config3,
                publisher_id = PublisherId,
                publisher_id_type = PublisherIdType}}.

add_reader_group(ReaderGroupCfg, #state{reader_groups = RG} = S) ->
    RG_id = uuid:get_v4(),
    {ok, ReaderGroup} = opcua_pubsub_reader_group:new(ReaderGroupCfg),
    RG2 = maps:put(RG_id, ReaderGroup, RG),
    {ok, RG_id, S#state{reader_groups = RG2}}.

add_dataset_reader(RG_id, DSR_cfg, #state{reader_groups = RGs} = S) ->
    RG = maps:get(RG_id, RGs),
    {ok, DSR_id, NewRG} = opcua_pubsub_reader_group:add_dataset_reader(DSR_cfg, RG),
    NewGroups = maps:put(RG_id, NewRG, RGs),
    {ok, DSR_id, S#state{reader_groups = NewGroups}}.

create_target_variables(RG_id, DSR_id, Config, #state{reader_groups = RGs} = S) ->
    RG = maps:get(RG_id, RGs),
    {ok, NewRG} = opcua_pubsub_reader_group:create_target_variables(DSR_id, Config, RG),
    NewGroups = maps:put(RG_id, NewRG, RGs),
    {ok, S#state{reader_groups = NewGroups}}.

add_writer_group(WriterGroupCfg, #state{
            publisher_id = PublisherId,
            publisher_id_type = PublisherIdType,
            writer_groups = WGs} = S) ->
    WG_id = uuid:get_v4(),
    {ok, WriterGroup} = opcua_pubsub_writer_group:new(PublisherId,
                                                      PublisherIdType,
                                                      WriterGroupCfg),
    WGs2 = maps:put(WG_id, WriterGroup, WGs),
    {ok, WG_id, S#state{writer_groups = WGs2}}.

add_dataset_writer(WG_id, PDS_id, WriterCfg, #state{writer_groups = WGs} = S) ->
    WG = maps:get(WG_id, WGs),
    {ok, DSW_is, NewWriterGroup} =
            opcua_pubsub_writer_group:add_dataset_writer(PDS_id, WriterCfg, WG),
    WGs2 = maps:put(WG_id, NewWriterGroup, WGs),
    {ok, DSW_is, S#state{writer_groups = WGs2}}.

%%% GEN_SERVER API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(ID, ConfiguredState) ->
    gen_server:start_link(?MODULE, [ID, ConfiguredState], []).

%%% GEN_SERVER CALLBACKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([ID, #state{transport_config = Config,
                 writer_groups = WriterGroups} = ConfiguredState]) ->
    case start_transport(Config) of
        {ok, Module, State} ->
            opcua_pubsub:register_connection(ID),
            WG2 = init_writer_groups(WriterGroups),
            {ok, ConfiguredState#state{
                id = ID,
                writer_groups = WG2,
                middleware = {Module, State}
            }};
        {error, E} -> error(E)
    end.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({publish, WG_ID}, #state{
            middleware = {Module, MiddlewareState},
            writer_groups = WriterGroups} = State) ->
    WG = maps:get(WG_ID, WriterGroups),
    {NetMsg, NewWG} = opcua_pubsub_writer_group:write_network_message(WG),
    % io:format("Sending NetworkMsg: ~p~n",[NetMsg]),
    MiddlewareState2 = Module:send(NetMsg, MiddlewareState),
    {noreply, State#state{
        middleware = {Module, MiddlewareState2},
        writer_groups = maps:put(WG_ID, NewWG, WriterGroups)}};
handle_info(Info, #state{middleware = {M, S}} = State) ->
    case M:handle_info(Info, S) of
        %ignored -> {noreply, State};
        NetMsg ->
            {ok, NewS} = handle_network_message(NetMsg, State),
            {noreply, NewS}
    end.

%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_transport(#{uri := #{scheme := <<"opc.udp">>}} = Config) ->
    {ok, Transport} = opcua_pubsub_udp:init(Config),
    {ok, opcua_pubsub_udp, Transport};
start_transport(_Config) ->
    {error, unsupported_transport}.

default_config() -> #{
        publisher_id_type => uint16,
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
        false -> % io:format("Skipped NetMsg = ~p\n",[Binary]),
            {ok, S};
        true -> % io:format("Accepting NetMsg = ~p\n",[Headers]),
            % we can procede with the security step if needed:
            % opcua_pubsub_security: ... not_implemented yet
            % Then we decode all messages
            DataSetMessages = opcua_pubsub_uadp:decode_payload(Headers, Payload),
            #{payload_header := #{dataset_writer_ids := DSW_ids}} = Headers,
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

init_writer_groups(WriterGroups) ->
    maps:from_list([begin
            NewWG = opcua_pubsub_writer_group:init(ID, G),
            {ID, NewWG}
        end || {ID, G} <- maps:to_list(WriterGroups)]).
