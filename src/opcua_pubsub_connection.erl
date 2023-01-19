-module(opcua_pubsub_connection).

-export([create/2]).
-export([destroy/1]).
-export([handle_network_message/2]).
-export([add_reader_group/2]).
-export([add_data_set_reader/3]).
-export([create_target_variables/4]).

-record(state, {
    connection_id,
    middleware :: {supervisor:child_id(), module()},
    reader_groups = #{},
    writer_groups = #{}
}).

-include("opcua_pubsub.hrl").

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Publised Data Set configuration: PDS are independent
create(Url, Config) ->
    PubSubConnectionID = uuid:get_v4(),
    TransportProcessID = uuid:get_v4(),
    Uri = uri_string:parse(Url),
    Config2 = maps:merge(default_config(), Config),
    Config3 = maps:put(uri, Uri, Config2),
    Config4 = maps:put(connection_id, PubSubConnectionID, Config3),
    case start_transport(TransportProcessID, Config4) of
        {ok, Module} -> {ok, PubSubConnectionID, #state{
            connection_id = PubSubConnectionID,
            middleware = {TransportProcessID, Module}
        }};
        {error, E} -> error(E)
    end.

destroy(#state{middleware = M}) ->
    supervisor:terminate_child(opcua_pubsub_middleware_sup, M),
    supervisor:delete_child(opcua_pubsub_middleware_sup, M).

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


add_reader_group(ReaderGroupCfg, #state{reader_groups = RG} = S) ->
    RG_id = uuid:get_v4(),
    {ok, ReaderGroup} = opcua_pubsub_reader_group:new(ReaderGroupCfg),
    RG2 = maps:put(RG_id, ReaderGroup, RG),
    {ok, RG_id, S#state{reader_groups = RG2}}.

add_data_set_reader(RG_id, DSR_cfg,
                            #state{reader_groups = RGs} = S) ->
    RG = maps:get(RG_id, RGs),
    {ok, DSR_id, NewRG} = opcua_pubsub_reader_group:add_data_set_reader(DSR_cfg, RG),
    NewGroups = maps:put(RG_id, NewRG, RGs),
    {ok, DSR_id, S#state{reader_groups = NewGroups}}.

create_target_variables(RG_id, DSR_id, Config, #state{reader_groups = RGs} = S) ->
    RG = maps:get(RG_id, RGs),
    {ok, NewRG} = opcua_pubsub_reader_group:create_target_variables(DSR_id, Config, RG),
    NewGroups = maps:put(RG_id, NewRG, RGs),
    {ok, DSR_id, S#state{reader_groups = NewGroups}}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_transport(ID, #{uri := #{scheme := <<"opc.udp">>}} = Config) ->
    start_supervised_transport(ID, opcua_pubsub_udp, [Config]);
start_transport(_ID, _Config) ->
    {error, unsupported_transport}.

start_supervised_transport(ID, Module, Args) ->
    Child = #{
        id => ID,
        start => {Module, start_link, Args},
        restart => transient
    },
    case supervisor:start_child(opcua_pubsub_middleware_sup, Child) of
        {ok, _Pid} -> {ok, Module};
        E -> E
    end.

default_config() -> #{
        publisher_id_type => ?UA_PUBLISHERIDTYPE_UINT16,
        publisher_id => 1111,
        name => "Unnamed"
    }.

dispatchMessages(BundledMessages, InterestedReaders) ->
    [begin
        NewRG = opcua_pubsub_reader_group:dispatch_messages(BundledMessages,
                                                                 DSR_ids, RG),
        {RG_id, NewRG}
     end || {RG_id, RG, DSR_ids} <- InterestedReaders].