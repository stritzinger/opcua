-module(opcua_pubsub).

-export([start_link/0]).


-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).


-export([add_published_dataset/1]).
-export([add_published_dataset_field/3]).

-export([new_connection/2]).

-export([add_reader_group/2]).
-export([add_writer_group/2]).

-export([add_dataset_reader/3]).
-export([create_target_variables/4]).

-export([add_dataset_writer/4]).

-export([start_connection/1]).
-export([stop_connection/1]).
-export([register_connection/1]).
-export([get_published_dataset/1]).

-include("opcua_pubsub.hrl").

-record(state, {
    state,
    connections = #{},% Maps Ids to Pids
    published_datasets = #{}
}).

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_connection(Connection) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Connection}).

stop_connection(ConnectionID) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, ConnectionID}).

% Publised Data Set configuration: PDS are independent
add_published_dataset(Config) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Config}).

% Adds definitions per-field for a PublishedDataSet
add_published_dataset_field(PDS_ID, FieldsMetaData, FieldsSource) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, PDS_ID, FieldsMetaData, FieldsSource}).

get_published_dataset(PDS_ID) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, PDS_ID}).

new_connection(Url, Opts) ->
    opcua_pubsub_connection:create(Url, Opts).

% Just a place to group DataSetReaders
add_reader_group(Connection, Config) ->
    opcua_pubsub_connection:add_reader_group(Config, Connection).

add_writer_group(Connection, Config) ->
    opcua_pubsub_connection:add_writer_group(Config, Connection).

% define a DataSetReader, this includes its DataSetFieldMetaData
add_dataset_reader(Connection, RG_id, DSR_cfg) ->
    opcua_pubsub_connection:add_dataset_reader(RG_id, DSR_cfg, Connection).

% Add target variables to tell a DataSetReader where to write the decoded Fields
create_target_variables(Connection, RG_id, DSR_id, Cfg) ->
    opcua_pubsub_connection:create_target_variables(RG_id, DSR_id, Cfg, Connection).

add_dataset_writer(Connection, WG_id, PDS_id, DWR_cfg) ->
    opcua_pubsub_connection:add_dataset_writer(WG_id, PDS_id, DWR_cfg, Connection).

% INTERNAL API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

register_connection(ID) ->
    gen_server:cast(?MODULE, {?FUNCTION_NAME, ID, self()}).

% GEN_SERVER callbacks %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    {ok, #state{state = operational}}.

handle_call({start_connection, ConnectionConfig}, _, S) ->
    ID = uuid:get_v4(),
    {ok, _} = supervisor:start_child(opcua_pubsub_connection_sup, [ID, ConnectionConfig]),
    {reply, {ok, ID}, S};
handle_call({stop_connection, ConnectionID}, _, #state{connections = Conns} = S) ->
    Pid = maps:get(ConnectionID, Conns),
    ok = supervisor:terminate_child(opcua_pubsub_connection_sup, Pid),
    NewMap = maps:remove(ConnectionID, Conns),
    {reply, ok, S#state{connections = NewMap}};
handle_call({add_published_dataset, Config}, _, #state{published_datasets = PDSs} = S) ->
    PDS = h_add_published_dataset(Config),
    ID = uuid:get_v4(),
    NewMap = maps:put(ID, PDS, PDSs),
    {reply, {ok, ID}, S#state{published_datasets = NewMap}};
handle_call({add_published_dataset_field, PDS_id, FieldsMetadata, FieldsSources},
            _, #state{published_datasets = PDSs} = S) ->
    PDS = maps:get(PDS_id, PDSs),
    NewPDS = h_add_published_dataset_field(PDS, FieldsMetadata, FieldsSources),
    NewMap = maps:put(PDS_id, NewPDS, PDSs),
    {reply, ok, S#state{published_datasets = NewMap}};
handle_call({get_published_dataset, PDS_ID},
            _, #state{published_datasets = PublishedDatasets} = S) ->
    {reply, maps:get(PDS_ID, PublishedDatasets), S}.

handle_cast({register_connection, ID, Pid}, #state{connections = Conns} = State) ->
    {noreply, State#state{connections =  maps:put(ID, Pid, Conns)}}.

handle_info(_, S) ->
    {noreply, S}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

h_add_published_dataset(Config) ->
    % TODO add the PDS as object to the address space
    Config.

h_add_published_dataset_field(
        #published_dataset{
            dataset_metadata = #dataset_metadata{fields = MDFields} = DM,
            dataset_source = DSSource
        } = PDS,
        FieldsMetaData, FieldsSource) ->

    % TODO do more then just copy the provided configuration
    % check for correctness in the config
    % and show this stuff in the address space
    PDS#published_dataset{
        dataset_metadata = DM#dataset_metadata{
            fields = MDFields ++ FieldsMetaData
        },
        dataset_source = DSSource ++ FieldsSource
    }.