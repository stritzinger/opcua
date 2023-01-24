-module(opcua_pubsub).

-export([start_link/0]).


-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).


-export([add_published_data_set/1]).
-export([add_data_set_field/2]).

-export([new_connection/2]).

-export([add_reader_group/2]).
-export([add_writer_group/2]).

-export([add_data_set_reader/3]).
-export([create_target_variables/4]).

-export([add_data_set_writer/3]).

-export([start_connection/1]).
-export([stop_connection/1]).

-export([register_connection/1]).

-record(state, {
    connections = #{},% Maps Ids to Pids
    published_data_sets = #{}
}).

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_connection(Connection) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Connection}).


stop_connection(ConnectionID) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, ConnectionID}).

% Publised Data Set configuration: PDS are independent
add_published_data_set(Config) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Config}).

% Adds definitions per-field for a PublishedDataSet
add_data_set_field(PublishedDataSetID, FieldConfig) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, PublishedDataSetID, FieldConfig}).

new_connection(Url, Opts) ->
    opcua_pubsub_connection:create(Url, Opts).

% Just a place to group DataSetReaders
add_reader_group(Connection, Config) ->
    opcua_pubsub_connection:add_reader_group(Config, Connection).

add_writer_group(_Connection, _Config) ->
    error(not_implemented).

% define a DataSetReader, this includes its DataSetFieldMetaData
add_data_set_reader(Connection, RG_id, DSR_cfg) ->
    opcua_pubsub_connection:add_data_set_reader(RG_id, DSR_cfg, Connection).

% Add target variables to tell a DataSetReader where to write the decoded Fields
create_target_variables(Connection, RG_id, DSR_id, Cfg) ->
    opcua_pubsub_connection:create_target_variables(RG_id, DSR_id, Cfg, Connection).

add_data_set_writer(Connection, WG_id, DWR_cfg) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Connection, WG_id, DWR_cfg}).

% INTERNAL API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

register_connection(ID) ->
    gen_server:cast(?MODULE, {?FUNCTION_NAME, ID, self()}).

% GEN_SERVER callbacks %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    {ok, #state{}}.

handle_call({start_connection, ConnectionConfig}, _, S) ->
    ID = uuid:get_v4(),
    {ok, _} = supervisor:start_child(opcua_pubsub_connection_sup, [ID, ConnectionConfig]),
    {reply, {ok, ID}, S};
handle_call({stop_connection, ConnectionID}, _, #state{connections = Conns} = S) ->
    Pid = maps:get(ConnectionID, Conns),
    ok = supervisor:terminate_child(opcua_pubsub_connection_sup, Pid),
    NewMap = maps:remove(ConnectionID, Conns),
    {reply, ok, S#state{connections = NewMap}}.

handle_cast({register_connection, ID, Pid}, #state{connections = Conns} = State) ->
    {noreply, State#state{connections =  maps:put(ID, Pid, Conns)}}.

handle_info(_, S) ->
    {noreply, S}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
