-module(opcua_pubsub).

-export([start_link/0]).


-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).


-export([add_published_data_set/1]).
-export([add_data_set_field/2]).

-export([add_connection/2]).
-export([new_network_message/2]).
-export([remove_connection/1]).

-export([add_reader_group/2]).
-export([add_writer_group/2]).

-export([add_data_set_reader/3]).
-export([create_target_variables/4]).

-export([add_data_set_writer/3]).

-record(state, {
    connections = #{},
    published_data_sets = #{}
}).

%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Publised Data Set configuration: PDS are independent
add_published_data_set(Config) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Config}).

% Adds definitions per-field for a PublishedDataSet
add_data_set_field(PublishedDataSetID, FieldConfig) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, PublishedDataSetID, FieldConfig}).

add_connection(Url, Config) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Url, Config}).

new_network_message(ConnectionId, Binary) ->
    gen_server:cast(?MODULE, {?FUNCTION_NAME, ConnectionId, Binary}).

remove_connection(ID) ->
    gen_server:cast(?MODULE, {?FUNCTION_NAME, ID}).

% Just a place to group DataSetReaders
add_reader_group(ConnectionID, Config) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, ConnectionID, Config}).

add_writer_group(ConnectionID, Config) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, ConnectionID, Config}).

% define a DataSetReader, this includes its DataSetFieldMetaData
add_data_set_reader(Conn_id, RG_id, DSR_cfg) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Conn_id, RG_id, DSR_cfg}).

% Add target variables to tell a DataSetReader where to write the decoded Fields
create_target_variables(Conn_id, RG_id, DSR_id, Config) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Conn_id, RG_id, DSR_id, Config}).

add_data_set_writer(Conn_id, WG_id, DWR_cfg) ->
    gen_server:call(?MODULE, {?FUNCTION_NAME, Conn_id, WG_id, DWR_cfg}).


% GEN_SERVER callbacks %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    {ok, #state{}}.


handle_call({add_connection, Url, Opts}, _, #state{connections = Conns} = S) ->
    {ok, ID, Conn} = opcua_pubsub_connection:create(Url, Opts),
    Conns2 = maps:put(ID, Conn, Conns),
    {reply, {ok, ID}, S#state{connections = Conns2}};
handle_call({add_reader_group, Conn_id, Opts}, _, #state{connections = Conns} = S) ->
    Conn = maps:get(Conn_id, Conns),
    {ok, ID, NewConn} = opcua_pubsub_connection:add_reader_group(Opts, Conn),
    Conns2 = maps:put(Conn_id, NewConn, Conns),
    {reply, {ok, ID}, S#state{connections = Conns2}};
handle_call({add_data_set_reader, Conn_id, RG_id, Cfg}, _,
                                            #state{connections = Conns} = S) ->
    Conn = maps:get(Conn_id, Conns),
    {ok, ID, NewConn} =
            opcua_pubsub_connection:add_data_set_reader(RG_id, Cfg, Conn),
    Conns2 = maps:put(Conn_id, NewConn, Conns),
    {reply, {ok, ID}, S#state{connections = Conns2}};
handle_call({create_target_variables, Conn_id, RG_id, DSR_id, Cfg}, _,
                                            #state{connections = Conns} = S) ->
    Conn = maps:get(Conn_id, Conns),
    {ok, ID, NewConn} =
            opcua_pubsub_connection:create_target_variables(RG_id, DSR_id, Cfg, Conn),
    Conns2 = maps:put(Conn_id, NewConn, Conns),
    {reply, {ok, ID}, S#state{connections = Conns2}}.

handle_cast({new_network_message, ConnId, Binary},
                                    #state{connections = Connections} = S) ->
    Connection = maps:get(ConnId, Connections),
    {ok, NewConn} =
            opcua_pubsub_connection:handle_network_message(Binary, Connection),
    {noreply, S#state{connections = maps:put(ConnId, NewConn, Connections)}};
handle_cast({remove_connection, ID}, #state{connections = Conns} = S) ->
    ok = opcua_pubsub_connection:destroy(maps:get(ID, Conns)),
    {noreply, S#state{connections = maps:remove(ID, Conns)}}.
handle_info(_, S) ->
    {noreply, S}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
