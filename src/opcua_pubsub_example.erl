-module(opcua_pubsub_example).

-export([subscription/0]).
-export([publication/0]).

-include("opcua.hrl").
-include("opcua_pubsub.hrl").

subscription() ->
    Url = <<"opc.udp://224.0.0.22:4840">>,
    ConnectionConfig = #connection_config{},
    {ok, Conn} = opcua_pubsub:new_connection(Url, ConnectionConfig, #{}),

    ReaderGroupconfig = #{ name => <<"Simple Reader Group">>},
    {ok, RG_id, Conn2} = opcua_pubsub:add_reader_group(Conn, ReaderGroupconfig),

    DSR_config = #dataset_reader_config{
        name = <<"Example Reader">>,
        publisher_id = 2234,
        publisher_id_type = uint16,
        writer_group_id = 100,
        dataset_writer_id = 62541,
        dataset_metadata = #dataset_metadata{
            name = "DataSet 1",
            description = "An example from 62541",
            fields = [
                #dataset_field_metadata{
                    name = "DateTime 1",
                    builtin_type = date_time,
                    data_type = opcua_node:id(date_time),
                    valueRank = -1 % a scalar,
                }]
        }
    },
    {ok, DSR_id, Conn3} =
            opcua_pubsub:add_dataset_reader(Conn2, RG_id, DSR_config),

    % A dedicated object on the server (or any address space available)
    % containing all variables that will be updated by the DSR
    DataSetObject = opcua_server:add_object(<<"Subscribed Data">>, numeric),
    VarNodeId = opcua_server:add_variable(DataSetObject, <<"Publisher Time">>,
                                          undefined, date_time, 0),

    TGT = #target_variable{
        dataset_field_id = 0,
        target_node_id = VarNodeId,
        attribute_id = ?UA_ATTRIBUTEID_VALUE
    },
    {ok, Conn4} = opcua_pubsub:create_target_variables(Conn3,RG_id,DSR_id,[TGT]),

    {ok, ID} = opcua_pubsub:start_connection(Conn4),
    ok.

publication() ->

    PDS_cfg = #published_dataset{
        name = "PublishedDataSet Example",
        dataset_metadata = #dataset_metadata{
            name = "My Metadata"
        }
    },
    {ok, PDS_id} = opcua_pubsub:add_published_dataset(PDS_cfg),

    % we specify the fields metadata and their sources
    % In this case we list available variables as sources
    FieldsMetaData = [#dataset_field_metadata{
        name = "DateTime 1",
        builtin_type = date_time,
        data_type = opcua_node:id(date_time),
        valueRank = -1 % a scalar,
    }],
    FieldsSource = [
        #published_variable{
            published_variable = ?NNID(2258),
            attribute_id = ?UA_ATTRIBUTEID_VALUE
        }],
    ok = opcua_pubsub:add_published_dataset_field(PDS_id, FieldsMetaData, FieldsSource),

    Url = <<"opc.udp://224.0.0.22:4840">>,
    ConnectionConfig = #connection_config{
        publisher_id = 2234,
        publisher_id_type = uint16
    },
    {ok, Conn} = opcua_pubsub:new_connection(Url, ConnectionConfig, #{}),

    WriterGroupconfig = #writer_group_config{
        name = <<"Simple Writer Group">>,
        writer_group_id = 100,
        publishing_interval = 100
    },
    {ok, WG_id, Conn2} = opcua_pubsub:add_writer_group(Conn, WriterGroupconfig),

    DataSetWriterConfig = #dataset_writer_config{
        name = <<"Simple DataSet Writer">>,
        dataset_writer_id = 62541,
        keyframe_count = 10
    },
    {ok, DSW_id, Conn3} = opcua_pubsub:add_dataset_writer(Conn2, WG_id,
                                                    PDS_id, DataSetWriterConfig),


    {ok, ID} = opcua_pubsub:start_connection(Conn3),

    ok.

