-module(opcua_pubsub_example).

-export([run/0]).

-include("opcua.hrl").
-include("opcua_pubsub.hrl").

run() ->
    Url = <<"opc.udp://224.0.0.22:4840">>,
    ConnectionConfig = #{},
    {ok, ConnectionID} = opcua_pubsub:add_connection(Url, ConnectionConfig),

    ReaderGroupconfig = #{ name => <<"Simple Reader Group">>},
    {ok, RG_id} = opcua_pubsub:add_reader_group(ConnectionID, ReaderGroupconfig),

    DSR_config = #data_set_reader_config{
        name = <<"Example Reader">>,
        publisher_id = 2234,
        publisher_id_type = uint16,
        writer_group_id = 100,
        data_set_writer_id = 62541,
        data_set_metadata = #data_set_metadata{
            name = "DataSet 1",
            description = "An example from 62541",
            fields = [
                #data_set_field_metadata{
                    name = "DateTime 1",
                    builtin_type = date_time,
                    data_type = opcua_node:id(date_time),
                    valueRank = -1 % a scalar,
                }]
        }
    },
    {ok, DSR_id} =
            opcua_pubsub:add_data_set_reader(ConnectionID, RG_id, DSR_config),

    % A dedicated object on the server (or any address space available)
    % containing all variables that will be updated by the DSR
    DataSetObject = opcua_server:add_object(<<"Subscribed Data">>, numeric),
    VarNodeId = opcua_server:add_variable(DataSetObject, <<"Publisher Time">>,
                                          undefined, date_time, 0),

    TGT = #target_variable{
        data_set_field_id = 0,
        target_node_id = VarNodeId,
        attribute_id = ?UA_ATTRIBUTEID_VALUE
    },
    opcua_pubsub:create_target_variables(ConnectionID,RG_id,DSR_id,[TGT]),
    ok.
