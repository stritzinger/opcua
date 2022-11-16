= OPCUA

== Generate certificates

To generate certificates please see `certificates/README.MD`

== Client

=== Connecting

==== Anonymous Connection

e.g.

	`{ok, Client} = opcua_client:connect(<<"opc.tcp://172.16.48.128:4840">>).`
	`{ok, Client} = opcua_client:connect(<<"opc.tcp://172.16.48.128:4840">>,
	    #{auth => anonymous}).`


==== Connecting with Username and Password

e.g.

	`{ok, Client} = opcua_client:connect(<<"opc.tcp://172.16.48.128:4840">>,
	   #{auth => {user_name, <<"test">>, <<"test">>}}).`


=== Browsing Nodes

e.g.

	`opcua_client:browse(Client, objects).`

With explicit namespace:

	`opcua_client:browse(Client, {1, <<"PLC1">>}).`


=== Reading Attributes

e.g.

With explicit namespace:

	`opcua_client:read(Client, {4, <<"OPCUA.int1">>}, value).`

Batch read:

	`opcua_client:read(Client, {4, <<"OPCUA.int1">>}, [node_id, value]).`

With array index:

	`opcua_client:read(Client, {4, <<"OPCUA.array1">>}, {value, 2}).`

With multidimensional array index:

	`opcua_client:read(Client, {4, <<"OPCUA.array2">>}, {value, [1, 0, 3]}).`

Batch read with indexes:

	`opcua_client:read(Client, {4, <<"OPCUA.array1">>}, [{value, 0}, {value, 5}]).`

With array range:

	`opcua_client:read(Client, {4, <<"OPCUA.array2">>}, {value, {1,4}}).`
	`opcua_client:read(Client, {4, <<"OPCUA.array2">>}, {value, [0,{0,3},{2,4}]}).`

With expanded arrays of structs (server dependent):

	`opcua_client:read(Client, {4, <<"OPCUA.array3[1]">>}, value).`


=== Writing Attributes

e.g.

With explicit namespace:

	`opcua_client:write(Client, {4, <<"OPCUA.int1">>}, {value, {opcua_variant, int16, 42}}).`

Full array write:

	`opcua_client:write(Client, {4, <<"OPCUA.array1">>}, {value, {opcua_variant, boolean, [false,true,false,true,false,true,false,true,false,true]}}).`

With array index:

	`opcua_client:write(Client, {4, <<"OPCUA.array1">>}, {{value, 2}, true}).`

With multidimensional array index:

	`opcua_client:read(Client, {4, <<"OPCUA.array2">>}, {{value, [1, 0, 3]}, false}).`

With expanded arrays of structs (server dependent):

	`opcua_client:write(Client, {4, <<"OPCUA.array3[1]">>}, {value, {opcua_variant, extension_object, {opcua_extension_object, {opcua_node_id, 4, string, <<"<StructuredDataType>:DataStruct_OpCon_MDT">>}, byte_string, #{chg_over_wpc_no => 0, date_time => 0, dest_cell => 0, dest_workpos => 0, error_no => 0, identifier => <<"foobar">>, origin_cell => 0, origin_workpos => 0, repeat_counter => 0, state => 0, stator_angel => 0, str_type_no => <<>>, w_p_c_no => 0, w_p_c_no_exter => 0, winding => 0}}}}).`


== Server

== Database

OPCUA needs a database of standard node, they are provided by XML NodeSet files.
To speedup the startup of the client and server, these files are preprocessed
and stored as term files. The NodeSet files used to generate current database
are stored in `priv/nodesets` as `XXX.NodeSet2.xml` files. The generated
database files are in `priv` as `nodesets.XXX.bterm` files.

To update the NodeSet, update the files in `priv/nodesets`, start a shell with
`rebar3 shell` and run the command `opcua_database_parser:parse().`.
