= OPCUA

== Client

=== Connecting

e.g.

	`Client = opcua_client:connect(<<"opc.tcp://172.16.48.6:4840">>).`


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

	`opcua_client:read(Client, {4, <<"OPCUA.array1">>}, {value, 2}).

With multidimensional array index:

	`opcua_client:read(Client, {4, <<"OPCUA.array2">>}, {value, [1, 0, 3]}).

Batch read with indexes:

	`opcua_client:read(Client, {4, <<"OPCUA.array1">>}, [{value, 0}, {value, 5}]).

=== Writing Attributes

e.g.

With explicit namespace:

	`opcua_client:write(Client, {4, <<"OPCUA.int1">>}, {value, {opcua_variant, int16, 42}}).`

Full array write:

	`opcua_client:write(Client, {4, <<"OPCUA.array1">>}, {value, {opcua_variant, boolean, [false,true,false,true,false,true,false,true,false,true]}}).`

With array index:

	`opcua_client:write(Client, {4, <<"OPCUA.array1">>}, {{value, 2}, true}).

With multidimensional array index:

	`opcua_client:read(Client, {4, <<"OPCUA.array2">>}, {{value, [1, 0, 3]}, false}).



== Server