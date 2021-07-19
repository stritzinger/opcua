= Stritzinger OPCUA Architecture Overview

== Database

The database part of the architecture has multiple functions.
 - Lookup static constants like status codes and attribute identifiers.
 - Lookup data type schema for encoding and decoding structured values.
 - Lookup nodes encoding identifiers.
 - Maintain the dynamic node address space.

All the static data, including the standard nodes instances, are loaded on
startup from a set of binary files in the `priv` directory. These binary files
have been pre-generated using the function `opcua_database_nodes:parse/1` by
parsing the standard OPCUA XML nodeset files and CSV files also available
in the `priv` directory. The original files can be found at:

    https://github.com/OPCFoundation/UA-Nodeset/tree/v1.04/Schema


=== Static Attributes

This database provides functions to convert attribute names to attribute id
and vice versa.

Modules: `opcus_database_attributes`.


==== TODO

Could be changed to use `persistent_term`


=== Static Status Codes

This database provides functions to convert status code to status name
and vice versa. It provide also a way to get a description of a status code.

Modules: `opcus_database_status_code`.


==== TODO

Could be changed to use `persistent_term`


=== Node Encodings

In OPCUA, every node is associated with an encoding node defining the encoding
supported. This database provides a way to get a node id from an encoding node id
and to get the encoding node id of a node given an encoding type.


=== Data Types

OPCUA supports structured data as value, including structures, enums, and unions.
To be able to decode and encode these values, the library needs a schema
describing it. This database provides a way to look up a schema given de data type.
These scemas are pre-generated from the XML nodeset files.


=== Address Space

The address space is a dynamic database representing all the addressable nodes.
It contains the nodes and the references between them.

TODO


== OPCUA Connection

The `opcua_connection` is just an opaque connection abstraction.
It is used to pass information of the current connection to the different parts
of the stack, like the UACP protocol, the channel, or the session handlers.


== UACP Protocol

This is the low-level OPCUA data protocol for transferring messages between
the client and the server. It supports multiple data channels and message
chunking. It must be used alongside a channel callback module to handle
the security model.

Incoming data is passed to the function `opcua_uacp:handle_data/3` that may
return an OPCUA message if the data resulted in a complete message.

Outgoing data is handled with a consumer/producer mechanism. When an OPCUA
message needs to be sent, it is consumed but the function `opcua_uacp:consume/4`
and then the functions `opcua_uacp:can_produce/2` and `opcua_uacp:produce/2`
can be used to generate binary chuck to be sent to the peer. This is used
to decouple the message multiplexing and chunking from the connection.

There is a common module `opcua_uacp` and a specialized module for the client
and server part of the protocol, `opcua_uacp_client` and `opcua_uacp_server`.
The main difference is the way these modules handle protocol handshake.


=== UACP Client Protocol

The client-side of the protocol is responsible to initiate the handshake by
sending an `hello` message and wait for an `acknowledge` message.
Then it must create a channel, and from there create a session.

==== TODO

 - Parametrise the security policy.


=== UACP Server Protocol

TODO


=== TODO

 - The chunking of outgoing messages is not yet implemented.
 - More validation and enforcement are needed.
 - Some node ids are hardcoded.
 - Better handling of decoding error, so non-fatal error in a channel would not break the chunk processing.
 - Enforce the maximum number of concurrent messages and maximum message size.


== Channels

Channels are multiplexes communication between a client and server, they also
define and enforce the security policy for the communication.

Because the channel creation is asymmetric, the client requests the creation,
the client and server implementation of the channels are different.

The channel modules provide two things. They handle incoming messages to 
open and close channels using a given security policy, and it provides a behaviour
for the UACP protocol handler `opcua_uacp` module to lock and unlock the blocks
that will form the OPCUA message.


=== Client Channel

The client channel `opcua_client_channel` opens a channel to an OPCUA server.
The security policy is currently hard-coded to `None`.

Another function of the client channel is to create the request for the client
session with all the required security context, this is done by the function
`make_request/6`.


==== TODO

 - Validate that messages are for the proper channel
 - Parametrise the security policy


=== Server Channel

The server channel `opcua_server_channel` handle the channel creation requests
sent by the OPCUA client.
The security policy is currently hard-coded to `None`.


==== TODO

 - Parametrise the security policy


== Security Policy


== OPCUA Session

The session layer is in charge of the creation and handling of the OPCUA
session. There is a different implementation for the client and the server-side.


=== Cient Session

The client OPCUA session is handled in the client process.
It is created and owned by the client UACP layer.
It implements the OPCUA commands for browsing nodes and reading node attributes.


==== TODO

 - Implement batch commands.
 - Implement autentication.
 - Better error handling.


=== Server Session Manager

TODO


=== Server Session

Delegate to service discovery, channel and session

TODO


==== TODO

 - Implement batch commands.
 - Implement autentication.
 - Better error handling.


== OPCUA Client

The OPCUA client is a gen_statem process.

 - Supervised by `opcua_client_pool_sup`
    - Supervised by `opcua_client_sup`
 - Own `opcua_connection` instance
 - Own `opcua_client_uacp` instance
    - Own `opcua_client_channel` instance
        - Use given `ocua_connection` reference
    - Own `opcua_uacp` instance
        - Use given `opcua_channel` behavior reference
        - Use given `ocua_connection` reference
    - Own `opcua_client_session` instance
        - Use given `opcua_client_channel` reference
        - Use given `opcua_connection` reference
    - Use given `ocua_connection` reference
 - Own `ocua_client_channel` instance
    - Implement `opcus_channel` behavior
    - Own `opcua_security` instance
    - Use given `ocua_connection` reference

TODO


=== TODO

 - Add jitter to the exponential backoff retry.


== OPCUA Server

The OPCUA server is a `ranch_protocol` handler.

 - Own `opcua_connection` instance
 - Own `opcua_server_uacp` instance
    - Own `opcua_server_channel` instance
        - Use `ocua_connection` reference
    - Own `opcua_uacp` instance
        - Use given `opcua_channel` behavior reference
        - Use given `ocua_connection` reference
    - Spawn an `opcua_server_session` process
        - Use given `ocua_connection` reference
 - Own `ocua_server_channel` instance
    - Implement `opcus_channel` behavior
    - Own given `opcua_security` instance

TODO


=== Server Registry

TODO


=== Server Discovery

TODO
