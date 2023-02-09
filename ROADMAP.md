= Erlang OPCUA Roadmap

== OPCUA Protocol

=== OCUA Codec

- Add support for OptionSet length different than 32 bits.
  https://reference.opcfoundation.org/Core/Part3/v105/docs/8.40
- Add support for subtypes in structures and union fields.
  https://reference.opcfoundation.org/Core/Part3/v105/docs/8.49


== UACP

- Add support for browse continuations (and BrowseNext).
- Enforce block and buffer sizes.
- Support browse filtering, and result fields configuration.
- Enforce a maximum number of inflight requests, maximum chunck size and
  maximum message size.
- Enforce that channel_open messages cannot be chuncked.
- Add support for reverse connection (ReverseHello).


== OPCUA NodeSet

=== OPCUA Parser

- Fix the namspaces indexing when multiple nodesets have diferent sets of namespaces.
- Make the aliases local to a nodespace definition.
- Properly parse XML formated values from standard nodeset XML files;
  e.g. Variables like EnumValues (i=15633) should have a value.
- Make the parser generate data type definition as they would be formated when
  requesting the attribute from a remote server.
- When opcua_space is able to maintain all the index and schemas dynamically,
  use a temporary instance of it to load all the parsed data and persiste it
  as a cache.


=== OPCUA Space

 - Move the is_subtype index from opcua_space_backend to opcua_space, so the
   backend is only responsible for storage.
 - Dynamically generate codec schemas from adding data type nodes in a space.
 - Use EnumValues, EnumFields, EnumStrings, OptionSetValues,
   and OptionsSetLength to generate missing data type definition, validate
   existing definition, and complete with extra information (description...).
 - Add consistency checks to space manipulation to prevent any state explicitly
   forbidden in the standard.
 - Add support for NodeVersion. Change the version of the node poroperty when
   adding or removing references. This must be optional, to be able to use
   a space as a cache of the server space in a client.
 - Add support for delegation, validation and notification; Any operation may
   be delegated to a registered actor (a service for example), that must
   allow the operation, and get notified on the applied operations. This should
   be used to interconnect services, e.g. the service that provide a client
   with node manipulation, and another service that apply side-effects from
   nodes being created.


== OPCUA Security


== OPCUA Client

- When space is updated to dynamically generate codec schemas, react to codec
  errors due to missing type information by dynamically retriving the types
  from the server.
- Add support for method calls.


== OPCUA Server

- Make node managment plugable, so multiple application could handle there own
  part of the node graph.
- Add support for method calls.
- Add node management service.


== Generic

=== Consistency and Validation Checks

- Client and server should check time synchronization (default: max 5 min)


=== Security

- Add support for role managment, authentication and access control.
- Verify we are enforcing the buffer size restriction correctly