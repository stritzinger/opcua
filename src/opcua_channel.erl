-module(opcua_channel).

%%% BEHAVIOUR opcua_channel DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-callback channel_id(State) -> ChannelId
    when State :: term(), ChannelId :: opcua:channel_id().

-callback lock(Chunk, Conn, State) -> {ok, Chunk, State} | {error, Reason}
    when Chunk :: opcua:chunk(),
         Conn :: opcua:connection(),
         State :: term(),
         Reason :: term().

-callback unlock(Chunk, Conn, State) -> {ok, Chunk, State} | {error, Reason}
    when Chunk :: opcua:chunk(),
         Conn :: opcua:connection(),
         State :: term(),
         Reason :: term().
