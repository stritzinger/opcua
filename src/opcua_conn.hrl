
%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(opcua_conn, {
    socket :: inet:socket(),
    transport :: module(),
    opts = #{} :: map(),
    peer = undefined :: {inet:ip_address(), inet:port_number()},
    sock = undefined :: {inet:ip_address(), inet:port_number()}
}).

-type opcua_conn() :: #opcua_conn{}.
