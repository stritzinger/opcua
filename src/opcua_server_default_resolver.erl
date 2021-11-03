-module(opcua_server_default_resolver).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVER_SERVER_ARRAY,                        ?NNID(2254)).
-define(SERVER_NAMESPACE_ARRAY,                     ?NNID(2255)).
-define(SERVER_SERVICE_LEVEL,                       ?NNID(2267)).
-define(SERVER_AUDITING,                            ?NNID(2994)).
-define(SERVER_ESTIMATED_RETURN_TIME,               ?NNID(12885)).
-define(SERVER_LOCAL_TIME,                          ?NNID(17634)).

-define(SERVER_STATUS,                              ?NNID(2256)).
-define(SERVER_STATUS_START_TIME,                   ?NNID(2257)).
-define(SERVER_STATUS_CURRENT_TIME,                 ?NNID(2258)).
-define(SERVER_STATUS_STATE,                        ?NNID(2259)).
-define(SERVER_STATUS_BUILD_INFO,                   ?NNID(2260)).
-define(SERVER_STATUS_SECONDS_TILL_SHUTDOWN,        ?NNID(2992)).
-define(SERVER_STATUS_SHUTDOWN_REASON,              ?NNID(2993)).
-define(SERVER_STATUS_BI_PRODUCT_URI,               ?NNID(2262)).
-define(SERVER_STATUS_BI_MANUFACTURER_NAME,         ?NNID(2263)).
-define(SERVER_STATUS_BI_PRODUCT_NAME,              ?NNID(2261)).
-define(SERVER_STATUS_BI_SOFTWARE_VERSION,          ?NNID(2264)).
-define(SERVER_STATUS_BI_BUILD_NUMBER,              ?NNID(2265)).
-define(SERVER_STATUS_BI_BUILD_DATE,                ?NNID(2266)).

-define(SERVER_CAPS_SERVER_PROFILE_ARRAY,           ?NNID(2269)).
-define(SERVER_CAPS_LOCALE_ID_ARRAY,                ?NNID(2271)).
-define(SERVER_CAPS_MIN_SUPPORTED_SAMPLE_RATE,      ?NNID(2272)).
-define(SERVER_CAPS_MAX_BROWSE_CONTINUATION_POINTS, ?NNID(2735)).
-define(SERVER_CAPS_MAX_QUERY_CONTINUATION_POINTS,  ?NNID(2736)).
-define(SERVER_CAPS_MAX_HISTORY_CONTINUATION_POINTS, ?NNID(2737)).
-define(SERVER_CAPS_SOFTWARE_CERTIFICATES,          ?NNID(3704)).
-define(SERVER_CAPS_MAX_ARRAY_LENGTH,               ?NNID(11702)).
-define(SERVER_CAPS_MAX_STRING_LENGTH,              ?NNID(11703)).
-define(SERVER_CAPS_MAX_BYTE_STRING_LENGTH,         ?NNID(12911)).

-define(OP_LIMITS_MAX_NODES_PER_READ,               ?NNID(11705)).
-define(OP_LIMITS_MAX_NODES_PER_HISTORY_READ_DATA,  ?NNID(12165)).
-define(OP_LIMITS_MAX_NODES_PER_HISTORY_READ_EVENTS, ?NNID(12166)).
-define(OP_LIMITS_MAX_NODES_PER_WRITE,              ?NNID(11707)).
-define(OP_LIMITS_MAX_NODES_PER_HISTORY_UPDATE_DATA, ?NNID(12167)).
-define(OP_LIMITS_MAX_NODES_PER_HISTORY_UPDATE_EVENTS, ?NNID(12168)).
-define(OP_LIMITS_MAX_NODES_PER_METHOD_CALL,        ?NNID(11709)).
-define(OP_LIMITS_MAX_NODES_PER_BROWSE,             ?NNID(11710)).
-define(OP_LIMITS_MAX_NODES_PER_REGISTER_NODES,     ?NNID(11711)).
-define(OP_LIMITS_MAX_NODES_PER_TRANSLATE_BROWSE_PATHS_TO_NODE_IDS, ?NNID(11712)).
-define(OP_LIMITS_MAX_NODES_PER_NODE_MANAGEMENT,    ?NNID(11713)).
-define(OP_LIMITS_MAX_MONITORED_ITEMS_PER_CALL,     ?NNID(11714)).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export([init/0]).
-export([get_node/2]).
-export([get_references/3]).
-export([get_value/4]).


%%% RECORDS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {start_time}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init() ->
    State = #state{start_time = current_filetime()},
    Values = [{ID, static_value(State, ID)} || ID <- [
        ?SERVER_SERVER_ARRAY,
        ?SERVER_NAMESPACE_ARRAY,
        ?SERVER_SERVICE_LEVEL,
        ?SERVER_AUDITING,
        ?SERVER_ESTIMATED_RETURN_TIME,
        ?SERVER_STATUS_START_TIME,
        ?SERVER_STATUS_BUILD_INFO,
        ?SERVER_STATUS_SECONDS_TILL_SHUTDOWN,
        ?SERVER_STATUS_BI_PRODUCT_URI,
        ?SERVER_STATUS_BI_MANUFACTURER_NAME,
        ?SERVER_STATUS_BI_PRODUCT_NAME,
        ?SERVER_STATUS_BI_SOFTWARE_VERSION,
        ?SERVER_STATUS_BI_BUILD_NUMBER,
        ?SERVER_STATUS_BI_BUILD_DATE,
        ?SERVER_CAPS_SERVER_PROFILE_ARRAY,
        ?SERVER_CAPS_LOCALE_ID_ARRAY,
        ?SERVER_CAPS_MIN_SUPPORTED_SAMPLE_RATE,
        ?SERVER_CAPS_MAX_BROWSE_CONTINUATION_POINTS,
        ?SERVER_CAPS_MAX_QUERY_CONTINUATION_POINTS,
        ?SERVER_CAPS_MAX_HISTORY_CONTINUATION_POINTS,
        ?SERVER_CAPS_SOFTWARE_CERTIFICATES,
        ?SERVER_CAPS_MAX_ARRAY_LENGTH,
        ?SERVER_CAPS_MAX_STRING_LENGTH,
        ?SERVER_CAPS_MAX_BYTE_STRING_LENGTH,
        ?OP_LIMITS_MAX_NODES_PER_READ,
        ?OP_LIMITS_MAX_NODES_PER_HISTORY_READ_DATA,
        ?OP_LIMITS_MAX_NODES_PER_HISTORY_READ_EVENTS,
        ?OP_LIMITS_MAX_NODES_PER_WRITE,
        ?OP_LIMITS_MAX_NODES_PER_HISTORY_UPDATE_DATA,
        ?OP_LIMITS_MAX_NODES_PER_HISTORY_UPDATE_EVENTS,
        ?OP_LIMITS_MAX_NODES_PER_METHOD_CALL,
        ?OP_LIMITS_MAX_NODES_PER_BROWSE,
        ?OP_LIMITS_MAX_NODES_PER_REGISTER_NODES,
        ?OP_LIMITS_MAX_NODES_PER_TRANSLATE_BROWSE_PATHS_TO_NODE_IDS,
        ?OP_LIMITS_MAX_NODES_PER_NODE_MANAGEMENT,
        ?OP_LIMITS_MAX_MONITORED_ITEMS_PER_CALL
    ]],
    Nodes = [],
    Refs = [],
    {ok, Values, Nodes, Refs, State}.

get_node(_State, _NodeId) -> undefined.

get_references(_State, _NodeId, _Opts) -> [].

get_value(_State, ?SERVER_LOCAL_TIME, _Type, _val) ->
    %TODO: Figure out how to get this in a clean way
    #{offset => 1, daylight_saving_in_offset => true};
get_value(State, ?SERVER_STATUS, _Type, _Val) ->
    (static_map(State, #{
        start_time => ?SERVER_STATUS_START_TIME,
        build_info => ?SERVER_STATUS_BUILD_INFO,
        seconds_till_shutdown => ?SERVER_STATUS_SECONDS_TILL_SHUTDOWN,
        shutdown_reason => ?SERVER_STATUS_SHUTDOWN_REASON
    }))#{
        current_time => current_filetime(),
        state => running
    };
get_value(_State, ?SERVER_STATUS_CURRENT_TIME, _Type, _Val) ->
    current_filetime();
get_value(_State, ?SERVER_STATUS_STATE, _Type, _Val) ->
    running;
get_value(_State, _NodeId, _Type, _Value) ->
    undefined.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

current_filetime() ->
    posix_nano_to_filetime(erlang:system_time(nanosecond)).

posix_nano_to_filetime(V) ->
    (V + (134774*24*60*60*1000*1000*1000)) div 100.

static_map(State, Fields) ->
    maps:map(fun(_Key, Val) -> static_value(State, Val) end, Fields).


static_value(_State, ?SERVER_SERVER_ARRAY) ->
    <<"urn:StritzingerGmbH:OPCUAServerLib">>;
static_value(_State, ?SERVER_NAMESPACE_ARRAY) ->
    [U || {_, U} <- lists:sort(maps:to_list(opcua_database_namespaces:all()))];
static_value(_State, ?SERVER_SERVICE_LEVEL) ->
    255;
static_value(_State, ?SERVER_AUDITING) ->
    false;
static_value(_State, ?SERVER_ESTIMATED_RETURN_TIME) ->
    0;
static_value(#state{start_time = Time}, ?SERVER_STATUS_START_TIME) ->
    Time;
static_value(_State, ?SERVER_STATUS_SECONDS_TILL_SHUTDOWN) ->
    0;
static_value(_State, ?SERVER_STATUS_SHUTDOWN_REASON) ->
    #opcua_localized_text{};
static_value(_State, ?SERVER_STATUS_BI_PRODUCT_URI) ->
    <<"urn:StritzingerGmbH:OPCUAServerLib">>;
static_value(_State, ?SERVER_STATUS_BI_MANUFACTURER_NAME) ->
    <<"Stritzinger GmbH">>;
static_value(_State, ?SERVER_STATUS_BI_PRODUCT_NAME) ->
    <<"OPCUAServerLib">>;
static_value(_State, ?SERVER_STATUS_BI_SOFTWARE_VERSION) ->
    <<"0.1.0">>;
static_value(_State, ?SERVER_STATUS_BI_BUILD_NUMBER) ->
    <<"1">>;
static_value(_State, ?SERVER_STATUS_BI_BUILD_DATE) ->
    0;
static_value(State, ?SERVER_STATUS_BUILD_INFO) ->
    static_map(State, #{
        build_date => ?SERVER_STATUS_BI_BUILD_DATE,
        build_number => ?SERVER_STATUS_BI_BUILD_NUMBER,
        manufacturer_name => ?SERVER_STATUS_BI_MANUFACTURER_NAME,
        product_name => ?SERVER_STATUS_BI_PRODUCT_NAME,
        product_uri => ?SERVER_STATUS_BI_PRODUCT_URI,
        software_version => ?SERVER_STATUS_BI_SOFTWARE_VERSION
    });
static_value(_State, ?SERVER_CAPS_SERVER_PROFILE_ARRAY) ->
    [<<"http://opcfoundation.org/UAProfile/Server/StandardUA">>,
     <<"http://opcfoundation.org/UAProfile/Server/DataAccess">>];
static_value(_State, ?SERVER_CAPS_LOCALE_ID_ARRAY) ->
    [<<"en">>];
static_value(_State, ?SERVER_CAPS_MIN_SUPPORTED_SAMPLE_RATE) ->
    0.0;
static_value(_State, ?SERVER_CAPS_MAX_BROWSE_CONTINUATION_POINTS) ->
    10;
static_value(_State, ?SERVER_CAPS_MAX_QUERY_CONTINUATION_POINTS) ->
    0;
static_value(_State, ?SERVER_CAPS_MAX_HISTORY_CONTINUATION_POINTS) ->
    100;
static_value(_State, ?SERVER_CAPS_SOFTWARE_CERTIFICATES) ->
    [];
static_value(_State, ?SERVER_CAPS_MAX_ARRAY_LENGTH) ->
    65535;
static_value(_State, ?SERVER_CAPS_MAX_STRING_LENGTH) ->
    65535;
static_value(_State, ?SERVER_CAPS_MAX_BYTE_STRING_LENGTH) ->
    1048560;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_READ) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_HISTORY_READ_DATA) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_HISTORY_READ_EVENTS) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_WRITE) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_HISTORY_UPDATE_DATA) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_HISTORY_UPDATE_EVENTS) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_METHOD_CALL) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_BROWSE) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_REGISTER_NODES) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_TRANSLATE_BROWSE_PATHS_TO_NODE_IDS) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_NODES_PER_NODE_MANAGEMENT) ->
    65535;
static_value(_State, ?OP_LIMITS_MAX_MONITORED_ITEMS_PER_CALL) ->
    65535.
