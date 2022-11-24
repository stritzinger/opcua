
%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(DUMP_IO, false).
-if(?DUMP_IO =:= true).
-define(DUMP(FMT, ARGS), ?LOG_DEBUG(FMT, ARGS)).
-else.
-define(DUMP(FMT, ARGS), ok).
-endif.

-define(UNDEF_EXT_NODE_ID,
    #opcua_expanded_node_id{node_id = ?UNDEF_NODE_ID,
                            namespace_uri = undefined,
                            server_index = undefined}).
-define(UNDEF_EXT_OBJ,
    #opcua_extension_object{type_id = ?UNDEF_NODE_ID,
                            body = undefined}).
-define(IS_BUILTIN_TYPE_NAME(T),
    T =:= boolean;
    T =:= sbyte;
    T =:= byte;
    T =:= uint16;
    T =:= uint32;
    T =:= uint64;
    T =:= int16;
    T =:= int32;
    T =:= int64;
    T =:= float;
    T =:= double;
    T =:= string;
    T =:= date_time;
    T =:= guid;
    T =:= xml;
    T =:= status_code;
    T =:= byte_string;
    T =:= node_id;
    T =:= expanded_node_id;
    T =:= diagnostic_info;
    T =:= qualified_name;
    T =:= localized_text;
    T =:= extension_object;
    T =:= variant;
    T =:= data_value
).

-define(IS_BUILTIN_TYPE_ID(T), is_integer(T), T > 0, T =< 25).

-define(POLICY_NONE, <<"http://opcfoundation.org/UA/SecurityPolicy#None">>).
-define(POLICY_BASIC256SHA256, <<"http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256">>).
-define(TRANSPORT_PROFILE_BINARY, <<"http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary">>).

-define(NID_SERVICE_FAULT,              ?NNID(395)).
-define(NID_GET_ENDPOINTS_REQ,          ?NNID(426)).
-define(NID_GET_ENDPOINTS_RES,          ?NNID(429)).
-define(NID_CHANNEL_OPEN_REQ,           ?NNID(444)).
-define(NID_CHANNEL_OPEN_RES,           ?NNID(447)).
-define(NID_CHANNEL_CLOSE_REQ,          ?NNID(450)).
-define(NID_CHANNEL_CLOSE_RES,          ?NNID(453)).
-define(NID_CREATE_SESS_REQ,            ?NNID(459)).
-define(NID_CREATE_SESS_RES,            ?NNID(462)).
-define(NID_ACTIVATE_SESS_REQ,          ?NNID(465)).
-define(NID_ACTIVATE_SESS_RES,          ?NNID(468)).
-define(NID_CLOSE_SESS_REQ,             ?NNID(471)).
-define(NID_CLOSE_SESS_RES,             ?NNID(474)).
-define(NID_BROWSE_REQ,                 ?NNID(525)).
-define(NID_BROWSE_RES,                 ?NNID(528)).
-define(NID_READ_REQ,                   ?NNID(629)).
-define(NID_READ_RES,                   ?NNID(632)).
-define(NID_WRITE_REQ,                  ?NNID(671)).
-define(NID_WRITE_RES,                  ?NNID(674)).
-define(NID_ANONYMOUS_IDENTITY_TOKEN,   ?NNID(319)).
-define(NID_USERNAME_IDENTITY_TOKEN,    ?NNID(322)).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(opcua_endpoint, {
    url                         :: binary(),
    host                        :: inet:ip4_address() | inet:hostname(),
    port                        :: inet:port_number(),
    cert                        :: undefined | binary()
}).

%-- Codec Records --------------------------------------------------------------

-record(opcua_diagnostic_info, {
    symbolic_id                 :: undefined | integer(),
    namespace_uri               :: undefined | integer(),
    locale                      :: undefined | integer(),
    localized_text              :: undefined | integer(),
    additional_info             :: undefined | binary(),
    inner_status_code           :: undefined | integer(),
    inner_diagnostic_info       :: undefined | term()
}).

-record(opcua_structure, {
    node_id = ?UNDEF_NODE_ID    :: opcua:node_id(),
    with_options = false        :: boolean(),
    fields = []                 :: opcua:fields()
}).

-record(opcua_union, {
    node_id = ?UNDEF_NODE_ID    :: opcua:node_id(),
    fields = []                 :: opcua:fields()
}).

-record(opcua_enum, {
    node_id = ?UNDEF_NODE_ID    :: opcua:node_id(),
    fields = []                 :: opcua:fields()
}).

-record(opcua_option_set, {
    node_id = ?UNDEF_NODE_ID    :: opcua:node_id(),
    mask_type = ?UNDEF_NODE_ID  :: opcua:node_id(),
    fields = []                 :: opcua:fields()
}).

-record(opcua_builtin, {
    node_id = ?UNDEF_NODE_ID    :: opcua:node_id(),
    builtin_node_id = ?UNDEF_NODE_ID :: opcua:node_id()
}).

-record(opcua_field, {
    name                        :: atom(),
    node_id = ?UNDEF_EXT_NODE_ID :: opcua:node_id(),
    value_rank = -1             :: opcua:value_rank(),
    is_optional = false         :: boolean(),
    value                       :: integer()
}).


%-- Protocol Records -----------------------------------------------------------

% Chunk sates:
%
%  - undefined:
%     Basic chunks that are never secured, like hello acknowledge or error
%     chunks, only the fields message_type, chunk_type and body are available.
%  - locked:
%     Secured chunks that are still locked, the fields message_type, chunk_type,
%     channel_id, and security are available, the body field contains the
%     locked payload.
%  - unlocked:
%     Secure chunks that are unlocked and validated, the extra fields request_id
%     and sequence_num are available, body contains the clear payload.
%
% Before an unlocked chunk can be locked, some preparation is required is this
% specific order:
%   - The security context must be setup, this is done by the security module.
%   - The header size must be calculated, this is done by having the codec module
%     prepare the chunk.
%   - The size of the data after locking it must be calculated including padding
%     and signature, this is done by having the security module prepare the chunk.
%   - The header data must be encoded, this is done by having the codec module
%     freeze the chunk.
%
% Steps to decode a chunk:
%     - opcua_protocole_codec:decode_chunk/1
%     - if chunk is locked:
%         - opcua_security:unlock/2
%
% Steps to encode a chunk:
%     - if chunk is unlocked:
%         - opcua_security:setup_sym/2 or opcua_security:setup_asym/2
%         - opcua_protocole_codec:prepare_chunk/1
%         - opcua_security:prepare/2
%         - opcua_protocole_codec:freeze_chunk/1
%         - opcua_security:lock/2
%     - opcua_protocole_codec:encode_chunk/1

-record(uacp_security_policy, {
    policy_uri                      :: binary(),
    symmetric_signature_algorithm   :: undefined | atom(),
    symmetric_encryption_algorithm  :: undefined | atom(),
    asymmetric_signature_algorithm  :: undefined | atom(),
    asymmetric_encryption_algorithm :: undefined | atom(),
    min_asymmetric_keyLength        :: undefined | non_neg_integer(),
    max_asymmetric_keyLength        :: undefined | non_neg_integer(),
    key_derivation_algorithm        :: undefined | atom(),
    derived_signature_keyLength     :: undefined | non_neg_integer(),
    certificate_signature_algorithm :: undefined | atom(),
    secureChannelNonceLength        :: undefined | non_neg_integer()
}).

-record(uacp_chunk_security, {
    policy_uri,
    sender_cert,
    receiver_thumbprint
}).

-record(uacp_chunk, {
    state                       :: opcua:chunk_state(),
    % Decoded Fields
    message_type                :: opcua:message_type(),
    chunk_type                  :: opcua:chunk_type(),
    channel_id                  :: undefined | 0 | opcua:channel_id(),
    security                    :: undefined | #uacp_chunk_security{} | opcua:token_id(),
    % Fields decoded when unlocked
    request_id                  :: undefined | pos_integer(),
    sequence_num                :: undefined | pos_integer(),
    % Preparation info
    header_size                 :: undefined | non_neg_integer(),
    unlocked_size               :: undefined | non_neg_integer(),
    locked_size                 :: undefined | non_neg_integer(),
    % Raw data
    header                      :: undefined | iodata(),
    body                        :: undefined | iodata()
}).

-record(uacp_message, {
    type                        :: opcua:message_type(),
    sender                      :: undefined | opcua:message_sender(),
    request_id                  :: undefined | pos_integer(),
    node_id                     :: undefined | opcua:node_spec(),
    payload                     :: undefined | term(),
    context                     :: undefined | term()
}).

-record(uacp_connection, {
    pid                         :: pid(),
    space                       :: opcua_space:state(),
    keychain                    :: opcua_keychain:state(),
    self_ident                  :: undefined | opcua_keychain:ident(),
    peer_ident                  :: undefined | opcua_keychain:ident(),
    endpoint                    :: opcua:endpoint(),
    peer                        :: {inet:ip_address(), inet:port_number()},
    sock                        :: {inet:ip_address(), inet:port_number()}
}).
