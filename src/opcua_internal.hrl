
%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%-- Codec Records --------------------------------------------------------------

-record(opcua_variant, {
    type                        :: opcua:builtin_type(),
    value                       :: term()
}).

-record(opcua_diagnostic_info, {
    symbolic_id                 :: integer(),
    namespace_uri               :: integer(),
    locale                      :: integer(),
    localized_text              :: integer(),
    additional_info             :: binary(),
    inner_status_code           :: integer(),
    inner_diagnostic_info       :: term()
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
    policy_url                  :: binary(),
    sender_cert                 :: opcua:optional(binary()),
    receiver_thumbprint         :: opcua:optional(binary())
}).

-record(uacp_chunk, {
    state                       :: opcua:chunk_state(),
    % Decoded Fields
    message_type                :: opcua:message_type(),
    chunk_type                  :: opcua:chunk_type(),
    channel_id                  :: undefined | 0 | opcua:channel_id(),
    security                    :: undefined | #uacp_security_policy{} | opcua:token_id(),
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
    request_id                  :: undefined | pos_integer(),
    node_id                     :: undefined | opcua:node_id(),
    payload                     :: undefined | term()
}).

-record(uacp_connection, {
    pid                         :: pid()
}).
