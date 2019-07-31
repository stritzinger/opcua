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

%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(uacp_security_policy, {
    policy_url :: binary(),
    sender_cert :: undefined | binary(),
    receiver_thumbprint :: undefined | binary()
}).

-record(uacp_chunk, {
    state :: undefined | locked | unlocked,
    % Decoded Fields
    message_type :: opcua_protocol:message_type(),
    chunk_type :: opcua_protocol:chunk_type(),
    channel_id :: undefined | 0 | opcua_protocol:channel_id(),
    security :: undefined | #uacp_security_policy{} | opcua_protocol:token_id(),
    % Fields decoded when unlocked
    request_id :: undefined | pos_integer(),
    sequence_num :: undefined | pos_integer(),
    % Preparation info
    header_size :: undefined | non_neg_integer(),
    unlocked_size :: undefined | non_neg_integer(),
    locked_size :: undefined | non_neg_integer(),
    % Raw data
    header :: undefined | iodata(),
    body :: undefined | iodata()
}).

-record(uacp_message, {
    type :: opcua_protocol:message_type(),
    request_id :: undefined | pos_integer(),
    node_id :: undefined | opcua:node_id(),
    payload :: undefined | term()
}).

-record(uacp_connection, {
    pid :: pid()
}).
