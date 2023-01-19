-module(opcua_pubsub_data_set_reader).

-export([new/1]).
-export([is_interested/2]).
-export([process_messages/2]).
-export([create_target_variables/2]).

-include("opcua_pubsub.hrl").

-record(state, {
    % status = operational,
    name                :: binary(),
    publisher_id,
    publisher_id_type,
    writer_group_id,
    data_set_writer_id,
    data_set_metadata  :: #data_set_metadata{},

    subscribed_dataset :: undefined | [#target_variable{}] | #dataset_mirror{}
}).


new(#data_set_reader_config{
        name = Name,
        publisher_id = PubId,
        publisher_id_type = PubIdType,
        writer_group_id = WGId,
        data_set_writer_id = DataSetWriterId,
        data_set_metadata = DataSetMetadata}) ->
    {ok, #state{name = Name, publisher_id = PubId,
        publisher_id_type = PubIdType,
        writer_group_id = WGId,
        data_set_writer_id = DataSetWriterId,
        data_set_metadata = DataSetMetadata}}.

create_target_variables(Variables, State) ->
    io:format("Target Vars, ~p~n", [Variables]),
    {ok, State#state{subscribed_dataset = Variables}}.

% Checklist:
% writergroup match
% payload contains at least one message from the desired writer
is_interested(#{
            publisher_id := Pub_id,
            group_header := #{
                writer_group_id := WG_id
            },
            payload_header := #{
                data_set_writer_ids := DSW_ids
            }
        } = _Headers,
        #state{ publisher_id = Pub_id,
                writer_group_id = WG_id,
                data_set_writer_id = DataSetWriterId}) ->
    lists:member(DataSetWriterId, DSW_ids);
is_interested(_, _) ->
    false.

process_messages([], State) -> State;
process_messages([{DataSetWriterId, {Header, Data}} | Messages],
        #state{data_set_writer_id = DataSetWriterId} = State) ->
    io:format("~p handling ~p~n",[?MODULE,Data]),
    % TODO: add msg version check, state machine management ecc..
    {DataSet, NewState} = decode_data_set_message(Header, Data, State),
    NewState2 = update_target_variables(DataSet, NewState),
    process_messages(Messages, NewState2);
process_messages([ _| Messages], State) ->
    process_messages(Messages, State).

decode_data_set_message( % case of invalid message
                #{data_set_flags1 =>
                    #{data_set_msg_valid => 0}},
                _, S) ->
    {[], S};
decode_data_set_message(
    #{
        data_set_flags1 => #{
            data_set_msg_valid => 1,
            field_encoding => Encoding,
            data_set_msg_seq_num => DataSetMsgSeqNum,
            status => Status,
            config_ver_minor_ver => ConfigVerMajorVer,
            config_ver_major_ver => ConfigVerMinorVer,
            data_set_flags2 => DataSetFlags2
        },
        data_set_flags2 => #{
            msg_type => MessageType,
            timestamp => Timestamp,
            picoseconds => PicoSeconds
        },
        data_set_seq_num => DataSetSeqNum,
        timestamp => Timestamp,
        picoseconds => Picoseconds,
        status => Status,
        config_ver_major_ver => ConfigVerMajorVer,
        config_ver_minor_ver => ConfigVerMinorVer
    },
    Data,
    #state{
        data_set_metadata = #data_set_metadata{
            configuration_version = {MajorV, MinorV}
        }} = S) ->




update_target_variables([], S) -> S;
update_target_variables(DataSet, S) -> S.
