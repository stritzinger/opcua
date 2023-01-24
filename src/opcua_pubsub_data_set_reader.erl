-module(opcua_pubsub_data_set_reader).

-export([new/1]).
-export([is_interested/2]).
-export([process_messages/2]).
-export([create_target_variables/2]).

-include("opcua.hrl").
-include("opcua_pubsub.hrl").
-include_lib("kernel/include/logger.hrl").

-record(state, {
    state = operational :: operational | error | enabled | paused,
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
        data_set_metadata = set_metadata_fields_ids(DataSetMetadata)}}.

create_target_variables(Variables, State) ->
    {ok, State#state{subscribed_dataset = set_tgt_var_ids(Variables)}}.

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
    % io:format("~p handling ~p~n",[?MODULE,Header]),
    % TODO: add msg version check, state machine management ecc..
    {DataSet, NewState} = decode_data_set_message(Header, Data, State),
    NewState2 = update_subscribed_dataset(DataSet, NewState),
    process_messages(Messages, NewState2);
process_messages([ _| Messages], State) ->
    process_messages(Messages, State).

decode_data_set_message( % case of invalid message
                #{data_set_flags1 :=
                    #{data_set_msg_valid := 0}},
                _, S) ->
    {[], S};
decode_data_set_message(
    #{
        data_set_flags1 := #{
            data_set_msg_valid := 1,
            field_encoding := Encoding,
            data_set_msg_seq_num := _,
            status := _,
            config_ver_minor_ver := _,
            config_ver_major_ver := _,
            data_set_flags2 := _
        },
        data_set_flags2 := #{
            msg_type := MessageType, % keyframe / deltaframe / event  ecc...
            timestamp := _,
            picoseconds := _
        },
        data_set_seq_num := _,
        timestamp := _,
        picoseconds := _,
        status := _,
        config_ver_major_ver := _,
        config_ver_minor_ver := _
    },
    Data,
    #state{
        data_set_metadata = #data_set_metadata{
            fields = FieldsMetaData,
            configuration_version = _Ver}
        } = S) ->
    case decode_fields(Encoding, MessageType, FieldsMetaData, Data) of
        {error, E} ->
            ?LOG_ERROR("Failure decoding DataSetMessageFields: ~p",[E]),
            {[], S#state{state = error}};
        DataSet -> {DataSet, S}
    end.

decode_fields(Encoding, data_key_frame, FieldsMetaData, Data) ->
    {FieldCount, FieldsBin} = opcua_codec_binary_builtin:decode(uint16, Data),
    decode_keyframe(Encoding, FieldsMetaData, FieldCount, FieldsBin, []);
decode_fields(_Encoding, _MessageType, _Fields, _Data) ->
    error(bad_not_implemented).

decode_keyframe( _, _, _, <<>>, DataSet) -> lists:reverse(DataSet);
decode_keyframe(Encoding, [FieldMD|NextMDMD], FieldCount, Binary, DataSet) ->
    {Decoded, Rest} = opcua_pubsub_uadp:decode_data_set_message_field(Encoding,
                                                                      FieldMD,
                                                                      Binary),
    Data = {FieldMD, Decoded},
    case Decoded of
        {error, E} -> {error, E};
        _ -> decode_keyframe(Encoding, NextMDMD, FieldCount-1, Rest, [Data|DataSet])
    end.

update_subscribed_dataset([], #state{state = error} = S) -> S; % skip
update_subscribed_dataset(_DataSet, #state{ subscribed_dataset = Sub })
    when #dataset_mirror{} == Sub ->
    error(dataset_mirror_not_implemented);
update_subscribed_dataset(DataSet, #state{subscribed_dataset = TGT_vars} = S)
    when is_list(TGT_vars)->
    ok = update_target_variables(DataSet, TGT_vars),
    S.

update_target_variables([], TGT_vars) -> ok;
update_target_variables([{FieldMD, Variable}|DataSet], TGT_vars) ->
    FieldId = FieldMD#data_set_field_metadata.data_set_field_id,
    [TGT|_] = [ Var || #target_variable{data_set_field_id = DataSetFieldId} = Var
                    <- TGT_vars, DataSetFieldId == FieldId],
    TargetNodeId = TGT#target_variable.target_node_id,
    AttrId = TGT#target_variable.attribute_id,
    update_tgt_var_attribute(TargetNodeId, AttrId, Variable),
    ok.

update_tgt_var_attribute(TargetNodeId, ?UA_ATTRIBUTEID_VALUE,
                                            #opcua_variant{value = Value}) ->
    opcua_server:set_value(TargetNodeId, Value).

set_metadata_fields_ids(#data_set_metadata{fields = Fields} = DSMD) ->
    Ids = lists:seq(0, length(Fields) - 1),
    DSMD#data_set_metadata{fields =
        [F#data_set_field_metadata{data_set_field_id = I}
                || {I,F} <- lists:zip(Ids, Fields)]}.

set_tgt_var_ids(Varables) ->
    Ids = lists:seq(0, length(Varables) - 1),
    [V#target_variable{data_set_field_id = I} || {I,V} <- lists:zip(Ids, Varables)].
