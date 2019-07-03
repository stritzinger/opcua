-module(opcua_oacp).

-export([decode/1, encode/1]).

decode(<<Type:3/binary, Reserved:1/binary, _MessageSize:4/binary, Bin/binary>>) ->
	Decoded = case Type of
			<<"HEL">> -> decode_hello(Bin);
			<<"ACK">> -> decode_ack(Bin);
			<<"ERR">> -> decode_err(Bin);
			<<"RHE">> -> decode_rhe(Bin)
		  end,
	case Decoded of
		{ok, Map, _} ->
			Map1 = Map#{message_type => Type, reserved => Reserved},
			{ok, Map1};
		Error ->
			Error
	end.

decode_hello(Bin) ->
	Types = [{protocol_version, uint32},
		 {receive_buffer_size, uint32},
		 {send_buffer_size, uint32},
		 {max_message_size, uint32},
		 {max_chunk_count, uint32},
		 {endpoint_url, string}],
	opcua_binary:decode(Types, Bin).

decode_ack(Bin) ->
	Types = [{protocol_version, uint32},
		 {receive_buffer_size, uint32},
		 {send_buffer_size, uint32},
		 {max_message_size, uint32},
		 {max_chunk_count, uint32}],
	opcua_binary:decode(Types, Bin).

decode_err(Bin) ->
	Types = [{error, uint32},
		 {reason, string}],
	opcua_binary:decode(Types, Bin).

decode_rhe(Bin) ->
	Types = [{error, uint32},
		 {reason, string}],
	opcua_binary:decode(Types, Bin).

encode(_Message) -> ok.
