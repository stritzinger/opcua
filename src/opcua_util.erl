-module(opcua_util).

-export([date_time/0, date_time_to_rfc3339/1, bin_to_hex/1, guid_to_hex/1,
	 bin_to_hex/2, hex_to_bin/1]).

date_time() ->
	Now = erlang:system_time(nanosecond),
	NowUTC = calendar:system_time_to_universal_time(Now, nanosecond),
	Seconds1 = calendar:datetime_to_gregorian_seconds(NowUTC),
	Seconds2 = calendar:datetime_to_gregorian_seconds({{1601,1,1}, {0,0,0}}),
	round((erlang:convert_time_unit(Seconds1 - Seconds2, second, nanosecond) + (Now rem 1000000000)) / 100).

date_time_to_rfc3339(DateTime) ->
	Seconds1 = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
	Seconds2 = calendar:datetime_to_gregorian_seconds({{1601,1,1},{0,0,0}}),
	Diff = erlang:convert_time_unit(Seconds1 - Seconds2, second, nanosecond),
	calendar:system_time_to_rfc3339(DateTime*100 - Diff, [{unit, nanosecond}]).

guid_to_hex(<<D1:4/binary, D2:2/binary, D3:2/binary, D41:2/binary, D42:6/binary>>) ->
	lists:concat([bin_to_hex(D1), "-", bin_to_hex(D2), "-", bin_to_hex(D3),
		      "-", bin_to_hex(D41), "-", bin_to_hex(D42)]).

bin_to_hex(Bin) ->
	bin_to_hex(Bin, <<>>).

bin_to_hex(Bin, Sep) ->
	lists:flatten([io_lib:format("~2.16.0B~s",[X, Sep]) || <<X:8>> <= Bin]).

hex_to_bin(S) ->
        hex_to_bin(lists:flatten(string:tokens(S, " ")), []).
hex_to_bin([], Acc) ->
        list_to_binary(lists:reverse(Acc));
hex_to_bin([X,Y|T], Acc) ->
        {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
        hex_to_bin(T, [V | Acc]);
hex_to_bin([X|T], Acc) ->
        {ok, [V], []} = io_lib:fread("~16u", lists:flatten([X,"0"])),
        hex_to_bin(T, [V | Acc]).
