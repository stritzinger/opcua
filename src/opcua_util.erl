-module(opcua_util).

-export([trace/2, trace/3, trace_clear/0, date_time/0,
         nonce/0, date_time_to_rfc3339/1, bin_to_hex/1,
         guid_to_hex/1, bin_to_hex/2, hex_to_bin/1,
         get_node_id/2, parse_node_id/1, get_attr/2,
         get_attr/3, get_int/2, get_int/3, convert_name/1,
         parse_range/1]).


-spec trace(atom(), atom()) -> non_neg_integer().
trace(Mod, Fun) ->
    trace(Mod, Fun, 10).

-spec trace(atom(), atom(), non_neg_integer()) -> non_neg_integer().
trace(Mod, Fun, N) ->
    recon_trace:calls({Mod, Fun, return_trace}, N).

-spec trace_clear() -> ok.
trace_clear() ->
    recon_trace:clear().

nonce() ->
    crypto:strong_rand_bytes(32).

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

%% helpers for parsing XML information models using a SAX parser
get_node_id(Key, Attributes) ->
    case get_attr(Key, Attributes) of
        undefined       -> undefined;
        NodeIdString    -> parse_node_id(NodeIdString)
    end.

parse_node_id(String) ->
    [_, String1] = string:split(String, "="),
    opcua_codec:node_id(list_to_integer(String1)).

get_attr(Key, Attributes) ->
    get_attr(Key, Attributes, undefined).

get_attr(Key, Attributes, Default) ->
    case lists:keyfind(Key, 3, Attributes) of
        false -> Default;
        Value -> element(4, Value)
    end.

get_int(Key, Attributes) ->
    get_int(Key, Attributes, undefined).

get_int(Key, Attributes, Default) ->
    case get_attr(Key, Attributes, Default) of
        Default     -> Default;
        StringInt   -> list_to_integer(StringInt)
    end.

%% converts CamelCase strings to snake_case atoms
convert_name(Name) when is_binary(Name) ->
    convert_name(binary_to_list(Name));
convert_name([FirstLetter|Rest]) ->
    list_to_atom(
      string:lowercase([FirstLetter]) ++ 
        lists:flatten(
          lists:map(fun(Char) ->
              case string:uppercase([Char]) of
                  [Char]  -> "_" ++ string:lowercase([Char]);
                  _     -> Char
              end
          end, Rest))).

parse_range(undefined) -> undefined;
parse_range(<<>>) -> undefined;
parse_range(Range) -> parse_range_dims(Range, []).

%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_range_dims(<<>>, Acc) -> lists:reverse(Acc);
parse_range_dims(Ranges, Acc) ->
    {Item, Rest} = parse_range_min(Ranges, []),
    parse_range_dims(Rest, [Item | Acc]).

parse_range_min(<<>>, Acc) ->
    {list_to_integer(lists:reverse(Acc)), <<>>};
parse_range_min(<<",", Rest/binary>>, Acc) ->
    {list_to_integer(lists:reverse(Acc)), Rest};
parse_range_min(<<":", Rest/binary>>, Acc) ->
    parse_range_max(Rest, list_to_integer(lists:reverse(Acc)), []);
parse_range_min(<<C, Rest/binary>>, Acc) ->
    parse_range_min(Rest, [C | Acc]).

parse_range_max(<<>>, Min, Acc) ->
    validate_range(Min, list_to_integer(lists:reverse(Acc)), <<>>);
parse_range_max(<<",", Rest/binary>>, Min, Acc) ->
    validate_range(Min, list_to_integer(lists:reverse(Acc)), Rest);
parse_range_max(<<C, Rest/binary>>, Min, Acc) ->
    parse_range_max(Rest, Min, [C | Acc]).

validate_range(Min, Max, Rest) when Min >= 0, Max > Min -> {{Min, Max}, Rest}.
