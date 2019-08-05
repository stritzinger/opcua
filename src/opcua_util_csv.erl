-module(opcua_util_csv).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([fold/3]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(FilePath, Fun, Acc) ->
    case file:open(FilePath, [read, raw]) of
        {error, Reason} -> error({file_error, Reason, FilePath});
        {ok, File} ->
            try parse(File, file:read_line(File), Fun, Acc)
            after file:close(File)
            end
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse(_File, eof, _Fun, Acc) -> Acc;
parse(File, {ok, Line}, Fun, Acc) ->
    parse(File, file:read_line(File), Fun, parse_line(Line, Fun, Acc)).

parse_line(Line, Fun, Acc) -> parse_line(Line, Fun, Acc, []).

parse_line([], Fun, Acc, Fields) -> Fun(lists:reverse(Fields), Acc);
parse_line("\n", Fun, Acc, Fields) -> Fun(lists:reverse(Fields), Acc);
parse_line(Line, Fun, Acc, Fields) ->
    {Field, Rest} = parse_field(Line),
    parse_line(Rest, Fun, Acc, [Field | Fields]).

parse_field(Line) -> parse_field(Line, []).

parse_field([], Acc) -> {lists:reverse(Acc), []};
parse_field("\n", Acc) -> {lists:reverse(Acc), []};
parse_field([$" | Line], Acc) -> parse_quoted(Line, Acc);
parse_field([$, | Line], Acc) -> {lists:reverse(Acc), Line};
parse_field([C | Line], Acc) -> parse_field(Line, [C | Acc]).

parse_quoted([$", $" | Line], Acc) -> parse_quoted(Line, [$" | Acc]);
parse_quoted([$" | Line], Acc) -> parse_field(Line, Acc);
parse_quoted([C | Line], Acc) -> parse_quoted(Line, [C | Acc]).
