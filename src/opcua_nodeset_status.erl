-module(opcua_nodeset_status).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([load/1]).
-export([encode/2]).
-export([decode/2]).
-export([name/1, name/2]).
-export([code/1]).
-export([is_name/1]).
-export([is_code/1]).

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ETS_OPTS, [named_table, protected, {read_concurrency, true}]).
-define(DB_STATUS_CODES, db_status_codes).
-define(DB_STATUS_NAME_TO_CODE, db_status_name_to_code).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load(FilePath) ->
    ets:new(?DB_STATUS_CODES, ?ETS_OPTS),
    ets:new(?DB_STATUS_NAME_TO_CODE, ?ETS_OPTS),
    opcua_util_csv:fold(FilePath,
        fun([NameStr, "0x" ++ CodeStr | DescList], ok) ->
            Name = opcua_util:convert_name(NameStr),
            Code = list_to_integer(CodeStr, 16),
            Desc = iolist_to_binary(lists:join("," , DescList)),
            ets:insert(?DB_STATUS_CODES, {Code, Name, Desc}),
            ets:insert(?DB_STATUS_NAME_TO_CODE, {Name, Code}),
            ok
        end,
    ok).

decode(Status, undefined) when is_integer(Status) ->
    case lookup(Status, undefined) of
        undefined -> {Status, undefined};
        {_, Name, DefaultDesc} -> {Name, DefaultDesc}
    end;
decode(Status, Desc) ->
    case lookup(Status, undefined) of
        undefined -> {Status, Desc};
        {_, Name, _} -> {Name, Desc}
    end.

encode(Status, undefined) ->
    case lookup(Status, undefined) of
        undefined when is_integer(Status) -> {Status, undefined};
        {Code, _, DefaultDesc} -> {Code, DefaultDesc}
    end;
encode(Status, Desc) ->
    case lookup(Status, undefined) of
        undefined when is_integer(Status) -> {Status, Desc};
        {Code, _, DefaultDesc} -> {Code, DefaultDesc}
    end.

name(Status) ->
    case lookup(Status, undefined) of
        undefined -> throw(bad_internal_error);
        {_, Name, _} -> Name
    end.

name(Status, Default) ->
    {_, Name, _} = lookup(Status, Default),
    Name.

code(Status) ->
    case lookup(Status, undefined) of
        undefined -> throw(bad_internal_error);
        {Code, _, _} -> Code
    end.

is_name(StatusName) when is_atom(StatusName) ->
    case ets:lookup(?DB_STATUS_NAME_TO_CODE, StatusName) of
        [] -> false;
        _ -> true
    end;
is_name(_Other) ->
    false.

is_code(StatusCode) when is_atom(StatusCode) ->
    case ets:lookup(?DB_STATUS_CODES, StatusCode) of
        [] -> false;
        _ -> true
    end;
is_code(_Other) ->
    false.

%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

lookup(good, _Default) -> {0, good, <<"Good">>};
lookup(0, _Default) -> {0, good, <<"Good">>};
lookup(StatusName, Default) when is_atom(StatusName) ->
    case ets:lookup(?DB_STATUS_NAME_TO_CODE, StatusName) of
        [{_, StatusCode}] -> lookup(StatusCode, Default);
        [] -> Default
    end;
lookup(StatusCode, Default) when is_integer(StatusCode), StatusCode >= 0 ->
    case ets:lookup(?DB_STATUS_CODES, StatusCode) of
        [{_, Name, Desc}] -> {StatusCode, Name, Desc};
        [] -> Default
    end.
