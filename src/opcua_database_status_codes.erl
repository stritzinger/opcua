-module(opcua_database_status_codes).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([load/1]).
-export([lookup/1]).
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
            Name = list_to_atom(NameStr),
            Code = list_to_integer(CodeStr, 16),
            Desc = iolist_to_binary(lists:join("," , DescList)),
            ets:insert(?DB_STATUS_CODES, {Code, Name, Desc}),
            ets:insert(?DB_STATUS_NAME_TO_CODE, {Name, Code}),
            ok
        end,
    ok).

lookup(StatusName) when is_atom(StatusName) ->
    case ets:lookup(?DB_STATUS_NAME_TO_CODE, StatusName) of
        [{_, StatusCode}] -> lookup(StatusCode);
        [] -> undefined
    end;
lookup(StatusCode) when is_integer(StatusCode), StatusCode >= 0 ->
    case ets:lookup(?DB_STATUS_CODES, StatusCode) of
        [{_, Name, Desc}] -> {StatusCode, Name, Desc};
        [] -> undefined
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
