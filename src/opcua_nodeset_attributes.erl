-module(opcua_nodeset_attributes).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([load/1]).
-export([name/1]).
-export([id/1]).

%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ETS_OPTS, [named_table, protected, {read_concurrency, true}]).
-define(DB_ATTRIBUTE_NAMES, db_attribute_names).
-define(DB_ATTRIBUTE_IDS, db_attribute_ids).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load(FilePath) ->
    ets:new(?DB_ATTRIBUTE_NAMES, ?ETS_OPTS),
    ets:new(?DB_ATTRIBUTE_IDS, ?ETS_OPTS),
    opcua_util_csv:fold(FilePath,
        fun([NameStr, IdStr], ok) ->
            Name = opcua_util:convert_name(NameStr),
            Id = list_to_integer(IdStr),
            ets:insert(?DB_ATTRIBUTE_NAMES, {Id, Name}),
            ets:insert(?DB_ATTRIBUTE_IDS, {Name, Id}),
            ok
        end,
    ok).

name(AttrName) when is_atom(AttrName) -> AttrName;
name(AttrId) when is_integer(AttrId), AttrId > 0 ->
    case ets:lookup(?DB_ATTRIBUTE_NAMES, AttrId) of
        [{_, AttrName}] -> AttrName;
        [] -> undefined
    end.

id(AttrId) when is_integer(AttrId), AttrId > 0 -> AttrId;
id(AttrName) when is_atom(AttrName) ->
    case ets:lookup(?DB_ATTRIBUTE_IDS, AttrName) of
        [{_, AttrId}] -> AttrId;
        [] -> undefined
    end.
