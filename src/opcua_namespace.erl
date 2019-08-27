-module(opcua_namespace).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions

-export([add/1]).
-export([all/0]).
-export([index/1]).
-export([uri/1]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type uri() :: binary().
-type index() :: pos_integer().

-export_type([uri/0]).
-export_type([index/0]).

%%% MACRO %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(DEFAULT, #{undefined => 0}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add(URI) when is_binary(URI); URI == undefined ->
    LockID = {?MODULE, self()},
    true = global:set_lock(LockID, [node()], infinity),
    try
        Namespaces = all(),
        case maps:find(URI, Namespaces) of
            error ->
                Index = maps:size(Namespaces),
                persistent_term:put(?MODULE, maps:put(URI, Index, Namespaces)),
                Index;
            {ok, Index} ->
                error({namespace_exists, URI, Index})
        end
    after
        global:del_lock(LockID, [node()])
    end.

all() -> persistent_term:get(?MODULE, #{undefined => 0}).

index(URI) when is_binary(URI); URI == undefined ->
    case maps:find(URI, all()) of
        {ok, Index} -> Index;
        error       -> error({unknown_uri, URI})
    end.

uri(Index) when is_integer(Index) ->
    case find_key(Index, all()) of
        {ok, URI} -> URI;
        error     -> error({unknown_index, Index})
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


find_key(Value, Map) ->
    maps:fold(
        fun
            (K, V, _Result) when V == Value -> {ok, K};
            (_K, _V, Result)                -> Result
        end,
        error,
        Map
    ).
