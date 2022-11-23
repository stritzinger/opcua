-module(opcua_util_bterm).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([fold/3]).
-export([save/2]).


%%% MACROS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Number of bytes to use as binary term size header
-define(SIZE_HEADER, 32).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(FilePath, Fun, Acc) ->
    case file:open(FilePath, [read, raw, binary]) of
        {error, Reason} -> error({file_error, Reason, FilePath});
        {ok, File} ->
            try load_terms(File, Fun, file:read(File, 4), Acc)
            after file:close(File)
            end
    end.

% @doc Saves terms into the file with given path.
% The terms can be given as list of terms, or a fold function
% can be given, that will be called with a consumer function that
% will store the term.
save(FilePath, TermsOrFoldFun) ->
    case file:open(FilePath, [write, raw, binary]) of
        {error, Reason} -> error({file_error, Reason, FilePath});
        {ok, File} ->
            try save_terms(File, TermsOrFoldFun)
            after file:close(File)
            end
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_terms(_File, _Fun, eof, Acc) -> Acc;
load_terms(File, Fun, {ok, <<Size:?SIZE_HEADER>>}, Acc) ->
    {ok, Bin} = file:read(File, Size),
    Acc2 = Fun(binary_to_term(Bin), Acc),
    load_terms(File, Fun, file:read(File, 4), Acc2).

save_terms(File, TermOrFoldFun) ->
    save_terms(File, TermOrFoldFun, 0).

save_terms(_File, [], Count) -> Count;
save_terms(File, [Term | Rest], Count) ->
    save_term(File, Term),
    save_terms(File, Rest, Count + 1);
save_terms(File, FoldFun, Count) when is_function(FoldFun, 2) ->
    StoreFun = fun(T, C) -> save_term(File, T), C + 1 end,
    FoldFun(StoreFun, Count).

save_term(File, Term) ->
    Bin = term_to_binary(Term),
    file:write(File, [<<(byte_size(Bin)):?SIZE_HEADER>>, Bin]).
