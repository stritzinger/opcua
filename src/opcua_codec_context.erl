%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Codec Context to be used during encoding and decoding to keep track
%%% of the position in the decoded/encoded data, and all the options.
%%%
%%% The functions issue_XXX must be used for all codec related errors,
%%% and the top function MUST either call finalize/3 or catch all the
%%% exceptions and call resolve/3.
%%%
%%% If a codec error could be ignored resulting in a partial result,
%%% the catch_and_continue/6 should be used to optionally support
%%% partial results.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_codec_context).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").
-include("opcua_codec_context.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([new/2]).
-export([first_issue/1]).
-export([finalize/2, finalize/3]).
-export([resolve/3]).
-export([catch_and_continue/6]).
-export([export_issues/1]).
-export([push/3]).
-export([pop/3]).
-export([format_path/1]).
-export([issue/2]).
-export([issue_schema_not_found/2]).
-export([issue_encoding_not_supported/2]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type mode() :: encoding | decoding.
-type issue() :: {generic_codec_error, Reason :: term(), Stack :: binary()}
               | {schema_not_found, NodeId :: opcua:node_id(), Stack :: binary()}
               | {encoding_not_supported, atom(), Stack :: binary()}.
-type issues() :: [] | [issue()].
-type ctx() :: #ctx{}.
-type options() :: #{
    % Address space to use, opcua_nodeset will be use if not specied.
    space => opcua_space:state(),
    % Allows partial decoding. Will returns the encoding/decoding issues as a
    % third element of the returned tupple.
    allow_partial => boolean()
}.

-export_type([mode/0, issue/0, issues/0, ctx/0, options/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Mode, Opts)
  when Mode =:= encoding; Mode =:= decoding ->
    #ctx{
        mode = Mode,
        space = maps:get(space, Opts, opcua_nodeset),
        allow_partial = maps:get(allow_partial, Opts, false)
    }.

first_issue(#ctx{issues = []}) -> undefined;
first_issue(#ctx{issues = Issues}) -> lists:last(Issues).

% Partial result is not yet supported for XML decoding
finalize(#ctx{allow_partial = false, issues = []}, Result) ->
    Result;
finalize(#ctx{allow_partial = false} = Ctx, _Result) ->
    throw_first_issue(Ctx).

finalize(#ctx{allow_partial = false, issues = []}, Result, Remaining) ->
    {Result, Remaining};
finalize(#ctx{allow_partial = false} = Ctx, _Result, _Remaining) ->
    throw_first_issue(Ctx);
finalize(#ctx{allow_partial = true} = Ctx, Result, Remaining) ->
    {Result, Remaining, opcua_codec_context:export_issues(Ctx)}.

-spec resolve(term(), term(), term()) -> no_return().
resolve(throw, {?MODULE, #ctx{} = Ctx}, _ExStack) ->
    throw_first_issue(Ctx);
resolve(Method, Reason, Stack) ->
    erlang:raise(Method, Reason, Stack).

% Handles and exception and optionally recover and continue with the given
% result and data.
% If the context is not configured for partial resolution, the exception
% will bubble up
catch_and_continue(throw, {?MODULE, #ctx{allow_partial = true} = ErrorCtx},
                   _Stack, #ctx{stack = OrigStack}, Result, Data) ->
    {Result, Data, ErrorCtx#ctx{stack = OrigStack}};
catch_and_continue(Method, Reason, Stack, _OrigCtx, _Result, _Data) ->
    erlang:raise(Method, Reason, Stack).

export_issues(#ctx{issues = Issues}) ->
    [{N, R, format_stack(S)} || {N, R, S} <- lists:reverse(Issues)].

% Throws the externally formated version of the first issue from the given context.
throw_first_issue(Ctx) ->
    throw(issue_as_reason(Ctx, first_issue(Ctx))).

push(#ctx{stack = Stack} = Ctx, key, Tag) ->
    Ctx#ctx{stack = [{Tag} | Stack]};
push(#ctx{stack = Stack} = Ctx, field, Tag) ->
    Ctx#ctx{stack = [Tag | Stack]}.

pop(#ctx{stack = [{Tag} | Stack]} = Ctx, key, Tag) ->
    Ctx#ctx{stack = Stack};
pop(#ctx{stack = [Tag | Stack]} = Ctx, field, Tag) ->
    Ctx#ctx{stack = Stack}.

format_path(#ctx{stack = Stack}) -> format_stack(Stack).

-spec issue(ctx(), term()) -> no_return().
issue(Ctx, Reason) ->
    throw_issue(Ctx, generic_codec_error, Reason).

-spec issue_schema_not_found(ctx(), term()) -> no_return().
issue_schema_not_found(Ctx, NodeId) ->
    throw_issue(Ctx, schema_not_found, NodeId).

-spec issue_encoding_not_supported(ctx(), term()) -> no_return().
issue_encoding_not_supported(Ctx, Details) ->
    throw_issue(Ctx, encoding_not_supported, Details).


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_issue(#ctx{issues = Issues, stack = Stack} = Ctx, Name, Params) ->
    Ctx#ctx{issues = [{Name, Params, Stack} | Issues]}.

throw_issue(Ctx, Name, Params) ->
    throw({?MODULE, add_issue(Ctx, Name, Params)}).

format_stack(undefined) -> <<>>;
format_stack(Stack) when is_list(Stack) ->
    iolist_to_binary([format_stack_item(I) || I <- lists:reverse(Stack)]).

format_stack_item({K}) -> [$[, format_stack_value(K), $]];
format_stack_item(V) -> [$., format_stack_value(V)].

format_stack_value(B) when is_binary(B) -> B;
format_stack_value(I) when is_integer(I) -> integer_to_binary(I);
format_stack_value(A) when is_atom(A) -> atom_to_binary(A);
format_stack_value(L) when is_list(L) -> list_to_binary(L).

issue_as_reason(#ctx{}, undefined) -> undefined;
issue_as_reason(#ctx{mode = decoding}, {generic_codec_error, Reason, Stack}) ->
    {bad_decoding_error, {Reason, format_stack(Stack)}};
issue_as_reason(#ctx{mode = encoding}, {generic_codec_error, Reason, Stack}) ->
    {bad_encoding_error, {Reason, format_stack(Stack)}};
issue_as_reason(#ctx{}, {encoding_not_supported, Params, Stack}) ->
    {bad_encoding_not_supported, {encoding_not_supported, Params, format_stack(Stack)}};
issue_as_reason(#ctx{mode = decoding}, {Name, Params, Stack}) ->
    {bad_decoding_error, {Name, Params, format_stack(Stack)}};
issue_as_reason(#ctx{mode = encoding}, {Name, Params, Stack}) ->
    {bad_encoding_error, {Name, Params, format_stack(Stack)}}.
