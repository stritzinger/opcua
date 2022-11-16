%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Codec Context to be used during encoding and decoding to keep track
%%% of the position in the decoded/encoded data, and all the options.
%%%
%%% The functions issue_XXX must be used for all codec related errors,
%%% and the top function MUST catch all the exception and call either
%%% catch_and_throw/3 or catch_first_issue/3 to resolve the issue format.
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
-export([export_issues/1]).
-export([throw_first_issue/1]).
-export([first_issue_reason/1]).
-export([catch_and_throw/3]).
-export([catch_first_issue/3]).
-export([catch_issues/3]).
-export([catch_and_continue/6]).
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

-export_type([mode/0, issue/0, issues/0, ctx/0]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Mode, Safe)
  when is_boolean(Safe), Mode =:= encoding;
       is_boolean(Safe), Mode =:= decoding ->
    #ctx{mode = Mode, allow_partial = Safe}.

export_issues(#ctx{issues = Issues}) ->
    [{N, R, format_stack(S)} || {N, R, S} <- lists:reverse(Issues)].

% Throws the externally formated version of the first issue from the given context.
throw_first_issue(Ctx) ->
    throw(issue_as_reason(Ctx, first_issue(Ctx))).

% Returns the externally formated version of the first issue from the given context.
first_issue_reason(Ctx) ->
    issue_as_reason(Ctx, first_issue(Ctx)).

% Handles an exception and externally format the context first issue if
% the exception was thrown by the context itself.
% All other exceptions are re-thrown as-is.
catch_and_throw(throw, {?MODULE, #ctx{} = Ctx}, _Stack) ->
    throw_first_issue(Ctx);
catch_and_throw(Method, Reason, Stack) ->
    erlang:raise(Method, Reason, Stack).

% Handles an exception and returns the externally formated version of the first
% issue if the exception was thrown by the context itself.
% All other exceptions are re-thrown as-is.
catch_first_issue(throw, {?MODULE, #ctx{} = Ctx}, _Stack) ->
    first_issue(Ctx);
catch_first_issue(Method, Reason, Stack) ->
    erlang:raise(Method, Reason, Stack).

% Handles an exception and returns all the externally formated issues.
% All other exceptions are re-thrown as-is.
catch_issues(throw, {?MODULE, #ctx{} = Ctx}, _Stack) ->
    export_issues(Ctx);
catch_issues(Method, Reason, Stack) ->
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

first_issue(#ctx{issues = []}) -> undefined;
first_issue(#ctx{issues = Issues}) -> lists:last(Issues).

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
