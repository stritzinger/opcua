-module(opcua_codec_binary).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua_codec.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([decode/2, decode/3]).
-export([encode/2, encode/3]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


decode(Spec, Data) -> decode(#{}, Spec, Data).


decode(Schema, Spec, Data) ->
    try decode_any(Schema, Spec, Data) of
        {Result, Rest} -> {ok, Result, Rest}
    catch
        _:decoding_error -> {error, decoding_error}
    end.


encode(Spec, Data) -> encode(#{}, Spec, Data).


encode(Schema, Spec, Data) ->
    try encode_any(Schema, Spec, Data) of
        {Result, Rest} -> {ok, Result, Rest}
    catch
        _:encoding_error -> {error, encoding_error}
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_any(_Schema, Spec, Data) when ?IS_BUILTIN_TYPE(Spec) ->
    decode_builtin(Spec, Data);

decode_any(_Schema, Spec, _Data) when is_atom(Spec) ->
    error(not_implemented);

decode_any(Schema, Spec, Data) when is_list(Spec) ->
    decode_list(Schema, Spec, Data, []).


decode_builtin(Type, Data) ->
    opcua_codec_binary_builtin:decode(Type, Data).


decode_list(_Schema, [], Data, Acc) ->
    {lists:reverse(Acc), Data};

decode_list(Schema, [Spec | Rest], Data, Acc) ->
    {Value, Data2}  = decode_any(Schema, Spec, Data),
    decode_list(Schema, Rest, Data2, [Value | Acc]).


encode_any(_Schema, Spec, Data) when ?IS_BUILTIN_TYPE(Spec) ->
    encode_builtin(Spec, Data);

encode_any(_Schema, Spec, _Data) when is_atom(Spec) ->
    error(not_implemented);

encode_any(Schema, Spec, Data) when is_list(Spec) ->
    encode_list(Schema, Spec, Data, []).


encode_builtin(Type, Data) ->
    {opcua_codec_binary_builtin:encode(Type, Data), undefined}.


encode_list(_Schema, [], Data, Acc) ->
    {lists:reverse(Acc), Data};

encode_list(Schema, [Spec | Rest], [Value | Data], Acc) ->
    {Result, _} = encode_any(Schema, Spec, Value),
    encode_list(Schema, Rest, Data, [Result | Acc]).
