-module(opcua_protocol_uacp).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua_conn.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([init/1]).
-export([handle_message/5]).


%%% TYPES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
    ver :: undefined | pos_integer(),
    max_res_chunk_size :: undefined | pos_integer(),
    max_req_chunk_size :: undefined | pos_integer(),
    max_msg_size :: undefined | pos_integer(),
    max_chunk_count :: undefined | pos_integer(),
    endpoint_url :: undefined | binary()
}).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Conn) ->
    {ok, Conn, #state{}}.


handle_message(<<"HEL">>, <<"F">>, Data, Conn, State) ->
    ?LOG_DEBUG("Handling UACP HEL message: ~p", [Data]),
    case handle_hello(Data, State) of
        {error, _Reason} = Error -> Error;
        {ok, State2} -> send_acknowledge(Conn, State2)
    end;

handle_message(<<"ERR">>, <<"F">>, Data, Conn, State) ->
    ?LOG_DEBUG("Handling UACP ERR message: ~p", [Data]),
    {ok, Conn, State};

handle_message(<<"OPN">>, <<"F">>, Data, Conn, State) ->
    ?LOG_DEBUG("Handling UACP OPN message: ~p", [Data]),
    {ok, Conn, State};

handle_message(<<"CLO">>, <<"F">>, Data, Conn, State) ->
    ?LOG_DEBUG("Handling UACP CLO message: ~p", [Data]),
    {ok, Conn, State};

handle_message(<<"MSG">>, ChunkType, Data, Conn, State)
  when ChunkType =:= <<"F">>; ChunkType =:= <<"C">>, ChunkType =:= <<"A">> ->
    ChunkTypeName = chunk_type_name(ChunkType),
    ?LOG_DEBUG("Handling UACP ~s MSG message: ~p", [ChunkTypeName, Data]),
    {ok, Conn, State};

handle_message(Type, ChunkType, Data, Conn, State) ->
    ?LOG_WARNING("Unexpected UACP transport message ~p/~p: ~p",
                 [Type, ChunkType, Data]),
    {ok, Conn, State}.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

send(Type, Data, Conn, State) ->
    case opcua_conn:send(Type, Data, Conn) of
        {ok, Conn2} -> {ok, Conn2, State};
        {error, _Reason} = Error -> Error
    end.


decode(Spec, Data) ->
    case opcua_codec_binary:decode(Spec, Data) of
        {error, _Reason} = Error -> Error;
        {ok, Result, <<>>} -> {ok, Result};
        {ok, Result, Extra} ->
            ?LOG_WARNING("UACP message with ~w extra unexpected bytes",
                         [byte_size(Extra)]),
            {ok, Result}
    end.


encode(Spec, Data) ->
    case opcua_codec_binary:encode(Spec, Data) of
        {error, _Reason} = Error -> Error;
        {ok, Result, _} -> {ok, Result}
    end.


handle_hello(Data, State) ->
    Spec = [uint32, uint32, uint32, uint32, uint32, string],
    case decode(Spec, Data) of
        {error, _Reason} = Error -> Error;
        {ok, [Ver, MaxResSize, MaxReqSize, MaxMsgSize, MaxChunk, Url]} ->
            ?LOG_DEBUG("Received UACP HEL message: ver=~w, "
                       "max_res_chunk_size=~w, max_req_chunk_size=~w, "
                       "max_msg_size=~w, max_chunk_count=~w, endpoint_url=~p" ,
                       [Ver, MaxResSize, MaxReqSize, MaxMsgSize, MaxChunk, Url]),
            State2 = State#state{
                ver = Ver,
                max_res_chunk_size = MaxResSize,
                max_req_chunk_size = MaxReqSize,
                max_msg_size = MaxMsgSize,
                max_chunk_count = MaxChunk,
                endpoint_url = Url
            },
            {ok, State2}
    end.


send_acknowledge(Conn, State) ->
    #state{
        ver = Ver,
        max_res_chunk_size = MaxResSize,
        max_req_chunk_size = MaxReqSize,
        max_msg_size = MaxMsgSize,
        max_chunk_count = MaxChunk
    } = State,
    ?LOG_DEBUG("Sending UACP ACK message: ver=~w, "
               "max_res_chunk_size=~w, max_req_chunk_size=~w, "
               "max_msg_size=~w, max_chunk_count=~w",
               [Ver, MaxResSize, MaxReqSize, MaxMsgSize, MaxChunk]),
    Spec = [uint32, uint32, uint32, uint32, uint32],
    Data = [Ver, MaxResSize, MaxReqSize, MaxMsgSize, MaxChunk],
    case encode(Spec, Data) of
        {error, _Reason} = Error -> Error;
        {ok, Msg} -> send(<<"ACK">>, Msg, Conn, State)
    end.


chunk_type_name(<<"F">>) -> final;
chunk_type_name(<<"C">>) -> intermediate;
chunk_type_name(<<"A">>) -> aborted.
