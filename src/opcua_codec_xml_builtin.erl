-module(opcua_codec_xml_builtin).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([decode/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode(localized_text, []) ->
    #opcua_localized_text{};
decode(localized_text, [{<<"Locale">>, _, [L]}, {<<"Text">>, _, [T]}]) ->
    #opcua_localized_text{locale = L, text = T};
decode(localized_text, [{<<"Locale">>, _, [L]}]) ->
    #opcua_localized_text{locale = L};
decode(localized_text, [{<<"Text">>, _, [T]}]) ->
    #opcua_localized_text{text = T};
decode(xml, _Bin) ->
    throw(bad_not_implemented);
decode(diagnostic_info, _Data) ->
    throw(bad_not_implemented);
decode(qualified_name, _Data) ->
    throw(bad_not_implemented);
decode(boolean, [<<"true">>]) ->
    true;
decode(boolean, [<<"false">>]) ->
    false;
decode(sbyte, [Bin]) ->
    try binary_to_integer(Bin) of
        Result when Result >= -128, Result =< 127 ->
            Result;
        _ -> throw(bad_decoding_error)
    catch
        error:badarg -> throw(bad_decoding_error)
    end;
decode(byte, [Bin]) ->
    try binary_to_integer(Bin) of
        Result when Result >= 0, Result =< 255 ->
            Result;
        _ -> throw(bad_decoding_error)
    catch
        error:badarg -> throw(bad_decoding_error)
    end;
decode(uint16, [Bin]) ->
    try binary_to_integer(Bin) of
        Result when Result >= 0, Result =< 65535 ->
            Result;
        _ -> throw(bad_decoding_error)
    catch
        error:badarg -> throw(bad_decoding_error)
    end;
decode(uint32, [Bin]) ->
    try binary_to_integer(Bin) of
        Result when Result >= 0, Result =< 4294967295 ->
            Result;
        _ -> throw(bad_decoding_error)
    catch
        error:badarg -> throw(bad_decoding_error)
    end;
decode(uint64, [Bin]) ->
    try binary_to_integer(Bin) of
        Result when Result >= 0, Result =< 18446744073709551615 ->
            Result;
        _ -> throw(bad_decoding_error)
    catch
        error:badarg -> throw(bad_decoding_error)
    end;
decode(int16, [Bin]) ->
    try binary_to_integer(Bin) of
        Result when Result >= -32768, Result =< 32767 ->
            Result;
        _ -> throw(bad_decoding_error)
    catch
        error:badarg -> throw(bad_decoding_error)
    end;
decode(int32, [Bin]) ->
    try binary_to_integer(Bin) of
        Result when Result >= -2147483648, Result =< 2147483647 ->
            Result;
        _ -> throw(bad_decoding_error)
    catch
        error:badarg -> throw(bad_decoding_error)
    end;
decode(int64, [Bin]) ->
    try binary_to_integer(Bin) of
        Result when Result >= -9223372036854775808, Result =< 9223372036854775807 ->
            Result;
        _ -> throw(bad_decoding_error)
    catch
        error:badarg -> throw(bad_decoding_error)
    end;
decode(float, [<<"INF">>]) ->
    inf;
decode(float, [<<"-INF">>]) ->
    '-inf';
decode(float, [<<"NaN">>]) ->
    nan;
decode(float, [Bin]) ->
    try binary_to_float(Bin)
    catch error:badarg -> throw(bad_decoding_error)
    end;
decode(double, [<<"INF">>]) ->
    inf;
decode(double, [<<"-INF">>]) ->
    '-inf';
decode(double, [<<"NaN">>]) ->
    nan;
decode(double, [Bin]) ->
    try binary_to_float(Bin)
    catch error:badarg -> throw(bad_decoding_error)
    end;
decode(string, []) ->
    <<"">>;
decode(string, [Bin]) ->
    Bin;
decode(date_time, [Bin])->
    try opcua_util:iso_to_datetime(Bin)
    catch error:badarg -> throw(bad_decoding_error)
    end;
decode(guid, [Bin]) ->
    try uuid:string_to_uuid(Bin)
    catch error:badarg -> throw(bad_decoding_error)
    end;
decode(status_code, [Bin]) ->
    decode(uint32, Bin);
decode(byte_string, [Bin]) ->
    try base64:decode(Bin)
    catch error:badarg -> throw(bad_decoding_error)
    end;
decode(node_id, [{<<"Identifier">>, _, [Bin]}]) ->
    try opcua_node:parse(Bin) of
        #opcua_node_id{} = Result -> Result;
        _ -> throw(bad_decoding_error)
    catch error:badarg -> throw(bad_decoding_error)
    end;
decode(expanded_node_id, [{<<"Identifier">>, _, [Bin]}]) ->
    try opcua_node:parse(Bin) of
        #opcua_node_id{} = NodeId -> ?XID(NodeId);
        #opcua_expanded_node_id{} = Result -> Result
    catch error:badarg -> throw(bad_decoding_error)
    end;
decode(_Type, _Data) ->
    throw({bad_decoding_error, _Type, _Data}).
