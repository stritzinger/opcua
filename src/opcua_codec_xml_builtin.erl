-module(opcua_codec_xml_builtin).


%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([decode/2]).
-export([tag_name/2]).


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

tag_name(ValueRank, #opcua_node_id{} = NodeId) ->
    tag_name(ValueRank, opcua_codec:builtin_type_name(NodeId));
tag_name(-1, boolean) -> <<"Boolean">>;
tag_name(1, boolean) -> <<"ListOfBoolean">>;
tag_name(-1, sbyte) -> <<"SByte">>;
tag_name(1, sbyte) -> <<"ListOfSByte">>;
tag_name(-1, byte) -> <<"Byte">>;
tag_name(1, byte) -> <<"ListOfByte">>;
tag_name(-1, int16) -> <<"Int16">>;
tag_name(1, int16) -> <<"ListOfInt16">>;
tag_name(-1, uint16) -> <<"UInt16">>;
tag_name(1, uint16) -> <<"ListOfUInt16">>;
tag_name(-1, int32) -> <<"Int32">>;
tag_name(1, int32) -> <<"ListOfInt32">>;
tag_name(-1, uint32) -> <<"UInt32">>;
tag_name(1, uint32) -> <<"ListOfUInt32">>;
tag_name(-1, int64) -> <<"Int64">>;
tag_name(1, int64) -> <<"ListOfInt64">>;
tag_name(-1, uint64) -> <<"UInt64">>;
tag_name(1, uint64) -> <<"ListOfUInt64">>;
tag_name(-1, float) -> <<"Float">>;
tag_name(1, float) -> <<"ListOfFloat">>;
tag_name(-1, double) -> <<"Double">>;
tag_name(1, double) -> <<"ListOfDouble">>;
tag_name(-1, string) -> <<"String">>;
tag_name(1, string) -> <<"ListOfString">>;
tag_name(-1, date_time) -> <<"DateTime">>;
tag_name(1, date_time) -> <<"ListOfDateTime">>;
tag_name(-1, guid) -> <<"String">>;
tag_name(1, guid) -> <<"ListOfString">>;
tag_name(-1, byte_string) -> <<"ByteString">>;
tag_name(1, byte_string) -> <<"ListOfByteString">>;
tag_name(_, xml) -> throw(bad_not_implemented);
tag_name(_, node_id) -> throw(bad_not_implemented);
tag_name(_, expanded_node_id) -> throw(bad_not_implemented);
tag_name(-1, status_code) -> <<"Code">>;
tag_name(1, status_code) -> <<"ListOfCode">>;
tag_name(_, qualified_name) -> throw(bad_not_implemented);
tag_name(-1, localized_text) -> <<"LocalizedText">>;
tag_name(1, localized_text) -> <<"ListOfLocalizedText">>;
tag_name(-1, extension_object) -> <<"ExtensionObject">>;
tag_name(1, extension_object) -> <<"ListOfExtensionObject">>;
tag_name(-1, data_value) -> <<"DataValue">>;
tag_name(1, data_value) -> <<"ListOfDataValue">>;
tag_name(_, variant) -> throw(bad_not_implemented);
tag_name(_, diagnostic_info) -> throw(bad_not_implemented).

