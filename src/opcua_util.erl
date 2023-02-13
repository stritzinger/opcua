-module(opcua_util).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([trace/2, trace/3]).
-export([trace_clear/0]).
-export([date_time/0]).
-export([nonce/0]).
-export([date_time_to_rfc3339/1]).
-export([bin_to_hex/1]).
-export([guid_to_hex/1]).
-export([bin_to_hex/2]).
-export([hex_to_bin/1]).
-export([get_node_id/2]).
-export([parse_node_id/1]).
-export([get_attr/2]).
-export([get_attr/3]).
-export([get_int/2]).
-export([get_int/3]).
-export([convert_name/1]).
-export([parse_range/1]).
-export([parse_endpoint/1]).
-export([algoritm_type/1]).
-export([algoritm_uri/1]).
-export([policy_uri/1]).
-export([policy_type/1]).
-export([for_human/1]).
-export([security_policy/1]).

%% Debug functions
-export([decode_client_message/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec trace(atom(), atom()) -> non_neg_integer().
trace(Mod, Fun) ->
    trace(Mod, Fun, 10).

-spec trace(atom(), atom(), non_neg_integer()) -> non_neg_integer().
trace(Mod, Fun, N) ->
    recon_trace:calls({Mod, Fun, return_trace}, N).

-spec trace_clear() -> ok.
trace_clear() ->
    recon_trace:clear().

nonce() ->
    crypto:strong_rand_bytes(32).

date_time() ->
    Now = erlang:system_time(nanosecond),
    NowUTC = calendar:system_time_to_universal_time(Now, nanosecond),
    Seconds1 = calendar:datetime_to_gregorian_seconds(NowUTC),
    Seconds2 = calendar:datetime_to_gregorian_seconds({{1601,1,1}, {0,0,0}}),
    round((erlang:convert_time_unit(Seconds1 - Seconds2, second, nanosecond) + (Now rem 1000000000)) / 100).

date_time_to_rfc3339(DateTime) ->
    Seconds1 = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    Seconds2 = calendar:datetime_to_gregorian_seconds({{1601,1,1},{0,0,0}}),
    Diff = erlang:convert_time_unit(Seconds1 - Seconds2, second, nanosecond),
    calendar:system_time_to_rfc3339(DateTime*100 - Diff, [{unit, nanosecond}]).

guid_to_hex(<<D1:4/binary, D2:2/binary, D3:2/binary, D41:2/binary, D42:6/binary>>) ->
    lists:concat([bin_to_hex(D1), "-", bin_to_hex(D2), "-", bin_to_hex(D3),
              "-", bin_to_hex(D41), "-", bin_to_hex(D42)]).

bin_to_hex(Bin) ->
    bin_to_hex(Bin, <<>>).

bin_to_hex(Bin, Sep) ->
    lists:flatten([io_lib:format("~2.16.0B~s",[X, Sep]) || <<X:8>> <= Bin]).

hex_to_bin(S) ->
        hex_to_bin(lists:flatten(string:tokens(S, " ")), []).
hex_to_bin([], Acc) ->
        list_to_binary(lists:reverse(Acc));
hex_to_bin([X,Y|T], Acc) ->
        {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
        hex_to_bin(T, [V | Acc]);
hex_to_bin([X|T], Acc) ->
        {ok, [V], []} = io_lib:fread("~16u", lists:flatten([X,"0"])),
        hex_to_bin(T, [V | Acc]).

%% helpers for parsing XML information models using a SAX parser
get_node_id(Key, Attributes) ->
    case get_attr(Key, Attributes) of
        undefined       -> undefined;
        NodeIdString    -> parse_node_id(NodeIdString)
    end.

parse_node_id(String) ->
    [_, String1] = string:split(String, "="),
    opcua_node:id(list_to_integer(String1)).

get_attr(Key, Attributes) ->
    get_attr(Key, Attributes, undefined).

get_attr(Key, Attributes, Default) ->
    case lists:keyfind(Key, 3, Attributes) of
        false -> Default;
        Value -> element(4, Value)
    end.

get_int(Key, Attributes) ->
    get_int(Key, Attributes, undefined).

get_int(Key, Attributes, Default) ->
    case get_attr(Key, Attributes, Default) of
        Default     -> Default;
        StringInt   -> list_to_integer(StringInt)
    end.

%% converts CamelCase strings to snake_case atoms
convert_name(Name) when is_binary(Name) ->
    convert_name(binary_to_list(Name));
convert_name([FirstLetter|Rest]) ->
    list_to_atom(
      string:lowercase([FirstLetter]) ++
        lists:flatten(
          lists:map(fun(Char) ->
              case string:uppercase([Char]) of
                  [Char]  -> "_" ++ string:lowercase([Char]);
                  _     -> Char
              end
          end, Rest))).

parse_range(undefined) -> undefined;
parse_range(<<>>) -> undefined;
parse_range(Range) -> parse_range_dims(Range, []).

parse_endpoint({{A, B, C, D} = Ip, Port})
  when is_integer(A), A >= 0, A < 256, is_integer(B), B >= 0, B < 256,
       is_integer(C), C >= 0, C < 256, is_integer(D), D >= 0, D < 256,
       is_integer(Port), Port >= 0, Port < 65536 ->
    Url = iolist_to_binary(io_lib:format("opc.tcp://~w.~w.~w.~w:~w", [A, B, C, D, Port])),
    #opcua_endpoint_url{url = Url, host = Ip, port = Port};
parse_endpoint(Url) when is_binary(Url) ->
    Map = #{host := BinHost} = uri_string:parse(Url),
    <<"opc.tcp">> = maps:get(scheme, Map, <<"opc.tcp">>),
    Port = maps:get(port, Map, 4840),
    Host = binary_to_list(BinHost),
    #opcua_endpoint_url{url = Url, host = Host, port = Port}.

algoritm_type(?RSA_OAEP) -> {sha, rsa_pkcs1_oaep_padding};
algoritm_type(?RSA_SHA256) -> {sha256, rsa_pkcs1_padding};
algoritm_type(?AES_128) -> {aes_128_cbc, 128, 16};
algoritm_type(?AES_256) -> {aes_256_cbc, 256, 16};
algoritm_type(_) -> unsupported.

algoritm_uri({sha, rsa_pkcs1_oaep_padding}) -> ?RSA_OAEP;
algoritm_uri({sha256, rsa_pkcs1_padding}) -> ?RSA_SHA256;
algoritm_uri({aes_128_cbc, 128, 16}) -> ?AES_128;
algoritm_uri({aes_256_cbc, 256, 16}) -> ?AES_256.

policy_uri(none) -> ?POLICY_NONE;
policy_uri(basic256sha256) -> ?POLICY_BASIC256SHA256;
policy_uri(aes128_sha256_RsaOaep) -> ?POLICY_AES128_SHA256_RSAOAEP.

policy_type(?POLICY_NONE) -> none;
policy_type(?POLICY_BASIC256SHA256) -> basic256sha256;
policy_type(?POLICY_AES128_SHA256_RSAOAEP) -> aes128_sha256_RsaOaep;
policy_type(_) -> unsupported.

% @doc Converts nodes, nodes id and references to something more human readable.
% May not be reversible, some type information and namespace is lost.
for_human(#opcua_node_id{} = NodeId) -> opcua_node:spec(NodeId);
for_human(#opcua_qualified_name{name = Name}) -> Name;
for_human(#opcua_localized_text{text = Text}) -> Text;
for_human(#opcua_expanded_node_id{node_id = NodeId,
            namespace_uri = undefined,server_index = undefined}) ->
    for_human(NodeId);
for_human(T) when is_tuple(T) ->
    list_to_tuple(for_human(tuple_to_list(T)));
for_human(L) when is_list(L) ->
    [for_human(V) || V <- L];
for_human(M) when is_map(M) ->
    maps:from_list([{for_human(K), for_human(V)} || {K, V} <- maps:to_list(M)]);
for_human(O) ->
    O.

security_policy(none) ->
    {ok, #uacp_security_policy{
        policy_uri = opcua_util:policy_uri(none)
    }};
security_policy(basic256sha256) ->
   {ok, #uacp_security_policy{
        policy_uri = policy_uri(basic256sha256),
        symmetric_signature_algorithm = sha256,
        symmetric_encryption_algorithm = algoritm_type(?AES_256),
        asymmetric_signature_algorithm = algoritm_type(?RSA_SHA256),
        asymmetric_encryption_algorithm = algoritm_type(?RSA_OAEP),
        min_asymmetric_keyLength = 2048,
        max_asymmetric_keyLength = 4096,
        key_derivation_algorithm = sha256,
        derived_signature_keyLength = 256,
        certificate_signature_algorithm = ?RSA_SHA256,
        secureChannelNonceLength = 32
    }};
security_policy(aes128_sha256_RsaOaep) ->
    {ok, #uacp_security_policy{
        policy_uri = policy_uri(aes128_sha256_RsaOaep),
        symmetric_signature_algorithm = sha256,
        symmetric_encryption_algorithm = algoritm_type(?AES_128),
        asymmetric_signature_algorithm =  algoritm_type(?RSA_SHA256),
        asymmetric_encryption_algorithm = algoritm_type(?RSA_OAEP),
        min_asymmetric_keyLength = 2048,
        max_asymmetric_keyLength = 4096,
        key_derivation_algorithm = sha256,
        derived_signature_keyLength = 256,
        certificate_signature_algorithm = ?RSA_SHA256,
        secureChannelNonceLength = 32
    }};
security_policy(_Policy) ->
    {error, bad_security_policy_rejected}.

%%% DEBUG FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode_client_message(Data) ->
    TokenId = 1234,
    Conn = #uacp_connection{
        pid = self(),
        space = opcua_nodeset,
        keychain = default,
        endpoint_url = #opcua_endpoint_url{
            url = <<"opc+tcp://localhost:4840">>,
            host = "localhost",
            port = 4840
        },
        peer = {{127, 0, 0, 1}, 4840},
        sock = {{127, 0, 0, 1}, 6666},
        security_mode = none,
        security_policy = none
    },
    case opcua_security:init_client(Conn) of
        {error, _Reason} = Error -> Error;
        {ok, _Conn2, Sec} ->
            Sec2 = opcua_security:token_id(TokenId, Sec),
            {[Chunk], <<>>} = opcua_uacp_codec:decode_chunks(Conn, Data),
            Chunk2 = Chunk#uacp_chunk{security = TokenId},
            {ok, Chunk3, _Conn3, _Sec3} = opcua_security:unlock(Chunk2, Conn, Sec2),
            #uacp_chunk{message_type = MsgType, body = Body} = Chunk3,
            opcua_uacp_codec:decode_payload(Conn, MsgType, Body)
    end.


%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_range_dims(<<>>, Acc) -> lists:reverse(Acc);
parse_range_dims(Ranges, Acc) ->
    {Item, Rest} = parse_range_min(Ranges, []),
    parse_range_dims(Rest, [Item | Acc]).

parse_range_min(<<>>, Acc) ->
    {list_to_integer(lists:reverse(Acc)), <<>>};
parse_range_min(<<",", Rest/binary>>, Acc) ->
    {list_to_integer(lists:reverse(Acc)), Rest};
parse_range_min(<<":", Rest/binary>>, Acc) ->
    parse_range_max(Rest, list_to_integer(lists:reverse(Acc)), []);
parse_range_min(<<C, Rest/binary>>, Acc) ->
    parse_range_min(Rest, [C | Acc]).

parse_range_max(<<>>, Min, Acc) ->
    validate_range(Min, list_to_integer(lists:reverse(Acc)), <<>>);
parse_range_max(<<",", Rest/binary>>, Min, Acc) ->
    validate_range(Min, list_to_integer(lists:reverse(Acc)), Rest);
parse_range_max(<<C, Rest/binary>>, Min, Acc) ->
    parse_range_max(Rest, Min, [C | Acc]).

validate_range(Min, Max, Rest) when Min >= 0, Max > Min -> {{Min, Max}, Rest}.
