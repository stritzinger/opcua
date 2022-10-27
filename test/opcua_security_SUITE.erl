-module(opcua_security_SUITE).
-behaviour(ct_suite).
-export([all/0]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    none/1,
    basic256sha256/1,
    aes128_sha256_RsaOaep/1
]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(URL, <<"opc.tcp://localhost:4840">>).
-define(AUTH, {user_name, <<"test">>, <<"test">>}).

all() -> [
    none,
    basic256sha256,
    aes128_sha256_RsaOaep
].

init_per_testcase(_, Config) ->
    Dir = ?config(data_dir, Config),
    ClientCFG = filename:join([Dir, "client.config"]),
    ServerCFG = filename:join([Dir, "server.config"]),
    {ok, Client} = opcua_test_node:start(ClientCFG),
    {ok, Server} = opcua_test_node:start(ServerCFG),
    [{client, Client}, {server, Server}| Config].

end_per_testcase(_, _Config) ->
    ok.

none(Config) ->
    connect_client(Config, #{}),
    connect_client(Config, #{auth => ?AUTH}).

basic256sha256(Config) ->
    connect_client(Config, #{mode => sign, policy => ?FUNCTION_NAME}),
    connect_client(Config, #{mode => sign_and_encrypt, policy => ?FUNCTION_NAME}),
    connect_client(Config, #{auth => ?AUTH, mode => sign, policy => ?FUNCTION_NAME}),
    connect_client(Config, #{auth => ?AUTH, mode => sign_and_encrypt, policy => ?FUNCTION_NAME}).

aes128_sha256_RsaOaep(Config) ->
    connect_client(Config, #{mode => sign, policy => ?FUNCTION_NAME}),
    connect_client(Config, #{mode => sign_and_encrypt, policy => ?FUNCTION_NAME}),
    connect_client(Config, #{auth => ?AUTH, mode => sign, policy => ?FUNCTION_NAME}),
    connect_client(Config, #{auth => ?AUTH, mode => sign_and_encrypt, policy => ?FUNCTION_NAME}).

% utilities
%
connect_client(Config, Opts) ->
    {_, ClientNode} = ?config(client, Config),
    {ok, ClientProc} = erpc:call(ClientNode, opcua_client, connect, [?URL, Opts]),
    ?assert(is_pid(ClientProc)),
    Result = erpc:call(ClientNode, opcua_client, browse, [ClientProc, objects]),
    ?assert(is_object_list(Result)).


is_object_list([
    #{
        browse_name := _,
        display_name := _,
        is_forward := _,
        node_class := object,
        node_id := _,
        reference_type_id := _,
        type_definition := _
    } | _])  -> true;
is_object_list(_) -> false.