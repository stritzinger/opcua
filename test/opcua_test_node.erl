-module(opcua_test_node).

-export([start/1]).
-export([stop/1]).

-include_lib("common_test/include/ct.hrl").

start(SysConfig) ->
    ReleaseDir = get_project_root() ++ "/_build/test/lib",
    Opts = #{
        %args => ["-config " ++ SysConfig], Does not work :(
        env => [{"ERL_LIBS", ReleaseDir}]
    },
    {ok, Pid, Node} = ?CT_PEER(Opts),
    % Manually setting the application env variable... sigh
    {ok, [Config]} = file:consult(SysConfig),
    ok = erpc:call(Node, application, set_env, [Config]),
    {ok, _} = erpc:call(Node, application, ensure_all_started, [opcua]),
    {ok, {Pid, Node}}.
stop(Node) ->
    peer:stop(Node).

% UTILITIES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_project_root() ->
    {ok, Path} = file:get_cwd(),
    filename:dirname(filename:dirname(filename:dirname(filename:dirname(Path)))).
