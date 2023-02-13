%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc The abstraction of the connection state.
%%%
%%% It is own by the client process or the server protocol handler,
%%% and it is passed alongside any state to most of the calls.
%%%
%%%
%%% WARNING:
%%%  - When passed to a different process, the opcua_keychain:shareable/1
%%%    function must be called. This will make the keychain read-only.
%%%    When getting the connection back from the process,
%%%    the merge/2 must be calledto reconciliate the result
%%%    and the original data. For now the result is just discarded.
%%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(opcua_connection).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([new/6]).
-export([merge/2]).
-export([endpoint_url/1]).
-export([security_policy/1]).
-export([security_mode/1]).
-export([set_security_policy/2]).
-export([set_security_mode/2]).
-export([monitor/1]).
-export([demonitor/2]).
-export([notify/2]).
-export([handle/2]).
-export([request/5]).
-export([response/4]).

%% Keychain API functions
-export([self_identity/1]).
-export([self_certificate/1]).
-export([self_private_key/1]).
-export([self_thumbprint/1]).
-export([peer_identity/1]).
-export([peer_certificate/1]).
-export([peer_public_key/1]).
-export([peer_thumbprint/1]).
-export([lock_peer/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Space, Keychain, Identity, EndpointUrl, PeerAddr, SockAddr) ->
    #uacp_connection{
        pid          = self(),
        space        = Space,
        keychain     = Keychain,
        self_ident   = Identity,
        endpoint_url = EndpointUrl,
        peer         = PeerAddr,
        sock         = SockAddr
    }.

% @doc Merge the changes of a connection that has been shared with another
% process with the previous unshared connection. The main role of this merge
% is to keep any meaningfull changes and the original keychain.
% For now, we don't have any meaningfull changes so we just discard the shared
% connection.
merge(#uacp_connection{} = Original, #uacp_connection{} = _Shared) ->
    Original.

endpoint_url(#uacp_connection{endpoint_url = #opcua_endpoint_url{url = Url}}) -> Url.

security_policy(#uacp_connection{security_policy = P}) -> P.

security_mode(#uacp_connection{security_mode = M}) -> M.

set_security_policy(#uacp_connection{security_policy = undefined} = Conn, Policy) ->
    Conn#uacp_connection{ security_policy = Policy}.

set_security_mode(#uacp_connection{security_mode = undefined} = Conn, Mode) ->
    Conn#uacp_connection{security_mode = Mode}.

monitor(#uacp_connection{pid = Pid}) ->
    erlang:monitor(process, Pid).

demonitor(#uacp_connection{}, MonRef) ->
    erlang:demonitor(MonRef, [flush]).

notify(#uacp_connection{pid = Pid}, Notif) ->
    Pid ! {opcua_connection, Notif}.

handle(#uacp_connection{}, #uacp_message{payload = #{request_header := #{request_handle := Handle}}}) -> Handle;
handle(#uacp_connection{}, #uacp_message{payload = #{response_header := #{request_handle := Handle}}}) -> Handle;
handle(#uacp_connection{}, _Msg) -> undefined.

request(#uacp_connection{}, Type, ReqId, NodeId, Payload) ->
    #uacp_message{
        type = Type,
        sender = client,
        request_id = ReqId,
        node_id = NodeId,
        payload = Payload
    }.

% @doc Create a UACP response message from a request message
response(#uacp_connection{}, #uacp_message{sender = client} = Req, NodeId, ResPayload) ->
    #uacp_message{type = T,request_id = ReqId, payload = ReqPayload} = Req,
    FinalPayload = case maps:is_key(response_header, ResPayload) of
        true -> ResPayload;
        false ->
            #{request_header := #{request_handle := ReqHandle}} = ReqPayload,
            Header = #{
                timestamp => opcua_util:date_time(),
                request_handle => ReqHandle,
                service_result => 0,
                service_diagnostics => #opcua_diagnostic_info{},
                string_table => [],
                additional_header => #opcua_extension_object{}
            },
            ResPayload#{response_header => Header}
    end,
    #uacp_message{type = T, sender = server, request_id = ReqId,
                  node_id = NodeId, payload = FinalPayload}.


%%% KEYCHAIN API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

self_identity(#uacp_connection{self_ident = SI}) -> SI.

self_certificate(#uacp_connection{self_ident = undefined}) -> undefined;
self_certificate(#uacp_connection{self_ident = Ident, keychain = Keychain}) ->
    case opcua_keychain:certificate(Keychain, Ident, der) of
        not_found -> {error, certificate_not_found};
        CertDer -> CertDer
    end.

self_private_key(#uacp_connection{self_ident = undefined}) -> undefined;
self_private_key(#uacp_connection{self_ident = Ident, keychain = Keychain}) ->
    case opcua_keychain:private_key(Keychain, Ident, rec) of
        not_found -> {error, certificate_not_found};
        KeyRecord -> KeyRecord
    end.

self_thumbprint(#uacp_connection{self_ident = undefined}) -> undefined;
self_thumbprint(#uacp_connection{self_ident = Ident, keychain = Keychain}) ->
    case opcua_keychain:info(Keychain, Ident) of
        not_found -> {error, certificate_not_found};
        #{thumbprint := Thumbprint} -> Thumbprint
    end.

peer_identity(#uacp_connection{peer_ident = PI}) -> PI.

peer_certificate(#uacp_connection{peer_ident = undefined}) -> undefined;
peer_certificate(#uacp_connection{peer_ident = Ident, keychain = Keychain}) ->
    case opcua_keychain:certificate(Keychain, Ident, der) of
        not_found -> {error, peer_certificate_not_found};
        CertDer -> CertDer
    end.

peer_public_key(#uacp_connection{peer_ident = undefined}) -> undefined;
peer_public_key(#uacp_connection{peer_ident = Ident, keychain = Keychain}) ->
    case opcua_keychain:public_key(Keychain, Ident, rec) of
        not_found -> {error, peer_certificate_not_found};
        KeyRec -> KeyRec
    end.

peer_thumbprint(#uacp_connection{peer_ident = undefined}) -> undefined;
peer_thumbprint(#uacp_connection{peer_ident = Ident, keychain = Keychain}) ->
    case opcua_keychain:info(Keychain, Ident) of
        not_found -> {error, peer_certificate_not_found};
        #{thumbprint := Thumbprint} -> Thumbprint
    end.

lock_peer(#uacp_connection{peer_ident = Ident} = Conn, Ident) -> {ok, Conn};
lock_peer(#uacp_connection{keychain = Keychain, peer_ident = undefined} = Conn, Ident) ->
    case opcua_keychain:add_alias(Keychain, Ident, peer) of
        not_found -> {error, peer_certificate_not_found};
        {ok, Keychain2} ->
            {ok, Conn#uacp_connection{keychain = Keychain2, peer_ident = Ident}}
    end.

%%% UTILITY FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


