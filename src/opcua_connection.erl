%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc The abstraction of the connection state.
%%%
%%% It is own by the client process or the server protocol handler,
%%% and it is passed alongside any state to most of the calls.
%%%
%%%
%%% WARNING:
%%%  - When passed to a different process, the share/1 function must be called.
%%%    This will make the keychain read-only. When getting the connection back
%%%    from the process, the merge/2 must be calledto reconciliate the result
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
-export([new/5]).
-export([share/1]).
-export([merge/2]).
-export([endpoint_url/1]).
-export([monitor/1]).
-export([demonitor/2]).
-export([notify/2]).
-export([handle/2]).
-export([request/5]).
-export([response/4]).

%% Keychain API functions
-export([self_certificate/1]).
-export([self_thumbprint/1]).
-export([peer_certificate/1]).
-export([peer_public_key/1]).
-export([peer_thumbprint/1]).
-export([lock_peer/2]).
-export([validate_peer/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Keychain, Identity, Endpoint, PeerAddr, SockAddr) ->
    #uacp_connection{
        pid         = self(),
        keychain    = Keychain,
        self_ident  = Identity,
        endpoint    = Endpoint,
        peer        = PeerAddr,
        sock        = SockAddr
    }.

% @doc Prepares the connection to be shared with other processes.
% The keychain of a shared connection is read-only.
share(#uacp_connection{keychain = Keychain} = Conn) ->
    Conn#uacp_connection{keychain = opcua_keychain:shareable(Keychain)}.

% @doc Merge the changes of a connection that has been shared with another
% process with the previous unshared connection. The main role of this merge
% is to keep any meaningfull changes and the original keychain.
% For now, we don't have any meaningfull changes so we just discard the shared
% connection.
merge(#uacp_connection{} = Original, #uacp_connection{} = _Shared) ->
    Original.

endpoint_url(#uacp_connection{endpoint = #opcua_endpoint{url = Url}}) -> Url.

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

self_certificate(#uacp_connection{self_ident = undefined}) -> undefined;
self_certificate(#uacp_connection{self_ident = Ident, keychain = Keychain}) ->
    %TODO: Figure out when more than one certificate is required
    %TODO: Maybe fail if the identity is not found ?
    case opcua_keychain:certificate(Keychain, Ident, der) of
        not_found -> undefined;
        CertDer -> CertDer
    end.

self_thumbprint(#uacp_connection{self_ident = undefined}) -> undefined;
self_thumbprint(#uacp_connection{self_ident = Ident, keychain = Keychain}) ->
    %TODO: Maybe fail if the identity is not found ?
    case opcua_keychain:info(Keychain, Ident) of
        not_found -> undefined;
        #{thumbprint := Thumbprint} -> Thumbprint
    end.

peer_certificate(#uacp_connection{peer_ident = undefined}) -> undefined;
peer_certificate(#uacp_connection{peer_ident = Ident, keychain = Keychain}) ->
    %TODO: Maybe fail if the identity is not found ?
    case opcua_keychain:certificate(Keychain, Ident, der) of
        not_found -> undefined;
        CertDer -> CertDer
    end.

peer_public_key(#uacp_connection{peer_ident = undefined}) -> undefined;
peer_public_key(#uacp_connection{peer_ident = Ident, keychain = Keychain}) ->
    %TODO: Maybe fail if the identity is not found ?
    case opcua_keychain:public_key(Keychain, Ident, rec) of
        not_found -> undefined;
        KeyRec -> KeyRec
    end.

peer_thumbprint(#uacp_connection{peer_ident = undefined}) -> undefined;
peer_thumbprint(#uacp_connection{peer_ident = Ident, keychain = Keychain}) ->
    %TODO: Maybe fail if the identity is not found ?
    case opcua_keychain:info(Keychain, Ident) of
        not_found -> undefined;
        #{thumbprint := Thumbprint} -> Thumbprint
    end.

lock_peer(#uacp_connection{peer_ident = Ident} = Conn, Ident) -> {ok, Conn};
lock_peer(#uacp_connection{keychain = Keychain, peer_ident = undefined} = Conn, Ident) ->
    %TODO: Should we validate anything, or just trust the caller ?
    case opcua_keychain:add_alias(Keychain, Ident, peer) of
        not_found -> {error, peer_certificate_not_found};
        {ok, Keychain2} ->
            {ok, Conn#uacp_connection{keychain = Keychain2, peer_ident = Ident}}
    end.

validate_peer(#uacp_connection{peer_ident = undefined} = Conn, undefined) ->
    {ok, Conn};
validate_peer(#uacp_connection{keychain = Keychain, peer_ident = undefined} = Conn, DerData) ->
    IdentOpts = #{alias => peer},
    case opcua_keychain:add_certificate(Keychain, DerData, IdentOpts) of
        {error, _Reason} = Error -> Error;
        {ok, #{id := Ident}, Keychain2} ->
            case opcua_keychain:validate(Keychain2, Ident) of
                {error, _Reason} = Error -> Error;
                ok ->
                    %TODO: Validate certificate hostname if needed
                    {ok, Conn#uacp_connection{keychain = Keychain2,
                                              peer_ident = Ident}}
            end
    end;
validate_peer(#uacp_connection{keychain = Keychain, peer_ident = Ident} = Conn, DerData) ->
    case opcua_keychain:certificate(Keychain, Ident, der) of
        not_found -> {error, internal_error};
        DerData -> {ok, Conn};
        _OtherDer -> {error, peer_identity_changed}
    end.
