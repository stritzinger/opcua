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
-export([sender_certificate/1]).
-export([receiver_certificate_thumbprint/1]).
-export([validate_peer_certificate/3]).


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
handle(#uacp_connection{}, #uacp_message{payload = #{response_header := #{request_handle := Handle}}}) -> Handle.

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

sender_certificate(#uacp_connection{self_ident = undefined}) -> undefined;
sender_certificate(#uacp_connection{self_ident = Ident, keychain = Keychain}) ->
    %TODO: Figure out when more than one certificate is required
    %TODO: Maybe fail if the certificate is not found ?
    case opcua_keychain:certificate(Keychain, Ident, der) of
        not_found -> undefined;
        CertDer -> CertDer
    end.

receiver_certificate_thumbprint(#uacp_connection{peer_ident = undefined}) -> undefined;
receiver_certificate_thumbprint(#uacp_connection{peer_ident = Ident, keychain = Keychain}) ->
    %TODO: Figure out when more than one certificate is required
    %TODO: Maybe fail if the certificate is not found ?
    case opcua_keychain:info(Keychain, Ident) of
        not_found -> undefined;
        #{thumbprint := Thumbprint} -> Thumbprint
    end.

validate_peer_certificate(#uacp_connection{keychain = Keychain,
                                           peer_ident = undefined} = Conn,
                          DerData, _PeerHostname) ->
    %TODO: Split concatenated DER certificates and validate
    %TODO: Validate the hostname
    case opcua_keychain:add_certificate(Keychain, DerData) of
        {error, _Reason} = Error -> Error;
        {ok, #{id := Ident}, Keychain2} ->
            case opcua_keychain:validate(Keychain2, Ident) of
                % No error case yet, commented to make dialyzer happy
                % {error, _Reason} = Error -> Error;
                ok ->
                    {ok, Conn#uacp_connection{keychain = Keychain2,
                                              peer_ident = Ident}}
            end
    end.
