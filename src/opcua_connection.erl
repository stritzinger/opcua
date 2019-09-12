-module(opcua_connection).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include_lib("kernel/include/logger.hrl").

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([new/3]).
-export([endpoint_url/1]).
-export([monitor/1]).
-export([demonitor/2]).
-export([notify/2]).
-export([handle/2]).
-export([request/5]).
-export([response/4]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new(Endpoint, PeerAddr, SockAddr) ->
    #uacp_connection{
        pid         = self(),
        endpoint    = Endpoint,
        peer        = PeerAddr,
        sock        = SockAddr
    }.

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
