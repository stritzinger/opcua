-module(opcua_connection).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua.hrl").
-include("opcua_internal.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([req2res/4]).
-export([monitor/1]).
-export([demonitor/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

req2res(#uacp_connection{},
        #uacp_message{type = T, request_id = ReqId, payload = ReqPayload},
        NodeId, ResPayload) ->
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
    #uacp_message{type = T, request_id = ReqId,
                  node_id = NodeId, payload = FinalPayload}.

monitor(#uacp_connection{pid = Pid}) ->
    erlang:monitor(process, Pid).

demonitor(#uacp_connection{}, MonRef) ->
    erlang:demonitor(MonRef, [flush]).
