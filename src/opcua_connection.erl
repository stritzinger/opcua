-module(opcua_connection).

%%% INCLUDES %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("opcua_protocol.hrl").
-include("opcua_codec.hrl").


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API functions
-export([req2res/4]).
-export([monitor/1]).
-export([demonitor/2]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

req2res(#uacp_connection{},
        #uacp_message{type = T, request_id = ReqId}, NodeId, Payload) ->
    FinalPayload = case maps:is_key(response_header, Payload) of
        true -> Payload;
        false ->
            Header = #{
                timestamp => opcua_util:date_time(),
                request_handle => ReqId,
                service_result => 0,
                service_diagnostics => #diagnostic_info{},
                string_table => [],
                additional_header => #extension_object{}
            },
            Payload#{response_header => Header}
    end,
    #uacp_message{type = T, request_id = ReqId,
                  node_id = NodeId, payload = FinalPayload}.

monitor(#uacp_connection{pid = Pid}) ->
    erlang:monitor(process, Pid).

demonitor(#uacp_connection{}, MonRef) ->
    erlang:demonitor(MonRef, [flush]).
