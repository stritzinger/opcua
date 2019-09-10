-module(opcua_pubsub).

-behaviour(gen_server).

-include("opcua_pubsub.hrl").


-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    pub_sub_connection,
    published_data_sets
}).

start_link() ->
    Args = [#pub_sub_connection{}, [#published_data_set{}]],
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

init([PubSubConn, PublishedDataSets]) ->
    #pub_sub_connection{
       writer_groups = WriterGroups,
       address = #network_address_url{url = Url}
    } = PubSubConn,
    %% don't care about supervision for now,
    %% also PDSs should be samples within an
    %% own collectors process
    [opcua_pubsub_publisher:start_link([Url, WriterGroup, PublishedDataSets]) || WriterGroup <- WriterGroups],
    {ok, #state{pub_sub_connection = PubSubConn, published_data_sets = PublishedDataSets}}.

%% configuration changes would happen here
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
