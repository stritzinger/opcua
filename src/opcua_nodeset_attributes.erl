-module(opcua_nodeset_attributes).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([name/1]).
-export([id/1]).

%% Functions to be used only by opcua_nodeset
-export([setup/0]).
-export([store/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

name(Attr) ->
    {_, Name} = persistent_term:get({?MODULE, Attr}),
    Name.


id(Attr) ->
    {Id, _} = persistent_term:get({?MODULE, Attr}),
    Id.


%%% PROTECTED FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup() ->
    ok.

store({Id, Name} = Spec) when is_integer(Id), is_atom(Name) ->
    persistent_term:put({?MODULE, Id}, Spec),
    persistent_term:put({?MODULE, Name}, Spec),
    ok.
