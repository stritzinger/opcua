-module(opcua_nodeset_status).


%%% EXPORTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API Functions
-export([encode/2]).
-export([decode/2]).
-export([name/1, name/2]).
-export([code/1]).
-export([is_name/1]).
-export([is_code/1]).

%% Functions to be used only by opcua_nodeset
-export([setup/0]).
-export([store/1]).


%%% API FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

decode(Status, undefined) ->
    {_, Name, DefaultDesc} = persistent_term:get({?MODULE, Status}),
    {Name, DefaultDesc};
decode(Status, Desc) ->
    {_, Name, _} = persistent_term:get({?MODULE, Status}),
    {Name, Desc}.

encode(Status, undefined) ->
    {Code, _, DefaultDesc} = persistent_term:get({?MODULE, Status}),
    {Code, DefaultDesc};
encode(Status, Desc) ->
    {Code, _, _} = persistent_term:get({?MODULE, Status}),
    {Code, Desc}.

name(Status) ->
    {_, Name, _} = persistent_term:get({?MODULE, Status}),
    Name.

name(Status, Default) ->
    {_, Name, _} = persistent_term:get({?MODULE, Status},
                                       {undefined, Default, undefined}),
    Name.

code(Status) ->
    {Code, _, _} = persistent_term:get({?MODULE, Status}),
    Code.

is_name(StatusName) when is_atom(StatusName) ->
    try persistent_term:get({?MODULE, StatusName}) of
        {_, StatusName, _} -> true
    catch  error:badarg -> false
    end;
is_name(_Other) ->
    false.

is_code(StatusCode) when is_integer(StatusCode) ->
    try persistent_term:get({?MODULE, StatusCode}) of
        {StatusCode, _, _} -> true
    catch  error:badarg -> false
    end;
is_code(_Other) ->
    false.


%%% PROTECTED FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup() ->
    ok.

store({Code, Name, Desc} = Spec)
  when is_integer(Code), is_atom(Name), is_binary(Desc) ->
    persistent_term:put({?MODULE, Code}, Spec),
    persistent_term:put({?MODULE, Name}, Spec),
    ok.
