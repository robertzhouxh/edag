%%%-------------------------------------------------------------------
%% @doc edag public API
%% @end
%%%-------------------------------------------------------------------

-module(edag_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    edag_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
