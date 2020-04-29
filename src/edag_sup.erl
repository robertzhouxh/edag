%%%-------------------------------------------------------------------
%%% @author zxh <robertzhouxh@gmail.com>
%%% @copyright (C) 2020, zxh
%%% @doc
%%%
%%% @end
%%% Created : 15 MAR 2020 by zxh <robertzhouxh@gmail.com>
%%%-------------------------------------------------------------------
-module(edag_sup).

-author("Xuehao Zhou<robertzhouxh@gmail.com>").

-include("edag.hrl").

-behaviour(supervisor).

-export([start_link/0]).
-export([new/2]).
-export([init/1]).

-define(SERVER, ?MODULE).

%% -----------------------------------------------------------------------
%% apis

new(GId, GDef) ->
   Spec = {GId, {edag, start_link, [GId, GDef]}, temporary, 5000, worker, dynamic},
   supervisor:start_child(?MODULE, Spec).
%%   {ok, Child} = ,
%%   Ch = supervisor:which_children(?MODULE),
%%   case lists:keytake(Id, 1, Ch) of
%%      {value, {Id, Pid, _T, _How}, _TupleList2} -> {ok, Pid};
%%      _ -> erlang:error(graph_not_started, [Id])
%%   end.

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% -----------------------------------------------------------------------
%% callbacks 

init([]) ->
    SupFlags = #{strategy => one_for_one,
		 intensity => 1000,
		 period => 3600},

    {ok, {SupFlags, []}}.

