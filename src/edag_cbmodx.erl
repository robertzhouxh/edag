%%%-------------------------------------------------------------------
%%% @author zxh <robertzhouxh@gmail.com>
%%% @copyright (C) 2020, zxh
%%% @doc
%%%
%%% @end
%%% Created : 14 Mar 2020 by zxh <robertzhouxh@gmail.com>
%%%-------------------------------------------------------------------

-module(edag_cbmodx).

-author("Xuehao Zhou<robertzhouxh@gmail.com>").

-behavior(edag_behavior).

-export([init/1, process/2, shutdown/1]).

-include("edag.hrl").

init(Ctx) -> 
    {ok, Ctx}.

process(InData, Ctx) ->
    {emit, InData, Ctx}.

shutdown(_Ctx) -> 
    ok.
