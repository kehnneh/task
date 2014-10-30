%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Oct 2014 3:18 PM
%%%-------------------------------------------------------------------
-module(task_pool_test).
-author("kehnneh").
-include("task_pool.hrl").

%% API
-export([
    start_link/0,
    run/3,
    stop/1
]).

start_link() ->
    PoolCfg = #cfg_pool{id = ?MODULE, maxws = 10, sup = self()},
    task_pool:start_link(PoolCfg).

run(Id, Opaque, Fun) ->
    task_pool:run(?MODULE, Id, Opaque, Fun).

stop(Id) ->
    task_pool:stop(?MODULE, Id).
