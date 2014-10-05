%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Oct 2014 7:23 PM
%%%-------------------------------------------------------------------
-module(task_SUITE).
-author("kehnneh").

-compile([{parse_transform, lager_transform}]).

-include_lib("common_test/include/ct.hrl").
-include("task_pool.hrl").

%% API
-export([
    all/0,
    groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2,
    test/1
]).

all() ->
    [{group, batch}].

groups() ->
    [{batch,
      [parallel],
      [test]}].

init_per_suite(Config) ->
    ok = application:start(compiler),
    ok = application:start(syntax_tools),
    ok = application:start(goldrush),
    ok = application:start(lager),
    ok = application:start(task),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(task),
    ok = application:stop(lager),
    ok = application:stop(goldrush),
    ok = application:stop(syntax_tools),
    ok = application:stop(compiler).

init_per_group(_, Config) ->
    Id = make_ref(),
    Sup = whereis(task_app_sup),
    PoolCfg = #pool_cfg{id = Id, sup = Sup, maxws = 3},
    ChildSpec = {Id, {task_pool, start_link, [PoolCfg]}, transient, infinity, worker, [task_pool]},
    {ok, Pid} = supervisor:start_child(Sup, ChildSpec),
    [{pool, Pid}] ++ Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

test(Config) ->
    Pool = ?config(pool, Config),
    TaskId = make_ref(),
    Opaque = ok,
    F = fun(Term) ->
        lager:info("~p: bye!", [TaskId]),
        timer:sleep(timer:seconds(10)),
        lager:info("~p: hi!", [TaskId]),
        {ok, Term}
    end,
    {ok, TaskId} = task_pool:add(Pool, TaskId, Opaque, F),
    exit(Pool, kill),
    receive
        {TaskId, Result} ->
            Result
    end.
