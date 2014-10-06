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
    task_kill_pool/1,
    task_stop_normal/1
]).

all() ->
    [{group, master}].

groups() ->
    [{master,
      [parallel, {repeat, 2}],
      [{group, batch}]},
     {batch,
      [parallel],
      [task_stop_normal, task_kill_pool, task_kill_pool, task_stop_normal]}].

init_per_suite(Config) ->
    ok = application:start(compiler),
    ok = application:start(syntax_tools),
    ok = application:start(goldrush),
    ok = application:start(lager),
    ok = application:start(task),
    lager:set_loglevel(lager_console_backend, debug),
    Sup = whereis(task_app_sup),
    ChildSpec = {task_pool_test, {task_pool_test, start_link, []}, transient, infinity, worker, [task_pool]},
    {ok, _Pid} = supervisor:start_child(Sup, ChildSpec),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(task),
    ok = application:stop(lager),
    ok = application:stop(goldrush),
    ok = application:stop(syntax_tools),
    ok = application:stop(compiler).

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    TaskId = make_ref(),
    [{taskid, TaskId}] ++ Config.

end_per_testcase(_, _Config) ->
    ok.

%% 1) Create a task
%% 2) Kill the task's pool server
%% 3) Obtain success
task_kill_pool(Config) ->
    TaskId = ?config(taskid, Config),
    Opaque = [],
    F = fun(Term) ->
        lager:info("~p: sleeping!", [TaskId]),
        timer:sleep(timer:seconds(1)),
        lager:info("~p: woke up!", [TaskId]),
        {ok, Term}
    end,
    {ok, TaskId} = task_pool_test:run(TaskId, Opaque, F),
    case whereis(task_pool_test) of
        Pid ->
            lager:info("killing task pool"),
            catch exit(Pid, kill);
        _ ->
            ok
    end,
    {TaskId, {ok, []}} = receive X -> X end.

%% 1) Create a task
%% 2) Stop the task,
task_stop_normal(Config) ->
    TaskId = ?config(taskid, Config),
    Opaque = 1,
    F = fun(1) ->
        lager:info("~p: sleeping!", [TaskId]),
        timer:sleep(timer:seconds(3)),
        lager:info("~p: woke up!", [TaskId]),
        {continue, 2};
    (Term) ->
        {ok, Term}
    end,
    {ok, TaskId} = task_pool_test:run(TaskId, Opaque, F),
    lager:info("stopping task ~p", [TaskId]),
    ok = task_pool_test:stop(TaskId),
    receive
        {TaskId, shutdown} ->
            lager:info("~p was shutdown successfully", [TaskId]);
        {TaskId, {ok, 2}} ->
            lager:info("~p finished and was not shut down", [TaskId])
    end.
