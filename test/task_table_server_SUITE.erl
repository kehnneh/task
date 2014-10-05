%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Oct 2014 5:25 PM
%%%-------------------------------------------------------------------
-module(task_table_server_SUITE).
-author("kehnneh").

-include_lib("common_test/include/ct.hrl").

%% API
-export([
    all/0,
    groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    test_1/1
]).

all() ->
    [{group, multiusers}].

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
    Config.

end_per_group(_, _Config) ->
    ok.

groups() ->
    [{multiusers,
      [], %% [parallel, {repeat, 10}],
      [test_1]}].

test_1(_Config) ->
    TableId = make_ref(),
    {ok, Ets} = task_table_server:acquire_table(TableId, [public, set]),
    true = ets:insert(Ets, {make_ref(), make_ref()}).
