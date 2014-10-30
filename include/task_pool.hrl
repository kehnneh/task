%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2014 10:56 PM
%%%-------------------------------------------------------------------
-author("kehnneh").

-ifndef(task_pool).
-define(task_pool, true).

-record(cfg_pool, {
    id = undefined :: atom(),

    %% the user of cfg_pool is typically a
    %% supervisor, the task supervisor for this
    %% pool will be another of the supervisor's
    %% children
    sup :: pid(),
    maxws = 1000 :: non_neg_integer()
}).

-endif. %% task_pool
