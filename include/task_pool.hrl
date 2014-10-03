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

-record(pool_cfg, {
    id = undefined :: term(),

    %% the user of pool_cfg is typically a
    %% supervisor, the task supervisor for this
    %% pool will be a child of it
    sup = self() :: pid(),
    maxws = 1000 :: non_neg_integer()
}).

-endif. %% task_pool
