%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2014 10:55 PM
%%%-------------------------------------------------------------------
-author("kehnneh").

-ifndef(task).
-define(task, true).

-record(task, {
    id :: term(),
    opaque :: term(),
    caller :: pid()
}).

-endif. %% task
