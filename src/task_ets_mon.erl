%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2014 9:21 PM
%%%-------------------------------------------------------------------
-module(task_ets_mon).
-author("kehnneh").

%% API
-export([
    start_link/0,
    acquire_table/2
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()} |
    ignore |
    {error, Reason :: term()}.

start_link() ->
    ets_mon:start_link(?MODULE).

%%--------------------------------------------------------------------
%% @doc
%% Creates a table, if necessary, and returns it to the caller.
%%
%% @end
%%--------------------------------------------------------------------
-spec acquire_table(Id :: term(), Args :: [term()]) ->
    {ok, EtsTid :: ets:tid()} |
    {error, Reason :: eperm | einval | term()}.

acquire_table(Id, Args) ->
    gen_server:call(?MODULE, Id, Args).
