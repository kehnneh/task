%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2014 8:48 PM
%%%-------------------------------------------------------------------
-module(task_app_sup).
-author("kehnneh").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()} |
    ignore |
    {error, Reason :: term()}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(), MaxR :: non_neg_integer(), MaxT :: non_neg_integer()}, [ChildSpec :: supervisor:child_spec()]}} |
    ignore |
    {error, Reason :: term()}.

init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    TableServer = {
        task_table_mon,
        {table_mon, start_link, [?TASK_TABLE_MON]},
        permanent,
        infinity,
        worker,
        [table_mon]
    },

    {ok, {SupFlags, [TableServer]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
