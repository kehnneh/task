%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2014 10:23 PM
%%%-------------------------------------------------------------------
-module(task_pool).
-author("kehnneh").
-include("task_pool.hrl").
-include("task.hrl").
-behaviour(gen_server).

-compile([{parse_transform, lager_transform}]).

%% API
-export([
    start_link/1,
    run/4,
    stop/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(taskreq, {
    id          :: term(),
    opaque      :: term(),
    callback    :: fun((term()) -> {ok | continue | error, term()})
}).

-record(taskstopreq, {
    id          :: term()
}).

-record(state, {
    %% The pool id is also the table id!
    id          :: atom(),
    ets         :: ets:tid(),

    %% supervisor
    sup         :: pid(),

    %% working set
    maxws       :: non_neg_integer(),
    ws      = 0 :: non_neg_integer(),

    %% run queue
    runq        :: non_neg_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(#cfg_pool{}) ->
    {ok, Pid :: pid()} |
    ignore |
    {error, Reason :: term()}.

start_link(#cfg_pool{id = Proc} = Config) when is_atom(Proc) ->
    gen_server:start_link({local, Proc}, ?MODULE, Config#cfg_pool{sup = self()}, []).

%%--------------------------------------------------------------------
%% @doc
%% Runs a task
%%
%% @end
%%--------------------------------------------------------------------
-spec run(Proc :: pid(), Id :: term(), Opaque :: term(), Fun :: fun()) ->
    {ok, Id :: term()} |
    {error, Reason :: term()}.

run(Proc, Id, Opaque, Fun) ->
    Request = #taskreq{id = Id, opaque = Opaque, callback = Fun},
    gen_server:call(Proc, Request, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously stops a task
%%
%% @end
%%--------------------------------------------------------------------
-spec stop(Proc :: pid(), Id :: term()) ->
    ok.

stop(Proc, Id) ->
    Request = #taskstopreq{id = Id},
    gen_server:cast(Proc, Request).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} |
    {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} |
    ignore.

init(#cfg_pool{id = Id}) when not is_atom(Id) ->
    {stop, {error, {badarg, {id, Id}}}};
init(#cfg_pool{sup = Sup}) when not is_pid(Sup) ->
    {stop, {error, {badarg, {sup, Sup}}}};
init(#cfg_pool{maxws = MaxWs}) when not is_integer(MaxWs); MaxWs =< 0 ->
    {stop, {error, {badarg, {maxws, MaxWs}}}};
init(#cfg_pool{id = Id, sup = Sup, maxws = MaxWs}) ->
    {ok, Ets} = task_ets_mon:acquire_table(Id, [public, set, {keypos, #task.id}]),
    gen_server:cast(self(), {init, Sup}),
    {ok, #state{id = Id, ets = Ets, maxws = MaxWs}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.

handle_call(#taskreq{id = Id, callback = F, opaque = Opaque}, {Pid, _Tag}, #state{id = PoolId, maxws = MaxWs, ws = Ws, runq = RunQ} = State)
  when is_function(F, 1) ->
    #state{ets = Ets, sup = Sup} = State,
    case ets:lookup(Ets, Id) of
        [] ->
            Task = #task{id = Id, callback = F, opaque = Opaque, caller = Pid, state = queued},
            ets:insert(Ets, Task),
            case MaxWs > Ws of
                true ->
                    case supervisor:start_child(Sup, [{Ets, Id}]) of
                        {ok, Pid} ->
                            erlang:monitor(process, Pid),
                            lager:debug("Task Pool ~p started task ~p at ~p", [PoolId, Id, Pid]),
                            {reply, {ok, Id}, State#state{ws = Ws + 1}};
                        Error ->
                            lager:warning("Task Pool ~p couldn't start task ~p: ~p", [PoolId, Id, Error]),
                            {reply, Error, State}
                    end;
                false ->
                    lager:debug("Task Pool ~p queued task ~p", [PoolId, Id]),
                    {reply, {ok, Id}, State#state{runq = RunQ + 1}}
            end;
        [#task{caller = Pid}] ->
            {reply, {ok, Id}, State};
        [#task{}] ->
            {reply, {error, eexist}, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, einval}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.

handle_cast({init, Parent}, #state{id = Id, ets = Ets} = State) ->
    %% This clause must only be entered as a result of init/1

    %% Acquire task process supervisor
    SupId = list_to_atom(atom_to_list(Id) ++ "_sup"),
    ChildSpec = {SupId, {task_sup, start_link, []}, transient, infinity, supervisor, [task_sup]},
    Sup = case supervisor:start_child(Parent, ChildSpec) of
                {ok, Pid} ->
                    Pid;
                {error, {already_started, Pid}} ->
                    Pid
            end,
    lager:debug("Task Pool ~p supervisor running at ~p", [Sup]),

    %% Scan task table and calibrate our working set and queued counts
    F = fun(#task{state = queued}, {Ws, RunQ}) ->
        {Ws, RunQ + 1};
    (#task{state = P}, {RunQ, Ws}) when is_pid(P) ->
        erlang:monitor(process, P),
        {Ws + 1, RunQ};
    (#task{}, Acc) ->
        Acc;
    (Entry, Acc) ->
        lager:warning("Found ~p in task pool table ~p", [Entry, Id]),
        Acc
    end,
    {Ws, RunQ} = ets:foldl(F, {0, 0}, Ets),
    lager:debug("Task Pool ~p initialized with ~p runners and ~p queued tasks", [Id, Ws, RunQ]),

    {noreply, State#state{sup = Sup, ws = Ws, runq = RunQ}};
handle_cast(#taskstopreq{id = Id}, #state{id = PoolId, ets = Ets, sup = Sup, runq = RunQ} = State) ->
    case ets:lookup(Ets, Id) of
        [#task{state = Pid}] when is_pid(Pid) ->
            supervisor:terminate_child(Sup, Pid),
            lager:debug("Task Pool ~p terminated task ~p", [PoolId, Id]),
            {noreply, State};
        [#task{caller = Caller, state = queued}] ->
            unqueue_task(Ets, Id, Caller),
            lager:debug("Task Pool ~p removed task ~p from queue", [PoolId, Id]),
            {noreply, State#state{runq = RunQ - 1}};
        _ ->
            lager:debug("Task Pool ~p cannot stop task ~p", [PoolId, Id]),
            {noreply, State}
    end;
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.

%% If a task dies when the pool has tasks queued, start a new
%% task, if able
handle_info({'DOWN', _, _, _, _} = Msg, #state{runq = RunQ} = State) when RunQ > 0 ->
    #state{id = Id, ets = Ets, sup = Sup} = State,
    Pattern = #task{id = '_', opaque = '_', callback = '_', caller = '_', state = queued},
    {[#task{id = TaskId, caller = Caller}], _Continuation} = ets:match_object(Ets, Pattern, 1),
    case catch supervisor:start_child(Sup, [{Ets, TaskId}]) of
        {ok, Pid} ->
            lager:debug("Task Pool ~p started task ~p at ~p", [Id, TaskId, Pid]),
            erlang:monitor(process, Pid),
            {noreply, State#state{runq = RunQ - 1}};
        {'EXIT', Reason} ->
            lager:warning("Task Pool ~p couldn't start task ~p: ~p", [Id, TaskId, Reason]),
            unqueue_task(Ets, TaskId, Caller),
            handle_info(Msg, State#state{runq = RunQ - 1})
    end;
handle_info({'DOWN', _, _, _, _}, #state{id = Id, ws = Ws} = State) ->
    lager:debug("Task Pool ~p has no tasks left to queue", [Id]),
    {noreply, State#state{ws = Ws - 1}};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: #state{}) ->
    term().

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{}, Extra :: term()) ->
    {ok, NewState :: #state{}} |
    {error, Reason :: term()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes a queued task from the ETS table and alerts its caller
%% of its shutdown
%%
%% @end
%%--------------------------------------------------------------------
-spec unqueue_task(Ets :: ets:tid(), Id :: term(), Pid :: pid()) ->
    true.

unqueue_task(Ets, Id, Pid) ->
    Pid ! {Id, shutdown},
    ets:delete(Ets, Id).
