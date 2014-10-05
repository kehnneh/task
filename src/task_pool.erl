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
    add/4
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
    id :: term(),
    opaque :: term(),
    callback :: fun((term()) -> {ok | continue | error, term()})
}).

-record(state, {
    %% The pool id is also the table id!
    id :: term(),
    ets :: ets:tid(),

    %% supervisor
    sup :: pid(),

    %% working set
    maxws :: non_neg_integer(),
    ws = 0 :: non_neg_integer(),

    %% run queue
    runq :: non_neg_integer()
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
-spec start_link(#pool_cfg{}) ->
    {ok, Pid :: pid()} |
    ignore |
    {error, Reason :: term()}.

start_link(#pool_cfg{} = Config) ->
    gen_server:start_link(?MODULE, Config#pool_cfg{sup = self()}, []).

add(Pid, Id, Opaque, Fun) ->
    gen_server:call(Pid, #taskreq{id = Id, opaque = Opaque, callback = Fun}, infinity).

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

init(#pool_cfg{id = Id, sup = Sup, maxws = MaxWs}) ->
    gen_server:cast(self(), {init, Sup}),
    {ok, Ets} = task_table_server:acquire_table(Id, [public, set, {keypos, #task.id}]),
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

handle_call(#taskreq{id = Id, callback = F, opaque = Opaque}, {Pid, _Tag}, #state{maxws = MaxWs, ws = Ws, runq = RunQ} = State) ->
    #state{ets = Ets, sup = Sup} = State,
    case ets:lookup(Ets, Id) of
        [] ->
            Task = #task{id = Id, callback = F, opaque = Opaque, caller = Pid, state = queued},
            ets:insert(Ets, Task),
            case MaxWs > Ws of
                true ->
                    start_task(Sup, Ets, Id),
                    {reply, {ok, Id}, State#state{ws = Ws + 1}};
                false ->
                    {reply, {ok, Id}, State#state{runq = RunQ + 1}}
            end;
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
    ChildSpec = {erlang:phash2(Id), {task_sup, start_link, []}, transient, infinity, supervisor, [task_sup]},
    Child = case supervisor:start_child(Parent, ChildSpec) of
                {ok, Pid} ->
                    Pid;
                {error, {already_started, Pid}} ->
                    Pid
            end,

    %% Scan task table and calibrate our working set and queued counts
    F = fun(#task{state = queued}, {RunQ, Ws}) ->
        {RunQ + 1, Ws};
    (#task{state = P}, {RunQ, Ws}) when is_pid(P) ->
        erlang:monitor(process, P),
        {RunQ, Ws + 1};
    (#task{}, Acc) ->
        Acc;
    (Entry, Acc) ->
        lager:warning("Found ~p in task pool table ~p", [Entry, Id]),
        Acc
    end,
    {RunQ, Ws} = ets:foldl(F, {0, 0}, Ets),

    {noreply, State#state{sup = Child, ws = Ws, runq = RunQ}};
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

handle_info({'DOWN', _Ref, _Type, _Pid, _Reason}, State) ->
    #state{
        ets = Ets,
        sup = Sup,
        ws = Ws,
        runq = RunQ
    } = State,
    case RunQ > 0 of
        true ->
            case ets:match_object(Ets, #task{id = '_', opaque = '_', callback = '_', caller = '_', state = queued}, 1) of
                '$end_of_table' ->
                    ok;
                {[#task{id = Id}], _Continuation} ->
                    start_task(Sup, Ets, Id)
            end,
            {noreply, State#state{runq = RunQ - 1}};
        false ->
            {noreply, State#state{ws = Ws - 1}}
    end;
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

terminate(normal, _State) ->
    ok;
terminate(Reason, State) ->
    lager:error("died with ~p in state ~p", [Reason, State]).

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

start_task(Sup, Ets, Id) ->
    {ok, Pid} = supervisor:start_child(Sup, [{Ets, Id}]),
    erlang:monitor(process, Pid).
