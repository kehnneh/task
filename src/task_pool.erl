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
    start_link/1
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
    opaque :: term()
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
    {ok, Ets} = task_table_server:acquire_table(Id, [public, set, {keypos, #task.id}]),
    F = fun(#task{state = queued}, {RunQ, Ws}) ->
        {RunQ + 1, Ws};
    (#task{state = Pid}, {RunQ, Ws}) when is_pid(Pid) ->
        erlang:monitor(process, Pid),
        {RunQ, Ws + 1};
    (#task{}, Acc) ->
        Acc;
    (Entry, Acc) ->
        lager:warning("Found ~p in task pool table ~p", [Entry, Id]),
        Acc
    end,
    ets:foldl(F, {0, 0}, Ets),
    gen_server:cast(self(), {init_sup, Sup}),
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

handle_call(#taskreq{id = Id, opaque = Opaque}, {Pid, _Tag}, #state{maxws = MaxWs, ws = Ws, runq = RunQ} = State) ->
    #state{ets = Ets, sup = Sup} = State,
    case ets:lookup(Ets, Id) of
        [] ->
            Task = #task{id = Id, opaque = Opaque, caller = Pid},
            ets:insert(Ets, Task),
            case MaxWs > Ws of
                true ->
                    {ok, Child} = supervisor:start_child(Sup, [{Ets, Id}]),
                    erlang:monitor(process, Child),
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

handle_cast({init_sup, Parent}, #state{id = Id} = State) ->
    ChildSpec = {Id, {task_sup, start_link, []}, transient, infinity, supervisor, [task_sup]},
    case supervisor:start_child(Parent, ChildSpec) of
        {ok, Child} ->
            lager:debug("pool ~p started task supervisor", [Id]),
            {noreply, State#state{sup = Child}};
        {error, {already_started, Child}} ->
            lager:debug("pool ~p acquired running task supervisor", [Id]),
            {noreply, State#state{sup = Child}};
        Error ->
            {stop, Error, State}
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

handle_info({'DOWN', _Ref, _Type, _Pid, _Reason}, State) ->
    #state{
        ets = Ets,
        sup = Sup,
        ws = Ws,
        runq = RunQ
    } = State,
    case RunQ > 0 of
        true ->
            case ets:match(Ets, #task{id = '_', opaque = '_', caller = '_'}, 1) of
                '$end_of_table' ->
                    ok;
                {[#task{id = Id}], _Continuation} ->
                    {ok, Child} = supervisor:start_child(Sup, [Ets, Id]),
                    erlang:monitor(process, Child)
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
