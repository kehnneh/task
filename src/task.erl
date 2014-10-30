%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2014 10:55 PM
%%%-------------------------------------------------------------------
-module(task).
-author("kehnneh").
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

-record(state, {
    ets :: ets:tid(),
    id :: term(),
    opaque :: term(),
    callback :: fun((term()) -> {ok | continue | error, term()}),
    caller :: pid()
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
-spec start_link(Args :: term()) ->
    {ok, Pid :: pid()} |
    ignore |
    {error, Reason :: term()}.

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

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

init({Ets, Id}) ->
    false = erlang:process_flag(trap_exit, true),
    [#task{opaque = Opaque, callback = Callback, caller = Pid}] = ets:lookup(Ets, Id),
    ets:update_element(Ets, Id, {#task.state, self()}),
    gen_server:cast(self(), continue),
    lager:debug("Task ~p initiated", [Id]),
    {ok, #state{ets = Ets, id = Id, opaque = Opaque, callback = Callback, caller = Pid}, 0}.

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

handle_cast(continue, #state{id = Id, opaque = Opaque, callback = F} = State) ->
    lager:debug("task ~p progressing", [Id]),
    case catch F(Opaque) of
        {ok, NewOpaque} ->
            {stop, normal, State#state{opaque = {ok, NewOpaque}}};
        {continue, NewOpaque} ->
            gen_server:cast(self(), continue),
            {noreply, State#state{opaque = NewOpaque}};
        Error ->
            {stop, normal, State#state{opaque = Error}}
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

terminate(shutdown, State) ->
    terminate(normal, State#state{opaque = shutdown});
terminate(Reason, #state{ets = Ets, id = Id, caller = Pid, opaque = Opaque}) ->
    lager:debug("Task ~p shutdown: ~p", [Id, Reason]),
    case deliver(Pid, #taskret{id = Id, opaque = Opaque}) of
        ok ->
            ets:delete(Ets, Id);
        noconnect ->
            ets:update_element(Ets, Id, [{#task.state, done}])
    end.

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

deliver(Process, Msg) ->
    try erlang:monitor(process, Process) of
        Mref ->
            Ret = (catch erlang:send(Process, Msg, [noconnect])),
            erlang:demonitor(Mref, [flush]),
            Ret
    catch
        error:_ ->
            Node = get_node(Process),
            monitor_node(Node, true),
            receive
                {nodedown, Node} ->
                    monitor_node(Node, false),
                    noconnect
            after 0 ->
                Process ! Msg,
                ok
            end
    end.

get_node(Process) ->
    case Process of
        {_S, N} when is_atom(N) ->
            N;
        _ when is_pid(Process) ->
            node(Process)
    end.
