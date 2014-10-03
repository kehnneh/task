%%%-------------------------------------------------------------------
%%% @author kehnneh
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2014 9:21 PM
%%%-------------------------------------------------------------------
-module(task_table_server).
-author("kehnneh").

-behaviour(gen_server).

-compile([{parse_transform, lager_transform}]).

%% API
-export([
    start_link/0,
    acquire_table/2
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

-record(table, {
    id = undefined :: term(),
    ets = undefined :: ets:tid(),
    args = [] :: [term()],
    owner = undefined :: pid()
}).

-record(tablereq, {
    id = undefined :: term(),
    args = [] :: [term()]
}).

-record(state, {
    ets = undefined :: ets:tid()
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
-spec start_link() ->
    {ok, Pid :: pid()} |
    ignore |
    {error, Reason :: term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
    gen_server:call(?MODULE, #tablereq{id = Id, args = Args}, infinity).

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

init([]) ->
    Ets = ets:new(?MODULE, [private, set, {keypos, #table.id}]),
    {ok, #state{ets = Ets}}.

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

handle_call(#tablereq{id = undefined}, _From, State) ->
    {reply, {error, einval}, State};
handle_call(#tablereq{id = Id, args = Args}, {Pid, _Tag}, #state{ets = Ets} = State) ->
    Self = self(),
    case validate(Args) of
        true ->
            Reply = case ets:lookup(Ets, Id) of
                        [] ->
                            TidArgs = Args ++ [{heir, self(), Id}],
                            case catch ets:new(?MODULE, TidArgs) of
                                {'EXIT', {badarg, _}} ->
                                    lager:warning("~p created table with faulty args: ~p", [Pid, Args]),
                                    {error, einval};
                                Tid ->
                                    ets:insert(Ets, #table{id = Id, ets = Tid, args = TidArgs, owner = Pid}),
                                    ets:give_away(Tid, Pid, <<>>),
                                    lager:debug("created table ~p with args ~p for ~p", [Id, Args, Pid]),
                                    {ok, Tid}
                            end;
                        [#table{ets = Tid, owner = Self, args = Args}] ->
                            ets:update_element(Ets, Id, {#table.owner, Pid}),
                            ets:give_away(Tid, Pid, <<>>),
                            lager:debug("gave table ~p with args ~p to ~p", [Tid, Args, Pid]),
                            {ok, Tid};
                        [#table{owner = PoS, args = OtherArgs}] when PoS =:= Pid; PoS =:= Self ->
                            lager:error("~p requested table ~p with args ~p, but it exists with args ~p", [Pid, Id, Args, OtherArgs]),
                            {error, enotuniq};
                        [#table{ets = Tid, owner = Pid}] ->
                            lager:notice("~p requested table ~p again", [Pid, Id]),
                            {ok, Tid};
                        [#table{owner = Owner}] ->
                            lager:error("~p requested table ~p with owner ~p", [Pid, Id, Owner]),
                            {error, eperm}
                    end,
            {reply, Reply, State};
        false ->
            {reply, {error, einval}, State}
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

validate([]) ->
    true;
validate([named_table | _]) ->
    false;
validate([{heir, _, _} | _]) ->
    false;
validate([_ | T]) ->
    validate(T).
