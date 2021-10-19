-module(rabbit_safe_dead_letter).

-behaviour(gen_server).

-export([start_link/1]).
%% gen_server callbacks
-export([init/1, terminate/2, handle_cast/2, handle_call/3, handle_info/2]).

%%TODO make configurable or leave at 0 which means 2000 as in
%% https://github.com/rabbitmq/rabbitmq-server/blob/1e7df8c436174735b1d167673afd3f1642da5cdc/deps/rabbit/src/rabbit_quorum_queue.erl#L726-L729
-define(CONSUMER_PREFETCH_COUNT, 10).

-record(state,{
          % q_name :: rabbit_amqqueue:name(),
          queue_type_state :: rabbit_queue_type:state(),
          limiter_state :: rabbit_limiter:lstate()
         }).
-type state() :: #state{}.

start_link(QName) ->
    gen_server:start_link(?MODULE, QName, [{hibernate_after, 60_000}]).

-spec init(rabbit_amqqueue:name()) -> {ok, state()}.
init(QName) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    QTypeState0 = rabbit_queue_type:init(),
    %%TODO supervise limiter
    {ok, LimiterPid} = rabbit_limiter:start_link(QName),
    LimiterState = rabbit_limiter:new(LimiterPid),
    ConsumerArgs = [{<<"x-internal-queue">>, longstr, <<"discards">>}],
    {ok, QTypeState1, _Actions = []} = rabbit_amqqueue:basic_consume(Q, false, self(),
                                                                     LimiterPid, rabbit_limiter:is_active(LimiterState),
                                                                     ?CONSUMER_PREFETCH_COUNT, name(QName), false,
                                                                     ConsumerArgs, undefined, none, QTypeState0),
    {ok, #state{queue_type_state = QTypeState1,
                limiter_state = LimiterState}}.

terminate(_Reason, _State) ->
    %% cancel subscription?
    ok.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

name({resource, Vhost, queue, Queue}) ->
    <<"internal-dead-letter-", Vhost/binary, "-", Queue/binary>>.
