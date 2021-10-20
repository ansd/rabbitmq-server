-module(rabbit_fifo_dlx_worker).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/1]).
%% gen_server2 callbacks
-export([init/1, terminate/2, handle_cast/2, handle_call/3, handle_info/2, code_change/3]).

%%TODO make configurable or leave at 0 which means 2000 as in
%% https://github.com/rabbitmq/rabbitmq-server/blob/1e7df8c436174735b1d167673afd3f1642da5cdc/deps/rabbit/src/rabbit_quorum_queue.erl#L726-L729
-define(CONSUMER_PREFETCH_COUNT, 10).

-record(pending, {
          consumed_msg_id :: non_neg_integer(),
          delivery :: rabbit_types:delivery(),
          unsettled :: [rabbit_amqqueue:name()],
          settled = [] :: [rabbit_amqqueue:name()],
          %% number of times the message was (tried to be) published
          %% to target dead-letter queues that haven't confirmed yet
          count = 1 :: non_neg_integer(),
          %% epoch time in milliseconds when the message was last (tried to be) published
          %% to target dead-letter queues that haven't confirmed yet
          last_publish :: integer()
         }).

-record(state, {
          %% In this version of the module, we have one rabbit_fifo_dlx_worker per quorum queue
          %% (if x-dead-letter-strategy at-least-once is used).
          %% Hence, there is a single queue we consume from.
          consumer_queue_ref :: rabbit_amqqueue:name(),
          consumer_tag :: rabbit_types:ctag(),
          queue_type_state :: rabbit_queue_type:state(),
          %% Consumed messages for which we haven't received a publisher confirms yet.
          %% Therefore, they also haven't been ACKed yet back to the source discards queue.
          %% This buffer contains at most CONSUMER_PREFETCH_COUNT pending messages at any given point in time.
          pendings = #{} :: #{OutSeq :: non_neg_integer() => #pending{}},
          %% next publisher confirm delivery tag sequence number
          next_out_seq = 1
         }).

-type state() :: #state{}.

start_link(QRef) ->
    gen_server:start_link(?MODULE, QRef, [{hibernate_after, 60_000}]).

-spec init(rabbit_amqqueue:name()) -> {ok, state()}.
init(QRef) ->
    {ok, Q} = rabbit_amqqueue:lookup(QRef),
    QTypeState0 = rabbit_queue_type:init(),
    ConsumerTag = name(QRef),
    ConsumeSpec = #{no_ack => false,
                    channel_pid => self(),
                    %% limiter is about global QoS which is not supported in quorum queues
                    %% and about to be deprecated in RabbitMQ
                    limiter_pid => undefined,
                    limiter_active => false,
                    prefetch_count => ?CONSUMER_PREFETCH_COUNT,
                    consumer_tag => ConsumerTag,
                    exclusive_consume => false,
                    args => [{<<"x-internal-queue">>, longstr, <<"discards">>}],
                    ok_msg => undefined,
                    acting_user =>  none},
    %%TODO call rabbit_fifo_client directly?
    %%TODO refactor fifo dlx stuff into separate module (and call e.g. rabbit_fifo_dlx:checkout() here)
    {ok, QTypeState1, _Actions = []} = rabbit_queue_type:consume(Q, ConsumeSpec, QTypeState0),
    {ok, #state{consumer_queue_ref = QRef,
                consumer_tag = ConsumerTag,
                queue_type_state = QTypeState1}}.

terminate(_Reason, _State) ->
    %% cancel subscription?
    ok.

handle_call(Request, From, State) ->
    rabbit_log:warning("~s received unhandled call from ~p: ~p", [?MODULE, From, Request]),
    {noreply, State}.

handle_cast({queue_event, QRef, Evt},
            #state{queue_type_state = QTypeState0} = State0) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QTypeState0) of
        {ok, QTypeState1, Actions} ->
            State1 = State0#state{queue_type_state = QTypeState1},
            State2 = handle_queue_actions(Actions, State1),
            {noreply, State2};
        %% TODO handle as done in
        %% https://github.com/rabbitmq/rabbitmq-server/blob/9cf18e83f279408e20430b55428a2b19156c90d7/deps/rabbit/src/rabbit_channel.erl#L771-L783
        eol ->
            {noreply, State0};
        {protocol_error, _Type, _Reason, _ReasonArgs} ->
            {noreply, State0}
    end;
%%TODO handle
%% {mandatory_received,1}
handle_cast(Request, State) ->
    rabbit_log:warning("~s received unhandled cast ~p", [?MODULE, Request]),
    {noreply, State}.

handle_info(Info, State) ->
    rabbit_log:warning("~s received unhandled info ~p", [?MODULE, Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% https://github.com/rabbitmq/rabbitmq-server/blob/9cf18e83f279408e20430b55428a2b19156c90d7/deps/rabbit/src/rabbit_channel.erl#L2855-L2888
handle_queue_actions(Actions, State0) ->
    lists:foldl(
      fun ({deliver, CTag, AckRequired, Msgs}, S0) ->
              handle_deliver(CTag, AckRequired, Msgs, S0);
          ({settled, QRef, MsgSeqs}, S0) ->
              S1 = handle_settled(QRef, MsgSeqs, S0),
              maybe_ack(S1);
          ({rejected, _QRef, _MsgSeqNos}, S0) ->
              rabbit_log:error("queue action rejected not yet implemented", []),
              S0
      end, State0, Actions).

handle_deliver(CTag, _AckRequired = true, Msgs,
               #state{consumer_tag = CTag,
                      consumer_queue_ref = {resource, Vhost, queue, _} = QRef
                     } = State) when is_list(Msgs) ->
    %% Lookup policies and routes on every deliver because they can change dynamically.
    {ok, Q} = rabbit_amqqueue:lookup(QRef),
    DLRKey = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-routing-key">>, fun res_arg/2, Q),
    DLXName = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-exchange">>, fun res_arg/2, Q),
    DLXRef = rabbit_misc:r(Vhost, exchange, DLXName),
    DLX = rabbit_exchange:lookup_or_die(DLXRef),
    lists:foldl(fun(Msg, S) ->
                        handle_deliver0(Msg, DLXRef, DLX, DLRKey, S)
                end, State, Msgs).

handle_deliver0({_QName, _QPid, MsgId, _Redelivered, #basic_message{content = Content}},
                DLXRef, DLX, DLRKey,
                State0 = #state{next_out_seq = OutSeq,
                                pendings = Pendings}) ->
    {ok, BasicMsg} = rabbit_basic:message(DLXRef, DLRKey, Content),
    Delivery = rabbit_basic:delivery(true, true, BasicMsg, OutSeq),
    QNames = rabbit_exchange:route(DLX, Delivery),
    Pend = #pending{
              consumed_msg_id = MsgId,
              delivery = Delivery,
              unsettled = QNames,
              last_publish = os:system_time(millisecond)
             },
    State1 = State0#state{next_out_seq = OutSeq + 1,
                          pendings = maps:put(OutSeq, Pend, Pendings)},
    deliver_to_queues({Delivery, QNames}, State1).

deliver_to_queues({Delivery = #delivery{message = #basic_message{exchange_name = XName,
                                                                 routing_keys = RKeys}},
                   RoutedToQueueNames}, State0 = #state{queue_type_state = QTypeState0}) ->
    Qs =  rabbit_amqqueue:lookup(RoutedToQueueNames),
    {ok, QTypeState1, Actions} = rabbit_queue_type:deliver(Qs, Delivery, QTypeState0),
    State1 = State0#state{queue_type_state = QTypeState1},
    % rabbit_global_counters:messages_routed(amqp091, length(Qs)),
    case Qs of
        [] ->
            rabbit_log:warning("No queue bound to dead-letter exchange ~p with routing keys ~p.",
                               [XName, RKeys]),
            State1;
        _ ->
            handle_queue_actions(Actions, State1)
    end.

handle_settled(QRef, MsgSeqs, #state{pendings = Pendings0} = State0) ->
    Pendings1 = lists:foldl(fun (MsgSeq, P0) ->
                                    handle_settled0(QRef, MsgSeq, P0)
                            end, Pendings0, MsgSeqs),
    State0#state{pendings = Pendings1}.

handle_settled0(QRef, MsgSeq, Pendings) ->
    #pending{unsettled = Unset0, settled = Set0} = Pend0 = maps:get(MsgSeq, Pendings),
    Unset1 = lists:delete(QRef, Unset0),
    Set1 = [QRef | Set0],
    Pend1 = Pend0#pending{unsettled = Unset1, settled = Set1},
    maps:update(MsgSeq, Pend1, Pendings).

maybe_ack(#state{consumer_queue_ref = QRef,
                 consumer_tag = CTag,
                 queue_type_state = QTypeState0,
                 pendings = Pendings0} = State0) ->
    Settled = maps:filter(fun(_OutSeq, #pending{unsettled = [], settled = [_|_]}) ->
                                  %% Ack because there is at least one target queue and all
                                  %% target queues settled (i.e. combining publisher confirm
                                  %% and mandatory flag semantics).
                                  true;
                             (_, _) ->
                                  false
                          end, Pendings0),
    %%TODO The order doesn't matter, does it?
    SettledOutSeqs = maps:keys(Settled),
    {ok, QTypeState1, Actions} = rabbit_queue_type:settle(QRef, complete, CTag,
                                                          SettledOutSeqs, QTypeState0),
    %%TODO Before deleting settled messages from our state,
    %% we don't have to wait until the quorum queue applied the ack, do we?
    Pendings1 = maps:without(SettledOutSeqs, Pendings0),
    State1 = State0#state{queue_type_state = QTypeState1,
                          pendings = Pendings1},
    handle_queue_actions(Actions, State1).

name({resource, Vhost, queue, Queue}) ->
    <<"internal-dead-letter-", Vhost/binary, "-", Queue/binary>>.

res_arg(_PolVal, ArgVal) -> ArgVal.
