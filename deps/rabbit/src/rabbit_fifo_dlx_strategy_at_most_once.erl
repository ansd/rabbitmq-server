-module(rabbit_fifo_dlx_strategy_at_most_once).

-include("rabbit_fifo.hrl").

-behaviour(rabbit_fifo_dlx_strategy).

-export([
         %% rabbit_fifo_dlx_strategy
         init/1,
         update_config/2,
         discard/3,
         %% mod_call effect
         publish/4
        ]).

-spec init(rabbit_fifo:state()) ->
    rabbit_fifo:state().
init(#rabbit_fifo{cfg = #cfg{resource = QRes}} = State) ->
    {ok, Q} = rabbit_amqqueue:lookup(QRes),
    RoutingKey = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-routing-key">>,
                                                           fun(_Pol, QArg) -> QArg end,
                                                           Q),
    Exchange = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-exchange">>,
                                                         fun(_Pol, QArg) -> QArg end,
                                                         Q),
    DLX = rabbit_misc:r(QRes, exchange, Exchange),
    MFA = {?MODULE, publish, [DLX, RoutingKey, QRes]},
    State#rabbit_fifo{dlx = MFA}.

-spec update_config(config(), rabbit_fifo:state()) ->
    {rabbit_fifo:state(), ra_machine:effects()}.
update_config(_, State) ->
    init(State).

publish(X, RK, QName, ReasonMsgs) ->
    case rabbit_exchange:lookup(X) of
        {ok, Exchange} ->
            [rabbit_dead_letter:publish(Msg, Reason, Exchange, RK, QName)
             || {Reason, Msg} <- ReasonMsgs];
        {error, not_found} ->
            ok
    end.

-spec discard([msg()], rabbit_dead_letter:reason(), rabbit_fifo:state()) ->
    {rabbit_fifo:state(), ra_machine:effects(), Delete :: boolean()}.
discard(Msgs, Reason, #rabbit_fifo{dlx = {Mod, Fun, Args}} = State) ->
    RaftIdxs = lists:filtermap(
                 fun (?INDEX_MSG(RaftIdx, ?DISK_MSG(_Header))) ->
                         {true, RaftIdx};
                     ({_PerMsgReason, ?INDEX_MSG(RaftIdx, ?DISK_MSG(_Header))})
                       when Reason =:= undefined ->
                         {true, RaftIdx};
                     (_IgnorePrefixMessage) ->
                         false
                 end, Msgs),
    Effect = {log, RaftIdxs,
              fun (Log) ->
                      Lookup = maps:from_list(lists:zip(RaftIdxs, Log)),
                      DeadLetters = lists:filtermap(
                                      fun (?INDEX_MSG(RaftIdx, ?DISK_MSG(_Header))) ->
                                              {enqueue, _, _, Msg} = maps:get(RaftIdx, Lookup),
                                              {true, {Reason, Msg}};
                                          (?INDEX_MSG(_, ?MSG(_Header, Msg))) ->
                                              {true, {Reason, Msg}};
                                          ({PerMsgReason, ?INDEX_MSG(RaftIdx, ?DISK_MSG(_Header))})
                                            when Reason =:= undefined ->
                                              {enqueue, _, _, Msg} = maps:get(RaftIdx, Lookup),
                                              {true, {PerMsgReason, Msg}};
                                          ({PerMsgReason, ?INDEX_MSG(_, ?MSG(_Header, Msg))})
                                            when Reason =:= undefined ->
                                              {true, {PerMsgReason, Msg}};
                                          (_IgnorePrefixMessage) ->
                                              false
                                      end, Msgs),
                      [{mod_call, Mod, Fun, Args ++ [DeadLetters]}]
              end},
    {State, [Effect], true}.
