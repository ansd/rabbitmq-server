-module(rabbit_dead_letter_forwarder).

%% This module is a generic server
%% supervised by the global supervisor rabbit_sup after the rabbit app has started.
%%
%% This module's purpose is to reliably forward dead-letter messages.
%% Traditionally, dead-lettering had at-most once semantics where messages were sent using rabbit_dead_letter:publish/5.
%%
%% Each quorum queue has an internal discards queue (queue of messages to be dead-lettered).
%% This module consumes from the discards queue of every quorum queue whose leader is on this node.
%% It then forwards the discarded messages to the target dead-letter exchange using publisher confirms.
%% Once publisher confirm is received, it acks the message to the discards queue.
%% If no publisher confirm is received, the message times out, gets written to a local stream (trash can),
%% and then acked in the discards queue. The latter is done so that the quorum queue's Raft log doesn't grow indefinitely.

-rabbit_boot_step({rabbit_dead_letter_forwarder,
                   [{description, "reliable dead letter forwarder"},
                    {mfa,         {rabbit_dead_letter_forwarder, start, []}},
                    {requires,    routing_ready}]}).

-behaviour(gen_server).

%% rabbit_boot_step exports
-export([start/0]).
%% called by supervisor2
-export([start_link/0]).
%% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2]).

-include_lib("amqp_client/include/amqp_client.hrl").

%% delivery_tag for consumer acknowledgements
-type(in_seq() :: non_neg_integer()).
%% delivery_tag for publisher confirms
-type(out_seq() :: non_neg_integer()).

-record(channel, {
          pid :: rabbit_types:channel(),
          connection :: rabbit_types:connection(),
          unacked = #{} :: #{in_seq() => out_seq()}
         }).

-record(state, {
          channels = #{} :: #{rabbit_types:vhost() => #channel{}},
          %% TODO @ansd there is no bimap in Erlang, is there?
          q_to_ctag = #{} :: #{rabbit_amqqueue:name() => rabbit_types:ctag()},
          ctag_to_q = #{} :: #{rabbit_types:ctag() => rabbit_amqqueue:name()}
         }).

-type state() :: #state{}.

-spec start() -> 'ok'.
start() ->
    %%TODO @ansd do not start if quorum_queue feature flag is disabled
    ok = rabbit_sup:start_restartable_child(?MODULE).

init([]) ->
    ok = application:ensure_started(amqp_client),
    {ok, #state{}}.

terminate(_Reason, _State) ->
    ok.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

handle_call({consume, {resource, Vhost, queue, Q} = QName},
            _From, #state{channels = Chans0,
                          q_to_ctag = QToCTag,
                          ctag_to_q = CTagToQ} = State0) ->
    %% Usually, it's recommended to have separate connections for publish and consume such that
    %% consume can continue when publisher connections are blocked due to alarms.
    %% However, in our case having a single connection is good enough because it's okay to stop
    %% consuming when we can't publish due to a cluster-wide alarm.
    %%TODO @ansd This approach doesn't work at all when flow control kicks in:
    %% a single slow destination queue should not slow down dead-lettering for all other queues.
    %% However, having a separate channel per queue sounds expensive when there are >10k quorum queues declared
    %% on the cluster?
    State = case maps:find(Vhost, Chans0) of
                        {ok, _} ->
                            %%TODO @ansd check whether connection and channel process are alive?
                            State0;
                        error ->
                            {ok, Connection} = amqp_connection:start(#amqp_params_direct{virtual_host = Vhost},
                                                                     <<"internal-dead-letter-", Vhost/binary>>),
                            {ok, Channel} = amqp_connection:open_channel(Connection),
                            #'basic.qos_ok'{} = amqp_channel:call(Channel, #'basic.qos'{
                                                                              prefetch_count = 10,
                                                                              global = false}),
                            #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
                            Chans = maps:put(Vhost,
                                             #channel{pid = Channel,
                                                      connection = Connection},
                                             Chans0),
                            ok = amqp_channel:register_confirm_handler(Channel, self()),
                            State0#state{channels = Chans}
                    end,
    #channel{pid = Chan} = maps:get(Vhost, State#state.channels),
    %%TODO @ansd: check whether queue consumer already exists?
    case amqp_channel:call(Chan, #'basic.consume'{queue = Q,
                                                  consumer_tag = <<"internal-dead-letter-", Vhost/binary, "-", Q/binary>>,
                                                  arguments = [{<<"x-internal-queue">>, longstr, <<"discards">>}]
                                                 }) of
        #'basic.consume_ok'{consumer_tag = CTag} ->
            rabbit_log:info("Registered ~s (with consumer tag ~s) with dead letter forwarder",
                            [rabbit_misc:rs(QName), CTag]),
            {reply, ok, State#state{q_to_ctag = maps:put(QName, CTag, QToCTag),
                                    ctag_to_q = maps:put(CTag, QName, CTagToQ)}};
        Other ->
            {reply, {error, Other}, State}
    end.

handle_cast(Request, State) ->
    rabbit_log:debug("dead_letter_forwarder: received cast ~p", [Request]),
    {noreply, State}.

handle_info({#'basic.deliver'{consumer_tag = CTag,
                              delivery_tag = InSeq,
                              exchange = Exchange,
                              routing_key = RKey},
             #amqp_msg{} = Content},
            #state{
               ctag_to_q = CTagToQ,
               channels = Channels0
              } = State) ->
    #{CTag := {resource, Vhost, queue, _} = QName} = CTagToQ,
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    DLX = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-exchange">>, fun res_arg/2, Q),
    DLRKey = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-routing-key">>, fun res_arg/2, Q),
    rabbit_log:debug("dead_letter_forwarder: forwarding message from ~s, old exchange=~s, old routing_key=~s, new exchange=~s, new routing_key=~s",
                     [rabbit_misc:rs(QName), Exchange, RKey, DLX, DLRKey]),
    #{Vhost := #channel{pid = Ch, unacked = Unacked0} = Channel} = Channels0,
    OutSeq = amqp_channel:next_publish_seqno(Ch),
    Method = #'basic.publish'{exchange = DLX, routing_key = DLRKey},
    ok = amqp_channel:cast(Ch, Method, Content),
    Unacked = Unacked0#{OutSeq => InSeq},
    Channels = Channels0#{Vhost := Channel#channel{unacked = Unacked}},
    {noreply, State#state{channels = Channels}};
handle_info(#'basic.ack'{delivery_tag = OutSeq, multiple = Multi},
            #state{channels = #{<<"/">> := #channel{pid = Ch, unacked = Unacked0} = Channel} = Channels} = State) ->
    %%TODO @ansd right now, this is hard-coded to default vhost "/"
    InSeq = maps:get(OutSeq, Unacked0),
    ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = InSeq,
                                           multiple = Multi}),
    Unacked = remove_acked(OutSeq, Multi, Unacked0),
    rabbit_log:debug("dead_letter_forwarder: forwarded ack", []),
    {noreply, State#state{channels = Channels#{<<"/">> := Channel#channel{unacked = Unacked}}}};
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{consumer_tag = CTag},
            #state{q_to_ctag = QToCTag,
                   ctag_to_q = CTagToQ} = State) ->
    QName = maps:get(CTag, CTagToQ),
    rabbit_log:info("De-registered ~s (with consumer tag ~s) from dead letter forwarder",
                    [rabbit_misc:rs(QName), CTag]),
    {noreply, State#state{q_to_ctag = maps:remove(QName, QToCTag),
                           ctag_to_q = maps:remove(CTag, CTagToQ)}}.

remove_acked(AckedSeq, false, Unacked) ->
    maps:remove(AckedSeq, Unacked);
remove_acked(AckedSeq, true, Unacked) ->
    maps:filter(fun(OutSeq, _InSeq) -> OutSeq > AckedSeq end, Unacked).

res_arg(_PolVal, ArgVal) -> ArgVal.
