-module(rabbit_fifo_dlx).

-include("rabbit_fifo.hrl").

% client API, e.g. for rabbit_fifo_dlx_client
-export([make_checkout/1,
         make_settle/1]).

% called by rabbit_fifo delegating DLX handling to this module
-export([init/0, apply/3, discard/2, overview/1, checkout/1]).

%% This module handles the dead letter (DLX) part of the rabbit_fifo state machine.
%% This is a separate module to better unit test and provide separation of concerns.
%% This module maintains its own state:
%% a queue of DLX messages, a single node local DLX consumer, and some stats.
%% The state of this module is included into rabbit_fifo state because there can only by one Ra state machine.
%% The rabbit_fifo module forwards all DLX commands to this module where we then update the DLX specific state only:
%% e.g. DLX consumer subscribed, adding / removing discarded messages, stats
%%
%% It also runs its own checkout logic sending DLX messages to the DLX consumer.
%%
%% TODO: Does it hook into the tick as well?

-record(dlx_consumer,{
          %% We don't require a consumer tag because a consumer tag is a means to distinguish
          %% multiple consumers in the same channel. The rabbit_fifo_dlx_worker channel like process however
          %% creates only a single consumer to this quorum queue's discards queue.
          pid :: pid(),
          prefetch :: non_neg_integer(),
          checked_out = #{} :: #{msg_id() => indexed_msg()},
          next_msg_id = 0 :: msg_id() % part of snapshot data
          % total number of checked out messages - ever
          % incremented for each delivery
          % delivery_count = 0 :: non_neg_integer(),
          % status = up :: up | suspected_down | cancelled
         }).

-record(state,{
          consumer = undefined :: #dlx_consumer{} | undefined,
          %% Queue of dead-lettered messages.
          discards = lqueue:new() :: lqueue:lqueue(indexed_msg()),
          msg_bytes = 0 :: non_neg_integer(),
          msg_bytes_checkout = 0 :: non_neg_integer()
         }).
-opaque state() :: #state{}.

-record(checkout,{
          consumer :: pid(),
          prefetch :: non_neg_integer()
         }).
-record(settle, {msg_ids :: [msg_id()]}).
-opaque protocol() :: {dlx, #checkout{} | #settle{}}.

-export_type([state/0, protocol/0]).

init() ->
    #state{}.

make_checkout(NumUnsettled) ->
    {dlx, #checkout{consumer = self(),
                    prefetch = NumUnsettled
                   }}.

make_settle(MessageIds) when is_list(MessageIds) ->
    {dlx, #settle{msg_ids = MessageIds}}.

overview(#state{consumer = undefined,
                msg_bytes = MsgBytes,
                msg_bytes_checkout = 0,
                discards = Discards}) ->
    overview0(Discards, #{}, MsgBytes, 0);
overview(#state{consumer = #dlx_consumer{checked_out = Checked},
                msg_bytes = MsgBytes,
                msg_bytes_checkout = MsgBytesCheckout,
                discards = Discards}) ->
    overview0(Discards, Checked, MsgBytes, MsgBytesCheckout).

overview0(Discards, Checked, MsgBytes, MsgBytesCheckout) ->
    #{num_discarded => lqueue:len(Discards),
      num_discard_checked_out => map_size(Checked),
      discard_message_bytes => MsgBytes,
      discard_checkout_message_bytes => MsgBytesCheckout}.

apply(_Meta, #checkout{consumer = CPid,
                       prefetch = Prefetch}, State) ->
    C = #dlx_consumer{pid = CPid, prefetch = Prefetch},
    {State#state{consumer = C}, ok, []};
apply(_Meta, #settle{msg_ids = MsgIds},
      #state{consumer = #dlx_consumer{checked_out = Checked} = C,
             msg_bytes_checkout = BytesCheckout} = State0) ->
    Acked = maps:with(MsgIds, Checked),
    AckedBytes = maps:fold(fun(_MsgId, Msg, Bytes) ->
                                   Header = rabbit_fifo:get_msg_header(Msg),
                                   Size = rabbit_fifo:get_header(size, Header),
                                   Bytes + Size
                           end, 0, Acked),
    Unacked = maps:without(MsgIds, Checked),
    State = State0#state{consumer = C#dlx_consumer{checked_out = Unacked},
                         msg_bytes_checkout = BytesCheckout - AckedBytes},
    {State, Acked}.

discard(Msg, #state{discards = Discards0,
                    msg_bytes = MsgBytes0} = State) ->
    Discards = lqueue:in(Msg, Discards0),
    Header = rabbit_fifo:get_msg_header(Msg),
    Size = rabbit_fifo:get_header(size, Header),
    MsgBytes = MsgBytes0 + Size,
    State#state{discards = Discards,
                msg_bytes = MsgBytes}.

checkout(#state{consumer = undefined,
                discards = Discards} = State) ->
    case lqueue:is_empty(Discards) of
        true ->
            ok;
        false ->
            rabbit_log:warning("there are dead-letter messages but no dead-letter consumer")
    end,
    {State, []};
checkout(State) ->
    checkout0(checkout_one(State), {[],[]}).

checkout0({success, MsgId, ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header)), State}, {InMemMsgs, LogMsgs}) when is_integer(RaftIdx) ->
    DelMsg = {RaftIdx, {MsgId, Header}},
    SendAcc = {InMemMsgs, [DelMsg|LogMsgs]},
    checkout0(checkout_one(State ), SendAcc);
checkout0({success, MsgId, ?INDEX_MSG(Idx, ?MSG(Header, Msg)), State}, {InMemMsgs, LogMsgs}) when is_integer(Idx) ->
    DelMsg = {MsgId, {Header, Msg}},
    SendAcc = {[DelMsg|InMemMsgs], LogMsgs},
    checkout0(checkout_one(State), SendAcc);
%TODO Is that a fallback for old message formats?
% checkout0({success, _MsgId, ?TUPLE(_, _), State}, SendAcc) ->
    % checkout0(checkout_one(State), SendAcc);
checkout0(#state{consumer = #dlx_consumer{pid = CPid}} = State, SendAcc) ->
    Effects = delivery_effects(CPid, SendAcc),
    {State, Effects}.

checkout_one(#state{consumer = #dlx_consumer{checked_out = Checked0,
                                         next_msg_id = Next} = Con0} = State0) ->
    case take_next_msg(State0) of
        {ConsumerMsg, State1} ->
            Checked = maps:put(Next, ConsumerMsg, Checked0),
            %%TODO check prefetch
            State2 = State1#state{consumer = Con0#dlx_consumer{checked_out = Checked,
                                                           next_msg_id = Next + 1}},
            Header = rabbit_fifo:get_msg_header(ConsumerMsg),
            State = add_bytes_checkout(Header, State2),
            {success, Next, ConsumerMsg, State};
        empty ->
            State0
    end.

take_next_msg(#state{discards = Discards0} = State) ->
    case lqueue:out(Discards0) of
        {empty, _} ->
            empty;
        {{value, IndexMsg}, Discards} ->
            {IndexMsg, State#state{discards = Discards}}
    end.

add_bytes_checkout(Header, #state{msg_bytes = Bytes,
                                  msg_bytes_checkout = BytesCheckout} = State) ->
    Size = rabbit_fifo:get_header(size, Header),
    State#state{msg_bytes = Bytes - Size,
                msg_bytes_checkout = BytesCheckout + Size}.

%% returns at most one delivery effect because there is only one consumer
delivery_effects(_CPid, {[], []}) ->
    [];
delivery_effects(CPid, {InMemMsgs, []}) ->
    [{send_msg, CPid, {delivery, lists:reverse(InMemMsgs)}, [local, ra_event]}];
delivery_effects(CPid, {InMemMsgs, IdxMsgs0}) ->
    IdxMsgs = lists:reverse(IdxMsgs0),
    {RaftIdxs, Data} = lists:unzip(IdxMsgs),
    [{log, RaftIdxs,
     fun(Log) ->
             Msgs0 = lists:zipwith(fun ({enqueue, _, _, Msg}, {MsgId, Header}) ->
                                           {MsgId, {Header, Msg}}
                                   end, Log, Data),
             Msgs = case InMemMsgs of
                        [] ->
                            Msgs0;
                        _ ->
                            lists:sort(InMemMsgs ++ Msgs0)
                    end,
             [{send_msg, CPid, {delivery, Msgs}, [local, ra_event]}]
     end,
     {local, node(CPid)}}].
