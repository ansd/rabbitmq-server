%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqp1_0_session).

-behaviour(gen_server).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_amqp1_0.hrl").

-define(HIBERNATE_AFTER, 6_000).
-define(MAX_SESSION_WINDOW_SIZE, 65_535).
-define(DEFAULT_MAX_HANDLE, 16#ffffffff).
-define(INIT_TXFR_COUNT, 0).
-define(DEFAULT_SEND_SETTLED, false).
%% [3.4]
-define(OUTCOMES, [?V_1_0_SYMBOL_ACCEPTED,
                   ?V_1_0_SYMBOL_REJECTED,
                   ?V_1_0_SYMBOL_RELEASED,
                   ?V_1_0_SYMBOL_MODIFIED]).
%% Just make these constant for the time being.
-define(INCOMING_CREDIT, 65_536).
-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(TOPIC_PERMISSION_CACHE, topic_permission_cache).
-define(UINT(N), {uint, N}).

% [2.8.4]
-type link_handle() :: non_neg_integer().
% [2.8.8]
-type delivery_number() :: sequence_no().
% [2.8.9]
-type transfer_number() :: sequence_no().

-export([start_link/6,
         process_frame/2]).

-export([init/1,
         terminate/2,
         handle_call/3,
         handle_cast/2, 
         handle_info/2]).

-import(rabbit_amqp1_0_util,
        [protocol_error/3]).
-import(serial_number,
        [add/2,
         diff/2,
         compare/2]).

-record(incoming_link, {
          exchange :: rabbit_exchange:name(),
          routing_key :: undefined | rabbit_types:routing_key(),
          %% Queue is only set if the link target address refers to a queue.
          queue :: undefined | rabbit_misc:resource_name(),
          delivery_id :: undefined | delivery_number(),
          delivery_count = 0 :: sequence_no(),
          send_settle_mode = undefined,
          recv_settle_mode = undefined,
          credit_used = ?INCOMING_CREDIT div 2,
          msg_acc = []
         }).

-record(outgoing_link, {
          %% Although the source address of a link might be an exchange name and binding key
          %% or a topic filter, an outgoing link will always consume from a queue.
          queue :: rabbit_misc:resource_name(),
          delivery_count = 0 :: sequence_no(),
          send_settled :: boolean()
         }).

-record(outgoing_unsettled, {
          %% The queue sent us this consumer scoped sequence number.
          msg_id :: rabbit_amqqueue:msg_id(),
          consumer_tag :: rabbit_types:ctag(),
          queue_name :: rabbit_amqqueue:name(),
          delivered_at :: integer()
         }).

-record(pending_transfer, {
          link_handle :: ?UINT(non_neg_integer()),
          frames :: iolist(),
          queue_ack_required :: boolean(),
          %% queue that sent us this message
          queue_pid :: pid(),
          delivery_id :: delivery_number(),
          outgoing_unsettled :: #outgoing_unsettled{}
         }).

%%TODO put rarely used fields into separate #cfg{}
-record(state, {
          frame_max,
          reader_pid :: pid(),
          writer_pid :: pid(),
          %% These messages were received from queues thanks to sufficient link credit.
          %% However, they are buffered here due to session flow control before being sent to the client.
          pending_transfers = queue:new() :: queue:queue(#pending_transfer{}),
          user :: rabbit_types:user(),
          vhost :: rabbit_types:vhost(),
          channel_num, %% we just use the incoming (AMQP 1.0) channel number
          remote_incoming_window, % keep track of the window until we're told
          remote_outgoing_window,
          next_incoming_id, % just to keep a check
          incoming_window_max, % )
          incoming_window,     % ) so we know when to open the session window
          %% "The next-outgoing-id MAY be initialized to an arbitrary value and is incremented after each
          %% successive transfer according to RFC-1982 [RFC1982] serial number arithmetic." [2.5.6]
          next_outgoing_id = 0 :: transfer_number(),
          next_delivery_id = 0 :: delivery_number(),
          outgoing_window,
          outgoing_window_max,
          %% Links are unidirectional.
          %% We receive messages from clients on incoming links.
          incoming_links = #{} :: #{link_handle() => #incoming_link{}},
          %% We send messages to clients on outgoing links.
          outgoing_links = #{} :: #{link_handle() => #outgoing_link{}},
          %% TRANSFER delivery IDs published to queues but not yet confirmed by queues
          incoming_unsettled_map = #{} :: #{delivery_number() =>
                                            {#{rabbit_amqqueue:name() := ok},
                                             AtLeastOneQueueConfirmed :: boolean()}},
          %% TRANSFER delivery IDs published to consuming clients but not yet acknowledged by clients.
          outgoing_unsettled_map = #{} :: #{delivery_number() => #outgoing_unsettled{}},
          %% Queue actions that we will process later such that we can confirm and reject
          %% delivery IDs in ranges to reduce the number of DISPOSITION frames sent to the client.
          stashed_rejected = [] :: [{rejected, rabbit_amqqueue:name(), [delivery_number(),...]}],
          stashed_settled = [] :: [{settled, rabbit_amqqueue:name(), [delivery_number(),...]}],
          stashed_eol = [] :: [rabbit_amqqueue:name()],
          queue_states = rabbit_queue_type:init() :: rabbit_queue_type:state()
         }).

start_link(ReaderPid, WriterPid, ChannelNum, FrameMax, User, Vhost) ->
    Args = {ReaderPid, WriterPid, ChannelNum, FrameMax, User, Vhost},
    Opts = [{hibernate_after, ?HIBERNATE_AFTER}],
    gen_server:start_link(?MODULE, Args, Opts).

process_frame(Pid, Frame) ->
    credit_flow:send(Pid),
    gen_server:cast(Pid, {frame, Frame, self()}).

init({ReaderPid, WriterPid, ChannelNum, FrameMax, User, Vhost}) ->
    %%TODO do we neeed to trap_exit?
    process_flag(trap_exit, true),
    %% TODO tick_timer with consumer_timeout and permission expiry as done in channel?
    % put(permission_cache_can_expire, rabbit_access_control:permission_cache_can_expire(User)),
    {ok, #state{reader_pid = ReaderPid,
                writer_pid = WriterPid,
                frame_max = FrameMax,
                user = User,
                vhost = Vhost,
                channel_num = ChannelNum}}.

terminate(_Reason, #state{queue_states = QStates,
                          incoming_unsettled_map = IncomingUnsettledMap}) ->
    ok = rabbit_queue_type:close(QStates),
    case maps:size(IncomingUnsettledMap) of
        0 ->
            ok;
        NumUnsettled ->
            rabbit_log:info("Session is terminating with ~b pending publisher confirms",
                            [NumUnsettled])
    end.

handle_call(Msg, _From, State) ->
    Reply = {error, {not_understood, Msg}},
    reply(Reply, State).

handle_info(timeout, State) ->
    noreply(State);
handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    noreply(State);
handle_info({{'DOWN', QName}, _MRef, process, QPid, Reason},
            #state{queue_states = QStates0,
                   stashed_eol = Eol} = State0) ->
    credit_flow:peer_down(QPid),
    case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
        {ok, QStates, Actions} ->
            State1 = State0#state{queue_states = QStates},
            {Reply, State} = handle_queue_actions(Actions, State1),
            reply0(Reply, State);
        {eol, QStates, QRef} ->
            State = State0#state{queue_states = QStates,
                                 stashed_eol = [QRef | Eol]},
            %%TODO see rabbit_channel:handle_eol()
            noreply(State)
    end;
handle_info({'EXIT', WriterPid, Reason = {writer, send_failed, _Error}},
            State = #state{writer_pid = WriterPid,
                           reader_pid = ReaderPid,
                           channel_num = ChannelNum}) ->
    %%TODO this branch seems not needed?
    ReaderPid ! {channel_exit, ChannelNum, Reason},
    {stop, normal, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State}.

handle_cast({frame, Frame, FlowPid},
            #state{reader_pid = ReaderPid,
                   writer_pid = WriterPid,
                   channel_num = Ch} = State0) ->
    credit_flow:ack(FlowPid),
    try handle_control(Frame, State0) of
        {reply, Replies, State} when is_list(Replies) ->
            lists:foreach(fun (Reply) ->
                                  rabbit_amqp1_0_writer:send_command(WriterPid, Ch, Reply)
                          end, Replies),
            noreply(State);
        {reply, Reply, State} ->
            rabbit_amqp1_0_writer:send_command(WriterPid, Ch, Reply),
            noreply(State);
        {noreply, State} ->
            noreply(State);
        {stop, _, _} = Stop ->
            Stop
    catch exit:Reason = #'v1_0.error'{} ->
              %% TODO shut down nicely like rabbit_channel
            End = #'v1_0.end'{error = Reason},
            rabbit_log:warning("Closing session for connection ~p: ~tp",
                               [ReaderPid, Reason]),
            ok = rabbit_amqp1_0_writer:send_command_sync(WriterPid, Ch, End),
            {stop, {shutdown, Reason}, State0};
          exit:normal ->
              {stop, normal, State0};
          _:Reason:Stacktrace ->
              {stop, {Reason, Stacktrace}, State0}
    end;
handle_cast({queue_event, _, _} = QEvent,
            #state{writer_pid = WriterPid,
                   channel_num = Ch} = State0) ->
    {Reply, State} = handle_queue_event(QEvent, State0),
    [rabbit_amqp1_0_writer:send_command(WriterPid, Ch, F) ||
     F <- flow_fields(Reply, State)],
    noreply_coalesce(State).

%% Batch confirms / rejects to publishers.
noreply_coalesce(#state{stashed_rejected = [],
                        stashed_settled = [],
                        stashed_eol = []} = State) ->
    {noreply, State};
noreply_coalesce(State) ->
    Timeout = 0,
    {noreply, State, Timeout}.

noreply(State0) ->
    State = send_delivery_state_changes(State0),
    {noreply, State}.

reply(Reply, State0) ->
    State = send_delivery_state_changes(State0),
    {reply, Reply, State}.

%% Send confirms / rejects to publishers.
send_delivery_state_changes(#state{stashed_rejected = [],
                                   stashed_settled = [],
                                   stashed_eol = []} = State) ->
    State;
send_delivery_state_changes(#state{writer_pid = Writer,
                                   channel_num = ChannelNum} = State0) ->
    case rabbit_node_monitor:pause_partition_guard() of
        ok ->
            %% 1. Process queue rejections.
            {RejectedDelIds, State1} = handle_stashed_rejected(State0),
            send_dispositions(RejectedDelIds, #'v1_0.rejected'{}, Writer, ChannelNum),
            %% 2. Process queue confirmations.
            {AcceptedDelIds0, State2} = handle_stashed_settled(State1),
            %% 3. Process queue deletions.
            {ReleasedDelIds, AcceptedDelIds1, DetachFrames, State} = handle_stashed_eol(State2),
            send_dispositions(ReleasedDelIds, #'v1_0.released'{}, Writer, ChannelNum),
            AcceptedDelIds2 = AcceptedDelIds1 ++ AcceptedDelIds0,
            if AcceptedDelIds2 =/= [] andalso
               map_size(State#state.incoming_unsettled_map) =:= 0 ->
                   AcceptedDelIds = serial_number:usort(AcceptedDelIds2),
                   %% Optimisation: Send single disposition.
                   Disposition = disposition(#'v1_0.accepted'{},
                                             hd(AcceptedDelIds),
                                             lists:last(AcceptedDelIds)),
                   rabbit_amqp1_0_writer:send_command(Writer, ChannelNum, Disposition);
               true ->
                   send_dispositions(AcceptedDelIds2, #'v1_0.accepted'{}, Writer, ChannelNum)
            end,
            %% Send DETACH frames after DISPOSITION frames such that
            %% clients can handle DISPOSITIONs before closing their links.
            lists:foreach(fun(Frame) ->
                                  rabbit_amqp1_0_writer:send_command(Writer, ChannelNum, Frame)
                          end, DetachFrames),
            State;
        pausing ->
            State0
    end.

handle_stashed_rejected(#state{stashed_rejected = []} = State) ->
    {[], State};
handle_stashed_rejected(#state{stashed_rejected = Actions,
                               incoming_unsettled_map = M0} = State0) ->
    {Ids, M} = lists:foldl(
                 fun({rejected, _QName, DeliveryIds}, Accum) ->
                         lists:foldl(
                           fun(DeliveryId, {L, U0} = Acc) ->
                                   case maps:take(DeliveryId, U0) of
                                       {_, U} ->
                                           {[DeliveryId | L], U};
                                       error ->
                                           Acc
                                   end
                           end, Accum, DeliveryIds)
                 end, {[], M0}, Actions),
    State = State0#state{stashed_rejected = [],
                         incoming_unsettled_map = M},
    {Ids, State}.

handle_stashed_settled(#state{stashed_settled = []} = State) ->
    {[], State};
handle_stashed_settled(#state{stashed_settled = Actions,
                              incoming_unsettled_map = M0} = State0) ->
    {Ids, M} = lists:foldl(
                 fun({settled, QName, DeliveryIds}, Accum) ->
                         lists:foldl(
                           fun(DeliveryId, {L, U0} = Acc) ->
                                   case maps:take(DeliveryId, U0) of
                                       {{Qs = #{QName := _}, _}, U1} ->
                                           UnconfirmedQs = maps:size(Qs),
                                           if UnconfirmedQs =:= 1 ->
                                                  %% last queue confirmed
                                                  {[DeliveryId | L], U1};
                                              UnconfirmedQs > 1 ->
                                                  U = maps:update(DeliveryId,
                                                                  {maps:remove(QName, Qs), true},
                                                                  U0),
                                                  {L, U}
                                           end;
                                       _ ->
                                           Acc
                                   end
                           end, Accum, DeliveryIds)
                 end, {[], M0}, Actions),
    State = State0#state{stashed_settled = [],
                         incoming_unsettled_map = M},
    {Ids, State}.

handle_stashed_eol(#state{stashed_eol = []} = State) ->
    {[], [], [], State};
handle_stashed_eol(#state{stashed_eol = Eols} = State0) ->
    {ReleasedIs, AcceptedIds, DetachFrames, State1} =
    lists:foldl(fun(QName, {RIds0, AIds0, DetachFrames0, S0 = #state{incoming_unsettled_map = M0,
                                                                     queue_states = QStates0}}) ->
                        {RIds, AIds, M} = settle_eol(QName, {RIds0, AIds0, M0}),
                        QStates = rabbit_queue_type:remove(QName, QStates0),
                        S1 = S0#state{incoming_unsettled_map = M,
                                      queue_states = QStates},
                        {DetachFrames1, S} = destroy_links(QName, DetachFrames0, S1),
                        {RIds, AIds, DetachFrames1, S}
                end, {[], [], [], State0}, Eols),
    State = State1#state{stashed_eol = []},
    {ReleasedIs, AcceptedIds, DetachFrames, State}.

settle_eol(QName, Acc = {_ReleasedIds, _AcceptedIds, IncomingUnsettledMap}) ->
    maps:fold(
      fun(DeliveryId, {Qs = #{QName := _}, AtLeastOneQueueConfirmed}, {RelIds, AcceptIds, M0}) ->
              UnconfirmedQs = maps:size(Qs),
              if UnconfirmedQs =:= 1 ->
                     %% The last queue that this delivery ID was waiting a confirm for got deleted.
                     M = maps:remove(DeliveryId, M0),
                     case AtLeastOneQueueConfirmed of
                         true ->
                             %% Since at least one queue confirmed this message, we reply to
                             %% the client with ACCEPTED. This allows e.g. for large fanout
                             %% scenarios where temporary target queues are deleted
                             %% (think about an MQTT subscriber disconnects).
                             {RelIds, [DeliveryId | AcceptIds], M};
                         false ->
                             %% Since no queue confirmed this message, we reply to the client
                             %% with RELEASED. (The client can then re-publish this message.)
                             {[DeliveryId | RelIds], AcceptIds, M}
                     end;
                 UnconfirmedQs > 1 ->
                     M = maps:update(DeliveryId,
                                     {maps:remove(QName, Qs), AtLeastOneQueueConfirmed},
                                     M0),
                     {RelIds, AcceptIds, M}
              end;
         (_, _, A) ->
              A
      end, Acc, IncomingUnsettledMap).

destroy_links(#resource{kind = queue,
                        name = QNameBin},
              Frames0,
              #state{incoming_links = IncomingLinks0,
                     outgoing_links = OutgoingLinks0} = State0) ->
    {Frames1, IncomingLinks} = maps:fold(fun(Handle, Link, Acc) ->
                                                 destroy_link(Handle, Link, QNameBin, #incoming_link.queue, Acc)
                                         end, {Frames0, IncomingLinks0}, IncomingLinks0),
    {Frames, OutgoingLinks} = maps:fold(fun(Handle, Link, Acc) ->
                                                destroy_link(Handle, Link, QNameBin, #outgoing_link.queue, Acc)
                                        end, {Frames1, OutgoingLinks0}, OutgoingLinks0),
    State = State0#state{incoming_links = IncomingLinks,
                         outgoing_links = OutgoingLinks},
    {Frames, State}.

destroy_link(Handle, Link, QNameBin, QPos, Acc = {Frames0, Links0}) ->
    case element(QPos, Link) of
        QNameBin ->
            Frame = detach_deleted(Handle),
            Frames = [Frame | Frames0],
            Links = maps:remove(Handle, Links0),
            {Frames, Links};
        _ ->
            Acc
    end.

detach_deleted(Handle) ->
    #'v1_0.detach'{handle = ?UINT(Handle),
                   closed = true,
                   error = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED}}.


send_dispositions(Ids, DeliveryState, Writer, ChannelNum) ->
    Ranges = serial_number:ranges(Ids),
    lists:foreach(fun({First, Last}) ->
                          Disposition = disposition(DeliveryState, First, Last),
                          rabbit_amqp1_0_writer:send_command(Writer, ChannelNum, Disposition)
                  end, Ranges).

disposition(DeliveryState, First, Last) ->
    Last1 = case First of
                Last ->
                    %% "If not set, this is taken to be the same as first." [2.7.6]
                    %% Save a few bytes.
                    undefined;
                _ ->
                    ?UINT(Last)
            end,
    #'v1_0.disposition'{
       role = ?RECV_ROLE,
       settled = true,
       state = DeliveryState,
       first = ?UINT(First),
       last = Last1}.

%% Session window:
%%
%% Each session has two abstract[1] buffers, one to record the
%% unsettled state of incoming messages, one to record the unsettled
%% state of outgoing messages.  In general we want to bound these
%% buffers; but if we bound them, and don't tell the other side, we
%% may end up deadlocking the other party.
%%
%% Hence the flow frame contains a session window, expressed as the
%% next-id and the window size for each of the buffers. The frame
%% refers to the window of the sender of the frame, of course.
%%
%% The numbers work this way: for the outgoing window, the next-id
%% counts the next transfer the session will send, and it will stop
%% sending at next-id + window.  For the incoming window, the next-id
%% counts the next transfer id expected, and it will not accept
%% messages beyond next-id + window (in fact it will probably close
%% the session, since sending outside the window is a transgression of
%% the protocol).
%%
%% We may as well just pick a value for the incoming and outgoing
%% windows; choosing based on what the client says may just stop
%% things dead, if the value is zero for instance.
%%
%% [1] Abstract because there probably won't be a data structure with
%% a size directly related to transfers; settlement is done with
%% delivery-id, which may refer to one or more transfers.
handle_control(#'v1_0.begin'{next_outgoing_id = ?UINT(RemoteNextOut),
                             incoming_window = ?UINT(RemoteInWindow),
                             outgoing_window = ?UINT(RemoteOutWindow),
                             handle_max = HandleMax0},
               #state{next_outgoing_id = LocalNextOut,
                      channel_num = Channel} = State0) ->
    InWindow = ?MAX_SESSION_WINDOW_SIZE,
    OutWindow = ?MAX_SESSION_WINDOW_SIZE,
    HandleMax = case HandleMax0 of
                    ?UINT(Max) -> Max;
                    _ -> ?DEFAULT_MAX_HANDLE
                end,
    Reply = #'v1_0.begin'{remote_channel = {ushort, Channel},
                          handle_max = ?UINT(HandleMax),
                          next_outgoing_id = ?UINT(LocalNextOut),
                          incoming_window = ?UINT(InWindow),
                          outgoing_window = ?UINT(OutWindow)},
    State = State0#state{outgoing_window = OutWindow,
                         outgoing_window_max = OutWindow,
                         next_incoming_id = RemoteNextOut,
                         remote_incoming_window = RemoteInWindow,
                         remote_outgoing_window = RemoteOutWindow,
                         incoming_window  = InWindow,
                         incoming_window_max = InWindow},
    reply0(Reply, State);

handle_control(#'v1_0.attach'{role = ?SEND_ROLE,
                              name = LinkName,
                              handle = InputHandle = ?UINT(HandleInt),
                              source = Source,
                              snd_settle_mode = SndSettleMode,
                              rcv_settle_mode = RcvSettleMode,
                              target = Target,
                              initial_delivery_count = ?UINT(InitTransfer)} = Attach,
               #state{vhost = Vhost,
                      user = User,
                      incoming_links = IncomingLinks0} = State0) ->
    ok = validate_attach(Attach),
    case ensure_target(Target, Vhost, User) of
        {ok, XName, RoutingKey, QNameBin} ->
            IncomingLink = #incoming_link{
                              exchange = XName,
                              routing_key = RoutingKey,
                              queue = QNameBin,
                              delivery_count = InitTransfer,
                              recv_settle_mode = RcvSettleMode},
            _Outcomes = outcomes(Source),
            % rabbit_global_counters:publisher_created(ProtoVer),

            OutputHandle = output_handle(InputHandle),
            AttachReply = #'v1_0.attach'{
                             name = LinkName,
                             handle = OutputHandle,
                             source = Source,
                             snd_settle_mode = SndSettleMode,
                             rcv_settle_mode = RcvSettleMode,
                             target = Target,
                             %% We are the receiver.
                             role = ?RECV_ROLE,
                             %% "ignored if the role is receiver"
                             initial_delivery_count = undefined},
            Flow = #'v1_0.flow'{
                      handle = OutputHandle,
                      link_credit = ?UINT(?INCOMING_CREDIT),
                      drain = false,
                      echo = false},
            %%TODO check that handle is not present in either incoming_links or outgoing_links:
            %%"The handle MUST NOT be used for other open links. An attempt to attach
            %% using a handle which is already associated with a link MUST be responded to
            %% with an immediate close carrying a handle-in-use session-error."
            IncomingLinks = IncomingLinks0#{HandleInt => IncomingLink},
            State = State0#state{incoming_links = IncomingLinks},
            reply0([AttachReply, Flow], State);
        {error, Reason} ->
            %% TODO proper link establishment protocol here?
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                           "Attach rejected: ~tp",
                           [Reason])
    end;

handle_control(#'v1_0.attach'{role = ?RECV_ROLE,
                              name = LinkName,
                              handle = InputHandle = ?UINT(HandleInt),
                              source = Source,
                              snd_settle_mode = SndSettleMode,
                              rcv_settle_mode = RcvSettleMode} = Attach,
               #state{vhost = Vhost,
                      user = User = #user{username = Username},
                      queue_states = QStates0,
                      outgoing_links = OutgoingLinks0} = State0) ->

    ok = validate_attach(Attach),
    SndSettled = case SndSettleMode of
                     ?V_1_0_SENDER_SETTLE_MODE_SETTLED -> true;
                     ?V_1_0_SENDER_SETTLE_MODE_UNSETTLED -> false;
                     _ -> ?DEFAULT_SEND_SETTLED
                 end,
    case ensure_source(Source, Vhost, User) of
        {ok, QNameBin} ->
            CTag = handle_to_ctag(HandleInt),
            Args = source_filters_to_consumer_args(Source) ++
            [{<<"x-credit">>, table, [{<<"credit">>, long, 0},
                                      {<<"drain">>,  bool, false}]}],
            Spec = #{no_ack => SndSettled,
                     channel_pid => self(),
                     %%TODO check if limiter required for consumer credit
                     limiter_pid => none,
                     limiter_active => false,
                     prefetch_count => ?MAX_SESSION_WINDOW_SIZE,
                     consumer_tag => CTag,
                     exclusive_consume => false,
                     args => Args,
                     ok_msg => undefined,
                     acting_user => Username},
            QName = rabbit_misc:r(Vhost, queue, QNameBin),
            check_read_permitted(QName, User),
            case rabbit_amqqueue:with(
                   QName,
                   fun(Q) ->
                           case rabbit_queue_type:consume(Q, Spec, QStates0) of
                               {ok, QStates} ->
                                   OutputHandle = output_handle(InputHandle),
                                   AttachReply = #'v1_0.attach'{
                                                    name = LinkName,
                                                    handle = OutputHandle,
                                                    initial_delivery_count = ?UINT(?INIT_TXFR_COUNT),
                                                    snd_settle_mode = case SndSettled of
                                                                          true -> ?V_1_0_SENDER_SETTLE_MODE_SETTLED;
                                                                          false -> ?V_1_0_SENDER_SETTLE_MODE_UNSETTLED
                                                                      end,
                                                    rcv_settle_mode = RcvSettleMode,
                                                    %% The queue process monitors our session process. When our session process terminates
                                                    %% (abnormally) any messages checked out to our session process will be requeued.
                                                    %% That's why the we only support RELEASED as the default outcome.
                                                    source = Source#'v1_0.source'{
                                                                      default_outcome = #'v1_0.released'{},
                                                                      outcomes = outcomes(Source)},
                                                    role = ?SEND_ROLE},
                                   Link = #outgoing_link{delivery_count = ?INIT_TXFR_COUNT,
                                                         queue = QNameBin,
                                                         send_settled = SndSettled},
                                   %%TODO check that handle is not present in either incoming_links or outgoing_links:
                                   %%"The handle MUST NOT be used for other open links. An attempt to attach
                                   %% using a handle which is already associated with a link MUST be responded to
                                   %% with an immediate close carrying a handle-in-use session-error."
                                   OutgoingLinks = OutgoingLinks0#{HandleInt => Link},
                                   State1 = State0#state{queue_states = QStates,
                                                         outgoing_links = OutgoingLinks},
                                   {ok, [AttachReply], State1};
                               {error, Reason} ->
                                   protocol_error(
                                     ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                     "Consuming from ~s failed: ~tp",
                                     [rabbit_misc:rs(QName), Reason])
                           end
                   end) of
                {ok, Reply, State} ->
                    reply0(Reply, State);
                {error, Reason} ->
                    protocol_error(
                      ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                      "Could not operate on ~s: ~tp",
                      [rabbit_misc:rs(QName), Reason])
            end;
        {error, Reason} ->
            %% TODO proper link establishment protocol here?
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                           "Attach rejected: ~tp",
                           [Reason])
    end;

handle_control({Txfr = #'v1_0.transfer'{handle = ?UINT(Handle)}, MsgPart},
               State0 = #state{incoming_links = IncomingLinks}) ->
    %%TODO check properties.user-id as done in rabbit_channel:check_user_id_header/2 ?
    case IncomingLinks of
        #{Handle := Link0} ->
            {Flows, State1} = incr_incoming_id(State0),
            case incoming_link_transfer(Txfr, MsgPart, Link0, State1) of
                {ok, Reply0, Link, State2} ->
                    Reply = Reply0 ++ Flows,
                    State = State2#state{incoming_links = maps:update(Handle, Link, IncomingLinks)},
                    reply0(Reply, State);
                {error, Reply0} ->
                    %% "When an error occurs at a link endpoint, the endpoint MUST be detached
                    %% with appropriate error information supplied in the error field of the
                    %% detach frame. The link endpoint MUST then be destroyed." [2.6.5]
                    Reply = Reply0 ++ Flows,
                    State = State1#state{incoming_links = maps:remove(Handle, IncomingLinks)},
                    reply0(Reply, State)
            end;
        _ ->
            protocol_error(?V_1_0_AMQP_ERROR_ILLEGAL_STATE,
                           "Unknown link handle ~p", [Handle])
    end;

handle_control(#'v1_0.disposition'{role = ?RECV_ROLE} = Disp, State) ->
    settle(Disp, State);

%% Flow control. These frames come with two pieces of information:
%% the session window, and optionally, credit for a particular link.
%% We'll deal with each of them separately.
handle_control(#'v1_0.flow'{handle = Handle} = Flow,
               #state{incoming_links = IncomingLinks,
                      outgoing_links = OutgoingLinks} = State0) ->
    State1 = handle_session_flow_control(Flow, State0),
    State2 = send_pending_transfers(State1),
    case Handle of
        undefined ->
            %% "If not set, the flow frame is carrying only information
            %% pertaining to the session endpoint." [2.7.4]
            {noreply, State2};
        ?UINT(HandleInt) ->
            %% "If set, indicates that the flow frame carries flow state information
            %% for the local link endpoint associated with the given handle." [2.7.4]
            case OutgoingLinks of
                #{HandleInt := OutgoingLink} ->
                    {ok, Reply, State} = handle_outgoing_link_flow_control(
                                           OutgoingLink, Flow, State2),
                    reply0(Reply, State);
                _ ->
                    case IncomingLinks of
                        #{HandleInt := _IncomingLink} ->
                            %% We're being told about available messages at
                            %% the sender.  Yawn. TODO at least check transfer-count?
                            {noreply, State2};
                        _ ->
                            %% "If set to a handle that is not currently associated with
                            %% an attached link, the recipient MUST respond by ending the
                            %% session with an unattached-handle session error." [2.7.4]
                            rabbit_log:warning(
                              "Received Flow frame for unknown link handle: ~tp", [Flow]),
                            protocol_error(
                              ?V_1_0_SESSION_ERROR_UNATTACHED_HANDLE,
                              "Unattached link handle: ~b", [HandleInt])
                    end
            end
    end;

handle_control(#'v1_0.detach'{handle = Handle = ?UINT(HandleInt),
                              closed = Closed},
               #state{writer_pid = WriterPid,
                      channel_num = Ch,
                      queue_states = QStates0,
                      vhost = Vhost,
                      user = #user{username = Username},
                      incoming_links = IncomingLinks,
                      outgoing_links = OutgoingLinks0} = State0) ->
    %% TODO delete queue if closed flag is set to true? see 2.6.6
    %% TODO keep the state around depending on the lifetime
    {QStates, OutgoingLinks} =
    case maps:take(HandleInt, OutgoingLinks0) of
        {#outgoing_link{queue = QNameBin}, OutgoingLinks1} ->
            case rabbit_amqqueue:lookup(QNameBin, Vhost) of
                {ok, Q} ->
                    Ctag = handle_to_ctag(HandleInt),
                    case rabbit_queue_type:cancel(Q, Ctag, undefined, Username, QStates0) of
                        {ok, QStates1} ->
                            {QStates1, OutgoingLinks1};
                        {error, Reason} ->
                            protocol_error(
                              ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                              "Failed to cancel consuming from ~s: ~tp",
                              [rabbit_misc:rs(amqqueue:get_name(Q)), Reason])
                    end;
                {error, not_found} ->
                    {QStates0, OutgoingLinks1}
            end;
        error ->
            {QStates0, OutgoingLinks0}
    end,
    State = State0#state{queue_states = QStates,
                         outgoing_links = OutgoingLinks,
                         incoming_links = maps:remove(HandleInt, IncomingLinks)},
    ok = rabbit_amqp1_0_writer:send_command(
           WriterPid, Ch, #'v1_0.detach'{handle = Handle,
                                         closed = Closed}),
    {noreply, State};

handle_control(#'v1_0.end'{}, #state{writer_pid = WriterPid,
                                     channel_num = Ch} = State0) ->
    State = send_delivery_state_changes(State0),
    ok = try rabbit_amqp1_0_writer:send_command_sync(WriterPid, Ch, #'v1_0.end'{})
         catch exit:{Reason, {gen_server, call, _ArgList}}
                 when Reason =:= shutdown orelse
                      Reason =:= noproc ->
                   %% AMQP connection and therefore the writer process got already terminated
                   %% before we had the chance to synchronously end the session.
                   ok
         end,
    {stop, normal, State};

handle_control(Frame, _State) ->
    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                   "Unexpected frame ~tp",
                   [amqp10_framing:pprint(Frame)]).

send_pending_transfers(#state{outgoing_window = LocalSpace,
                              remote_incoming_window = RemoteSpace,
                              writer_pid = WriterPid,
                              channel_num = Ch,
                              pending_transfers = Buf0,
                              queue_states = QStates} = State0)
  when RemoteSpace > 0 andalso LocalSpace > 0 ->
    Space = erlang:min(LocalSpace, RemoteSpace),
    case queue:out(Buf0) of
        {empty, Buf} ->
            State0#state{pending_transfers = Buf};
        {{value, #pending_transfer{
                    frames = Frames,
                    link_handle = ?UINT(Handle),
                    queue_pid = QPid,
                    outgoing_unsettled = #outgoing_unsettled{
                                            queue_name = QName
                                           }} = Pending}, Buf1} ->
            SendFun = case rabbit_queue_type:module(QName, QStates) of
                          {ok, rabbit_classic_queue} ->
                              fun(T, C) ->
                                      rabbit_amqp1_0_writer:send_command_and_notify(
                                        WriterPid, Ch, QPid, self(), T, C)
                              end;
                          {ok, _QType} ->
                              fun(T, C) ->
                                      rabbit_amqp1_0_writer:send_command(
                                        WriterPid, Ch, T, C)
                              end
                      end,
            %% rabbit_basic:maybe_gc_large_msg(Content, GCThreshold)
            case send_frames(SendFun, Frames, Space) of
                {all, SpaceLeft} ->
                    State1 = #state{outgoing_links = OutgoingLinks0} = record_transfers(
                                                                         Space - SpaceLeft, State0),
                    OutgoingLinks = maps:update_with(
                                      Handle,
                                      fun(Link = #outgoing_link{delivery_count = C}) ->
                                              Link#outgoing_link{delivery_count = add(C, 1)}
                                      end,
                                      OutgoingLinks0),
                    State2 = State1#state{outgoing_links = OutgoingLinks},
                    State = record_outgoing_unsettled(Pending, State2),
                    send_pending_transfers(State#state{pending_transfers = Buf1});
                {some, Rest} ->
                    State = record_transfers(Space, State0),
                    Buf = queue:in_r(Pending#pending_transfer{frames = Rest}, Buf1),
                    send_pending_transfers(State#state{pending_transfers = Buf})
            end
    end;
send_pending_transfers(#state{remote_incoming_window = RemoteSpace,
                              writer_pid = WriterPid,
                              channel_num = Ch} = State0)
  when RemoteSpace > 0 ->
    {Flow = #'v1_0.flow'{}, State} = bump_outgoing_window(State0),
    rabbit_amqp1_0_writer:send_command(WriterPid, Ch, flow_fields(Flow, State)),
    send_pending_transfers(State);
send_pending_transfers(State) ->
    State.

send_frames(_, [], Left) ->
    {all, Left};
send_frames(_, Rest, 0) ->
    {some, Rest};
send_frames(SendFun, [[T, C] | Rest], Left) ->
    ok = SendFun(T, C),
    send_frames(SendFun, Rest, Left - 1).

record_outgoing_unsettled(#pending_transfer{queue_ack_required = true,
                                            delivery_id = DeliveryId,
                                            outgoing_unsettled = Unsettled},
                          #state{outgoing_unsettled_map = Map0} = State) ->
    %% Record by DeliveryId such that we will ack this message to the queue
    %% once we receive the DISPOSITION from the AMQP client.
    Map = Map0#{DeliveryId => Unsettled},
    State#state{outgoing_unsettled_map = Map};
record_outgoing_unsettled(#pending_transfer{queue_ack_required = false}, State) ->
    %% => 'snd-settle-mode' at attachment must have been 'settled'.
    %% => 'settled' field in TRANSFER must have been 'true'.
    %% => AMQP client won't ack this message.
    %% Also, queue client already acked to queue on behalf of us.
    State.

reply0([], State) ->
    {noreply, State};
reply0(Reply, State) ->
    {reply, flow_fields(Reply, State), State}.

incr_incoming_id(#state{next_incoming_id = NextIn,
                        incoming_window = InWindow,
                        incoming_window_max = InWindowMax,
                        remote_outgoing_window = RemoteOut} = State) ->
    NewOutWindow = RemoteOut - 1,
    InWindow1 = InWindow - 1,
    NewNextIn = add(NextIn, 1),
    %% If we've reached halfway, open the window
    {Flows, NewInWindow} =
    if InWindow1 =< (InWindowMax div 2) ->
           {[#'v1_0.flow'{}], InWindowMax};
       true ->
           {[], InWindow1}
    end,
    {Flows, State#state{next_incoming_id = NewNextIn,
                        incoming_window = NewInWindow,
                        remote_outgoing_window = NewOutWindow}}.

record_transfers(NumTransfers,
                 #state{remote_incoming_window = RemoteInWindow,
                        outgoing_window = OutWindow,
                        next_outgoing_id = NextOutId} = State) ->
    State#state{remote_incoming_window = RemoteInWindow - NumTransfers,
                outgoing_window = OutWindow - NumTransfers,
                next_outgoing_id = add(NextOutId, NumTransfers)}.

%% Make sure we have "room" in our outgoing window by bumping the
%% window if necessary. TODO this *could* be based on how much
%% notional "room" there is in outgoing_unsettled.
bump_outgoing_window(State = #state{outgoing_window_max = OutMax}) ->
    {#'v1_0.flow'{}, State#state{outgoing_window = OutMax}}.

settle(#'v1_0.disposition'{first = ?UINT(First),
                           last = Last0,
                           state = Outcome,
                           settled = DispositionSettled} = Disposition,
       #state{outgoing_unsettled_map = UnsettledMap,
              queue_states = QStates0} = State0) ->
    Last = case Last0 of
               ?UINT(L) ->
                   L;
               undefined ->
                   %% "If not set, this is taken to be the same as first." [2.7.6]
                   First
           end,
    UnsettledMapSize = maps:size(UnsettledMap),
    case UnsettledMapSize of
        0 ->
            {noreply, State0};
        _ ->
            DispositionRangeSize = diff(Last, First) + 1,
            {Settled, UnsettledMap1} =
            case DispositionRangeSize =< UnsettledMapSize of
                true ->
                    %% It is cheaper to iterate over the range of settled delivery IDs.
                    maybe_settle(First, Last, #{}, UnsettledMap);
                false ->
                    %% It is cheaper to iterate over the outgoing unsettled map.
                    maps:fold(
                      fun (DeliveryId,
                           #outgoing_unsettled{queue_name = QName,
                                               consumer_tag = Ctag,
                                               msg_id = MsgId} = Unsettled,
                           {SettledAcc, UnsettledAcc}) ->
                              DeliveryIdComparedToFirst = compare(DeliveryId, First),
                              DeliveryIdComparedToLast = compare(DeliveryId, Last),
                              if DeliveryIdComparedToFirst =:= less orelse
                                 DeliveryIdComparedToLast =:= greater ->
                                     %% Delivery ID is outside the DISPOSITION range.
                                     {SettledAcc, UnsettledAcc#{DeliveryId => Unsettled}};
                                 true ->
                                     %% Delivery ID is inside the DISPOSITION range.
                                     SettledAcc1 = maps:update_with(
                                                     {QName, Ctag},
                                                     fun(MsgIds) -> [MsgId | MsgIds] end,
                                                     [MsgId],
                                                     SettledAcc),
                                     {SettledAcc1, UnsettledAcc}
                              end
                      end,
                      {#{}, #{}}, UnsettledMap)
            end,

            SettleOp = settle_op_from_outcome(Outcome),
            {QStates, Actions} =
            maps:fold(
              fun({QName, Ctag}, MsgIds0, {QS0, ActionsAcc}) ->
                      %% Classic queues expect message IDs in sorted order.
                      MsgIds = lists:usort(MsgIds0),
                      case rabbit_queue_type:settle(QName, SettleOp, Ctag, MsgIds, QS0) of
                          {ok, QS, Actions0} ->
                              {QS, ActionsAcc ++ Actions0};
                          {protocol_error, _ErrorType, Reason, ReasonArgs} ->
                              protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                             Reason, ReasonArgs)
                      end
              end, {QStates0, []}, Settled),

            State1 = State0#state{outgoing_unsettled_map = UnsettledMap1,
                                  queue_states = QStates},
            Reply0 = case DispositionSettled of
                         true  -> [];
                         false -> [Disposition#'v1_0.disposition'{settled = true,
                                                                  role = ?SEND_ROLE}]
                     end,
            {Reply, State} = handle_queue_actions(Actions, State1),
            reply0(Reply0 ++ Reply, State)
    end.

maybe_settle(Current, Last, Settled, Unsettled) ->
    case compare(Current, Last) of
        less ->
            {Settled1, Unsettled1} = maybe_settle0(Current, Settled, Unsettled),
            Next = add(Current, 1),
            maybe_settle(Next, Last, Settled1, Unsettled1);
        equal ->
            maybe_settle0(Current, Settled, Unsettled)
    end.

maybe_settle0(Current, Settled, Unsettled) ->
    case maps:take(Current, Unsettled) of
        {#outgoing_unsettled{queue_name = QName,
                             consumer_tag = Ctag,
                             msg_id = MsgId}, Unsettled1} ->
            Settled1 = maps:update_with(
                         {QName, Ctag},
                         fun(MsgIds) -> [MsgId | MsgIds] end,
                         [MsgId],
                         Settled),
            {Settled1, Unsettled1};
        error ->
            {Settled, Unsettled}
    end.

settle_op_from_outcome(#'v1_0.accepted'{}) ->
    complete;
settle_op_from_outcome(#'v1_0.rejected'{}) ->
    discard;
settle_op_from_outcome(#'v1_0.released'{}) ->
    requeue;
settle_op_from_outcome(#'v1_0.modified'{delivery_failed = true,
                                        undeliverable_here = UndelHere})
  when UndelHere =/= true ->
    requeue;
settle_op_from_outcome(#'v1_0.modified'{}) ->
    %% If delivery_failed is not true, we can't increment its delivery_count.
    %% So, we will have to reject without requeue.
    %%TODO Quorum queues can increment the delivery_count, can't they?
    %%
    %% If undeliverable_here is true, this is not quite correct because
    %% undeliverable_here refers to the link not the message in general.
    %% However, we cannot filter messages from being assigned to individual consumers.
    %% That's why we will have to reject it without requeue.
    discard;
settle_op_from_outcome(Outcome) ->
    protocol_error(
      ?V_1_0_AMQP_ERROR_INVALID_FIELD,
      "Unrecognised state: ~tp in DISPOSITION",
      [Outcome]).

flow_fields(Frames, State) when is_list(Frames) ->
    [flow_fields(F, State) || F <- Frames];

flow_fields(Flow = #'v1_0.flow'{},
            #state{next_outgoing_id = NextOut,
                   next_incoming_id = NextIn,
                   outgoing_window = OutWindow,
                   incoming_window = InWindow}) ->
    Flow#'v1_0.flow'{
           next_outgoing_id = ?UINT(NextOut),
           outgoing_window = ?UINT(OutWindow),
           next_incoming_id = ?UINT(NextIn),
           incoming_window = ?UINT(InWindow)};

flow_fields(Frame, _State) ->
    Frame.

%% We should already know the next outgoing transfer sequence number,
%% because it's one more than the last transfer we saw; and, we don't
%% need to know the next incoming transfer sequence number (although
%% we might use it to detect congestion -- e.g., if it's lagging far
%% behind our outgoing sequence number). We probably care about the
%% outgoing window, since we want to keep it open by sending back
%% settlements, but there's not much we can do to hurry things along.
%%
%% We do care about the incoming window, because we must not send
%% beyond it. This may cause us problems, even in normal operation,
%% since we want our unsettled transfers to be exactly those that are
%% held as unacked by the backing channel; however, the far side may
%% close the window while we still have messages pending transfer, and
%% indeed, an individual message may take more than one 'slot'.
%%
%% Note that this isn't a race so far as AMQP 1.0 is concerned; it's
%% only because AMQP 0-9-1 defines QoS in terms of the total number of
%% unacked messages, whereas 1.0 has an explicit window.
handle_session_flow_control(
  #'v1_0.flow'{next_incoming_id = FlowNextIn0,
               incoming_window  = ?UINT(FlowInWindow),
               next_outgoing_id = ?UINT(FlowNextOut),
               outgoing_window  = ?UINT(FlowOutWindow)},
  #state{next_incoming_id = LocalNextIn,
         next_outgoing_id = LocalNextOut} = State) ->
    %% The far side may not have our begin{} with our next-transfer-id
    FlowNextIn = case FlowNextIn0 of
                     ?UINT(Id) -> Id;
                     undefined  -> LocalNextOut
                 end,
    case compare(FlowNextOut, LocalNextIn) of
        equal ->
            case compare(FlowNextIn, LocalNextOut) of
                greater ->
                    protocol_error(?V_1_0_SESSION_ERROR_WINDOW_VIOLATION,
                                   "Remote incoming id (~tp) leads "
                                   "local outgoing id (~tp)",
                                   [FlowNextIn, LocalNextOut]);
                equal ->
                    State#state{
                      remote_outgoing_window = FlowOutWindow,
                      remote_incoming_window = FlowInWindow};
                less ->
                    State#state{
                      remote_outgoing_window = FlowOutWindow,
                      remote_incoming_window = diff(add(FlowNextIn, FlowInWindow),
                                                    LocalNextOut)}
            end;
        _ ->
            case application:get_env(rabbitmq_amqp1_0, protocol_strict_mode) of
                {ok, false} ->
                    State#state{next_incoming_id = FlowNextOut};
                {ok, true} ->
                    protocol_error(?V_1_0_SESSION_ERROR_WINDOW_VIOLATION,
                                   "Remote outgoing id (~tp) not equal to "
                                   "local incoming id (~tp)",
                                   [FlowNextOut, LocalNextIn])
            end
    end.

set_delivery_id(?UINT(D), #incoming_link{delivery_id = undefined} = Link) ->
    %% "The delivery-id MUST be supplied on the first transfer of a multi-transfer delivery.
    Link#incoming_link{delivery_id = D};
set_delivery_id(undefined, #incoming_link{} = Link) ->
    %% On continuation transfers the delivery-id MAY be omitted.
    Link;
set_delivery_id(?UINT(D), #incoming_link{delivery_id = D} = Link) ->
    %% It is an error if the delivery-id on a continuation transfer differs from the
    %% delivery-id on the first transfer of a delivery." [2.7.5]
    Link.

effective_send_settle_mode(undefined, undefined) ->
    false;
effective_send_settle_mode(undefined, SettleMode)
  when is_boolean(SettleMode) ->
    SettleMode;
effective_send_settle_mode(SettleMode, undefined)
  when is_boolean(SettleMode) ->
    SettleMode;
effective_send_settle_mode(SettleMode, SettleMode)
  when is_boolean(SettleMode) ->
    SettleMode.

effective_recv_settle_mode(undefined, undefined) ->
    ?V_1_0_RECEIVER_SETTLE_MODE_FIRST;
effective_recv_settle_mode(undefined, Mode) ->
    Mode;
effective_recv_settle_mode(Mode, _) ->
    Mode.

% TODO: validate effective settle modes against
%       those declared during attach

% TODO: handle aborted transfers

handle_queue_event({queue_event, QRef, Evt},
                   #state{queue_states = QStates0} = S0) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QStates0) of
        {ok, QStates1, Actions} ->
            S = S0#state{queue_states = QStates1},
            handle_queue_actions(Actions, S);
        {eol, Actions} ->
            {Reply, S1} = handle_queue_actions(Actions, S0),
            S = S1#state{stashed_eol = [QRef | S1#state.stashed_eol]},
            %%TODO see rabbit_channel:handle_eol()
            {Reply, S};
        {protocol_error, _Type, Reason, ReasonArgs} ->
            protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, Reason, ReasonArgs)
    end.

handle_queue_actions(Actions, State0) ->
    {ReplyRev, State} =
    lists:foldl(
      fun ({settled, _QName, _DelIds} = Action,
           {Reply, S0 = #state{stashed_settled = As}}) ->
              S = S0#state{stashed_settled = [Action | As]},
              {Reply, S};
          ({rejected, _QName, _DelIds} = Action,
           {Reply, S0 = #state{stashed_rejected = As}}) ->
              S = S0#state{stashed_rejected = [Action | As]},
              {Reply, S};
          ({deliver, CTag, AckRequired, Msgs}, {Reply, S0}) ->
              S1 = lists:foldl(fun(Msg, S) ->
                                       handle_deliver(CTag, AckRequired, Msg, S)
                               end, S0, Msgs),
              {Reply, S1};
          ({send_credit_reply, Ctag, LinkCreditSnd, Available},
           {Reply, S = #state{outgoing_links = OutgoingLinks}}) ->
              Handle = ctag_to_handle(Ctag),
              #outgoing_link{delivery_count = DeliveryCountSnd} = maps:get(Handle, OutgoingLinks),
              Flow = #'v1_0.flow'{
                        handle = ?UINT(Handle),
                        delivery_count = ?UINT(DeliveryCountSnd),
                        link_credit = ?UINT(LinkCreditSnd),
                        available = ?UINT(Available),
                        drain = false},
              {[Flow | Reply], S};
          ({send_drained, {CTag, CreditDrained}},
           {Reply, S0 = #state{outgoing_links = OutgoingLinks0}}) ->
              Handle = ctag_to_handle(CTag),
              Link = #outgoing_link{delivery_count = Count0} = maps:get(Handle, OutgoingLinks0),
              %%TODO delivery-count is a 32-bit RFC-1982 serial number [2.7.4]
              Count = Count0 + CreditDrained,
              OutgoingLinks = maps:update(Handle,
                                          Link#outgoing_link{delivery_count = Count},
                                          OutgoingLinks0),
              S = S0#state{outgoing_links = OutgoingLinks},
              Flow = #'v1_0.flow'{
                        handle = ?UINT(Handle),
                        delivery_count = ?UINT(Count),
                        link_credit = ?UINT(0),
                        %% TODO Why do we set available to 0?
                        available = ?UINT(0),
                        drain = true},
              {[Flow | Reply], S};
          ({block, _QName}, Acc) ->
              %%TODO
              Acc;
          ({unblock, _QName}, Acc) ->
              %%TODO
              Acc;
          ({queue_down, _QName}, Acc) ->
              %%TODO
              Acc
      end, {[], State0}, Actions),
    {lists:reverse(ReplyRev), State}.

handle_deliver(ConsumerTag, AckRequired,
               {QName, QPid, MsgId, Redelivered, Mc0},
               #state{pending_transfers = Pendings,
                      next_delivery_id = DeliveryId,
                      outgoing_links = OutgoingLinks,
                      frame_max = FrameMax} = State0) ->
    Handle = ctag_to_handle(ConsumerTag),
    case OutgoingLinks of
        #{Handle := #outgoing_link{send_settled = SendSettled}} ->
            %% "The delivery-tag MUST be unique amongst all deliveries that could be
            %% considered unsettled by either end of the link." [2.6.12]
            Dtag = if is_integer(MsgId) ->
                          %% We use MsgId (the consumer scoped sequence number from the queue) as
                          %% delivery-tag since delivery-tag must be unique only per link (not per session).
                          %% "A delivery-tag can be up to 32 octets of binary data." [2.8.7]
                          case MsgId =< 16#ffffffff of
                              true -> <<MsgId:32>>;
                              false -> <<MsgId:64>>
                          end;
                      MsgId =:= undefined andalso SendSettled ->
                          %% Both ends of the link will always consider this message settled because
                          %% "the sender will send all deliveries settled to the receiver" [3.8.2].
                          %% Hence, the delivery tag does not have to be unique on this link.
                          %% However, the spec still mandates to send a delivery tag.
                          <<>>
                   end,
            Transfer = #'v1_0.transfer'{
                          handle = ?UINT(Handle),
                          delivery_id = ?UINT(DeliveryId),
                          delivery_tag = {binary, Dtag},
                          %% [3.2.16]
                          message_format = ?UINT(0),
                          settled = SendSettled,
                          more = false,
                          resume = false,
                          aborted = false,
                          %% TODO: actually batchable would be fine
                          batchable = false},
            Mc1 = mc:convert(mc_amqp, Mc0),
            Mc = mc:set_annotation(redelivered, Redelivered, Mc1),
            Sections0 = mc:protocol_state(Mc),
            Sections = mc_amqp:serialize(Sections0),
            ?DEBUG("Outbound content:~n  ~tp",
                   [[amqp10_framing:pprint(Section) ||
                     Section <- amqp10_framing:decode_bin(iolist_to_binary(Sections))]]),
            %% TODO Ugh
            TLen = iolist_size(amqp10_framing:encode_bin(Transfer)),
            Frames = case FrameMax of
                         unlimited -> [[Transfer, Sections]];
                         _ -> encode_frames(Transfer, Sections, FrameMax - TLen, [])
                     end,
            Del = #outgoing_unsettled{
                     msg_id = MsgId,
                     consumer_tag = ConsumerTag,
                     queue_name = QName,
                     %% The consumer timeout interval starts already from the point in time the
                     %% queue sent us the message so that the Ra log can be truncated even if
                     %% the message is sitting here for a long time.
                     delivered_at = os:system_time(millisecond)},
            Pending = #pending_transfer{
                         link_handle = ?UINT(Handle),
                         frames = Frames,
                         queue_ack_required = AckRequired,
                         queue_pid = QPid,
                         delivery_id = DeliveryId,
                         outgoing_unsettled = Del},
            State = State0#state{next_delivery_id = add(DeliveryId, 1),
                                 pending_transfers = queue:in(Pending, Pendings)},
            send_pending_transfers(State);
        _ ->
            %% TODO handle missing link -- why does the queue think it's there?
            rabbit_log:warning(
              "No link handle ~b exists for delivery with consumer tag ~p from queue ~tp",
              [Handle, ConsumerTag, QName]),
            State0
    end.

%%%%%%%%%%%%%%%%%%%%%
%%% Incoming Link %%%
%%%%%%%%%%%%%%%%%%%%%

incoming_link_transfer(
  #'v1_0.transfer'{delivery_id = DeliveryId,
                   more        = true,
                   settled     = Settled},
  MsgPart,
  #incoming_link{msg_acc = MsgAcc,
                 send_settle_mode = SSM} = Link0,
  State) ->
    Link1 = Link0#incoming_link{msg_acc = [MsgPart | MsgAcc],
                                send_settle_mode = effective_send_settle_mode(Settled, SSM)},
    Link = set_delivery_id(DeliveryId, Link1),
    {ok, [], Link, State};
incoming_link_transfer(
  #'v1_0.transfer'{delivery_id = DeliveryId0,
                   delivery_tag = DeliveryTag,
                   settled = Settled,
                   rcv_settle_mode = RcvSettleMode,
                   handle = Handle = ?UINT(HandleInt)},
  MsgPart,
  #incoming_link{exchange = XName = #resource{name = XNameBin},
                 routing_key      = LinkRKey,
                 delivery_count   = Count,
                 credit_used      = CreditUsed,
                 msg_acc          = MsgAcc,
                 send_settle_mode = SSM,
                 recv_settle_mode = RSM} = Link0,
  #state{queue_states = QStates0,
         incoming_unsettled_map = U0,
         user = User
        } = State0) ->
    #incoming_link{delivery_id = DeliveryId} = set_delivery_id(DeliveryId0, Link0),
    MsgBin = iolist_to_binary(lists:reverse([MsgPart | MsgAcc])),
    Sections = amqp10_framing:decode_bin(MsgBin),
    ?DEBUG("Inbound content:~n  ~tp",
           [[amqp10_framing:pprint(Section) || Section <- Sections]]),
    Anns0 = #{exchange => XNameBin},
    Anns = case LinkRKey of
               undefined -> Anns0;
               _ -> Anns0#{routing_keys => [LinkRKey]}
           end,
    Mc = mc:init(mc_amqp, Sections, Anns),
    RoutingKeys = mc:get_annotation(routing_keys, Mc),
    RoutingKey = routing_key(RoutingKeys, XName),
    % Mc1 = rabbit_message_interceptor:intercept(Mc),
    % rabbit_global_counters:messages_received(ProtoVer, 1),
    case rabbit_exchange:lookup(XName) of
        {ok, Exchange} ->
            check_write_permitted_on_topic(Exchange, User, RoutingKey),
            RoutedQNames = rabbit_exchange:route(Exchange, Mc, #{return_binding_keys => true}),
            % rabbit_trace:tap_in(Msg, QNames, ConnName, Username, TraceState),
            EffectiveSendSettleMode = effective_send_settle_mode(Settled, SSM),
            EffectiveRecvSettleMode = effective_recv_settle_mode(RcvSettleMode, RSM),
            case not EffectiveSendSettleMode andalso
                 EffectiveRecvSettleMode =:= ?V_1_0_RECEIVER_SETTLE_MODE_SECOND of
                false -> ok;
                true  -> protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                                        "rcv-settle-mode second not supported", [])
            end,
            Opts = case EffectiveSendSettleMode of
                       true -> #{};
                       false -> #{correlation => DeliveryId}
                   end,
            % Opts1 = maps_put_truthy(flow, Flow, Opts),
            Qs0 = rabbit_amqqueue:lookup_many(RoutedQNames),
            Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
            %% TODO convert message container to AMQP 0.9.1 message if feature flag message_containers
            %% is disabled, see rabbit_mqtt_processor:compat/2:
            %% https://github.com/rabbitmq/rabbitmq-server/blob/49b357ebd89f8f250afb292d9aeadf0357657c4a/deps/rabbitmq_mqtt/src/rabbit_mqtt_processor.erl#L2564-L2576
            case rabbit_queue_type:deliver(Qs, Mc, Opts, QStates0) of
                {ok, QStates, Actions} ->
                    % rabbit_global_counters:messages_routed(ProtoVer, length(Qs)),
                    %% Confirms must be registered before processing actions
                    %% because actions may contain rejections of publishes.
                    {U, Reply0} = process_routing_confirm(
                                    Qs, EffectiveSendSettleMode, DeliveryId, U0),
                    State1 = State0#state{queue_states = QStates,
                                          incoming_unsettled_map = U},
                    {Reply1, State} = handle_queue_actions(Actions, State1),
                    {SendFlow, CreditUsed1} = case CreditUsed - 1 of
                                                  C when C =< 0 ->
                                                      {true,  ?INCOMING_CREDIT div 2};
                                                  D ->
                                                      {false, D}
                                              end,
                    Link = Link0#incoming_link{
                             delivery_id      = undefined,
                             send_settle_mode = undefined,
                             delivery_count   = add(Count, 1),
                             credit_used      = CreditUsed1,
                             msg_acc          = []},
                    Reply = case SendFlow of
                                true  -> ?DEBUG("sending flow for incoming ~tp", [Link]),
                                         Reply0 ++ Reply1 ++ [incoming_flow(Link, Handle)];
                                false -> Reply0 ++ Reply1
                            end,
                    {ok, Reply, Link, State};
                {error, Reason} ->
                    rabbit_log:warning(
                      "Failed to deliver message to queues, "
                      "delivery_tag=~p, delivery_id=~p, reason=~p",
                      [DeliveryTag, DeliveryId, Reason])
                    %%TODO handle error
            end;
        {error, not_found} ->
            Disposition = released(DeliveryId),
            Detach = detach_deleted(HandleInt),
            {error, [Disposition, Detach]}
    end.

process_routing_confirm([], _SenderSettles = true, _, U) ->
    % rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    {U, []};
process_routing_confirm([], _SenderSettles = false, DeliveryId, U) ->
    % rabbit_global_counters:messages_unroutable_returned(ProtoVer, 1),
    Disposition = released(DeliveryId),
    {U, [Disposition]};
process_routing_confirm([_|_], _SenderSettles = true, _, U) ->
    {U, []};
process_routing_confirm([_|_] = Qs, _SenderSettles = false, DeliveryId, U0) ->
    QNames = rabbit_amqqueue:queue_names(Qs),
    false = maps:is_key(DeliveryId, U0),
    U = U0#{DeliveryId => {maps:from_keys(QNames, ok), false}},
    {U, []}.

released(DeliveryId) ->
    #'v1_0.disposition'{role = ?RECV_ROLE,
                        first = ?UINT(DeliveryId),
                        settled = true,
                        state = #'v1_0.released'{}}.

%% TODO default-outcome and outcomes, dynamic lifetimes

ensure_target(#'v1_0.target'{dynamic = true}, _, _) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "Dynamic targets not supported", []);
ensure_target(#'v1_0.target'{address = Address,
                             durable = Durable}, Vhost, User) ->
    case Address of
        {utf8, Destination} ->
            case rabbit_routing_util:parse_endpoint(Destination, true) of
                {ok, Dest} ->
                    QNameBin = ensure_terminus(target, Dest, Vhost, User, Durable),
                    {XNameList1, RK} = rabbit_routing_util:parse_routing(Dest),
                    XName = rabbit_misc:r(Vhost, exchange, list_to_binary(XNameList1)),
                    {ok, X} = rabbit_exchange:lookup(XName),
                    check_internal_exchange(X),
                    check_write_permitted(XName, User),
                    RoutingKey = case RK of
                                     undefined -> undefined;
                                     []        -> undefined;
                                     _         -> list_to_binary(RK)
                                 end,
                    {ok, XName, RoutingKey, QNameBin};
                {error, _} = E ->
                    E
            end;
        _Else ->
            {error, {address_not_utf8_string, Address}}
    end.

incoming_flow(#incoming_link{ delivery_count = Count }, Handle) ->
    #'v1_0.flow'{handle         = Handle,
                 delivery_count = ?UINT(Count),
                 link_credit    = ?UINT(?INCOMING_CREDIT)}.

%%%%%%%%%%%%%%%%%%%%%
%%% Outgoing Link %%%
%%%%%%%%%%%%%%%%%%%%%

handle_outgoing_link_flow_control(
  #outgoing_link{delivery_count = DeliveryCountSnd,
                 queue = QNameBin},
  #'v1_0.flow'{handle = Handle = ?UINT(HandleInt),
               delivery_count = DeliveryCountRcv0,
               link_credit    = ?UINT(LinkdCreditRcv),
               drain          = Drain0},
  #state{vhost = Vhost,
         queue_states = QStates0} = State0) ->
    ?UINT(DeliveryCountRcv) = default(DeliveryCountRcv0, ?UINT(DeliveryCountSnd)),
    Drain = default(Drain0, false),
    %% See section 2.6.7
    LinkCreditSnd = DeliveryCountRcv + LinkdCreditRcv - DeliveryCountSnd,
    Ctag = handle_to_ctag(HandleInt),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    {ok, QStates, Actions} = rabbit_queue_type:credit(QName, Ctag, LinkCreditSnd, Drain, QStates0),
    State1 = State0#state{queue_states = QStates},
    {Reply0, State} = handle_queue_actions(Actions, State1),
    %% TODO Prior to Native AMQP crediting was a synchronous call into the queue processs.
    %% This means for every received FLOW, a single slow queue will block the entire AMQP Session.
    %% There is no need to respond with a FLOW unless the 'echo' field is set.
    %% For the Native AMQP PoC, we keep this synchronous behaviour.
    %% However, for productive Native AMQP, the queue should include the consumer tag (and link_credit) into
    %% the credit reply st. the AMQP session can correlate the credit reply to the outgoing link handle and
    %% (possibly) reply with a FLOW when handling the queue action.
    %% => add feature flag
    %% => for backwards compat (when ff disabled) transform below into new queue_event here, and call
    %% handle_queue_event()
    Reply = case lists:any(fun(Action) ->
                                   element(1, Action) =:= send_credit_reply
                           end, Actions) of
                true ->
                    %% stream queue returned send_credit_reply action
                    Reply0;
                false ->
                    Available = receive {'$gen_cast',{queue_event,
                                                      {resource, Vhost, queue, QNameBin},
                                                      {send_credit_reply, Avail}}} ->
                                            %% from classic queue
                                            Avail;
                                        {'$gen_cast',{queue_event,
                                                      {resource, Vhost, queue, QNameBin},
                                                      {_QuorumQueue,
                                                       {applied,
                                                        [{_RaIdx,
                                                          {send_credit_reply, Avail}}]}}}} ->
                                            %% from quorum queue queue when Drain=false
                                            Avail;
                                        {'$gen_cast',{queue_event,
                                                      {resource, Vhost, queue, QNameBin},
                                                      {_QuorumQueue,
                                                       {applied,
                                                        [{_RaIdx,
                                                          {multi,
                                                           [{send_credit_reply, _Avail0},
                                                            {send_drained, {Ctag, Avail1}}]}}]}}}} ->
                                            %% from quorum queue queue when Drain=true
                                            Avail1
                                end,
                    case Available of
                        -1 ->
                            %% We don't know - probably because this flow relates
                            %% to a handle that does not yet exist
                            %% TODO is this an error?
                            Reply0;
                        _  ->
                            Reply0 ++ #'v1_0.flow'{
                                         handle         = Handle,
                                         delivery_count = ?UINT(DeliveryCountSnd),
                                         link_credit    = ?UINT(LinkCreditSnd),
                                         available      = ?UINT(Available),
                                         drain          = Drain}
                    end
            end,
    {ok, Reply, State}.

default(undefined, Default) -> Default;
default(Thing,    _Default) -> Thing.

ensure_source(#'v1_0.source'{dynamic = true}, _, _) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED, "Dynamic sources not supported", []);
ensure_source(#'v1_0.source'{address = Address,
                             durable = Durable},
              Vhost,
              User = #user{username = Username}) ->
    case Address of
        {utf8, SourceAddr} ->
            case rabbit_routing_util:parse_endpoint(SourceAddr, false) of
                {ok, Src} ->
                    QNameBin = ensure_terminus(source, Src, Vhost, User, Durable),
                    %%TODO remove dependency on rabbit_routing_util
                    %% and always operator on binaries
                    case rabbit_routing_util:parse_routing(Src) of
                        {"", QNameList} ->
                            true = string:equal(QNameList, QNameBin),
                            {ok, QNameBin};
                        {XNameList, RoutingKeyList} ->
                            RoutingKey = list_to_binary(RoutingKeyList),
                            XNameBin = list_to_binary(XNameList),
                            XName = rabbit_misc:r(Vhost, exchange, XNameBin),
                            QName = rabbit_misc:r(Vhost, queue, QNameBin),
                            Binding = #binding{source = XName,
                                               destination = QName,
                                               key = RoutingKey},
                            check_write_permitted(QName, User),
                            check_read_permitted(XName, User),
                            {ok, X} = rabbit_exchange:lookup(XName),
                            check_read_permitted_on_topic(X, User, RoutingKey),
                            case rabbit_binding:add(Binding, Username) of
                                ok ->
                                    {ok, QNameBin};
                                {error, _} = Err ->
                                    Err
                            end
                    end;
                {error, _} = Err ->
                    Err
            end;
        _ ->
            {error, {address_not_utf8_string, Address}}
    end.

encode_frames(_T, _Msg, MaxContentLen, _Transfers) when MaxContentLen =< 0 ->
    protocol_error(?V_1_0_AMQP_ERROR_FRAME_SIZE_TOO_SMALL,
                   "Frame size is too small by ~tp bytes",
                   [-MaxContentLen]);
encode_frames(T, Msg, MaxContentLen, Transfers) ->
    case iolist_size(Msg) > MaxContentLen of
        true  ->
            <<Chunk:MaxContentLen/binary, Rest/binary>> = iolist_to_binary(Msg),
            T1 = T#'v1_0.transfer'{more = true},
            encode_frames(T, Rest, MaxContentLen, [[T1, Chunk] | Transfers]);
        false ->
            lists:reverse([[T, Msg] | Transfers])
    end.

source_filters_to_consumer_args(#'v1_0.source'{filter = {map, KVList}}) ->
    Key = {symbol, <<"rabbitmq:stream-offset-spec">>},
    case keyfind_unpack_described(Key, KVList) of
        {_, {timestamp, Ts}} ->
            [{<<"x-stream-offset">>, timestamp, Ts div 1000}]; %% 0.9.1 uses second based timestamps
        {_, {utf8, Spec}} ->
            [{<<"x-stream-offset">>, longstr, Spec}]; %% next, last, first and "10m" etc
        {_, {_, Offset}} when is_integer(Offset) ->
            [{<<"x-stream-offset">>, long, Offset}]; %% integer offset
        _ ->
            []
    end;
source_filters_to_consumer_args(_Source) ->
    [].

keyfind_unpack_described(Key, KvList) ->
    %% filterset values _should_ be described values
    %% they aren't always however for historical reasons so we need this bit of
    %% code to return a plain value for the given filter key
    case lists:keyfind(Key, 1, KvList) of
        {Key, {described, Key, Value}} ->
            {Key, Value};
        {Key, _} = Kv ->
            Kv;
        false ->
            false
    end.

validate_attach(#'v1_0.attach'{target = #'v1_0.coordinator'{}}) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "Transactions not supported", []);
validate_attach(#'v1_0.attach'{unsettled = Unsettled,
                               incomplete_unsettled = IncompleteSettled})
  when Unsettled =/= undefined andalso Unsettled =/= {map, []} orelse
       IncompleteSettled =:= true ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "Link recovery not supported", []);
validate_attach(
  #'v1_0.attach'{snd_settle_mode = SndSettleMode,
                 rcv_settle_mode = ?V_1_0_RECEIVER_SETTLE_MODE_SECOND})
  when SndSettleMode =/= ?V_1_0_SENDER_SETTLE_MODE_SETTLED ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "rcv-settle-mode second not supported", []);
validate_attach(#'v1_0.attach'{}) ->
    ok.

ensure_terminus(Type, {exchange, {XNameList, _RoutingKey}}, Vhost, User, Durability) ->
    ok = exit_if_absent(exchange, Vhost, XNameList),
    case Type of
        target -> undefined;
        source -> declare_queue(generate_queue_name(), Vhost, User, Durability)
    end;
ensure_terminus(target, {topic, _bindingkey}, _, _, _) ->
    %% exchange amq.topic exists
    undefined;
ensure_terminus(source, {topic, _BindingKey}, Vhost, User, Durability) ->
    %% exchange amq.topic exists
    declare_queue(generate_queue_name(), Vhost, User, Durability);
ensure_terminus(_, {queue, QNameList}, Vhost, User, Durability) ->
    declare_queue(list_to_binary(QNameList), Vhost, User, Durability);
ensure_terminus(_, {amqqueue, QNameList}, Vhost, _, _) ->
    %% Target "/amq/queue/" is handled specially due to AMQP legacy:
    %% "Queue names starting with "amq." are reserved for pre-declared and
    %% standardised queues. The client MAY declare a queue starting with "amq."
    %% if the passive option is set, or the queue already exists."
    QNameBin = list_to_binary(QNameList),
    ok = exit_if_absent(queue, Vhost, QNameBin),
    QNameBin.

exit_if_absent(Type, Vhost, Name) ->
    ResourceName = rabbit_misc:r(Vhost, Type, rabbit_data_coercion:to_binary(Name)),
    Mod = case Type of
              exchange -> rabbit_exchange;
              queue -> rabbit_amqqueue
          end,
    case Mod:exists(ResourceName) of
        true ->
            ok;
        false ->
            protocol_error(?V_1_0_AMQP_ERROR_NOT_FOUND, "no ~ts", [rabbit_misc:rs(ResourceName)])
    end.

generate_queue_name() ->
    rabbit_guid:binary(rabbit_guid:gen_secure(), "amq.gen").

declare_queue(QNameBin, Vhost, User = #user{username = Username}, TerminusDurability) ->
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    check_configure_permitted(QName, User),
    %%TODO rabbit_vhost_limit:is_over_queue_limit(VHost)
    rabbit_core_metrics:queue_declared(QName),
    Q0 = amqqueue:new(QName,
                      _Pid = none,
                      queue_is_durable(TerminusDurability),
                      _AutoDelete = false,
                      _QOwner = none,
                      _QArgs = [],
                      Vhost,
                      #{user => Username},
                      rabbit_classic_queue),
    case rabbit_queue_type:declare(Q0, node()) of
        {new, _Q}  ->
            rabbit_core_metrics:queue_created(QName),
            QNameBin;
        {existing, _Q} ->
            QNameBin;
        Other ->
            protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, "Failed to declare ~s: ~p", [rabbit_misc:rs(QName), Other])
    end.

outcomes(#'v1_0.source'{outcomes = undefined}) ->
    {array, symbol, ?OUTCOMES};
outcomes(#'v1_0.source'{outcomes = {array, symbol, Syms} = Outcomes}) ->
    case lists:filter(fun(O) -> not lists:member(O, ?OUTCOMES) end, Syms) of
        [] ->
            Outcomes;
        Unsupported ->
            rabbit_amqp1_0_util:protocol_error(
              ?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
              "Outcomes not supported: ~tp",
              [Unsupported])
    end;
outcomes(#'v1_0.source'{outcomes = Unsupported}) ->
    rabbit_amqp1_0_util:protocol_error(
      ?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
      "Outcomes not supported: ~tp",
      [Unsupported]);
outcomes(_) ->
    {array, symbol, ?OUTCOMES}.

-spec handle_to_ctag(link_handle()) -> rabbit_types:ctag().
handle_to_ctag(Handle) ->
    <<"ctag-", Handle:32/integer>>.

-spec ctag_to_handle(rabbit_types:ctag()) -> link_handle().
ctag_to_handle(<<"ctag-", Handle:32/integer>>) ->
    Handle.

queue_is_durable(?V_1_0_TERMINUS_DURABILITY_NONE) ->
    false;
queue_is_durable(?V_1_0_TERMINUS_DURABILITY_CONFIGURATION) ->
    true;
queue_is_durable(?V_1_0_TERMINUS_DURABILITY_UNSETTLED_STATE) ->
    true;
queue_is_durable(undefined) ->
    %% <field name="durable" type="terminus-durability" default="none"/>
    %% [3.5.3]
    queue_is_durable(?V_1_0_TERMINUS_DURABILITY_NONE).

%% "The two endpoints are not REQUIRED to use the same handle. This means a peer
%% is free to independently chose its handle when a link endpoint is associated
%% with the session. The locally chosen handle is referred to as the output handle.
%% The remotely chosen handle is referred to as the input handle." [2.6.2]
%% For simplicity, we choose to use the same handle.
output_handle(InputHandle) ->
    _Outputhandle = InputHandle.

routing_key(undefined, XName) ->
    protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                   "Publishing to ~ts failed since no routing key was provided",
                   [rabbit_misc:rs(XName)]);
routing_key([RoutingKey], _XName) ->
    RoutingKey.

check_internal_exchange(#exchange{internal = true,
                                  name = XName}) ->
    protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                   "attach to internal ~ts is forbidden",
                   [rabbit_misc:rs(XName)]);
check_internal_exchange(_) ->
    ok.

check_write_permitted(Resource, User) ->
    check_resource_access(Resource, User, write).

check_read_permitted(Resource, User) ->
    check_resource_access(Resource, User, read).

check_configure_permitted(Resource, User) ->
    check_resource_access(Resource, User, configure).

check_resource_access(Resource, User, Perm) ->
    Context = #{},
    ok = try rabbit_access_control:check_resource_access(User, Resource, Perm, Context)
         catch exit:#amqp_error{name = access_refused,
                                explanation = Msg} ->
                   protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Msg, [])
         end.

check_write_permitted_on_topic(Resource, User, RoutingKey) ->
    check_topic_authorisation(Resource, User, RoutingKey, write).

check_read_permitted_on_topic(Resource, User, RoutingKey) ->
    check_topic_authorisation(Resource, User, RoutingKey, read).

check_topic_authorisation(#exchange{type = topic,
                                    name = XName = #resource{virtual_host = VHost}},
                          User = #user{username = Username},
                          RoutingKey,
                          Permission) ->
    Resource = XName#resource{kind = topic},
    CacheElem = {Resource, RoutingKey, Permission},
    Cache = case get(?TOPIC_PERMISSION_CACHE) of
                undefined -> [];
                List -> List
            end,
    case lists:member(CacheElem, Cache) of
        true ->
            ok;
        false ->
            VariableMap = #{<<"vhost">> => VHost,
                            <<"username">> => Username},
            Context = #{routing_key => RoutingKey,
                        variable_map => VariableMap},
            try rabbit_access_control:check_topic_access(User, Resource, Permission, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
                    put(?TOPIC_PERMISSION_CACHE, [CacheElem | CacheTail])
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Msg, [])
            end
    end;
check_topic_authorisation(_, _, _, _) ->
    ok.

%%TODO every rabbit.channel_tick_interval:
% put(permission_cache_can_expire, rabbit_access_control:permission_cache_can_expire(User)),
% If permission_cache_can_expire:
% clear cache AND check permissions on all links
% clear_permission_cache() ->
%     erase(?TOPIC_PERMISSION_CACHE),
%     ok.
