%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(amqp10_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_ct_broker_helpers,
        [rpc/4, rpc/5,
         get_node_config/3]).
-import(rabbit_ct_helpers,
        [eventually/1, eventually/3]).
-import(event_recorder,
        [assert_event_type/2,
         assert_event_prop/2]).

all() ->
    [
      {group, cluster_size_1},
      {group, cluster_size_3},
      {group, metrics}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       reliable_send_receive_with_outcomes_classic_queue,
       reliable_send_receive_with_outcomes_quorum_queue,
       sender_settle_mode_unsettled,
       sender_settle_mode_unsettled_fanout,
       sender_settle_mode_mixed,
       quorum_queue_rejects,
       receiver_settle_mode_first,
       publishing_to_non_existing_queue_should_settle_with_released,
       open_link_to_non_existing_destination_should_end_session,
       roundtrip_classic_queue_with_drain,
       roundtrip_quorum_queue_with_drain,
       roundtrip_stream_queue_with_drain,
       amqp_stream_amqpl,
       message_headers_conversion,
       multiple_sessions,
       server_closes_link_classic_queue,
       server_closes_link_quorum_queue,
       server_closes_link_stream,
       server_closes_link_exchange,
       link_target_classic_queue_deleted,
       link_target_quorum_queue_deleted,
       target_queues_deleted_accepted,
       no_routing_key,
       events,
       sync_get_unsettled_classic_queue,
       sync_get_unsettled_quorum_queue,
       sync_get_unsettled_stream,
       sync_get_unsettled_2_classic_queue,
       sync_get_unsettled_2_quorum_queue,
       sync_get_unsettled_2_stream,
       sync_get_settled_classic_queue,
       sync_get_settled_quorum_queue,
       sync_get_settled_stream,
       timed_get_classic_queue,
       timed_get_quorum_queue,
       timed_get_stream,
       stop_classic_queue,
       stop_quorum_queue,
       stop_stream,
       single_active_consumer_classic_queue,
       single_active_consumer_quorum_queue
      ]},

     {cluster_size_3, [shuffle],
      [
       last_queue_confirms,
       target_queue_deleted,
       credit_reply_quorum_queue,
       async_notify_settled_classic_queue,
       async_notify_settled_quorum_queue,
       async_notify_settled_stream,
       async_notify_unsettled_classic_queue,
       async_notify_unsettled_quorum_queue,
       async_notify_unsettled_stream
      ]},

     {metrics, [shuffle],
      [
       auth_attempt_metrics
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Nodes = case Group of
                cluster_size_3 -> 3;
                _ -> 1
            end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, Nodes},
                         {rmq_nodename_suffix, Suffix}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(T = message_headers_conversion, Config) ->
    case rabbit_ct_broker_helpers:rpc(
           Config, rabbit_feature_flags, is_enabled, [credit_api_v2]) of
        true ->
            rabbit_ct_helpers:testcase_started(Config, T);
        false ->
            {skip, "Quorum queues are known to behave incorrectly with feature flag "
             "credit_api_v2 disabled because they send a send_drained queue event "
             "before sending all available messages."}
    end;
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Assert that every testcase cleaned up.
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, []))),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

reliable_send_receive_with_outcomes_classic_queue(Config) ->
    reliable_send_receive_with_outcomes(<<"classic">>, Config).

reliable_send_receive_with_outcomes_quorum_queue(Config) ->
    reliable_send_receive_with_outcomes(<<"quorum">>, Config).

reliable_send_receive_with_outcomes(QType, Config) ->
    Outcomes = [
                accepted,
                modified,
                {modified, true, false, #{<<"fruit">> => <<"banana">>}},
                {modified, false, true, #{}},
                rejected,
                released
               ],
    [ok = reliable_send_receive(QType, Outcome, Config) || Outcome <- Outcomes].

reliable_send_receive(QType, Outcome, Config) ->
    OutcomeBin = if is_atom(Outcome) ->
                        atom_to_binary(Outcome);
                    is_tuple(Outcome) ->
                        O1 = atom_to_binary(element(1, Outcome)),
                        O2 = atom_to_binary(element(2, Outcome)),
                        <<O1/binary, "_", O2/binary>>
                 end,
    ct:pal("~s testing ~s", [?FUNCTION_NAME, OutcomeBin]),

    QName = <<QType/binary, OutcomeBin/binary>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    %% reliable send and consume
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    DTag1 = <<"dtag-1">>,
    %% create an unsettled message,
    %% link will be in "mixed" mode by default
    Body = <<"body-1">>,
    Msg1 = amqp10_msg:new(DTag1, Body, false),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = wait_for_settlement(DTag1),

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    flush("post sender close"),

    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    {ok, Session2} = amqp10_client:begin_session_sync(Connection2),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session2, <<"test-receiver">>, Address, unsettled),
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([Body], amqp10_msg:body(Msg)),

    ok = amqp10_client:settle_msg(Receiver, Msg, Outcome),
    flush("post accept"),

    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session2),
    ok = amqp10_client:close_connection(Connection2),
    ok = delete_queue(Config, QName).

%% Tests that confirmations are returned correctly
%% when sending many messages async to a quorum queue.
sender_settle_mode_unsettled(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    %% Send many messages aync.
    NumMsgs = 30,
    DTags = [begin
                 DTag = integer_to_binary(N),
                 Msg = amqp10_msg:new(DTag, <<"body">>, false),
                 ok = amqp10_client:send_msg(Sender, Msg),
                 DTag
             end  || N <- lists:seq(1, NumMsgs)],

    %% Wait for confirms.
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 5000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = NumMsgs},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

sender_settle_mode_unsettled_fanout(Config) ->
    QNames = [<<"q1">>, <<"q2">>, <<"q3">>],
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    [begin
         #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
         #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                                  exchange = <<"amq.fanout">>})
     end || QName <- QNames],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/amq.fanout/ignored">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    %% Send many messages aync.
    NumMsgs = 20,
    DTags = [begin
                 DTag = integer_to_binary(N),
                 Msg = amqp10_msg:new(DTag, <<"body">>, false),
                 ok = amqp10_client:send_msg(Sender, Msg),
                 DTag
             end  || N <- lists:seq(1, NumMsgs)],

    %% Wait for confirms.
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 5000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    [?assertEqual(#'queue.delete_ok'{message_count = NumMsgs},
                  amqp_channel:call(Ch, #'queue.delete'{queue = QName})) ||
     QName <- QNames],
    ok = rabbit_ct_client_helpers:close_channel(Ch).

%% Tests that confirmations are returned correctly
%% when sending many messages async to a quorum queue where
%% every 3rd message is settled by the sender.
sender_settle_mode_mixed(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, mixed),
    ok = wait_for_credit(Sender),

    %% Send many messages aync.
    NumMsgs = 30,
    DTags = lists:filtermap(
              fun(N) ->
                      DTag = integer_to_binary(N),
                      {Settled, Ret} = case N rem 3 of
                                           0 -> {true, false};
                                           _ -> {false, {true, DTag}}
                                       end,
                      Msg = amqp10_msg:new(DTag, <<"body">>, Settled),
                      ok = amqp10_client:send_msg(Sender, Msg),
                      Ret
              end,  lists:seq(1, NumMsgs)),
    20 = length(DTags),

    %% Wait for confirms.
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 5000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = NumMsgs},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

quorum_queue_rejects(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                  {<<"x-max-length">>, long, 1},
                                                  {<<"x-overflow">>, longstr, <<"reject-publish">>}
                                                 ]}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, mixed),
    ok = wait_for_credit(Sender),

    %% Quorum queue's x-max-length limit is known to be off by 1.
    %% Therefore, we expect the first 2 messages to be accepted.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag a">>, <<>>, false)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag b">>, <<>>, false)),
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 5000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- [<<"tag a">>, <<"tag b">>]],

    %% From now on the quorum queue should reject our publishes.
    %% Send many messages aync.
    NumMsgs = 20,
    DTags = [begin
                 DTag = integer_to_binary(N),
                 Msg = amqp10_msg:new(DTag, <<"body">>, false),
                 ok = amqp10_client:send_msg(Sender, Msg),
                 DTag
             end  || N <- lists:seq(1, NumMsgs)],
    %% Since our sender settle mode is mixed, let's also test sending one as settled.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag c">>, <<>>, true)),
    %% and the final one as unsettled again
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag d">>, <<>>, false)),

    [receive {amqp10_disposition, {rejected, DTag}} -> ok
     after 5000 -> ct:fail({missing_rejected, DTag})
     end || DTag <- DTags ++ [<<"tag d">>]],

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),

    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

receiver_settle_mode_first(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, settled),
    ok = wait_for_credit(Sender),

    %% Send 10 messages.
    [begin
         DTag = Body = integer_to_binary(N),
         Msg = amqp10_msg:new(DTag, Body, true),
         ok = amqp10_client:send_msg(Sender, Msg)
     end  || N <- lists:seq(1, 10)],
    ok = amqp10_client:detach_link(Sender),
    flush("post sender close"),

    %% Receive the first 9 messages.
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, unsettled),
    ok = amqp10_client:flow_link_credit(Receiver, 9, never),
    Msgs_1_to_9 = receive_messages(Receiver, 9),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    assert_messages(QName, 10, 9, Config),

    %% What follows is white box testing: We want to hit a few different branches in the
    %% server code. Although this test is a bit artificial, the AMQP spec does not prohibit
    %% clients to ack in such ranges.

    %% 1. Ack a range smaller than the number of unacked messages where some delivery IDs
    %% are outside the [min, max] range of unacked messages.
    {Msgs_1_to_7, [Msg8, Msg9]} = lists:split(7, Msgs_1_to_9),
    DeliveryIdMsg8 = amqp10_msg:delivery_id(Msg8),
    DeliveryIdMsg9 = amqp10_msg:delivery_id(Msg9),
    ?assertEqual(DeliveryIdMsg9, serial_number_increment(DeliveryIdMsg8)),
    Last1 = serial_number_increment(serial_number_increment(DeliveryIdMsg9)),
    ok = amqp10_client_session:disposition(
           Session, receiver, DeliveryIdMsg8, Last1, true, accepted),
    assert_messages(QName, 8, 7, Config),

    %% 2. Ack a range smaller than the number of unacked messages where all delivery IDs
    %% are inside the [min, max] range of unacked messages.
    [Msg1, Msg2, _Msg3, Msg4, _Msg5, Msg6, Msg7] = Msgs_1_to_7,
    DeliveryIdMsg4 = amqp10_msg:delivery_id(Msg4),
    DeliveryIdMsg6 = amqp10_msg:delivery_id(Msg6),
    ok = amqp10_client_session:disposition(
           Session, receiver, DeliveryIdMsg4, DeliveryIdMsg6, true, accepted),
    assert_messages(QName, 5, 4, Config),

    %% 3. Ack a range larger than the number of unacked messages where all delivery IDs
    %% are inside the [min, max] range of unacked messages.
    DeliveryIdMsg2 = amqp10_msg:delivery_id(Msg2),
    DeliveryIdMsg7 = amqp10_msg:delivery_id(Msg7),
    ok = amqp10_client_session:disposition(
           Session, receiver, DeliveryIdMsg2, DeliveryIdMsg7, true, accepted),
    assert_messages(QName, 2, 1, Config),

    %% Consume the last message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    [Msg10] = receive_messages(Receiver, 1),
    ?assertEqual([<<"10">>], amqp10_msg:body(Msg10)),

    %% 4. Ack a range larger than the number of unacked messages where some delivery IDs
    %% are outside the [min, max] range of unacked messages.
    DeliveryIdMsg1 = amqp10_msg:delivery_id(Msg1),
    DeliveryIdMsg10 = amqp10_msg:delivery_id(Msg10),
    Last2 = serial_number_increment(DeliveryIdMsg10),
    ok = amqp10_client_session:disposition(
           Session, receiver, DeliveryIdMsg1, Last2, true, accepted),
    assert_messages(QName, 0, 0, Config),

    %% 5. Ack single delivery ID when there are no unacked messages.
    ok = amqp10_client_session:disposition(
           Session, receiver, DeliveryIdMsg1, DeliveryIdMsg1, true, accepted),

    %% 6. Ack multiple delivery IDs when there are no unacked messages.
    ok = amqp10_client_session:disposition(
           Session, receiver, DeliveryIdMsg1, DeliveryIdMsg6, true, accepted),
    assert_messages(QName, 0, 0, Config),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok = delete_queue(Config, QName).

publishing_to_non_existing_queue_should_settle_with_released(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    QName = <<"queue does not exist">>,
    Address = <<"/exchange/amq.direct/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    DTag1 = <<"dtag-1">>,
    %% create an unsettled message,
    %% link will be in "mixed" mode by default
    Msg1 = amqp10_msg:new(DTag1, <<"body-1">>, false),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = wait_for_settlement(DTag1, released),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),
    flush("post sender close"),
    ok.

open_link_to_non_existing_destination_should_end_session(Config) ->
    OpnConf = connection_config(Config),
    Name = "non-existing-destination",
    Addresses = ["/exchange/" ++ Name ++ "/bar",
                 "/amq/queue/" ++ Name],
    SenderLinkName = <<"test-sender">>,
    [begin
         {ok, Connection} = amqp10_client:open_connection(OpnConf),
         {ok, Session} = amqp10_client:begin_session_sync(Connection),
         ct:pal("Address ~p", [Address]),
         {ok, _} = amqp10_client:attach_sender_link(
                     Session, SenderLinkName, list_to_binary(Address)),
         wait_for_session_end(Session),
         ok = amqp10_client:close_connection(Connection),
         flush("post sender close")
     end || Address <- Addresses],
    ok.

roundtrip_classic_queue_with_drain(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    roundtrip_queue_with_drain(Config, <<"classic">>, QName).

roundtrip_quorum_queue_with_drain(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    roundtrip_queue_with_drain(Config, <<"quorum">>, QName).

roundtrip_stream_queue_with_drain(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    roundtrip_queue_with_drain(Config, <<"stream">>, QName).

roundtrip_queue_with_drain(Config, QueueType, QName)
  when is_binary(QueueType) ->
    Address = <<"/amq/queue/", QName/binary>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    Args = [{<<"x-queue-type">>, longstr, QueueType}],
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = Args}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    % Create a new message using a delivery-tag, body and indicate
    % its settlement status (true meaning no disposition confirmation
    % will be sent by the receiver).
    OutMsg = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, true),
    ok = amqp10_client:send_msg(Sender, OutMsg),

    flush("pre-receive"),
    % create a receiver link
    TerminusDurability = none,
    Filter = consume_from_first(QueueType),
    Properties = #{},
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver">>,
                                                        Address, unsettled,
                                                        TerminusDurability,
                                                        Filter, Properties),

    % grant credit and drain
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),

    % wait for a delivery
    receive
        {amqp10_msg, Receiver, InMsg} ->
            ok = amqp10_client:accept_msg(Receiver, InMsg),
            wait_for_accepts(1),
            ok
    after 2000 ->
              exit(delivery_timeout)
    end,
    OutMsg2 = amqp10_msg:new(<<"my-tag">>, <<"my-body2">>, true),
    ok = amqp10_client:send_msg(Sender, OutMsg2),

    %% no delivery should be made at this point
    receive
        {amqp10_msg, _, _} ->
            exit(unexpected_delivery)
    after 500 ->
              ok
    end,

    flush("final"),
    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),
    ok = delete_queue(Config, QName).

%% Send a message with a body containing a single AMQP 1.0 value section
%% to a stream and consume via AMQP 0.9.1.
amqp_stream_amqpl(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QName = atom_to_binary(?FUNCTION_NAME),

    amqp_channel:call(Ch, #'queue.declare'{
                             queue = QName,
                             durable = true,
                             arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),

    Address = <<"/amq/queue/", QName/binary>>,
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),
    OutMsg = amqp10_msg:new(<<"my-tag">>, {'v1_0.amqp_value', {binary, <<0, 255>>}}, true),
    ok = amqp10_client:send_msg(Sender, OutMsg),
    flush("final"),
    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),

    #'basic.qos_ok'{} =  amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                            prefetch_count = 1}),
    CTag = <<"my-tag">>,
    #'basic.consume_ok'{} = amqp_channel:subscribe(
                              Ch,
                              #'basic.consume'{
                                 queue = QName,
                                 consumer_tag = CTag,
                                 arguments = [{<<"x-stream-offset">>, longstr, <<"first">>}]},
                              self()),
    receive
        {#'basic.deliver'{consumer_tag = CTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{type = <<"amqp-1.0">>}}} ->
            ok
    after 5000 ->
              exit(basic_deliver_timeout)
    end,
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

message_headers_conversion(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    Address = <<"/amq/queue/", QName/binary>>,
    %% declare a quorum queue
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch, #'queue.declare'{
                             queue = QName,
                             durable = true,
                             arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),

    amqp10_to_amqp091_header_conversion(Session, Ch, QName, Address),
    amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address),

    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = delete_queue(Config, QName),
    ok = amqp10_client:close_connection(Connection).

amqp10_to_amqp091_header_conversion(Session,Ch, QName, Address) -> 
    {ok, Sender} = create_amqp10_sender(Session, Address),

    OutMsg1 = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, true),
    OutMsg2 = amqp10_msg:set_application_properties(
                #{"string" => "string-val",
                  "int" => 2,
                  "bool" => false},
                OutMsg1),
    OutMsg3 = amqp10_msg:set_message_annotations(
                #{"x-string" => "string-value",
                  "x-int" => 3,
                  "x-bool" => true},
                OutMsg2),
    ok = amqp10_client:send_msg(Sender, OutMsg3),
    wait_for_accepts(1),

    {ok, Headers} = amqp091_get_msg_headers(Ch, QName),

    ?assertEqual({longstr, <<"string-val">>}, rabbit_misc:table_lookup(Headers, <<"string">>)),
    ?assertEqual({unsignedint, 2}, rabbit_misc:table_lookup(Headers, <<"int">>)),
    ?assertEqual({bool, false}, rabbit_misc:table_lookup(Headers, <<"bool">>)),

    ?assertEqual({longstr, <<"string-value">>}, rabbit_misc:table_lookup(Headers, <<"x-string">>)),
    ?assertEqual({unsignedint, 3}, rabbit_misc:table_lookup(Headers, <<"x-int">>)),
    ?assertEqual({bool, true}, rabbit_misc:table_lookup(Headers, <<"x-bool">>)).

amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address) -> 
    Amqp091Headers = [{<<"x-forwarding">>, array, 
                       [{table, [{<<"uri">>, longstr,
                                  <<"amqp://localhost/%2F/upstream">>}]}]},
                      {<<"x-string">>, longstr, "my-string"},
                      {<<"x-int">>, long, 92},
                      {<<"x-bool">>, bool, true},
                      {<<"string">>, longstr, "my-str"},
                      {<<"int">>, long, 101},
                      {<<"bool">>, bool, false}],

    amqp_channel:cast(
      Ch,
      #'basic.publish'{routing_key = QName},
      #amqp_msg{props = #'P_basic'{headers = Amqp091Headers},
                payload = <<"foobar">>}),

    {ok, [Msg]} = drain_queue(Session, Address, 1),
    Amqp10MA = amqp10_msg:message_annotations(Msg),
    ?assertEqual(<<"my-string">>, maps:get(<<"x-string">>, Amqp10MA, undefined)),
    ?assertEqual(92, maps:get(<<"x-int">>, Amqp10MA, undefined)),
    ?assertEqual(true, maps:get(<<"x-bool">>, Amqp10MA, undefined)),

    Amqp10Props = amqp10_msg:application_properties(Msg),
    ?assertEqual(<<"my-str">>, maps:get(<<"string">>, Amqp10Props, undefined)),
    ?assertEqual(101, maps:get(<<"int">>, Amqp10Props, undefined)),
    ?assertEqual(false, maps:get(<<"bool">>, Amqp10Props, undefined)).

%% Test sending and receiving concurrently on multiple sessions of the same connection.
multiple_sessions(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    %% Create 2 sessions on the same connection.
    {ok, Session1} = amqp10_client:begin_session(Connection),
    {ok, Session2} = amqp10_client:begin_session(Connection),

    %% Receive on each session.
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Qs = [Q1, Q2],
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session1, <<"receiver link 1">>, Q1, settled, configuration),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session2, <<"receiver link 2">>, Q2, settled, configuration),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    NMsgsPerSender = 20,
    NMsgsPerReceiver = NMsgsPerSender * 2, % due to fanout
    ok = amqp10_client:flow_link_credit(Receiver1, NMsgsPerReceiver, never),
    ok = amqp10_client:flow_link_credit(Receiver2, NMsgsPerReceiver, never),
    flush("receiver attached"),

    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    [#'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                              exchange = <<"amq.fanout">>})
     || QName <- Qs],
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    %% Send on each session.
    TargetAddr = <<"/exchange/amq.fanout/ignored">>,
    {ok, Sender1} = amqp10_client:attach_sender_link_sync(
                      Session1, <<"sender link 1">>, TargetAddr, settled, configuration),
    ok = wait_for_credit(Sender1),
    {ok, Sender2} = amqp10_client:attach_sender_link_sync(
                      Session2, <<"sender link 2">>, TargetAddr, settled, configuration),
    ok = wait_for_credit(Sender2),

    %% Send concurrently.
    Group1 = <<"group 1">>,
    Group2 = <<"group 2">>,
    spawn_link(?MODULE, send_messages, [Sender1, NMsgsPerSender, Group1]),
    spawn_link(?MODULE, send_messages, [Sender2, NMsgsPerSender, Group2]),

    Q1Msgs = receive_messages(Receiver1, NMsgsPerReceiver),
    Q2Msgs = receive_messages(Receiver2, NMsgsPerReceiver),
    ExpectedBodies = [integer_to_binary(I) || I <- lists:seq(1, NMsgsPerSender)],
    [begin
         {G1Msgs, G2Msgs} = lists:partition(
                              fun(Msg) ->
                                      #{group_id := GroupId} = amqp10_msg:properties(Msg),
                                      case GroupId of
                                          Group1 -> true;
                                          Group2 -> false
                                      end
                              end, Msgs),
         [begin
              Bodies = [begin
                            [Bin] = amqp10_msg:body(M),
                            Bin
                        end || M <- GMsgs],
              ?assertEqual(ExpectedBodies, Bodies)
          end || GMsgs <- [G1Msgs, G2Msgs]]
     end || Msgs <- [Q1Msgs, Q2Msgs]],

    %% Clean up.
    [ok = amqp10_client:detach_link(Link) || Link <- [Receiver1, Receiver2, Sender1, Sender2]],
    ok = end_session_sync(Session1),
    ok = end_session_sync(Session2),
    ok = amqp10_client:close_connection(Connection),
    [ok = delete_queue(Config, Q) || Q <- Qs].

server_closes_link_classic_queue(Config) ->
    server_closes_link(<<"classic">>, Config).

server_closes_link_quorum_queue(Config) ->
    server_closes_link(<<"quorum">>, Config).

server_closes_link_stream(Config) ->
    server_closes_link(<<"stream">>, Config).

server_closes_link(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    DTag = <<0>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag, <<"body">>, false)),
    ok = wait_for_settlement(DTag),

    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing ATTACH frame from server")
    end,

    %% Server closes the link endpoint due to some AMQP 1.0 external condition:
    %% In this test, the external condition is that an AMQP 0.9.1 client deletes the queue.
    delete_queue(Config, QName),

    %% We expect that the server closes the link endpoints,
    %% i.e. the server sends us DETACH frames.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our outgoing link")
    end,

    receive {amqp10_event, {link, Receiver, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our incoming link")
    end,

    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

server_closes_link_exchange(Config) ->
    XName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = XName}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/", XName/binary, "/some-routing-key">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),

    %% Server closes the link endpoint due to some AMQP 1.0 external condition:
    %% In this test, the external condition is that an AMQP 0.9.1 client deletes the exchange.
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = XName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    %% When we publish the next message, we expect:
    %% 1. that the message is released because the exchange doesn't exist anymore, and
    DTag = <<255>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag, <<"body">>, false)),
    receive {amqp10_disposition, {released, DTag}} -> ok
    after 5000 -> ct:fail(released_timeout)
    end,
    %% 2. that the server closes the link, i.e. sends us a DETACH frame.

    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our outgoing link")
    end,

    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

link_target_classic_queue_deleted(Config) ->
    link_target_queue_deleted(<<"classic">>, Config).

link_target_quorum_queue_deleted(Config) ->
    link_target_queue_deleted(<<"quorum">>, Config).

link_target_queue_deleted(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    DTag1 = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_settlement(DTag1),

    %% Mock delivery to the target queue to do nothing.
    rabbit_ct_broker_helpers:setup_meck(Config, [?MODULE]),
    Mod = rabbit_queue_type,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, deliver, fun ?MODULE:rabbit_queue_type_deliver_noop/4]),

    %% Send 2nd message.
    DTag2 = <<2>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, false)),
    receive {amqp10_disposition, Unexpected} -> ct:fail({unexpected_disposition, Unexpected})
    after 200 -> ok
    end,

    %% Now, the server AMQP session contains a delivery that did not get confirmed by the target queue.
    %% If we now delete that target queue, RabbitMQ must not reply to us with ACCEPTED.
    %% Instead, we expect RabbitMQ to reply with RELEASED since no queue ever received our 2nd message.
    delete_queue(Config, QName),
    receive {amqp10_disposition, {released, DTag2}} -> ok
    after 5000 -> ct:fail(released_timeout)
    end,

    %% After the 2nd message got released, we additionally expect RabbitMQ to close the link given
    %% that the target link endpoint - the queue - got deleted.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our outgoing link")
    end,

    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

rabbit_queue_type_deliver_noop(_TargetQs, _Msg, _Opts, QTypeState) ->
    Actions = [],
    {ok, QTypeState, Actions}.

target_queues_deleted_accepted(Config) ->
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    QNames = [Q1, Q2, Q3],
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    [begin
         #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
         #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                                  exchange = <<"amq.fanout">>})
     end || QName <- QNames],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/amq.fanout/ignored">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    DTag1 = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_settlement(DTag1),

    %% Mock to deliver only to q1.
    rabbit_ct_broker_helpers:setup_meck(Config, [?MODULE]),
    Mod = rabbit_queue_type,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, deliver, fun ?MODULE:rabbit_queue_type_deliver_to_q1/4]),

    %% Send 2nd message.
    DTag2 = <<2>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, false)),
    receive {amqp10_disposition, Disp1} -> ct:fail({unexpected_disposition, Disp1})
    after 200 -> ok
    end,

    %% Now, the server AMQP session contains a delivery that got confirmed by only q1.
    %% If we delete q2, we should still receive no DISPOSITION since q3 hasn't confirmed.
    ?assertEqual(#'queue.delete_ok'{message_count = 1},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q2})),
    receive {amqp10_disposition, Disp2} -> ct:fail({unexpected_disposition, Disp2})
    after 100 -> ok
    end,
    %% If we delete q3, RabbitMQ should reply with ACCEPTED since at least one target queue (q1) confirmed.
    ?assertEqual(#'queue.delete_ok'{message_count = 1},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q3})),
    receive {amqp10_disposition, {accepted, DTag2}} -> ok
    after 5000 -> ct:fail(accepted_timeout)
    end,

    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q1})),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

rabbit_queue_type_deliver_to_q1(Qs, Msg, Opts, QTypeState) ->
    %% Drop q2 and q3.
    3 = length(Qs),
    Q1 = lists:filter(fun({Q, _RouteInos}) ->
                              amqqueue:get_name(Q) =:= rabbit_misc:r(<<"/">>, queue, <<"q1">>)
                      end, Qs),
    1 = length(Q1),
    meck:passthrough([Q1, Msg, Opts, QTypeState]).

%% Set routing key neither in target address nor in message subject.
no_routing_key(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/amq.direct">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    Msg = amqp10_msg:new(<<0>>, <<1>>, true),
    ok = amqp10_client:send_msg(Sender, Msg),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_INVALID_FIELD,
              description = {utf8, <<"Publishing to exchange 'amq.direct' in vhost '/' "
                                     "failed since no routing key was provided">>}}}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive expected error")
    end,
    ok = amqp10_client:close_connection(Connection).

events(Config) ->
    ok = event_recorder:start(Config),
    open_and_close_connection(Config),
    [E0, E1, E2] = event_recorder:get_events(Config),
    ok = event_recorder:stop(Config),

    assert_event_type(user_authentication_success, E0),
    Protocol = {protocol, {'AMQP', {1, 0}}},
    assert_event_prop([{name, <<"guest">>},
                       {auth_mechanism, <<"PLAIN">>},
                       {ssl, false},
                       Protocol],
                      E0),

    assert_event_type(connection_created, E1),
    Node = get_node_config(Config, 0, nodename),
    assert_event_prop(
      [Protocol,
       {node, Node},
       {vhost, <<"/">>},
       {user, <<"guest">>},
       {type, network}],
      E1),
    Props = E1#event.props,
    Name = proplists:lookup(name, Props),
    Pid = proplists:lookup(pid, Props),
    ClientProperties = proplists:lookup(client_properties, Props),

    assert_event_type(connection_closed, E2),
    assert_event_prop(
      [{node, Node},
       Name,
       Pid,
       ClientProperties],
      E2).

sync_get_unsettled_classic_queue(Config) ->
    sync_get_unsettled(<<"classic">>, Config).

sync_get_unsettled_quorum_queue(Config) ->
    sync_get_unsettled(<<"quorum">>, Config).

sync_get_unsettled_stream(Config) ->
    sync_get_unsettled(<<"stream">>, Config).

%% Test synchronous get, figure 2.43 with sender settle mode unsettled.
sync_get_unsettled(QType, Config) ->
    SenderSettleMode = unsettled,
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 1 receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    %% Grant 1 credit to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),

    %% Since the queue has no messages yet, we shouldn't receive any message.
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail("received unexpected message ~p", [Unexp1])
    after 10 -> ok
    end,

    %% Let's send 4 messages to the queue.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag1">>, <<"m1">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag2">>, <<"m2">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag3">>, <<"m3">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag4">>, <<"m4">>, true)),

    %% Since we previously granted only 1 credit, we should get only the 1st message.
    M1 = receive {amqp10_msg, Receiver, Msg1} ->
                     ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),
                     Msg1
         after 5000 -> ct:fail("missing m1")
         end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 10 -> ok
    end,

    %% Synchronously get the 2nd message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    M2 = receive {amqp10_msg, Receiver, Msg2} ->
                     ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2)),
                     Msg2
         after 5000 -> ct:fail("missing m2")
         end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp3 -> ct:fail("received unexpected message ~p", [Unexp3])
    after 10 -> ok
    end,

    %% Accept the first 2 messages.
    ok = amqp10_client:accept_msg(Receiver, M1),
    ok = amqp10_client:accept_msg(Receiver, M2),
    %% Settlements should not top up credit. We are still out of credits.
    receive {amqp10_msg, _, _} = Unexp4 -> ct:fail("received unexpected message ~p", [Unexp4])
    after 10 -> ok
    end,

    %% Synchronously get the 3rd message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    receive {amqp10_msg, Receiver, Msg3} ->
                ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3))
    after 5000 -> ct:fail("missing m3")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp5 -> ct:fail("received unexpected message ~p", [Unexp5])
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

sync_get_unsettled_2_classic_queue(Config) ->
    sync_get_unsettled_2(<<"classic">>, Config).

sync_get_unsettled_2_quorum_queue(Config) ->
    sync_get_unsettled_2(<<"quorum">>, Config).

sync_get_unsettled_2_stream(Config) ->
    sync_get_unsettled_2(<<"stream">>, Config).

%% Synchronously get 2 messages from queue.
sync_get_unsettled_2(QType, Config) ->
    SenderSettleMode = unsettled,
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach a sender and a receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    %% Grant 2 credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),

    %% Let's send 5 messages to the queue.
    [ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true)) ||
     Bin <- [<<"m1">>, <<"m2">>, <<"m3">>, <<"m4">>, <<"m5">>]],

    %% We should receive exactly 2 messages.
    receive {amqp10_msg, Receiver, Msg1} -> ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1))
    after 5000 -> ct:fail("missing m1")
    end,
    receive {amqp10_msg, Receiver, Msg2} -> ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2))
    after 5000 -> ct:fail("missing m2")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail("received unexpected message ~p", [Unexp1])
    after 50 -> ok
    end,

    %% Grant 2 more credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),
    %% Again, we should receive exactly 2 messages.
    receive {amqp10_msg, Receiver, Msg3} -> ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3))
    after 5000 -> ct:fail("missing m3")
    end,
    receive {amqp10_msg, Receiver, Msg4} -> ?assertEqual([<<"m4">>], amqp10_msg:body(Msg4))
    after 5000 -> ct:fail("missing m4")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 50 -> ok
    end,

    %% Grant 2 more credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),

    %% We should receive the last (5th) message.
    receive {amqp10_msg, Receiver, Msg5} -> ?assertEqual([<<"m5">>], amqp10_msg:body(Msg5))
    after 5000 -> ct:fail("missing m5")
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

sync_get_settled_classic_queue(Config) ->
    sync_get_settled(<<"classic">>, Config).

sync_get_settled_quorum_queue(Config) ->
    sync_get_settled(<<"quorum">>, Config).

sync_get_settled_stream(Config) ->
    sync_get_settled(<<"stream">>, Config).

%% Test synchronous get, figure 2.43 with sender settle mode settled.
sync_get_settled(QType, Config) ->
    SenderSettleMode = settled,
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 1 receivers to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"my receiver">>, Address, SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    %% Grant 1 credit to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),

    %% Since the queue has no messages yet, we shouldn't receive any message.
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail("received unexpected message ~p", [Unexp1])
    after 10 -> ok
    end,

    %% Let's send 3 messages to the queue.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag1">>, <<"m1">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag2">>, <<"m2">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag3">>, <<"m3">>, true)),

    %% Since we previously granted only 1 credit, we should get only the 1st message.
    receive {amqp10_msg, Receiver, Msg1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1))
    after 5000 -> ct:fail("missing m1")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 10 -> ok
    end,

    %% Synchronously get the 2nd message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    receive {amqp10_msg, Receiver, Msg2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2))
    after 5000 -> ct:fail("missing m2")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp3 -> ct:fail("received unexpected message ~p", [Unexp3])
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

timed_get_classic_queue(Config) ->
    timed_get(<<"classic">>, Config).

timed_get_quorum_queue(Config) ->
    timed_get(<<"quorum">>, Config).

timed_get_stream(Config) ->
    timed_get(<<"stream">>, Config).

%% Synchronous get with a timeout, figure 2.44.
timed_get(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach a sender and a receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, false),

    Timeout = 10,
    receive Unexpected0 -> ct:fail("received unexpected ~p", [Unexpected0])
    after Timeout -> ok
    end,

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,

    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"my tag">>, <<"my msg">>, true)),

    %% Since our consumer didn't grant any new credit, we shouldn't receive the message we
    %% just sent.
    receive Unexpected1 -> ct:fail("received unexpected ~p", [Unexpected1])
    after 50 -> ok
    end,

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),
    receive {amqp10_msg, Receiver, Msg1} -> ?assertEqual([<<"my msg">>], amqp10_msg:body(Msg1))
    after 5000 -> ct:fail("missing 'my msg'")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

stop_classic_queue(Config) ->
    stop(<<"classic">>, Config).

stop_quorum_queue(Config) ->
    stop(<<"quorum">>, Config).

stop_stream(Config) ->
    stop(<<"stream">>, Config).

%% Test stopping a link, figure 2.46.
stop(QType, Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    %% Attach 1 sender and 1 receiver to the queue.
    OpnConf0 = connection_config(Config),
    NumSent = 300,
    %% Allow in flight messages to be received after stopping the link.
    OpnConf = OpnConf0#{transfer_limit_margin => -NumSent},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, settled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    ok = amqp10_client:flow_link_credit(Receiver, 10, 5),

    %% Send 300 messages.
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true))
     end || N <- lists:seq(1, NumSent)],

    %% Let's await the first 20 messages.
    NumReceived = 20,
    Msgs = receive_messages(Receiver, NumReceived),

    %% Stop the link.
    %% "Stopping the transfers on a given link is accomplished by updating
    %% the link-credit to be zero and sending the updated flow state." [2.6.10]
    ok = amqp10_client:stop_receiver_link(Receiver),
    %% "It is possible that some transfers could be in flight at the time the flow
    %% state is sent, so incoming transfers could still arrive on the link." [2.6.10]
    NumInFlight = count_received_messages(Receiver),

    ct:pal("After receiving the first ~b messages and stopping the link, "
           "we received ~b more in flight messages", [NumReceived, NumInFlight]),
    ?assert(NumInFlight > 0,
            "expected some in flight messages, but there were actually none"),
    ?assert(NumInFlight < NumSent - NumReceived,
            "expected the link to stop, but actually received all messages"),

    %% Check that contents of the first 20 messages are correct.
    FirstMsg = hd(Msgs),
    LastMsg = lists:last(Msgs),
    ?assertEqual([<<"1">>], amqp10_msg:body(FirstMsg)),
    ?assertEqual([integer_to_binary(NumReceived)], amqp10_msg:body(LastMsg)),

    %% Let's resume the link.
    ok = amqp10_client:flow_link_credit(Receiver, 50, 40),

    %% We expect to receive all remaining messages.
    NumRemaining = NumSent - NumReceived - NumInFlight,
    ct:pal("Waiting for the remaining ~b messages", [NumRemaining]),
    Msgs1 = receive_messages(Receiver, NumRemaining),
    ?assertEqual([integer_to_binary(NumSent)], amqp10_msg:body(lists:last(Msgs1))),

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = amqp10_client:close_connection(Connection).

single_active_consumer_classic_queue(Config) ->
    single_active_consumer(<<"classic">>, Config).

single_active_consumer_quorum_queue(Config) ->
    single_active_consumer(<<"quorum">>, Config).

single_active_consumer(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-single-active-consumer">>, bool, true},
                                                  {<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    %% Attach 1 sender and 2 receivers to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),

    %% The 1st consumer will become active.
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session,
                        <<"test-receiver-1">>,
                        Address,
                        unsettled),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    ok = amqp10_client:flow_link_credit(Receiver1, 2, never),

    %% The 2nd consumer will become inactive.
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session,
                        <<"test-receiver-2">>,
                        Address,
                        unsettled),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    ok = amqp10_client:flow_link_credit(Receiver2, 2, never),

    NumMsgs = 5,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true))
     end || N <- lists:seq(1, NumMsgs)],

    %% Only the active consumer should receive messages.
    receive {amqp10_msg, Receiver1, Msg1} -> ?assertEqual([<<"1">>], amqp10_msg:body(Msg1))
    after 5000 -> ct:fail("missing 1st msg")
    end,
    receive {amqp10_msg, Receiver1, Msg2} -> ?assertEqual([<<"2">>], amqp10_msg:body(Msg2))
    after 5000 -> ct:fail("missing 2nd msg")
    end,
    receive {amqp10_event, {link, Receiver1, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive Unexpected0 -> ct:fail("received unexpected ~p", [Unexpected0])
    after 10 -> ok
    end,

    %% Cancelling the active consumer should cause the inactive to become active.
    ok = amqp10_client:detach_link(Receiver1),
    receive {amqp10_event, {link, Receiver1, {detached, normal}}} -> ok
    after 5000 -> ct:fail("missing detached")
    end,
    receive {amqp10_msg, Receiver2, Msg3} -> ?assertEqual([<<"3">>], amqp10_msg:body(Msg3))
    after 5000 -> ct:fail("missing 3rd msg")
    end,
    receive {amqp10_msg, Receiver2, Msg4} -> ?assertEqual([<<"4">>], amqp10_msg:body(Msg4))
    after 5000 -> ct:fail("missing 4th msg")
    end,
    receive {amqp10_event, {link, Receiver2, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive Unexpected1 -> ct:fail("received unexpected ~p", [Unexpected1])
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Receiver2),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    delete_queue(Config, QName).

auth_attempt_metrics(Config) ->
    open_and_close_connection(Config),
    [Attempt1] = rpc(Config, rabbit_core_metrics, get_auth_attempts, []),
    ?assertEqual(false, proplists:is_defined(remote_address, Attempt1)),
    ?assertEqual(false, proplists:is_defined(username, Attempt1)),
    ?assertEqual(<<"amqp10">>, proplists:get_value(protocol, Attempt1)),
    ?assertEqual(1, proplists:get_value(auth_attempts, Attempt1)),
    ?assertEqual(0, proplists:get_value(auth_attempts_failed, Attempt1)),
    ?assertEqual(1, proplists:get_value(auth_attempts_succeeded, Attempt1)),

    rpc(Config, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    ok = rpc(Config, application, set_env, [rabbit, track_auth_attempt_source, true]),
    open_and_close_connection(Config),
    Attempts = rpc(Config, rabbit_core_metrics, get_auth_attempts_by_source, []),
    [Attempt2] = lists:filter(fun(Props) ->
                                      proplists:is_defined(remote_address, Props)
                              end, Attempts),
    ?assertEqual(<<>>, proplists:get_value(remote_address, Attempt2)),
    ?assertEqual(<<"guest">>, proplists:get_value(username, Attempt2)),
    ?assertEqual(<<"amqp10">>, proplists:get_value(protocol, Attempt2)),
    ?assertEqual(1, proplists:get_value(auth_attempts, Attempt2)),
    ?assertEqual(0, proplists:get_value(auth_attempts_failed, Attempt2)),
    ?assertEqual(1, proplists:get_value(auth_attempts_succeeded, Attempt2)).

last_queue_confirms(Config) ->
    ClassicQ = <<"my classic queue">>,
    QuorumQ = <<"my quorum queue">>,
    Qs = [ClassicQ, QuorumQ],
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = ClassicQ}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QuorumQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                  {<<"x-quorum-initial-group-size">>, long, 3}
                                                 ]}),
    [#'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                              exchange = <<"amq.fanout">>})
     || QName <- Qs],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    AddressFanout = <<"/exchange/amq.fanout/ignored">>,
    {ok, SenderFanout} = amqp10_client:attach_sender_link(
                           Session, <<"sender-1">>, AddressFanout, unsettled),
    ok = wait_for_credit(SenderFanout),

    AddressClassicQ = <<"/amq/queue/", ClassicQ/binary>>,
    {ok, SenderClassicQ} = amqp10_client:attach_sender_link(
                             Session, <<"sender-2">>, AddressClassicQ, unsettled),
    ok = wait_for_credit(SenderClassicQ),

    DTag1 = <<"t1">>,
    ok = amqp10_client:send_msg(SenderFanout, amqp10_msg:new(DTag1, <<"m1">>, false)),
    receive {amqp10_disposition, {accepted, DTag1}} -> ok
    after 5000 -> ct:fail({missing_accepted, DTag1})
    end,

    %% Make quorum queue unavailable.
    ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 1),

    DTag2 = <<"t2">>,
    DTag3 = <<"t3">>,
    ok = amqp10_client:send_msg(SenderFanout, amqp10_msg:new(DTag2, <<"m2">>, false)),
    ok = amqp10_client:send_msg(SenderClassicQ, amqp10_msg:new(DTag3, <<"m3">>, false)),

    %% Since quorum queue is down, we should only get a confirmation for m3.
    receive {amqp10_disposition, {accepted, DTag3}} -> ok
    after 5000 -> ct:fail({missing_accepted, DTag3})
    end,
    receive {amqp10_disposition, Unexpected} -> ct:fail({unexpected_disposition, Unexpected})
    after 200 -> ok
    end,

    ok = rabbit_ct_broker_helpers:start_node(Config, 1),
    ok = rabbit_ct_broker_helpers:start_node(Config, 2),
    %% Since the quorum queue has become available, we should now get a confirmation for m2.
    receive {amqp10_disposition, {accepted, DTag2}} -> ok
    after 10_000 -> ct:fail({missing_accepted, DTag2})
    end,

    ok = amqp10_client:detach_link(SenderClassicQ),
    ok = amqp10_client:detach_link(SenderFanout),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = 3},
                 amqp_channel:call(Ch, #'queue.delete'{queue = ClassicQ})),
    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QuorumQ})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

target_queue_deleted(Config) ->
    ClassicQ = <<"my classic queue">>,
    QuorumQ = <<"my quorum queue">>,
    Qs = [ClassicQ, QuorumQ],
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = ClassicQ}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QuorumQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                  {<<"x-quorum-initial-group-size">>, long, 3}
                                                 ]}),
    [#'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                              exchange = <<"amq.fanout">>})
     || QName <- Qs],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    Address = <<"/exchange/amq.fanout/ignored">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    DTag1 = <<"t1">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    receive {amqp10_disposition, {accepted, DTag1}} -> ok
    after 5000 -> ct:fail({missing_accepted, DTag1})
    end,

    %% Make quorum queue unavailable.
    ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 1),

    flush("quorum queue is down"),
    DTag2 = <<"t2">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, false)),
    %% Target classic queue should receive m2.
    assert_messages(ClassicQ, 2, 0, Config),
    %% Delete target classic queue.
    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = ClassicQ})),

    %% Since quorum queue is down, we should still receive no DISPOSITION.
    receive {amqp10_disposition, Unexpected} -> ct:fail({unexpected_disposition, Unexpected})
    after 100 -> ok
    end,

    ok = rabbit_ct_broker_helpers:start_node(Config, 1),
    ok = rabbit_ct_broker_helpers:start_node(Config, 2),
    %% Since the quorum queue has become available, we should now get a confirmation for m2.
    receive {amqp10_disposition, {accepted, DTag2}} -> ok
    after 10_000 -> ct:fail({missing_accepted, DTag2})
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QuorumQ})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

%% This test is mostly interesting in mixed version mode with feature flag
%% consumer_tag_in_credit_reply disabled.
credit_reply_quorum_queue(Config) ->
    %% Place quorum queue leader on the old version node.
    OldVersionNode = 1,
    Ch = rabbit_ct_client_helpers:open_channel(Config, OldVersionNode),
    QName = atom_to_binary(?FUNCTION_NAME),
    #'queue.declare_ok'{} =  amqp_channel:call(
                               Ch, #'queue.declare'{
                                      queue = QName,
                                      durable = true,
                                      arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                   {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}]}),
    %% Connect to the new node.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    NumMsgs = 10,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true))
     end || N <- lists:seq(1, NumMsgs)],

    %% Grant credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, NumMsgs, never),

    %% We should receive all messages.
    Msgs = receive_messages(Receiver, NumMsgs),
    FirstMsg = hd(Msgs),
    LastMsg = lists:last(Msgs),
    ?assertEqual([<<"1">>], amqp10_msg:body(FirstMsg)),
    ?assertEqual([integer_to_binary(NumMsgs)], amqp10_msg:body(LastMsg)),

    ExpectedReadyMsgs = 0,
    ?assertEqual(#'queue.delete_ok'{message_count = ExpectedReadyMsgs},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = amqp10_client:close_connection(Connection).

async_notify_settled_classic_queue(Config) ->
    async_notify(settled, <<"classic">>, Config).

async_notify_settled_quorum_queue(Config) ->
    async_notify(settled, <<"quorum">>, Config).

async_notify_settled_stream(Config) ->
    async_notify(settled, <<"stream">>, Config).

async_notify_unsettled_classic_queue(Config) ->
    async_notify(unsettled, <<"classic">>, Config).

async_notify_unsettled_quorum_queue(Config) ->
    async_notify(unsettled, <<"quorum">>, Config).

async_notify_unsettled_stream(Config) ->
    async_notify(unsettled, <<"stream">>, Config).

%% Test asynchronous notification, figure 2.45.
async_notify(SenderSettleMode, QType, Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 1),
    QName = atom_to_binary(?FUNCTION_NAME),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% Send 30 messages to the queue.
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    NumMsgs = 30,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, false))
     end || N <- lists:seq(1, NumMsgs)],
    %% Wait for last message to be confirmed.
    ok = wait_for_settlement(integer_to_binary(NumMsgs)),
    ok = amqp10_client:detach_link(Sender),
    receive {amqp10_event, {link, Sender, {detached, normal}}} -> ok
    after 5000 -> ct:fail("missing detached")
    end,

    Filter = consume_from_first(QType),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address,
                       SenderSettleMode, configuration, Filter),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,

    %% Initially, grant 10 credits to the sending queue.
    %% Whenever credits drops below 5, renew back to 10.
    ok = amqp10_client:flow_link_credit(Receiver, 10, 5),

    %% We should receive all messages.
    Msgs = receive_messages(Receiver, NumMsgs),
    FirstMsg = hd(Msgs),
    LastMsg = lists:last(Msgs),
    ?assertEqual([<<"1">>], amqp10_msg:body(FirstMsg)),
    ?assertEqual([integer_to_binary(NumMsgs)], amqp10_msg:body(LastMsg)),

    case SenderSettleMode of
        settled ->
            ok;
        unsettled ->
            ok = amqp10_client_session:disposition(
                   Session,
                   receiver,
                   amqp10_msg:delivery_id(FirstMsg),
                   amqp10_msg:delivery_id(LastMsg),
                   true,
                   accepted)
    end,

    %% No further messages should be delivered.
    receive Unexpected -> ct:fail({received_unexpected_message, Unexpected})
    after 50 -> ok
    end,

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = amqp10_client:close_connection(Connection).

%% internal
%%

connection_config(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>}}.

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~ts flushed: ~p~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

open_and_close_connection(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> ct:fail(opened_timeout)
    end,

    ok = amqp10_client:close_connection(Connection),
    receive {amqp10_event, {connection, Connection, {closed, normal}}} -> ok
    after 5000 -> ct:fail(closed_timeout)
    end.

% before we can send messages we have to wait for credit from the server
wait_for_credit(Sender) ->
    receive
        {amqp10_event, {link, Sender, credited}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_credit timed out"),
              ct:fail(credited_timeout)
    end.

end_session_sync(Session) ->
    ok = amqp10_client:end_session(Session),
    ok = wait_for_session_end(Session).

wait_for_session_end(Session) ->
    receive
        {amqp10_event, {session, Session, {ended, _}}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_session_end timed out"),
              ct:fail({session_end_timeout, Session})
    end.

wait_for_settlement(Tag) ->
    wait_for_settlement(Tag, accepted).

wait_for_settlement(Tag, State) ->
    receive
        {amqp10_disposition, {State, Tag}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_settlement timed out"),
              ct:fail(settled_timeout)
    end.

wait_for_accepts(0) -> ok;
wait_for_accepts(N) ->
    receive
        {amqp10_disposition,{accepted,_}} ->
            wait_for_accepts(N -1)
    after 250 ->
              ok
    end.

delete_queue(Config, QName) -> 
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

amqp091_get_msg_headers(Channel, QName) -> 
    {#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{ headers= Headers}}}
        = amqp_channel:call(Channel, #'basic.get'{queue = QName, no_ack = true}),
    {ok, Headers}.

create_amqp10_sender(Session, Address) -> 
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),
    {ok, Sender}.

drain_queue(Session, Address, N) ->
    flush("Before drain_queue"),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       settled,
                       configuration),
    ok = amqp10_client:flow_link_credit(Receiver, 1000, never, true),
    Msgs = receive_messages(Receiver, N),
    flush("after drain"),
    ok = amqp10_client:detach_link(Receiver),
    {ok, Msgs}.

receive_messages(Receiver, N) ->
    receive_messages0(Receiver, N, []).

receive_messages0(_Receiver, 0, Acc) ->
    lists:reverse(Acc);
receive_messages0(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} -> 
            receive_messages0(Receiver, N - 1, [Msg | Acc])
    after 5000  ->
              exit({timeout, {num_received, length(Acc)}, {num_missing, N}})
    end.

count_received_messages(Receiver) ->
    count_received_messages0(Receiver, 0).

count_received_messages0(Receiver, Count) ->
    receive
        {amqp10_msg, Receiver, _Msg} ->
            count_received_messages0(Receiver, Count + 1)
    after 200 ->
              Count
    end.

send_messages(Sender, N, GroupId) ->
    [begin
         Bin = integer_to_binary(I),
         Msg0 = amqp10_msg:new(Bin, Bin, true),
         Props = #{group_id => GroupId},
         Msg = amqp10_msg:set_properties(Props, Msg0),
         ok = amqp10_client:send_msg(Sender, Msg)
     end || I <- lists:seq(1, N)].

assert_messages(QNameBin, NumTotalMsgs, NumUnackedMsgs, Config) ->
    Vhost = ?config(rmq_vhost, Config),
    eventually(
      ?_assertEqual(
         lists:sort([{messages, NumTotalMsgs}, {messages_unacknowledged, NumUnackedMsgs}]),
         begin
             {ok, Q} = rpc(Config, rabbit_amqqueue, lookup, [QNameBin, Vhost]),
             Infos = rpc(Config, rabbit_amqqueue, info, [Q, [messages, messages_unacknowledged]]),
             lists:sort(Infos)
         end
        ), 500, 5).

serial_number_increment(S) ->
    case S + 1 of
        16#ffffffff + 1 -> 0;
        S1 -> S1
    end.

consume_from_first(<<"stream">>) ->
    #{<<"rabbitmq:stream-offset-spec">> => <<"first">>};
consume_from_first(_) ->
    #{}.
