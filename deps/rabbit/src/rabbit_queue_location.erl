%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_location).

-include("amqqueue.hrl").

-export([select_leader_and_followers/2]).

select_leader_and_followers(Q, Size)
  when (?amqqueue_is_quorum(Q) orelse ?amqqueue_is_stream(Q)) andalso is_integer(Size) ->
    {AllNodes, _DiscNodes, RunningNodes} = rabbit_mnesia:cluster_nodes(status),
    GetQueueNames0 = fun() -> rabbit_amqqueue:list_names() end,
    QueueType = amqqueue:get_type(Q),
    {Replicas, GetQueueNames} = select_replicas(Size, AllNodes, RunningNodes, GetQueueNames0, QueueType),
    LeaderLocator = leader_locator(
                      rabbit_queue_type_util:args_policy_lookup(
                        <<"queue-leader-locator">>,
                        fun (PolVal, _ArgVal) ->
                                PolVal
                        end, Q)),
    Leader = leader_node(LeaderLocator, Replicas, RunningNodes, GetQueueNames, QueueType),
    Followers = lists:delete(Leader, Replicas),
    {Leader, Followers}.

select_replicas(Size, AllNodes, _, Fun, _)
  when length(AllNodes) =< Size ->
    {AllNodes, Fun};
select_replicas(Size, _, RunningNodes, Fun, _)
  when length(RunningNodes) =:= Size ->
    {RunningNodes, Fun};
select_replicas(Size, AllNodes, RunningNodes, GetQueueNames, QueueType) ->
    %% Select nodes in the following order:
    %% 1. local node (to have data locality for declaring client)
    %% 2. running nodes
    %% 3. nodes with least replicas (to have a "balanced" RabbitMQ cluster).
    Local = node(),
    true = lists:member(Local, AllNodes),
    true = lists:member(Local, RunningNodes),
    Counters0 = maps:from_list([{Node, 0} || Node <- lists:delete(Local, AllNodes)]),
    QueueNames = GetQueueNames(),
    Counters = lists:foldl(fun(QueueResource, Acc) ->
                                   case rabbit_amqqueue:lookup(QueueResource) of
                                       {ok, Q}
                                         when ?is_amqqueue_v2(Q) andalso
                                              ?amqqueue_v2_field_type(Q) =:= QueueType ->
                                           #{nodes := Nodes} = amqqueue:get_type_state(Q),
                                           lists:foldl(fun(N, A)
                                                             when is_map_key(N, A) ->
                                                               maps:update_with(N, fun(C) -> C+1 end, A);
                                                          (_, A) ->
                                                               A
                                                       end, Acc, Nodes);
                                       _ ->
                                           Acc
                                   end
                           end, Counters0, QueueNames),
    L0 = maps:to_list(Counters),
    L1 = lists:sort(fun({N0, C0}, {N1, C1}) ->
                            case {lists:member(N0, RunningNodes),
                                  lists:member(N1, RunningNodes)} of
                                {true, false} ->
                                    true;
                                {false, true} ->
                                    false;
                                _ ->
                                    C0 =< C1
                            end
                    end, L0),
    {L2, _} = lists:split(Size - 1, L1),
    L = lists:map(fun({N, _}) -> N end, L2),
    {[Local | L], fun() -> QueueNames end}.

leader_locator(undefined) -> <<"client-local">>;
leader_locator(Val) -> Val.

leader_node(<<"client-local">>, _, _, _, _) ->
    node();
leader_node(<<"random">>, Nodes0, RunningNodes, _, _) ->
    Nodes = potential_leaders(Nodes0, RunningNodes),
    lists:nth(rand:uniform(length(Nodes)), Nodes);
leader_node(<<"least-leaders">>, Nodes0, RunningNodes, GetQueueNames, QueueType)
  when is_function(GetQueueNames, 0) ->
    Nodes = potential_leaders(Nodes0, RunningNodes),
    Counters0 = maps:from_list([{N, 0} || N <- Nodes]),
    Counters = lists:foldl(fun(QueueResource, Acc) ->
                                   case rabbit_amqqueue:lookup(QueueResource) of
                                       {ok, Q}
                                         when ?is_amqqueue_v2(Q) andalso
                                              ?amqqueue_v2_field_type(Q) =:= QueueType ->
                                           case amqqueue:get_pid(Q) of
                                               {RaName, LeaderNode}
                                                 when ?amqqueue_v2_field_type(Q) =:= rabbit_quorum_queue,
                                                      is_atom(RaName), is_atom(LeaderNode),
                                                      is_map_key(LeaderNode, Acc) ->
                                                   maps:update_with(LeaderNode, fun(C) -> C+1 end, Acc);
                                               StreamLeaderPid
                                                 when ?amqqueue_v2_field_type(Q) =:= rabbit_stream_queue,
                                                      is_pid(StreamLeaderPid),
                                                      is_map_key(node(StreamLeaderPid), Acc) ->
                                                   maps:update_with(node(StreamLeaderPid), fun(C) -> C+1 end, Acc);
                                               _ ->
                                                   Acc
                                           end;
                                       _ ->
                                           Acc
                                   end
                           end, Counters0, GetQueueNames()),
    {Node, _} = hd(lists:keysort(2, maps:to_list(Counters))),
    Node.

potential_leaders(Nodes, AllRunningNodes) ->
    RunningNodes = lists:filter(fun(N) ->
                                        lists:member(N, AllRunningNodes)
                                end, Nodes),
    case rabbit_maintenance:filter_out_drained_nodes_local_read(RunningNodes) of
        [] ->
            %% All running nodes are drained. Let's place the leader on a drained node
            %% respecting the requested queue-leader-locator streategy.
            RunningNodes;
        Filtered ->
            Filtered
    end.
