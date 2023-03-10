%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt).

-behaviour(application).

-include("rabbit_mqtt.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([start/2, stop/1]).
-export([emit_connection_info_all/4,
         emit_connection_info_local/3,
         close_local_client_connections/1,
         %% Exported for tests, but could also be used for debugging.
         local_connection_pids/0]).

start(normal, []) ->
    init_global_counters(),
    persist_static_configuration(),
    {ok, Listeners} = application:get_env(tcp_listeners),
    {ok, SslListeners} = application:get_env(ssl_listeners),
    mqtt_machine:maybe_delete(),
    Result = rabbit_mqtt_sup:start_link({Listeners, SslListeners}, []),
    EMPid = case rabbit_event:start_link() of
                {ok, Pid}                       -> Pid;
                {error, {already_started, Pid}} -> Pid
            end,
    gen_event:add_handler(EMPid, rabbit_mqtt_internal_event_handler, []),
    Result.

stop(_) ->
    rabbit_mqtt_sup:stop_listeners().

-spec emit_connection_info_all([node()], rabbit_types:info_keys(), reference(), pid()) -> term().
emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [spawn_link(Node, ?MODULE, emit_connection_info_local,
                       [Items, Ref, AggregatorPid])
            || Node <- Nodes],
    rabbit_control_misc:await_emitters_termination(Pids).

-spec emit_connection_info_local(rabbit_types:info_keys(), reference(), pid()) -> ok.
emit_connection_info_local(Items, Ref, AggregatorPid) ->
    LocalPids = local_connection_pids(),
    emit_connection_info(Items, Ref, AggregatorPid, LocalPids).

emit_connection_info(Items, Ref, AggregatorPid, Pids) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref,
      fun(Pid) ->
              rabbit_mqtt_reader:info(Pid, Items)
      end, Pids).

-spec close_local_client_connections(string() | binary()) -> {'ok', non_neg_integer()}.
close_local_client_connections(Reason) ->
    Pids = local_connection_pids(),
    lists:foreach(fun(Pid) ->
                          rabbit_mqtt_reader:close_connection(Pid, Reason)
                  end, Pids),
    {ok, length(Pids)}.

-spec local_connection_pids() -> [pid()].
local_connection_pids() ->
    PgScope = persistent_term:get(?PG_SCOPE),
    lists:flatmap(fun(Group) ->
                          pg:get_local_members(PgScope, Group)
                  end, pg:which_groups(PgScope)).

init_global_counters() ->
    init_global_counters(?MQTT_PROTO_V3),
    init_global_counters(?MQTT_PROTO_V4).

init_global_counters(ProtoVer) ->
    Proto = {protocol, ProtoVer},
    rabbit_global_counters:init([Proto]),
    rabbit_global_counters:init([Proto, {queue_type, ?QUEUE_TYPE_QOS_0}]),
    rabbit_global_counters:init([Proto, {queue_type, rabbit_classic_queue}]),
    rabbit_global_counters:init([Proto, {queue_type, rabbit_quorum_queue}]).

persist_static_configuration() ->
    rabbit_mqtt_util:init_sparkplug(),

    {ok, MailboxSoftLimit} = application:get_env(?APP_NAME, mailbox_soft_limit),
    ?assert(is_integer(MailboxSoftLimit)),
    ok = persistent_term:put(?PERSISTENT_TERM_MAILBOX_SOFT_LIMIT, MailboxSoftLimit).
