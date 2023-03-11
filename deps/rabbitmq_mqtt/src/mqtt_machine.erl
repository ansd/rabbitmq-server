%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% Up to RabbitMQ 3.11 MQTT client ID tracking was done with Ra. Starting in
%% RabbitMQ 3.12 MQTT client ID tracking is done with pg. This module only
%% exists to allow upgrading from 3.11 to delete the node's Ra membership in
%% the new version. Once direct upgrades from 3.11 to this version become
%% disallowed, we can delete this module.

-module(mqtt_machine).
-behaviour(ra_machine).

-include("mqtt_machine.hrl").
-include_lib("kernel/include/logger.hrl").

%% client API
-export([ensure_deleted/0,
         notify_connection/2
        ]).

%% ra_machine callbacks
-export([version/0,
         which_module/1,
         init/1,
         apply/3,
         state_enter/2,
         overview/1]).

-define(RA_SYSTEM, coordination).
-define(RA_NAME, mqtt_node).
-define(RA_SERVER_ID, {?RA_NAME, node()}).
-define(TIMEOUT, 60_000).

-type state() :: #machine_state{}.

-type config() :: map().

-type reply() :: {ok, term()} | {error, term()}.
-type client_id() :: term().

-type command() :: {register, client_id(), pid()} |
                   {unregister, client_id(), pid()} |
                   list.
version() -> 1.

which_module(1) -> ?MODULE;
which_module(0) -> mqtt_machine_v0.

-spec init(config()) -> state().
init(_Conf) ->
    #machine_state{}.

-spec apply(map(), command(), state()) ->
    {state(), reply(), ra_machine:effects()}.
apply(_Meta, {register, ClientId, Pid},
      #machine_state{client_ids = Ids,
                     pids = Pids0} = State0) ->
    {Effects, Ids1, Pids} =
        case maps:find(ClientId, Ids) of
            {ok, OldPid} when Pid =/= OldPid ->
                Effects0 = [{demonitor, process, OldPid},
                            {monitor, process, Pid},
                            {mod_call, ?MODULE, notify_connection,
                             [OldPid, duplicate_id]}],
                Pids2 = case maps:take(OldPid, Pids0) of
                            error ->
                                Pids0;
                            {[ClientId], Pids1} ->
                                Pids1;
                            {ClientIds, Pids1} ->
                                Pids1#{ClientId => lists:delete(ClientId, ClientIds)}
                        end,
                Pids3 = maps:update_with(Pid, fun(CIds) -> [ClientId | CIds] end,
                                         [ClientId], Pids2),
                {Effects0, maps:remove(ClientId, Ids), Pids3};

            {ok, Pid}  ->
                {[], Ids, Pids0};
            error ->
                Pids1 = maps:update_with(Pid, fun(CIds) -> [ClientId | CIds] end,
                                         [ClientId], Pids0),
                Effects0 = [{monitor, process, Pid}],
                {Effects0, Ids, Pids1}
        end,
    State = State0#machine_state{client_ids = maps:put(ClientId, Pid, Ids1),
                                 pids = Pids},
    {State, ok, Effects};

apply(Meta, {unregister, ClientId, Pid}, #machine_state{client_ids = Ids,
                                                        pids = Pids0} = State0) ->
    State = case maps:find(ClientId, Ids) of
                {ok, Pid} ->
                    Pids = case maps:get(Pid, Pids0, undefined) of
                               undefined ->
                                   Pids0;
                               [ClientId] ->
                                   maps:remove(Pid, Pids0);
                               Cids ->
                                   Pids0#{Pid => lists:delete(ClientId, Cids)}
                           end,

                    State0#machine_state{client_ids = maps:remove(ClientId, Ids),
                                         pids = Pids};
                %% don't delete client id that might belong to a newer connection
                %% that kicked the one with Pid out
                {ok, _AnotherPid} ->
                    State0;
                error ->
                    State0
            end,
    Effects0 = [{demonitor, process, Pid}],
    %% snapshot only when the map has changed
    Effects = case State of
      State0 -> Effects0;
      _      -> Effects0 ++ snapshot_effects(Meta, State)
    end,
    {State, ok, Effects};

apply(_Meta, {down, DownPid, noconnection}, State) ->
    %% Monitor the node the pid is on (see {nodeup, Node} below)
    %% so that we can detect when the node is re-connected and discover the
    %% actual fate of the connection processes on it
    Effect = {monitor, node, node(DownPid)},
    {State, ok, Effect};

apply(Meta, {down, DownPid, _}, #machine_state{client_ids = Ids,
                                               pids = Pids0} = State0) ->
    case maps:get(DownPid, Pids0, undefined) of
        undefined ->
            {State0, ok, []};
        ClientIds ->
            Ids1 = maps:without(ClientIds, Ids),
            State = State0#machine_state{client_ids = Ids1,
                                         pids = maps:remove(DownPid, Pids0)},
            Effects = lists:map(fun(Id) ->
                                        [{mod_call, rabbit_log, debug,
                                          ["MQTT connection with client id '~ts' failed", [Id]]}]
                                end, ClientIds),
            {State, ok, Effects ++ snapshot_effects(Meta, State)}
    end;

apply(_Meta, {nodeup, Node}, State) ->
    %% Work out if any pids that were disconnected are still
    %% alive.
    %% Re-request the monitor for the pids on the now-back node.
    Effects = [{monitor, process, Pid} || Pid <- all_pids(State), node(Pid) == Node],
    {State, ok, Effects};
apply(_Meta, {nodedown, _Node}, State) ->
    {State, ok};

apply(Meta, {leave, Node}, #machine_state{client_ids = Ids,
                                          pids = Pids0} = State0) ->
    {Keep, Remove} = maps:fold(
                       fun (ClientId, Pid, {In, Out}) ->
                               case node(Pid) =/= Node of
                                   true ->
                                       {In#{ClientId => Pid}, Out};
                                   false ->
                                       {In, Out#{ClientId => Pid}}
                               end
                       end, {#{}, #{}}, Ids),
    Effects = maps:fold(fun (ClientId, _Pid, Acc) ->
                                Pid = maps:get(ClientId, Ids),
                                [
                                 {demonitor, process, Pid},
                                 {mod_call, ?MODULE, notify_connection, [Pid, decommission_node]},
                                 {mod_call, rabbit_log, debug,
                                  ["MQTT will remove client ID '~ts' from known "
                                   "as its node has been decommissioned", [ClientId]]}
                                ]  ++ Acc
                        end, [], Remove),

    State = State0#machine_state{client_ids = Keep,
                                 pids = maps:without(maps:values(Remove), Pids0)},
    {State, ok, Effects ++ snapshot_effects(Meta, State)};
apply(_Meta, {machine_version, 0, 1}, {machine_state, Ids}) ->
    Pids = maps:fold(
             fun(Id, Pid, Acc) ->
                     maps:update_with(Pid,
                                      fun(CIds) -> [Id | CIds] end,
                                      [Id], Acc)
             end, #{}, Ids),
    {#machine_state{client_ids = Ids,
                    pids = Pids}, ok, []};
apply(_Meta, Unknown, State) ->
    logger:error("MQTT Raft state machine v1 received unknown command ~tp", [Unknown]),
    {State, {error, {unknown_command, Unknown}}, []}.

-spec state_enter(ra_server:ra_state() | eol, state()) ->
    ra_machine:effects().
state_enter(leader, State) ->
    %% re-request monitors for all known pids, this would clean up
    %% records for all connections are no longer around, e.g. right after node restart
    [{monitor, process, Pid} || Pid <- all_pids(State)];
state_enter(_, _) ->
    [].

-spec overview(state()) -> map().
overview(#machine_state{client_ids = ClientIds,
                        pids = Pids}) ->
    #{num_client_ids => maps:size(ClientIds),
      num_pids => maps:size(Pids)}.

%% ==========================

%% Avoids blocking the Raft leader.
-spec notify_connection(pid(), duplicate_id | decommission_node) -> pid().
notify_connection(Pid, Reason) ->
  spawn(fun() -> gen_server2:cast(Pid, Reason) end).

-spec snapshot_effects(map(), state()) -> ra_machine:effects().
snapshot_effects(#{index := RaftIdx}, State) ->
    [{release_cursor, RaftIdx, State}].

all_pids(#machine_state{client_ids = Ids}) ->
    maps:values(Ids).

ensure_deleted() ->
    case ra_directory:uid_of(?RA_SYSTEM, ?RA_NAME) of
        undefined ->
            ok;
        Uid ->
            %% This is an upgrade from a RabbitMQ version < 3.12
            ?LOG_INFO("Detected Ra directory ~p for Ra cluster '~s' in Ra system '~s'",
                      [Uid, ?RA_NAME, ?RA_SYSTEM]),
            try delete() of
                ok ->
                    ok;
                _Error ->
                    force_delete()
            catch Class:Reason ->
                      ?LOG_WARNING("Failed to delete Ra member ~p: ~p ~p",
                                   [?RA_SERVER_ID, Class, Reason]),
                      force_delete()
            end
    end.

delete() ->
    RaServerId = ?RA_SERVER_ID,
    case ra:restart_server(?RA_SYSTEM, RaServerId) of
        ok ->
            case ra:members(RaServerId, ?TIMEOUT) of
                {ok, Members = [RaServerId], _Leader} ->
                    case ra:delete_cluster(Members, ?TIMEOUT) of
                        {ok, _} ->
                            ?LOG_INFO("Successfully deleted Ra cluster '~s'",
                                      [?RA_NAME]),
                            ok;
                        {error, Reason} = Error ->
                            ?LOG_WARNING("Failed to delete Ra cluster '~s' in Ra system '~s': ~p",
                                         [?RA_NAME, ?RA_SYSTEM, Reason]),
                            Error
                    end;
                {ok, Members, Leader} ->
                    ?LOG_INFO("Members for Ra cluster '~s': ~p leader: ~p",
                              [?RA_NAME, Members, Leader]),
                    case ra:leave_and_delete_server(?RA_SYSTEM, Leader, RaServerId, ?TIMEOUT) of
                        ok ->
                            ?LOG_INFO("Successfully deleted Ra member ~p from Ra cluster '~s'",
                                      [RaServerId, ?RA_NAME]),
                            ok;
                        Error ->
                            ?LOG_WARNING("Failed to delete Ra member ~p from Ra cluster '~s' in Ra system '~s': ~p",
                                         [RaServerId, ?RA_NAME, ?RA_SYSTEM, Error]),
                            Error
                    end;
                Error ->
                    ?LOG_WARNING("Failed to list members for Ra cluster '~s' in Ra system '~s': ~p",
                                 [?RA_NAME, ?RA_SYSTEM, Error]),
                    Error
            end;
        {error, Reason} = Error ->
            ?LOG_WARNING("Failed to restart Ra server ~p: ~p",
                         [RaServerId, Reason]),
            Error
    end.

force_delete() ->
    case ra:force_delete_server(?RA_SYSTEM, ?RA_SERVER_ID) of
        ok ->
            ?LOG_INFO("Successfully force deleted Ra member ~p from Ra cluster '~s'",
                      [?RA_SERVER_ID, ?RA_NAME]);
        Error ->
            ?LOG_ERROR("Failed to force delete Ra member ~p from Ra cluster '~s' in Ra system '~s': ~p",
                       [?RA_SERVER_ID, ?RA_NAME, ?RA_SYSTEM, Error])
    end.
