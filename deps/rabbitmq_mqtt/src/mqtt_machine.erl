%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(mqtt_machine).

-behaviour(ra_machine).

-include_lib("kernel/include/logger.hrl").

%% ra_machine callbacks
-export([version/0,
         init/1,
         apply/3]).

%% client API
-export([maybe_delete/0]).

-define(STATE, {machine_state, #{}, #{}, undefined, undefined}).
-define(RA_SYSTEM, coordination).
-define(RA_NAME, mqtt_node).
-define(RA_SERVER_ID, {?RA_NAME, node()}).
-define(TIMEOUT, 60_000).

version() -> 1.

init(_Conf) ->
    ?STATE.

apply(_, _, _) ->
    {?STATE, ok}.

maybe_delete() ->
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
                      ?LOG_WARNING("Failed to delete Ra server ~p: ~p ~p",
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
                            ?LOG_INFO("Successfully deleted Ra cluster '~s' in Ra system '~s'",
                                      [?RA_NAME, ?RA_SYSTEM]),
                            ok;
                        {error, Reason} = Error ->
                            ?LOG_WARNING("Failed to delete Ra cluster '~s' in Ra system '~s': ~p",
                                         [?RA_NAME, ?RA_SYSTEM, Reason]),
                            Error
                    end;
                {ok, Members, Leader} ->
                    ?LOG_INFO("Members for Ra cluster '~s' in Ra system '~s': ~p Leader: ~p",
                              [?RA_NAME, ?RA_SYSTEM, Members, Leader]),
                    case ra:leave_and_delete_server(?RA_SYSTEM, Leader, RaServerId, ?TIMEOUT) of
                        ok ->
                            ?LOG_INFO("Successfully deleted Ra member ~p from Ra cluster '~s' in Ra system '~s'",
                                      [RaServerId, ?RA_NAME, ?RA_SYSTEM]),
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
            ?LOG_INFO("Successfully force deleted Ra member ~p from Ra cluster '~s' in Ra system '~s'",
                      [?RA_SERVER_ID, ?RA_NAME, ?RA_SYSTEM]);
        Error ->
            ?LOG_WARNING("Failed to force delete Ra member ~p from Ra cluster '~s' in Ra system '~s': ~p",
                         [?RA_SERVER_ID, ?RA_NAME, ?RA_SYSTEM, Error])
    end.
