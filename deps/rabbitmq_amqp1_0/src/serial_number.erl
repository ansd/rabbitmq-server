%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

%% https://www.ietf.org/rfc/rfc1982.txt
-module(serial_number).
-include("rabbit_amqp1_0.hrl").

-export([add/2,
         compare/2,
         usort/1,
         ranges/1,
         diff/2]).

-type serial_number() :: sequence_no().
-export_type([serial_number/0]).

%% SERIAL_BITS = 32
%% 2 ^ SERIAL_BITS
-define(SERIAL_SPACE, 16#100000000).
%% 2 ^ (SERIAL_BITS - 1) - 1
-define(SERIAL_MAX_ADDEND, 16#7fffffff).

-spec add(serial_number(), non_neg_integer()) ->
    serial_number().
add(S, N)
  when N >= 0 andalso
       N =< ?SERIAL_MAX_ADDEND ->
    (S + N) rem ?SERIAL_SPACE;
add(S, N) ->
    exit({undefined_serial_addition, S, N}).

%% 2 ^ (SERIAL_BITS - 1)
-define(COMPARE, 2_147_483_648).

-spec compare(serial_number(), serial_number()) ->
    equal | less | greater.
compare(A, B) ->
    if A =:= B ->
           equal;
       (A < B andalso B - A < ?COMPARE) orelse
       (A > B andalso A - B > ?COMPARE) ->
           less;
       (A < B andalso B - A > ?COMPARE) orelse
       (A > B andalso A - B < ?COMPARE) ->
           greater;
       true ->
           exit({undefined_serial_comparison, A, B})
    end.

-spec usort([serial_number()]) ->
    [serial_number()].
usort(L) ->
    lists:usort(fun(A, B) ->
                        case compare(A, B) of
                            greater -> false;
                            _ -> true
                        end
                end, L).

%% Takes a list with unique and sorted serial numbers
%% (e.g. via serial_number:usort/1) and returns
%% tuples {First, Last} representing contiguous serial numbers.
-spec ranges([serial_number()]) ->
    [{First :: serial_number(), Last :: serial_number()}].
ranges([]) ->
    [];
ranges([First | Rest]) ->
    ranges0(Rest, [{First, First}]).

ranges0([], Acc) ->
    lists:reverse(Acc);
ranges0([H | Rest], [{First, Last} | AccRest] = Acc0) ->
    case add(Last, 1) of
        H ->
            Acc = [{First, H} | AccRest],
            ranges0(Rest, Acc);
        _ ->
            Acc = [{H, H} | Acc0],
            ranges0(Rest, Acc)
    end.

%%TODO diffing isn't even allowed?
-define(SERIAL_DIFF_BOUND, 16#80000000).
-spec diff(serial_number(), serial_number()) -> integer().
diff(A, B) ->
    Diff = A - B,
    if Diff > (?SERIAL_DIFF_BOUND) ->
           %% B is actually greater than A
           - (?SERIAL_SPACE - Diff);
       Diff < - (?SERIAL_DIFF_BOUND) ->
           ?SERIAL_SPACE + Diff;
       Diff < ?SERIAL_DIFF_BOUND andalso Diff > -?SERIAL_DIFF_BOUND ->
           Diff;
       true ->
           exit({undefined_serial_diff, A, B})
    end.
