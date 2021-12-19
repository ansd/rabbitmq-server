-module(rabbit_fifo_dlx_strategy_none).

-include("rabbit_fifo.hrl").

-behaviour(rabbit_fifo_dlx_strategy).

-export([init/1, discard/3]).

-spec init(rabbit_fifo:state()) ->
    rabbit_fifo:state().
init(State) ->
    State#rabbit_fifo{dlx = undefined}.

-spec discard([msg()], rabbit_dead_letter:reason(), rabbit_fifo:state()) ->
    {rabbit_fifo:state(), ra_machine:effects(), Delete :: boolean()}.
discard(_, _, State) ->
    {State, [], true}.
