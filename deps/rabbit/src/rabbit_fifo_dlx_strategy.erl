-module(rabbit_fifo_dlx_strategy).

-include("rabbit_fifo.hrl").

-export([init/1,
         discard/3,
         apply/3,
         state_enter/2,
         handle_aux/4,
         purge/1,
         stat/1,
         overview/1,
         checkout/1,
         dehydrate/1,
         normalize/1,
         update_config/2
        ]).

-define(init_spec,
        init(rabbit_fifo:state()) ->
            rabbit_fifo:state()).

-define(discard_spec,
        discard([msg()], rabbit_dead_letter:reason(), rabbit_fifo:state()) ->
            {rabbit_fifo:state(), ra_machine:effects(), Delete :: boolean()}).

-define(apply_spec,
        apply(ra_machine:command_meta_data(), rabbit_fifo:command(), rabbit_fifo:state()) ->
            {rabbit_fifo:state(), ra_machine:effects()}).

-define(state_enter_spec,
        state_enter(ra_server:ra_state(), rabbit_fifo:state()) ->
            ra_machine:effects()).

-define(handle_aux_spec,
        handle_aux(ra_server:ra_state(), Cmd :: term(), term(), rabbit_fifo:state()) ->
            term()).

-define(purge_spec,
        purge(rabbit_fifo:state()) ->
            {rabbit_fifo:state(), [msg()]}).

-define(stat_spec,
        stat(rabbit_fifo:state()) ->
            {Num :: non_neg_integer(), Bytes :: non_neg_integer()}).

-define(overview_spec,
        overview(rabbit_fifo:state()) ->
            map()).

-define(checkout_spec,
        checkout(rabbit_fifo:state()) ->
            {rabbit_fifo:state(), ra_machine:effects()}).

-define(dehydrate_spec,
        dehydrate(rabbit_fifo:state()) ->
            rabbit_fifo:state()).

-define(normalize_spec,
        normalize(rabbit_fifo:state()) ->
            rabbit_fifo:state()).

-define(update_config_spec,
        update_config(config(), rabbit_fifo:state()) ->
            {rabbit_fifo:state(), ra_machine:effects()}).

-callback ?init_spec.
-callback ?discard_spec.
-callback ?apply_spec.
-callback ?state_enter_spec.
-callback ?handle_aux_spec.
-callback ?purge_spec.
-callback ?stat_spec.
-callback ?overview_spec.
-callback ?checkout_spec.
-callback ?dehydrate_spec.
-callback ?normalize_spec.
-callback ?update_config_spec.

-optional_callbacks([apply/3,
                     state_enter/2,
                     handle_aux/4,
                     purge/1,
                     stat/1,
                     overview/1,
                     checkout/1,
                     dehydrate/1,
                     normalize/1,
                     update_config/2]).

-spec ?init_spec.
init(#rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    Mod:init(State).

-spec ?discard_spec.
discard(Msgs, Reason, #rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State)
  when is_list(Msgs) ->
    Mod:discard(Msgs, Reason, State).

-spec ?apply_spec.
apply(Meta, Cmd, #rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [Meta, Cmd, State], {State, ok}).

-spec ?state_enter_spec.
state_enter(RaState, #rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [RaState, State], []).

-spec ?handle_aux_spec.
handle_aux(RaState, Cmd, Aux, #rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [RaState, Cmd, Aux, State], Aux).

-spec ?purge_spec.
purge(#rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [State], {State, []}).

-spec ?stat_spec.
stat(#rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [State], {0, 0}).

-spec ?overview_spec.
overview(#rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [State], #{}).

-spec ?checkout_spec.
checkout(#rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [State], {State, []}).

-spec ?dehydrate_spec.
dehydrate(#rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [State], State).

-spec ?normalize_spec.
normalize(#rabbit_fifo{cfg = #cfg{dead_letter_strategy = Mod}} = State) ->
    opt_call(Mod, ?FUNCTION_NAME, [State], State).

-spec ?update_config_spec.
update_config(#{dead_letter_strategy := DLS} = Conf,
              #rabbit_fifo{cfg = #cfg{dead_letter_strategy = DLS}} = State) ->
    %% dead_letter_strategy stayed the same
    opt_call(DLS, ?FUNCTION_NAME, [Conf, State], {State, []});
update_config(#{dead_letter_strategy := NewDLS},
              #rabbit_fifo{cfg = #cfg{dead_letter_strategy = OldDLS,
                                      resource = Res}} = State0) ->
    rabbit_log:debug("Switching dead-letter strategy from ~s to ~s for ~s",
                     [OldDLS, NewDLS, rabbit_misc:rs(Res)]),
    {#rabbit_fifo{cfg = Cfg} = State1, Effects0} =
    opt_call(OldDLS, switch_from, [State0], {State0, []}),
    State2 = State1#rabbit_fifo{cfg = Cfg#cfg{dead_letter_strategy = NewDLS}},
    State3 = NewDLS:init(State2),
    opt_call(NewDLS, switch_to, [State3, Effects0], {State3, Effects0}).

opt_call(M, F, A, Return)
  when is_list(A) ->
    case erlang:function_exported(M, F, length(A)) of
        true ->
            erlang:apply(M, F, A);
        false ->
            Return
    end.
