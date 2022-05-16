-record(ctx, {module :: module(),
              name ,
              %% "publisher confirm queue accounting"
              %% queue type implementation should emit a:
              %% {settle, Success :: boolean(), msg_tag()}
              %% to either settle or reject the delivery of a
              %% message to the queue instance
              %% The queue type module will then emit a {confirm | reject, [msg_tag()}
              %% action to the channel or channel like process when a msg_tag
              %% has reached its conclusion
              state }).


-record(rabbit_queue_type, {ctxs = #{},
                 monitor_registry = #{}
                }).


-record(stream, {name :: rabbit_types:r('queue'),
                 credit :: integer(),
                 max :: non_neg_integer(),
                 start_offset = 0 :: non_neg_integer(),
                 listening_offset = 0 :: non_neg_integer(),
                 log :: undefined | osiris_log:state()}).

-record(stream_client, {stream_id :: string(),
                        name :: term(),
                        leader :: pid(),
                        local_pid :: undefined | pid(),
                        next_seq = 1 :: non_neg_integer(),
                        correlation = #{} ,
                        soft_limit :: non_neg_integer(),
                        slow = false :: boolean(),
                        readers = #{} :: #{term() => #stream{}},
                        writer_id :: binary(),
                        debug :: boolean()
                       }).
