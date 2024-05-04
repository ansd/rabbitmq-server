%% good enough for most use cases
-define(IS_MC(Msg), element(1, Msg) == mc andalso tuple_size(Msg) == 5).

%% "Short strings can carry up to 255 octets of UTF-8 data, but
%% may not contain binary zero octets." [AMQP 0.9.1 $4.2.5.3]
-define(IS_SHORTSTR_LEN(B), byte_size(B) < 256).

%% We keep the following atom annotation keys short as they are stored per message on disk.
-define(ANN_EXCHANGE, x).
-define(ANN_ROUTING_KEYS, rk).
-define(ANN_TIMESTAMP, ts).
-define(ANN_RECEIVED_AT_TIMESTAMP, rts).
-define(ANN_DURABLE, d).
-define(ANN_PRIORITY, p).

%% RabbitMQ >= 3.13.3
-record(death_v2, {source_queue :: rabbit_misc:resource_name(),
                   reason :: rabbit_dead_letter:reason(),
                   %% how many times this message was dead lettered
                   %% from this source_queue for this reason
                   count :: pos_integer(),
                   %% timestamp when this message was dead lettered the first time
                   %% from this source_queue for this reason
                   first_death_timestamp :: pos_integer(),
                   original_exchange :: rabbit_misc:resource_name(),
                   original_routing_keys :: [rabbit_types:routing_key(),...],
                   %% set iff reason == expired
                   original_ttl :: undefined | non_neg_integer()}).

%% These records were used in RabbitMQ 3.13.0 - 3.13.2.
-type death_v1_key() :: {SourceQueue :: rabbit_misc:resource_name(), rabbit_dead_letter:reason()}.
-type death_v1_anns() :: #{first_time := non_neg_integer(),
                           last_time := non_neg_integer(),
                           ttl => OriginalExpiration :: non_neg_integer()}.
-record(death, {exchange :: OriginalExchange :: rabbit_misc:resource_name(),
                routing_keys = [] :: OriginalRoutingKeys :: [rabbit_types:routing_key()],
                count = 0 :: non_neg_integer(),
                anns :: death_v1_anns()}).
-record(deaths, {first :: death_v1_key(),
                 last :: death_v1_key(),
                 records = #{} :: #{death_v1_key() := #death{}}}).
