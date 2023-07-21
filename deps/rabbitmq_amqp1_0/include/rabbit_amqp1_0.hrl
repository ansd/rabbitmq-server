%%-define(debug, true).

-ifdef(debug).
-define(DEBUG0(F), ?SAFE(io:format(F, []))).
-define(DEBUG(F, A), ?SAFE(io:format(F, A))).
-else.
-define(DEBUG0(F), ok).
-define(DEBUG(F, A), ok).
-endif.

-define(pprint(F), io:format("~p~n", [amqp10_framing:pprint(F)])).

-define(SAFE(F),
        ((fun() ->
                  try F
                  catch __T:__E:__ST ->
                          io:format("~p:~p thrown debugging~n~p~n",
                                    [__T, __E, __ST])
                  end
          end)())).

%% General consts
-define(FRAME_1_0_MIN_SIZE, 512).

-define(SEND_ROLE, false).
-define(RECV_ROLE, true).

-define(ITEMS,
        [pid,
         auth_mechanism,
         frame_max,
         timeout,
         user,
         ssl,
         ssl_protocol,
         ssl_key_exchange,
         ssl_cipher,
         ssl_hash,
         peer_cert_issuer,
         peer_cert_subject,
         peer_cert_validity,
         node,
         protocol,
         host,
         port,
         peer_host,
         peer_port
        ]).

-define(INFO_ITEMS,
        ?ITEMS ++
        [connection_state,
         recv_oct,
         recv_cnt,
         send_oct,
         send_cnt
        ]).

%% Connection opened or closed.
-define(EVENT_KEYS,
        ?ITEMS ++
        [name,
         type,
         client_properties,
         connected_at
        ]).

-include_lib("amqp10_common/include/amqp10_framing.hrl").

% [2.8.10]
-type sequence_no() :: 0..16#ffffffff.
