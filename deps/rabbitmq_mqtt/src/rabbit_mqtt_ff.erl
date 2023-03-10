%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_ff).

-include("rabbit_mqtt.hrl").

-rabbit_feature_flag(
   {?QUEUE_TYPE_QOS_0,
    #{desc          => "Support pseudo queue type for MQTT QoS 0 subscribers omitting a queue process",
      stability     => stable
     }}).
