## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.DumpCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:diagnostics, :streams]

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name] = _args, %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :dump, [vhost, name]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot get dump of a classic queue"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot get dump of a quorum queue"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "dump [--vhost <vhost>] <stream>"
  end

  def usage_additional do
    [
      ["<stream>", "Name of the stream"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.stream_queues()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Dumps the on-disk append-only log of a stream"

  def banner([name], %{node: node_name}),
    do: "Dumping log of stream #{name} on node #{node_name} ..."
end
