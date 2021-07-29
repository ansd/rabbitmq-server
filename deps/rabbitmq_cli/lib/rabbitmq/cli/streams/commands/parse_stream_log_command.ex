## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.ParseStreamLogCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  # use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def validate(args, _) when length(args) < 3 do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 3 do
    {:validation_failure, :too_many_args}
  end

  def validate([_, start_offset, end_offset], _) when is_integer(start_offset) and is_integer(end_offset) do
    :ok
  end

  def validate([_, start_offset, _], _) do
    case Integer.parse(start_offset) do
      {n, _} when n >= 0 -> :ok
      :error -> {:validation_failure, {:bad_argument, "start_offset must be a non-negative integer"}}
    end
  end

  def validate([_, _, end_offset], _) do
    case Integer.parse(end_offset) do
      {n, _} when n >= 0 -> :ok
      :error -> {:validation_failure, {:bad_argument, "end_offset must be a non-negative integer"}}
    end
  end

  def validate(_, _, _), do: :ok

  def run([name, start_offset, end_offset] = _args, %{node: node_name, vhost: vhost}) do
    {start_off, _} = Integer.parse(start_offset)
    {end_off, _} = Integer.parse(end_offset)
    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :parse, [vhost, name, start_off, end_off]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot parse stream log of a classic queue"}

      {:error, :quorum_queue_not_supported} ->
        {:error, "Cannot parse stream log of a quorum queue"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "parse_stream_log [--vhost <vhost>] <stream> <start offset> <end offset>"
  end

  def usage_additional do
    [
      ["<stream>", "Name of the stream"],
      ["<start offset>", "Offset to start parsing"],
      ["<end offset>", "Offset to stop parsing"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.streams()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Parses the on-disk binary append-only log of a stream"

  def banner([name, start_offset, end_offset], %{node: node_name}),
  do: "Parsing log of stream #{name} from offset #{start_offset} to offset #{end_offset} node #{node_name} ..."
end
