defmodule SmolNet.Stack.Options do
  @moduledoc false

  alias SmolNet.Stack

  @default_mtu 1_500
  @default_ingress_queue %{packets: 64, bytes: 64 * 1_500}
  @max_limits %{
    bytes_copied: 16 * 1024 * 1024,
    output_packets: 1_024,
    ready_events: 4_096,
    maintenance_work: 4_096
  }
  @allowed [:egress, :mtu, :addresses, :routes, :limits, :ingress_queue, :link_down]

  @spec parse(keyword()) :: {:ok, map()} | {:error, atom()}
  def parse(options) when is_list(options) do
    with :ok <- validate_keyword(options),
         {:ok, egress} <- egress(Keyword.get(options, :egress)),
         {:ok, mtu} <- mtu(Keyword.get(options, :mtu, @default_mtu)),
         {:ok, addresses} <- addresses(Keyword.get(options, :addresses, [])),
         {:ok, routes} <- routes(Keyword.get(options, :routes, [])),
         {:ok, limits} <- limits(Keyword.get(options, :limits, %{}), mtu),
         {:ok, ingress_queue} <- ingress_queue(Keyword.get(options, :ingress_queue)),
         {:ok, link_down} <- link_down(Keyword.get(options, :link_down, :stop)) do
      {:ok,
       %{
         egress: egress,
         link_down: link_down,
         limits: limits,
         ingress_queue: ingress_queue,
         native_config: %{
           mtu: mtu,
           addresses: addresses,
           routes: routes
         }
       }}
    end
  end

  def parse(_options), do: {:error, :invalid_options}

  @spec default_native_config() :: map()
  def default_native_config do
    %{mtu: @default_mtu, addresses: [], routes: []}
  end

  @spec default_ingress_queue() :: map()
  def default_ingress_queue, do: @default_ingress_queue

  defp validate_keyword(options) do
    if Keyword.keyword?(options) do
      keys = Keyword.keys(options)

      if Enum.uniq(keys) == keys and Enum.all?(keys, &(&1 in @allowed)) do
        :ok
      else
        {:error, :invalid_options}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp egress(nil), do: {:ok, nil}
  defp egress({pid, link_ref}) when is_pid(pid), do: {:ok, {pid, link_ref}}
  defp egress(_egress), do: {:error, :invalid_egress}

  defp mtu(value) when is_integer(value) and value in 1_280..65_575, do: {:ok, value}
  defp mtu(_value), do: {:error, :invalid_mtu}

  defp addresses(values) when is_list(values) and length(values) <= 8 do
    map_while(values, &address/1, :invalid_addresses)
  end

  defp addresses(_values), do: {:error, :invalid_addresses}

  defp address({address, prefix_length})
       when is_integer(prefix_length) and prefix_length in 0..128 do
    with {:ok, bytes} <- ipv6_bytes(address),
         true <- not multicast?(bytes) do
      {:ok, %{address: bytes, prefix_length: prefix_length}}
    else
      _invalid -> :error
    end
  end

  defp address(_address), do: :error

  defp routes(values) when is_list(values) and length(values) <= 4 do
    map_while(values, &route/1, :invalid_routes)
  end

  defp routes(_values), do: {:error, :invalid_routes}

  defp route({destination, prefix_length, gateway})
       when is_integer(prefix_length) and prefix_length in 0..128 do
    with {:ok, destination_bytes} <- ipv6_bytes(destination),
         {:ok, gateway_bytes} <- ipv6_bytes(gateway),
         true <- not multicast?(destination_bytes),
         true <- not multicast?(gateway_bytes),
         true <- not unspecified?(gateway_bytes) do
      {:ok,
       %{
         destination: destination_bytes,
         prefix_length: prefix_length,
         gateway: gateway_bytes
       }}
    else
      _invalid -> :error
    end
  end

  defp route(_route), do: :error

  defp ipv6_bytes(address) when is_tuple(address) and tuple_size(address) == 8 do
    segments = Tuple.to_list(address)

    if Enum.all?(segments, &(is_integer(&1) and &1 in 0..65_535)) do
      {:ok, Enum.flat_map(segments, &[div(&1, 256), rem(&1, 256)])}
    else
      :error
    end
  end

  defp ipv6_bytes(_address), do: :error

  defp multicast?([0xFF | _rest]), do: true
  defp multicast?(_bytes), do: false

  defp unspecified?(bytes), do: Enum.all?(bytes, &(&1 == 0))

  defp limits(overrides, mtu) when is_map(overrides) do
    limits = Map.merge(Stack.default_limits(), overrides)

    if MapSet.new(Map.keys(limits)) == MapSet.new(Map.keys(Stack.default_limits())) and
         Enum.all?(limits, fn {name, value} ->
           is_integer(value) and value > 0 and value <= Map.fetch!(@max_limits, name)
         end) and limits.bytes_copied >= mtu do
      {:ok, limits}
    else
      {:error, :invalid_limits}
    end
  end

  defp limits(_overrides, _mtu), do: {:error, :invalid_limits}

  defp ingress_queue(nil), do: {:ok, @default_ingress_queue}

  defp ingress_queue(value) when is_list(value) do
    if Keyword.keyword?(value) and Enum.uniq(Keyword.keys(value)) == Keyword.keys(value) do
      value |> Map.new() |> ingress_queue()
    else
      {:error, :invalid_ingress_queue}
    end
  end

  defp ingress_queue(%{packets: packets, bytes: bytes} = value)
       when map_size(value) == 2 and is_integer(packets) and packets > 0 and
              packets <= 1_000_000 and is_integer(bytes) and bytes > 0 and
              bytes <= 1_073_741_824 do
    {:ok, %{packets: packets, bytes: bytes}}
  end

  defp ingress_queue(_value), do: {:error, :invalid_ingress_queue}

  defp link_down(:stop), do: {:ok, :stop}
  defp link_down(:mark_down), do: {:ok, :mark_down}
  defp link_down({:notify, pid}) when is_pid(pid), do: {:ok, {:notify, pid}}
  defp link_down(_policy), do: {:error, :invalid_link_down_policy}

  defp map_while(values, mapper, error) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case mapper.(value) do
        {:ok, mapped} -> {:cont, {:ok, [mapped | acc]}}
        :error -> {:halt, {:error, error}}
      end
    end)
    |> then(fn
      {:ok, mapped} -> {:ok, Enum.reverse(mapped)}
      {:error, _reason} = result -> result
    end)
  end
end
