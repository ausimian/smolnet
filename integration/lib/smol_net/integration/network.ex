defmodule SmolNet.Integration.Network do
  @moduledoc """
  The network an integration run uses, and how a workload opens sockets on
  it.

  `integration/setup.sh` gives the host `10.77.0.1/24` and `fd00:77::1/64`
  on the TUN device. SmolNet is `10.77.0.2` and `fd00:77::2`, with default
  routes through the host, which forwards and masquerades both prefixes to
  the internet.

  A workload names the two ends of a local exchange by role rather than by
  stack, so that the same workload runs in every mode:

    * `:subject` - the stack under test: SmolNet, or in baseline mode the
      kernel.
    * `:peer` - the far end on this host: the kernel, reached through the
      device. In self-check mode no host is reachable, so the peer is SmolNet
      itself, over the helper's loopback.

  In baseline mode both roles are the kernel, on its loopback addresses, so
  a baseline run needs no device.

  For an internet peer, open the subject's socket without an `:ip`.
  """

  alias SmolNet.Integration.Soak.Context
  alias SmolNet.Integration.TunLink

  @host %{inet: {10, 77, 0, 1}, inet6: {0xFD00, 0x77, 0, 0, 0, 0, 0, 1}}
  @smolnet %{inet: {10, 77, 0, 2}, inet6: {0xFD00, 0x77, 0, 0, 0, 0, 0, 2}}
  @prefix %{inet: 24, inet6: 64}
  @unspecified %{inet: {0, 0, 0, 0}, inet6: {0, 0, 0, 0, 0, 0, 0, 0}}
  @loopback %{inet: {127, 0, 0, 1}, inet6: {0, 0, 0, 0, 0, 0, 0, 1}}

  @type family :: :inet | :inet6
  @type role :: :subject | :peer

  @doc "Returns the host's address on the TUN device."
  @spec host_address(family()) :: :inet.ip_address()
  def host_address(family), do: Map.fetch!(@host, family)

  @doc "Returns SmolNet's address on the TUN device."
  @spec smolnet_address(family()) :: :inet.ip_address()
  def smolnet_address(family), do: Map.fetch!(@smolnet, family)

  @doc "Returns the addresses and routes a SmolNet stack on the device needs."
  @spec stack_options() :: keyword()
  def stack_options do
    families = [:inet, :inet6]

    [
      addresses: Enum.map(families, &{smolnet_address(&1), Map.fetch!(@prefix, &1)}),
      routes: Enum.map(families, &{Map.fetch!(@unspecified, &1), 0, host_address(&1)})
    ]
  end

  @doc """
  Starts the network for `mode`: a TUN link and its stack, a loopback link
  and its stack, or nothing for the kernel baseline.

  `options` are passed to `SmolNet.Integration.TunLink.start/1`, over
  `stack_options/0`.
  """
  @spec start(SmolNet.Integration.Soak.Options.mode(), String.t(), keyword()) ::
          {:ok, %{stack: SmolNet.Stack.Ref.t() | nil, link: pid() | nil}} | {:error, term()}
  def start(:kernel, _device, _options), do: {:ok, %{stack: nil, link: nil}}

  def start(mode, device, options) do
    target = if mode == :self_check, do: [loopback: true], else: [device: device]

    case TunLink.start(target ++ Keyword.merge(stack_options(), options)) do
      {:ok, link, stack} -> {:ok, %{stack: stack, link: link}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns whether `role` is a SmolNet socket in this run."
  @spec smolnet?(Context.t(), role()) :: boolean()
  def smolnet?(%Context{mode: :kernel}, _role), do: false
  def smolnet?(%Context{mode: :smolnet}, :peer), do: false
  def smolnet?(%Context{}, _role), do: true

  @doc "Returns whether the host can reach SmolNet, as a ping from the host needs."
  @spec host_reachable?(Context.t()) :: boolean()
  def host_reachable?(%Context{mode: mode}), do: mode == :smolnet

  @doc "Returns the local address of `role`."
  @spec address(Context.t(), role(), family()) :: :inet.ip_address()
  def address(%Context{mode: :kernel}, _role, family), do: Map.fetch!(@loopback, family)

  def address(context, role, family) do
    if smolnet?(context, role), do: smolnet_address(family), else: host_address(family)
  end

  @doc """
  Returns the `:gen_tcp` options that open a socket for `role`: the SmolNet
  callback and stack, or nothing extra for the kernel, plus the family.
  """
  @spec tcp_options(Context.t(), role(), family()) :: list()
  def tcp_options(context, role, family) do
    if smolnet?(context, role) do
      [{:tcp_module, tcp_module(family)}, {:smolnet_stack, context.stack}, family]
    else
      [family]
    end
  end

  @doc "Returns the `:gen_udp` options that open a socket for `role`."
  @spec udp_options(Context.t(), role(), family()) :: list()
  def udp_options(context, role, family) do
    if smolnet?(context, role) do
      [{:udp_module, udp_module(family)}, {:smolnet_stack, context.stack}, family]
    else
      [family]
    end
  end

  defp tcp_module(:inet), do: SmolNet.Inet.Tcp
  defp tcp_module(:inet6), do: SmolNet.Inet6.Tcp

  defp udp_module(:inet), do: SmolNet.Inet.Udp
  defp udp_module(:inet6), do: SmolNet.Inet6.Udp
end
