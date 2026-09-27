defmodule SmolNet.Integration.Scenarios.Smoke do
  @moduledoc """
  The smoke scenario: proves the TUN bridge and the runner end to end.

  For each family, every round:

    * the peer connects to a listener on the subject, sends a random
      payload and reads it back, checked byte for byte;
    * the subject connects to a listener on the peer and does the same.

  Over the device the subject is SmolNet and the peer is the host's kernel,
  so these are TCP connections each way through the TUN device. Before the
  first round the host pings SmolNet, which answers ICMP echo itself. Rounds
  repeat for the run's duration, and then the stack's sockets must all be
  released.

  `--stall` then parks a receive that can never complete under a short
  deadline, which must fail the run with a `:deadline` failure and its
  diagnostics: the check that deadlines work.
  """

  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak

  @operation_timeout 30_000
  @round_pause 1_000

  @doc "Returns the scenario's runner config."
  @spec config() :: keyword()
  def config do
    [
      name: "smoke",
      default_duration: "30s",
      switches: [bytes: :integer, stall: :boolean, stall_timeout: :integer],
      defaults: [bytes: 1_048_576, stall: false, stall_timeout: 2_000],
      counters: [:rounds, :bytes_echoed],
      # A socket that closes first holds its slot for TIME-WAIT, and every
      # round closes several.
      stack: [limits: %{sockets: 256}],
      usage: """

      smoke options:
        --bytes N             the payload each echo sends (default 1048576)
        --stall               finish with an operation that stalls, which must fail the run
        --stall-timeout MS    the stalled operation's deadline (default 2000)
      """
    ]
  end

  @doc "Runs the scenario's workload."
  @spec run(SmolNet.Integration.Soak.Context.t()) :: :ok
  def run(context) do
    if Network.host_reachable?(context) do
      Enum.each(context.families, &ping(context, &1))
    end

    Soak.loop(
      context,
      fn ->
        Enum.each(context.families, fn family ->
          echo(context, family, :subject, :peer)
          echo(context, family, :peer, :subject)
        end)

        Soak.count(context, :rounds)
      end,
      pause: @round_pause
    )

    if context.extra.stall do
      stall(context, hd(context.families))
    end

    Soak.await_socket_count(context, 0)
  end

  defp ping(context, family) do
    address = family |> Network.smolnet_address() |> :inet.ntoa() |> to_string()

    case System.find_executable("ping") do
      nil ->
        Soak.note(context, "ping not found; not pinging #{address}")

      ping ->
        flag = if family == :inet6, do: "-6", else: "-4"
        arguments = [flag, "-c", "3", "-i", "0.2", "-W", "2", address]

        {output, status} =
          Soak.within(context, {:ping, family}, @operation_timeout, fn ->
            System.cmd(ping, arguments, stderr_to_stdout: true)
          end)

        if status == 0 do
          Soak.log(context, "#{address} answered ping")
        else
          Soak.fail(context, :ping, "#{address} did not answer ping", [{"output", output}])
        end
    end
  end

  # `client` connects to a listener on `server` and echoes a payload.
  defp echo(context, family, server, client) do
    label = "#{family} #{client} to #{server}"
    bytes = context.extra.bytes
    listener = listen(context, family, server, label)
    {:ok, {_address, port}} = :inet.sockname(listener)

    echoer =
      Task.async(fn ->
        {:ok, socket} = within(context, {:accept, label}, fn -> :gen_tcp.accept(listener) end)
        {:ok, data} = within(context, {:server_recv, label}, fn -> recv(socket, bytes) end)
        :ok = within(context, {:server_send, label}, fn -> :gen_tcp.send(socket, data) end)
        :gen_tcp.close(socket)
      end)

    payload = :crypto.strong_rand_bytes(bytes)
    socket = connect(context, family, server, client, port, label)
    :ok = within(context, {:client_send, label}, fn -> :gen_tcp.send(socket, payload) end)
    {:ok, echoed} = within(context, {:client_recv, label}, fn -> recv(socket, bytes) end)
    :ok = :gen_tcp.close(socket)
    within(context, {:echoer, label}, fn -> Task.await(echoer, :infinity) end)
    :ok = :gen_tcp.close(listener)

    if echoed == payload do
      Soak.count(context, :bytes_echoed, 2 * bytes)
    else
      Soak.fail(context, :integrity, "#{label}: the echo differs from the payload", [
        {"sizes", %{sent: byte_size(payload), echoed: byte_size(echoed)}},
        {"sha256", %{sent: sha256(payload), echoed: sha256(echoed)}}
      ])
    end
  end

  # A receive that can never complete: the peer connects and never sends.
  defp stall(context, family) do
    label = "#{family} stall"
    listener = listen(context, family, :subject, label)
    {:ok, {_address, port}} = :inet.sockname(listener)
    _client = connect(context, family, :subject, :peer, port, label)
    {:ok, socket} = within(context, {:accept, label}, fn -> :gen_tcp.accept(listener) end)

    Soak.within(context, :stalled_recv, context.extra.stall_timeout, fn ->
      :gen_tcp.recv(socket, 1, :infinity)
    end)
  end

  defp listen(context, family, role, label) do
    options =
      Network.tcp_options(context, role, family) ++
        [:binary, active: false, ip: Network.address(context, role, family)]

    {:ok, listener} = within(context, {:listen, label}, fn -> :gen_tcp.listen(0, options) end)
    listener
  end

  defp connect(context, family, server, client, port, label) do
    address = Network.address(context, server, family)
    options = Network.tcp_options(context, client, family) ++ [:binary, active: false]

    {:ok, socket} =
      within(context, {:connect, label}, fn ->
        :gen_tcp.connect(address, port, options, :infinity)
      end)

    socket
  end

  # Exact-length receives; SmolNet and the kernel both accumulate to the
  # requested length in raw mode.
  defp recv(socket, bytes), do: :gen_tcp.recv(socket, bytes, :infinity)

  defp within(context, name, fun), do: Soak.within(context, name, @operation_timeout, fun)

  defp sha256(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)
end
