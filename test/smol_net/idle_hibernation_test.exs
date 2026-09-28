defmodule SmolNet.IdleHibernationTest do
  # An idle socket process hibernates, so that it stops holding the binaries
  # of its last transfer (#135). The hibernation timeout is fixed, so the
  # test waits it out once, for every process at the same time, and runs
  # alongside the other async modules.
  use ExUnit.Case, async: true

  alias SmolNet.Loopback
  alias SmolNet.Test.Timing

  @localhost {127, 0, 0, 1}
  @payload_bytes 256 * 1024
  @datagram_bytes 1_200
  @datagrams 64

  # What a hibernated process may still hold: far less than one transfer.
  @retained_limit div(@payload_bytes, 16)

  # The processes hibernate after 5 s idle; the rest is a liveness bound.
  # See `SmolNet.Test.Timing`.
  @hibernated_within 5_000 + Timing.liveness(10_000)
  @wait_5s Timing.liveness(5_000)

  # A read whose deadline falls after its socket has hibernated.
  @read_timeout Timing.quiescence(6_000)

  setup do
    {:ok, link, stack} = Loopback.start_link(addresses: [{@localhost, 8}])
    on_exit(fn -> SmolNet.stop_stack(stack) end)
    %{link: link, stack: stack}
  end

  test "idle sockets and the link hibernate, drop their binaries, and keep working",
       %{link: link, stack: stack} do
    {listener, client, server} = tcp_connection(stack)
    {sender, receiver} = udp_pair(stack)

    payload = :crypto.strong_rand_bytes(@payload_bytes)
    echo(client, server, payload)
    datagrams(sender, receiver)

    # Operations left waiting across hibernation: a read with no deadline,
    # and one whose deadline falls after its socket has hibernated.
    waiting = Task.async(fn -> :gen_tcp.recv(server, 4) end)
    expiring = Task.async(fn -> :gen_tcp.recv(client, 0, @read_timeout) end)

    processes = [link | Enum.map([listener, client, server, sender, receiver], &socket_pid/1)]
    eventually(fn -> Enum.all?(processes, &hibernating?/1) end, @hibernated_within)

    for pid <- processes do
      assert retained_binary(pid) < @retained_limit
    end

    assert {:error, :timeout} = Task.await(expiring, @read_timeout + @wait_5s)

    assert :ok = :gen_tcp.send(client, "wake")
    assert {:ok, "wake"} = Task.await(waiting, @wait_5s)

    echo(client, server, payload)
    datagrams(sender, receiver)
  end

  defp tcp_connection(stack) do
    options = [tcp_module: SmolNet.Inet.Tcp, smolnet_stack: stack, mode: :binary, active: false]
    {:ok, listener} = :gen_tcp.listen(0, options)
    {:ok, {_address, port}} = :inet.sockname(listener)
    {:ok, client} = :gen_tcp.connect(@localhost, port, options, @wait_5s)
    {:ok, server} = :gen_tcp.accept(listener, @wait_5s)
    {listener, client, server}
  end

  defp udp_pair(stack) do
    options = [udp_module: SmolNet.Inet.Udp, smolnet_stack: stack, mode: :binary, active: false]
    {:ok, sender} = :gen_udp.open(0, options)
    {:ok, receiver} = :gen_udp.open(0, options)
    {sender, receiver}
  end

  # Carries `payload` from `client` to `server` and back.
  defp echo(client, server, payload) do
    size = byte_size(payload)
    reading = Task.async(fn -> :gen_tcp.recv(server, size, @wait_5s) end)
    assert :ok = :gen_tcp.send(client, payload)
    assert {:ok, ^payload} = Task.await(reading, @wait_5s * 2)
    assert :ok = :gen_tcp.send(server, payload)
    assert {:ok, ^payload} = :gen_tcp.recv(client, size, @wait_5s)
  end

  # One datagram at a time, so none is dropped for want of buffer space.
  defp datagrams(sender, receiver) do
    {:ok, {_address, port}} = :inet.sockname(receiver)

    for _index <- 1..@datagrams do
      datagram = :crypto.strong_rand_bytes(@datagram_bytes)
      assert :ok = :gen_udp.send(sender, @localhost, port, datagram)
      assert {:ok, {@localhost, _port, ^datagram}} = :gen_udp.recv(receiver, 0, @wait_5s)
    end
  end

  defp socket_pid({:"$inet", _module, pid}) when is_pid(pid), do: pid

  # OTP 28 and later hibernate inside the behaviour's own loop; earlier
  # releases hibernate in the BIF.
  defp hibernating?(pid) do
    case Process.info(pid, :current_function) do
      {:current_function, {:erlang, :hibernate, 3}} -> true
      {:current_function, {_module, :loop_hibernate, _arity}} -> true
      _other -> false
    end
  end

  # Bytes of off-heap binaries the process references, each counted once.
  defp retained_binary(pid) do
    {:binary, binaries} = Process.info(pid, :binary)

    binaries
    |> Enum.uniq_by(fn {address, _size, _refs} -> address end)
    |> Enum.map(fn {_address, size, _refs} -> size end)
    |> Enum.sum()
  end

  defp eventually(condition, within) do
    deadline = System.monotonic_time(:millisecond) + within
    poll(condition, deadline)
  end

  defp poll(condition, deadline) do
    cond do
      condition.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition not met in time")

      true ->
        Process.sleep(50)
        poll(condition, deadline)
    end
  end
end
