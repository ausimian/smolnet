defmodule SmolNet.Core.SocketsTest do
  use ExUnit.Case, async: true

  # The socket table on its own. How UDP uses it is compared with the NIF in
  # test/conformance/udp_sockets_test.exs.

  alias SmolNet.Core.Sockets

  @local <<192, 0, 2, 1>>
  @peer <<192, 0, 2, 9>>
  @any <<0, 0, 0, 0>>

  defp open(table, kind \\ :udp) do
    {:ok, identity, table} = Sockets.open(table, kind, :state, 8)
    {identity, table}
  end

  describe "identities" do
    test "are never reused, in one table or across tables" do
      {first, table} = open(Sockets.new(49_152))
      {[], table} = Sockets.close(table, first)
      {second, _table} = open(table)
      {third, _other} = open(Sockets.new(49_152))

      assert length(Enum.uniq([first, second, third])) == 3
      assert {:error, :invalid_socket} = Sockets.fetch(table, first, :udp)
    end

    test "are checked for generation and kind" do
      {{id, generation} = identity, table} = open(Sockets.new(49_152))

      assert {:ok, %{kind: :udp}} = Sockets.fetch(table, identity, :any)
      assert {:error, :invalid_socket} = Sockets.fetch(table, {id, generation + 1}, :udp)
      assert {:error, :wrong_socket_kind} = Sockets.fetch(table, identity, :tcp)
    end

    test "are limited by the socket limit" do
      table = Sockets.new(49_152)
      {:ok, _identity, table} = Sockets.open(table, :udp, :state, 1)
      assert {:error, :system_limit} = Sockets.open(table, :udp, :state, 1)
    end
  end

  describe "waiters" do
    test "a woken waiter is already sent, a cancelled one gone, another not found" do
      {identity, table} = open(Sockets.new(49_152))
      pid = self()
      {woken, cancelled} = {make_ref(), make_ref()}

      table = Sockets.arm(table, identity, :read, {pid, :recvfrom, woken})
      {:ok, entry} = Sockets.fetch(table, identity, :udp)
      assert Sockets.waiting?(entry, :read)
      refute Sockets.waiting?(entry, :write)

      assert {[{:wake, ^pid, ^identity, ^woken}], table} = Sockets.wake(table, identity, :read)
      assert {[], table} = Sockets.wake(table, identity, :read)
      assert {:ok, :already_sent, table} = Sockets.cancel(table, identity, :recvfrom, woken)
      assert {:ok, :not_found, table} = Sockets.cancel(table, identity, :sendto, woken)

      table = Sockets.arm(table, identity, :read, {pid, :recvfrom, cancelled})
      assert {:ok, :not_found, table} = Sockets.cancel(table, identity, :recvfrom, woken)
      assert {:ok, :ok, table} = Sockets.cancel(table, identity, :recvfrom, cancelled)
      assert {:ok, :not_found, table} = Sockets.cancel(table, identity, :recvfrom, cancelled)

      assert {:error, :invalid_operation} = Sockets.cancel(table, identity, :listen, cancelled)
      {[], table} = Sockets.close(table, identity)
      assert {:error, :invalid_socket} = Sockets.cancel(table, identity, :recvfrom, cancelled)
    end

    test "closing returns the waiters, read first" do
      {identity, table} = open(Sockets.new(49_152))
      read = {self(), :recvfrom, make_ref()}
      write = {self(), :sendto, make_ref()}
      table = table |> Sockets.arm(identity, :write, write) |> Sockets.arm(identity, :read, read)

      assert %{read: 1, write: 1} = Sockets.counts(table)
      assert {[read: ^read, write: ^write], table} = Sockets.close(table, identity)
      assert %{udp: 0, read: 0, write: 0} = Sockets.counts(table)
    end
  end

  describe "ephemeral ports" do
    test "go round-robin from the start, skip ports in use and wrap" do
      table = Sockets.new(50_174)
      {identity, table} = open(table)
      table = Sockets.bind(table, identity, :udp, {@local, 49_152}, nil)

      {ports, _table} =
        Enum.map_reduce(1..3, table, fn _n, table ->
          {:ok, port, table} = Sockets.allocate_port(table, :udp, :inet)
          {port, table}
        end)

      assert ports == [50_174, 50_175, 49_153]
    end

    test "share one cursor per protocol across families, but not ports in use" do
      table = Sockets.new(49_152)
      {identity, table} = open(table)
      table = Sockets.bind(table, identity, :udp, {@local, 49_153}, nil)

      {:ok, 49_152, table} = Sockets.allocate_port(table, :udp, :inet6)
      {:ok, 49_154, table} = Sockets.allocate_port(table, :udp, :inet)
      assert {:ok, 49_152, _table} = Sockets.allocate_port(table, :tcp, :inet)

      assert Sockets.port_in_use?(table, :udp, :inet, 49_153, {0, 0})
      refute Sockets.port_in_use?(table, :udp, :inet, 49_153, identity)
      refute Sockets.port_in_use?(table, :udp, :inet6, 49_153, {0, 0})
      refute Sockets.port_in_use?(table, :tcp, :inet, 49_153, {0, 0})
    end

    test "run out when all 1024 are taken" do
      {identity, table} = open(Sockets.new(49_152))

      table =
        Enum.reduce(49_152..50_175, table, fn port, table ->
          Sockets.bind(table, identity, :udp, {@local, port}, nil)
        end)

      assert {:error, :ephemeral_ports_exhausted} = Sockets.allocate_port(table, :udp, :inet)
      assert {:ok, 49_152, _table} = Sockets.allocate_port(table, :udp, :inet6)
    end
  end

  describe "demultiplexing" do
    test "prefers the connection, then the bound address, then the wildcard" do
      table = Sockets.new(49_152)
      {wildcard, table} = open(table)
      {bound, table} = open(table)
      {connected, table} = open(table)

      table =
        table
        |> Sockets.bind(wildcard, :udp, {@any, 7}, nil)
        |> Sockets.bind(bound, :udp, {@local, 7}, nil)
        |> Sockets.bind(connected, :udp, {@local, 7}, {@peer, 40_000})

      {wildcard_id, _generation} = wildcard
      {bound_id, _generation} = bound
      {connected_id, _generation} = connected

      assert {^connected_id, _entry} = Sockets.lookup(table, :udp, {@local, 7}, {@peer, 40_000})
      assert {^bound_id, _entry} = Sockets.lookup(table, :udp, {@local, 7}, {@peer, 40_001})

      assert {^wildcard_id, _entry} =
               Sockets.lookup(table, :udp, {<<192, 0, 2, 3>>, 7}, {@peer, 40_000})

      assert Sockets.lookup(table, :tcp, {@local, 7}, {@peer, 40_000}) == nil
      assert Sockets.lookup(table, :udp, {@local, 8}, {@peer, 40_000}) == nil

      {[], table} = Sockets.close(table, bound)
      assert {^wildcard_id, _entry} = Sockets.lookup(table, :udp, {@local, 7}, {@peer, 40_001})

      assert Sockets.port_owners(table, :udp, :inet, 7) ==
               Enum.sort([wildcard_id, connected_id])
    end
  end

  describe "endpoints" do
    test "are checked as the NIF checks them, address, then port, then scope" do
      link_local = [0xFE, 0x80 | List.duplicate(0, 13)] ++ [1]

      for {endpoint, use, expected} <- [
            {%{address: [192, 0, 2, 1], port: 7, scope_id: 0}, :remote, {:ok, {@local, 7, 0}}},
            {%{address: [0, 0, 0, 0], port: 0, scope_id: 0}, :bind, {:ok, {@any, 0, 0}}},
            {%{address: [0, 0, 0, 0], port: 7, scope_id: 0}, :remote, {:error, :invalid_address}},
            {%{address: [192, 0, 2, 1], port: 0, scope_id: 0}, :remote, {:error, :invalid_port}},
            {%{address: [255, 255, 255, 255], port: -1, scope_id: 0}, :bind,
             {:error, :invalid_address}},
            {%{address: [224, 0, 0, 1], port: 7, scope_id: 0}, :bind, {:error, :invalid_address}},
            {%{address: [192, 0, 2], port: 7, scope_id: 0}, :bind, {:error, :invalid_address}},
            {%{address: List.duplicate(0, 10) ++ [255, 255, 192, 0, 2, 1], port: 7, scope_id: 0},
             :bind, {:error, :invalid_address}},
            {%{address: [192, 0, 2, 1], port: 65_536, scope_id: 1}, :bind,
             {:error, :invalid_port}},
            {%{address: [192, 0, 2, 1], port: 7, scope_id: 1}, :bind, {:error, :invalid_scope}},
            {%{address: link_local, port: 7, scope_id: 0}, :bind, {:error, :scope_required}},
            {%{address: link_local, port: 7, scope_id: 0x1_0000_0000}, :bind,
             {:error, :invalid_scope}},
            {%{address: link_local, port: 7, scope_id: 3}, :bind,
             {:ok, {:erlang.list_to_binary(link_local), 7, 3}}}
          ] do
        assert Sockets.endpoint(endpoint, use) == expected, inspect(endpoint)
      end
    end
  end
end
