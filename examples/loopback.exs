# A complete, self-contained `gen_tcp` request and response over one stack.
#
# `SmolNet.Loopback` feeds every packet the stack emits back into that same
# stack, so the client and the server below share one network with no external
# transport, no peer stack, and no privileges.
#
#     mix run examples/loopback.exs

{:ok, _link, stack} =
  SmolNet.Loopback.start_link(
    addresses: [{{127, 0, 0, 1}, 8}, {{0, 0, 0, 0, 0, 0, 0, 1}, 128}]
  )

options = [
  {:tcp_module, SmolNet.InetBackend.Tcp4},
  {:smolnet_stack, stack},
  :inet,
  :binary,
  {:active, false},
  {:ip, :loopback}
]

{:ok, listener} = :gen_tcp.listen(8080, options)

# The accepted socket belongs to the process that accepts it, so the server
# runs in its own process just as it would against a real network.
server =
  Task.async(fn ->
    {:ok, socket} = :gen_tcp.accept(listener, 5_000)
    {:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
    :ok = :gen_tcp.send(socket, "echo: " <> request)
    :ok = :gen_tcp.close(socket)
  end)

{:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, 8080, options, 5_000)
:ok = :gen_tcp.send(client, "hello")
{:ok, "echo: hello"} = :gen_tcp.recv(client, 0, 5_000)

:ok = Task.await(server, 5_000)
:ok = :gen_tcp.close(client)
:ok = :gen_tcp.close(listener)

# Stopping the stack stops the link that loops it.
:ok = SmolNet.stop_stack(stack)
