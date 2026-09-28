# Using `:ssl`

`:ssl` can run its TLS connections over a SmolNet stack. `SmolNet.Inet.Tcp`
and `SmolNet.Inet6.Tcp`, the callback modules behind `:gen_tcp`, also work as
`:ssl`'s transport, for clients and servers. `:ssl` does the TLS: the
handshake, the encryption, and certificate verification. SmolNet carries the
bytes.

The socket under each TLS connection is an ordinary SmolNet TCP socket, so the
options, limits, and errors in [Using `:gen_tcp`](gen_tcp.md) apply to it.

## Before you start

`:ssl` is an OTP application and must be running before it is used. List
`:ssl` in `extra_applications` in `mix.exs`, or start it with
`Application.ensure_all_started/1`, as the example below does.

A SmolNet stack must be running too, with a link that carries its packets.
The example uses `SmolNet.Loopback`, as the `:gen_tcp` guide's loopback
example does, so the client and the server share one stack and need no
network.

## A complete loopback example

This example makes a throwaway certificate authority and a server certificate
with `:public_key.pkix_test_data/1`, then runs a TLS echo over one loopback
stack. It needs nothing beyond OTP:

```elixir
{:ok, _apps} = Application.ensure_all_started(:ssl)

{:ok, _link, stack} =
  SmolNet.Loopback.start_link(
    addresses: [{{127, 0, 0, 1}, 8}, {{0, 0, 0, 0, 0, 0, 0, 1}, 128}]
  )

# A CA, and a server certificate it signs for this host's name.
key = [key: {:namedCurve, :secp256r1}, digest: :sha256]
chain = %{root: key, intermediates: [], peer: key}

%{server_config: server_config, client_config: client_config} =
  :public_key.pkix_test_data(%{server_chain: chain, client_chain: chain})

transport = [
  :inet,
  {:cb_info, {SmolNet.Inet.Tcp, :tcp, :tcp_closed, :tcp_error}},
  {:smolnet_stack, stack},
  :binary,
  {:active, false}
]

server_tls = [cert: server_config[:cert], key: server_config[:key]]

client_tls = [
  verify: :verify_peer,
  cacerts: client_config[:cacerts],
  server_name_indication: :net_adm.localhost()
]

{:ok, listener} =
  :ssl.listen(8443, [{:ip, {127, 0, 0, 1}} | transport] ++ server_tls)

server =
  Task.async(fn ->
    {:ok, accepted} = :ssl.transport_accept(listener, 5_000)
    {:ok, tls} = :ssl.handshake(accepted, 5_000)
    {:ok, request} = :ssl.recv(tls, 0, 5_000)
    :ok = :ssl.send(tls, ["echo: ", request])
    :ok = :ssl.close(tls)
  end)

{:ok, client} =
  :ssl.connect({127, 0, 0, 1}, 8443, transport ++ client_tls, 5_000)

:ok = :ssl.send(client, "hello")
{:ok, "echo: hello"} = :ssl.recv(client, 0, 5_000)

:ok = Task.await(server, 5_000)
:ok = :ssl.close(client)
:ok = :ssl.close(listener)
:ok = SmolNet.stop_stack(stack)
```

`pkix_test_data/1` names the server certificate after this host, as
`:net_adm.localhost/0` returns it, so the client asks for that name. The
client verifies the certificate against the throwaway CA, as a real client
verifies one against a real CA; see
[Verifying the server](#verifying-the-server).

The examples in the sections below continue from this one, as if its last
line, which stops the stack, had not run yet. The one exception is in
[Verifying the server](#verifying-the-server), which needs a real network.

## Transport options

`:ssl` takes its transport through the `cb_info` option. Use the callback for
the address family, with the matching family option:

| Family | `cb_info` | Family option |
| --- | --- | --- |
| IPv4 | `{SmolNet.Inet.Tcp, :tcp, :tcp_closed, :tcp_error}` | `:inet` |
| IPv6 | `{SmolNet.Inet6.Tcp, :tcp, :tcp_closed, :tcp_error}` | `:inet6` |

The three atoms are the tags of the transport's active-mode messages, the same
as `:gen_tcp`'s. `:ssl` receives those messages itself; the process that owns
a TLS connection receives `:ssl`'s messages instead.

The transport options go in the same list as the TLS options. `:ssl` keeps the
options it knows, and passes the rest, such as `:smolnet_stack`, the family,
`:ip`, and `:nodelay`, to the transport. An option that neither supports
returns `{:error, :einval}`. `:ssl` handles `:mode`, `:active`, and `:packet`
itself, on the decrypted data.

Over IPv6, only the family and the callback change:

```elixir
transport6 = [
  :inet6,
  {:cb_info, {SmolNet.Inet6.Tcp, :tcp, :tcp_closed, :tcp_error}},
  {:smolnet_stack, stack},
  :binary,
  {:active, false}
]

{:ok, listener6} =
  :ssl.listen(8443, [{:ip, {0, 0, 0, 0, 0, 0, 0, 1}} | transport6] ++ server_tls)

server =
  Task.async(fn ->
    {:ok, accepted} = :ssl.transport_accept(listener6, 5_000)
    {:ok, tls} = :ssl.handshake(accepted, 5_000)
    {:error, :closed} = :ssl.recv(tls, 0, 5_000)
  end)

{:ok, client6} =
  :ssl.connect({0, 0, 0, 0, 0, 0, 0, 1}, 8443, transport6 ++ client_tls, 5_000)

:ok = :ssl.close(client6)
{:error, :closed} = Task.await(server, 5_000)
:ok = :ssl.close(listener6)
```

## Verifying the server

Certificate verification is `:ssl`'s job, not SmolNet's. SmolNet carries the
handshake's bytes and never looks at a certificate, so configure verification
as for `:ssl` over the host's network. A client of a public server verifies
it against the operating system's trusted CAs. This example needs a stack
whose link reaches that server, rather than the loopback stack above, with
`transport` naming that stack:

```elixir
public_tls = [
  verify: :verify_peer,
  cacerts: :public_key.cacerts_get(),
  server_name_indication: ~c"example.com",
  customize_hostname_check: [
    match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
  ]
]

# A placeholder: example.com's address, resolved as your network requires.
server_address = {192, 0, 2, 10}

{:ok, tls} = :ssl.connect(server_address, 443, transport ++ public_tls, 5_000)
```

- `verify: :verify_peer` checks the server's certificate chain against
  `cacerts`. It is the default for an `:ssl` client since OTP 26, which then
  refuses to connect without `cacerts` or `cacertfile`.
- `:public_key.cacerts_get/0` returns the operating system's trusted CAs.
- `server_name_indication` names the server. `:ssl` sends the name in the
  handshake and checks the certificate against it. Without it, `:ssl` checks
  the certificate against the address, which a certificate rarely names.
- `customize_hostname_check` with the `:https` match function accepts
  wildcard certificates, such as one for `*.example.com`, as browsers do.

Connect to an address. SmolNet's transport does not resolve names, so
`:ssl.connect/4` with a host name returns `{:error, :einval}`. Resolve the
name as your network requires, and give it to `:ssl` as
`server_name_indication`.

## Servers

`:ssl.listen/2` opens a SmolNet listener through the transport, with the
transport options, such as `:ip` and `:backlog`, that `:gen_tcp.listen/2`
takes. Port 0 picks a free port, and `:ssl.sockname/1` reports it:

```elixir
{:ok, listener} =
  :ssl.listen(0, [{:ip, {127, 0, 0, 1}}, {:backlog, 16} | transport] ++ server_tls)

{:ok, {{127, 0, 0, 1}, port}} = :ssl.sockname(listener)
```

A server accepts in two steps. `:ssl.transport_accept/2` accepts a TCP
connection, and `:ssl.handshake/2` runs the TLS handshake on it. A server
with many clients accepts in one process and hands each accepted socket to a
process of its own, with `:ssl.controlling_process/2`, to run the handshake,
so that a slow handshake does not hold up the next accept.

Closing the listener with `:ssl.close/1` closes the SmolNet listener.
Connections already accepted stay open.

## Upgrading a connected socket

A `:gen_tcp` socket already connected over SmolNet can become a TLS
connection, for a protocol that starts in the clear and then switches, such as
SMTP's STARTTLS. Upgrade the client side with `:ssl.connect/3` and the server
side with `:ssl.handshake/3`, and pass `cb_info` in their options, as for a
new connection:

```elixir
cb_info = {SmolNet.Inet.Tcp, :tcp, :tcp_closed, :tcp_error}

tcp_options = [
  {:tcp_module, SmolNet.Inet.Tcp},
  {:smolnet_stack, stack},
  :inet,
  :binary,
  {:active, false}
]

{:ok, tcp_listener} = :gen_tcp.listen(2525, [{:ip, {127, 0, 0, 1}} | tcp_options])

server =
  Task.async(fn ->
    {:ok, socket} = :gen_tcp.accept(tcp_listener, 5_000)
    {:ok, "STARTTLS\r\n"} = :gen_tcp.recv(socket, 10, 5_000)
    :ok = :gen_tcp.send(socket, "GO\r\n")
    {:ok, tls} = :ssl.handshake(socket, [{:cb_info, cb_info} | server_tls], 5_000)
    {:ok, request} = :ssl.recv(tls, 0, 5_000)
    :ok = :ssl.send(tls, ["echo: ", request])
    :ok = :ssl.close(tls)
  end)

{:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, 2525, tcp_options, 5_000)
:ok = :gen_tcp.send(socket, "STARTTLS\r\n")
{:ok, "GO\r\n"} = :gen_tcp.recv(socket, 4, 5_000)
{:ok, tls} = :ssl.connect(socket, [{:cb_info, cb_info} | client_tls], 5_000)

:ok = :ssl.send(tls, "hello")
{:ok, "echo: hello"} = :ssl.recv(tls, 0, 5_000)

:ok = Task.await(server, 5_000)
:ok = :ssl.close(tls)
:ok = :gen_tcp.close(tcp_listener)
```

Once upgraded, the socket belongs to `:ssl`. Use it only through the TLS
connection that `:ssl` returned.

## Active mode, options, and closing

A TLS connection has an active mode of its own, with the same values as
`:gen_tcp`'s: `false`, `true`, `:once`, or a count. Set it with
`:ssl.setopts/2`. The owner then receives `{:ssl, tls, data}`,
`{:ssl_closed, tls}`, `{:ssl_error, tls, reason}`, and, when a count runs
out, `{:ssl_passive, tls}`.

Socket options that `:ssl` does not handle itself pass through to SmolNet,
with the meanings and limits that [Using `:gen_tcp`](gen_tcp.md) gives them.
They include `nodelay`, `keepalive`, `buffer`, `recbuf`, `sndbuf`, and
`send_timeout`. Set them when connecting or listening, and read them with
`:ssl.getopts/2`. All but `recbuf` and `sndbuf` can also be changed later
with `:ssl.setopts/2`:

```elixir
server =
  Task.async(fn ->
    {:ok, accepted} = :ssl.transport_accept(listener, 5_000)
    {:ok, tls} = :ssl.handshake(accepted, 5_000)
    :ok = :ssl.send(tls, "hello")
    :ssl.recv(tls, 0, 5_000)
  end)

{:ok, tls} =
  :ssl.connect({127, 0, 0, 1}, port, transport ++ [nodelay: true] ++ client_tls, 5_000)

:ok = :ssl.setopts(tls, keepalive: true, active: :once)
{:ok, options} = :ssl.getopts(tls, [:nodelay, :keepalive])
[keepalive: true, nodelay: true] = Enum.sort(options)

receive do
  {:ssl, ^tls, "hello"} -> :ok
after
  5_000 -> exit(:timeout)
end

:ok = :ssl.close(tls)
{:error, :closed} = Task.await(server, 5_000)
```

`recbuf` and `sndbuf` are fixed when the socket is created, and
`:ssl.setopts/2` refuses them with
`{:error, {:options, {:socket_options, [recbuf: 4096], :einval}}}`.

`:ssl.close/1` sends TLS's `close_notify` alert and closes the SmolNet socket.
The peer's `:ssl.recv/3` then returns `{:error, :closed}`, as above. A TLS
connection also closes when its owner exits.

## Errors and timeouts

Connecting returns the transport's errors, such as `{:error, :econnrefused}`
when nothing listens on the port, or `{:error, :enetunreach}` when the stack
has no route. A failed handshake returns `:ssl`'s own
`{:error, {:tls_alert, {alert, description}}}`. The timeout given to
`:ssl.connect/4` applies to the TCP connection, and again to the handshake.
A receive that times out returns `{:error, :timeout}` and leaves the
connection open.

Once a connection is up, `:ssl` does not pass on the transport's reason. When
the SmolNet socket underneath fails, `:ssl` ends the TLS connection as if the
peer had closed it: a pending or later `:ssl.recv/3` or `:ssl.send/2` returns
`{:error, :closed}`, and an active connection's owner receives
`{:ssl_closed, tls}`. That covers:

- A reset from the peer, where `:gen_tcp` would report `:econnreset`.
- A peer that vanishes while data is outstanding. After 924.6 s without an
  answer, the user timeout fails the socket with `:etimedout`.
- A peer that vanishes from an idle connection. Nothing notices unless
  `keepalive` is on: then, after 2 hours of silence and 9 unanswered probes
  75 s apart, the socket fails with `:etimedout`. `:ssl` sends nothing on an
  idle connection itself, so a protocol that needs to notice sooner sends
  its own pings.
- A stack that fails, or whose link dies under `link_down: :stop`, where
  `:gen_tcp` would report `:enetdown`.
- A stack stopped with `SmolNet.stop_stack/1`, where `:gen_tcp` would report
  `:closed`.

The [`:gen_tcp` guide](gen_tcp.md) describes these timers and failures. To
tell a stack's loss from a peer's, watch the stack with `SmolNet.monitor/1`.

```elixir
{:error, :econnrefused} =
  :ssl.connect({127, 0, 0, 1}, 9, transport ++ client_tls, 5_000)

server =
  Task.async(fn ->
    {:ok, accepted} = :ssl.transport_accept(listener, 5_000)
    {:ok, tls} = :ssl.handshake(accepted, 5_000)
    :ok = :ssl.send(tls, "ready")
    :ssl.recv(tls, 0, 5_000)
  end)

{:ok, tls} = :ssl.connect({127, 0, 0, 1}, port, transport ++ client_tls, 5_000)
{:ok, "ready"} = :ssl.recv(tls, 0, 5_000)

# A receive timeout leaves the connection open.
{:error, :timeout} = :ssl.recv(tls, 0, 100)

# Stopping the stack ends both TLS connections.
:ok = SmolNet.stop_stack(stack)
{:error, :closed} = :ssl.recv(tls, 0, 5_000)
{:error, :closed} = Task.await(server, 5_000)
```

## Memory

A SmolNet socket's process hibernates after 5 s without work, which drops
what its last transfer left on its heap. `:ssl`'s own connection processes do
not, by default, so an idle TLS connection keeps its last records alive for
as long as it idles. For many long-lived, mostly idle connections, set
`:ssl`'s `hibernate_after` option, in milliseconds, at both ends:

```elixir
server_tls = [{:hibernate_after, 5_000} | server_tls]
client_tls = [{:hibernate_after, 5_000} | client_tls]
```

Each hibernation costs a garbage collection. Choose a time well above the gaps
within an exchange, so that a busy connection never pays for it.
