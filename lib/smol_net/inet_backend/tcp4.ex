defmodule SmolNet.InetBackend.Tcp4 do
  @moduledoc """
  IPv4 callback module for Erlang `:gen_tcp` and `:inet`.

  Socket operations are implemented by `SmolNet.InetBackend.Tcp`; this module
  supplies OTP's IPv4 address-resolution callbacks and selects `:inet` during
  connection and listener setup.
  """

  alias SmolNet.InetBackend.Tcp

  @spec family() :: :inet
  def family, do: :inet

  def mask(mask, address), do: :inet_tcp.mask(mask, address)
  def parse_address(address), do: :inet_tcp.parse_address(address)
  def translate_ip(address), do: :inet_tcp.translate_ip(address)
  def getserv(port), do: :inet_tcp.getserv(port)
  def getaddr(address), do: :inet_tcp.getaddr(address)
  def getaddr(address, timer), do: :inet_tcp.getaddr(address, timer)
  def getaddrs(address), do: :inet_tcp.getaddrs(address)
  def getaddrs(address, timer), do: :inet_tcp.getaddrs(address, timer)

  def connect(address, port, options) when is_integer(port) and is_list(options),
    do: Tcp.connect(address, port, [:inet | options])

  def connect(sockaddr, options, timeout) when is_map(sockaddr) and is_list(options),
    do: Tcp.connect(sockaddr, [:inet | options], timeout)

  def connect(address, port, options, timeout),
    do: Tcp.connect(address, port, [:inet | options], timeout)

  def listen(port, options), do: Tcp.listen(port, [:inet | options])
  defdelegate accept(socket), to: Tcp
  defdelegate accept(socket, timeout), to: Tcp
  defdelegate fdopen(fd, options), to: Tcp
  defdelegate send(socket, packet), to: Tcp
  defdelegate send(socket, packet, options), to: Tcp
  defdelegate recv(socket, length), to: Tcp
  defdelegate recv(socket, length, timeout), to: Tcp
  defdelegate unrecv(socket, data), to: Tcp
  defdelegate shutdown(socket, how), to: Tcp
  defdelegate close(socket), to: Tcp
  defdelegate controlling_process(socket, owner), to: Tcp
  defdelegate setopts(socket, options), to: Tcp
  defdelegate getopts(socket, names), to: Tcp
  defdelegate sockname(socket), to: Tcp
  defdelegate peername(socket), to: Tcp
  defdelegate info(socket), to: Tcp
  defdelegate socket_to_list(socket), to: Tcp
  defdelegate getstat(socket, names), to: Tcp
end
