defmodule SmolNet.Inet.Tcp do
  @moduledoc """
  IPv4 callback module for Erlang `:gen_tcp` and `:inet`.

  Socket operations are implemented by `SmolNet.Inet6.Tcp`; this module
  supplies OTP's IPv4 address-resolution callbacks and selects `:inet` during
  connection and listener setup.
  """

  alias SmolNet.Inet6.Tcp, as: Tcp6

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
    do: Tcp6.connect(address, port, [:inet | options])

  def connect(sockaddr, options, timeout) when is_map(sockaddr) and is_list(options),
    do: Tcp6.connect(sockaddr, [:inet | options], timeout)

  def connect(address, port, options, timeout),
    do: Tcp6.connect(address, port, [:inet | options], timeout)

  def listen(port, options), do: Tcp6.listen(port, [:inet | options])
  defdelegate accept(socket), to: Tcp6
  defdelegate accept(socket, timeout), to: Tcp6
  defdelegate fdopen(fd, options), to: Tcp6
  defdelegate send(socket, packet), to: Tcp6
  defdelegate send(socket, packet, options), to: Tcp6
  defdelegate recv(socket, length), to: Tcp6
  defdelegate recv(socket, length, timeout), to: Tcp6
  defdelegate unrecv(socket, data), to: Tcp6
  defdelegate shutdown(socket, how), to: Tcp6
  defdelegate close(socket), to: Tcp6
  defdelegate controlling_process(socket, owner), to: Tcp6
  defdelegate setopts(socket, options), to: Tcp6
  defdelegate getopts(socket, names), to: Tcp6
  defdelegate sockname(socket), to: Tcp6
  defdelegate peername(socket), to: Tcp6
  defdelegate info(socket), to: Tcp6
  defdelegate socket_to_list(socket), to: Tcp6
  defdelegate getstat(socket, names), to: Tcp6
end
