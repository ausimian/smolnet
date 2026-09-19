defmodule SmolNet.Inet.Udp do
  @moduledoc """
  IPv4 callback module for Erlang `:gen_udp` and `:inet`.

  Socket operations are implemented by `SmolNet.Inet6.Udp`; this module
  supplies OTP's IPv4 address-resolution callbacks and selects `:inet` when a
  socket is opened.
  """

  alias SmolNet.Inet6.Udp, as: Udp6

  @spec family() :: :inet
  def family, do: :inet

  def getserv(port), do: :inet_udp.getserv(port)
  def getaddr(address), do: :inet_udp.getaddr(address)
  def getaddr(address, timer), do: :inet_udp.getaddr(address, timer)
  def translate_ip(address), do: :inet_udp.translate_ip(address)

  def open(port), do: Udp6.open(port, [:inet])
  def open(port, options), do: Udp6.open(port, [:inet | options])
  defdelegate fdopen(fd, options), to: Udp6
  defdelegate send(socket, packet), to: Udp6
  defdelegate send(socket, destination, packet), to: Udp6
  defdelegate send(socket, destination, ancillary, packet), to: Udp6
  defdelegate send(socket, address, port, ancillary, packet), to: Udp6
  defdelegate recv(socket, length), to: Udp6
  defdelegate recv(socket, length, timeout), to: Udp6
  defdelegate connect(socket, sockaddr), to: Udp6
  defdelegate connect(socket, address, port), to: Udp6
  defdelegate close(socket), to: Udp6
  defdelegate controlling_process(socket, owner), to: Udp6
  defdelegate setopts(socket, options), to: Udp6
  defdelegate getopts(socket, names), to: Udp6
  defdelegate sockname(socket), to: Udp6
  defdelegate peername(socket), to: Udp6
  defdelegate info(socket), to: Udp6
  defdelegate socket_to_list(socket), to: Udp6
  defdelegate getstat(socket, names), to: Udp6
end
