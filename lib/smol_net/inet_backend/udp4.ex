defmodule SmolNet.InetBackend.Udp4 do
  @moduledoc """
  IPv4 callback module for Erlang `:gen_udp` and `:inet`.

  Socket operations are implemented by `SmolNet.InetBackend.Udp`; this module
  supplies OTP's IPv4 address-resolution callbacks and selects `:inet` when a
  socket is opened.
  """

  alias SmolNet.InetBackend.Udp

  @spec family() :: :inet
  def family, do: :inet

  def getserv(port), do: :inet_udp.getserv(port)
  def getaddr(address), do: :inet_udp.getaddr(address)
  def getaddr(address, timer), do: :inet_udp.getaddr(address, timer)
  def translate_ip(address), do: :inet_udp.translate_ip(address)

  def open(port), do: Udp.open(port, [:inet])
  def open(port, options), do: Udp.open(port, [:inet | options])
  defdelegate fdopen(fd, options), to: Udp
  defdelegate send(socket, packet), to: Udp
  defdelegate send(socket, destination, packet), to: Udp
  defdelegate send(socket, destination, ancillary, packet), to: Udp
  defdelegate send(socket, address, port, ancillary, packet), to: Udp
  defdelegate recv(socket, length), to: Udp
  defdelegate recv(socket, length, timeout), to: Udp
  defdelegate connect(socket, sockaddr), to: Udp
  defdelegate connect(socket, address, port), to: Udp
  defdelegate close(socket), to: Udp
  defdelegate controlling_process(socket, owner), to: Udp
  defdelegate setopts(socket, options), to: Udp
  defdelegate getopts(socket, names), to: Udp
  defdelegate sockname(socket), to: Udp
  defdelegate peername(socket), to: Udp
  defdelegate info(socket), to: Udp
  defdelegate socket_to_list(socket), to: Udp
  defdelegate getstat(socket, names), to: Udp
end
