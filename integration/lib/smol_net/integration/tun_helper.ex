defmodule SmolNet.Integration.TunHelper do
  @moduledoc """
  Builds and locates the `tun_helper` program that `SmolNet.Integration.TunLink`
  runs as a port.

  The helper is one C file, `integration/tun_helper/tun_helper.c`, compiled
  with the system C compiler into `integration/tun_helper/_build/`. It is
  rebuilt whenever the source is newer than the binary, so a checkout never
  runs a stale helper. See the source for the port protocol.
  """

  alias SmolNet.Integration.Ownership

  @root Path.expand("../../../tun_helper", __DIR__)
  @source Path.join(@root, "tun_helper.c")
  @binary Path.join([@root, "_build", "tun_helper"])
  @cflags ~w(-O2 -std=c11 -Wall -Wextra -Werror)

  @doc "Returns the path of the helper binary, building it first if it is stale."
  @spec ensure_built!() :: Path.t()
  def ensure_built! do
    if stale?() do
      build!()
    end

    @binary
  end

  @doc "Returns the path the helper binary is built to."
  @spec path() :: Path.t()
  def path, do: @binary

  defp stale? do
    case {File.stat(@binary, time: :posix), File.stat(@source, time: :posix)} do
      {{:ok, binary}, {:ok, source}} -> binary.mtime < source.mtime
      {{:error, _reason}, _source} -> true
      {_binary, {:error, reason}} -> raise "cannot read #{@source}: #{inspect(reason)}"
    end
  end

  defp build! do
    compiler =
      Enum.find_value(["cc", "gcc", "clang"], &System.find_executable/1) ||
        raise "no C compiler (cc, gcc or clang) found to build #{@source}"

    File.mkdir_p!(Path.dirname(@binary))
    temporary = @binary <> ".#{System.unique_integer([:positive])}"

    case System.cmd(compiler, @cflags ++ ["-o", temporary, @source], stderr_to_stdout: true) do
      {_output, 0} ->
        File.rename!(temporary, @binary)
        Ownership.restore(Path.dirname(@binary))

      {output, status} ->
        File.rm(temporary)
        raise "building #{@source} failed (exit #{status}):\n#{output}"
    end
  end
end
