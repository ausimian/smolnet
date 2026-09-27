defmodule SmolNet.Integration.Ownership do
  @moduledoc """
  Hands files a script created under `sudo` back to the user who ran it, so
  that run artifacts and the built helper stay usable without root.
  """

  @doc """
  Changes the owner of `path` to the user named by `SUDO_UID` and
  `SUDO_GID`: recursively, for a directory, unless `recursive: false`. Does
  nothing when not running under `sudo`, or when `path` does not exist.
  """
  @spec restore(Path.t(), keyword()) :: :ok
  def restore(path, options \\ []) do
    flags = if Keyword.get(options, :recursive, true), do: ["-R"], else: []

    with uid when is_binary(uid) <- System.get_env("SUDO_UID"),
         gid when is_binary(gid) <- System.get_env("SUDO_GID"),
         true <- File.exists?(path) do
      _result = System.cmd("chown", flags ++ ["#{uid}:#{gid}", path], stderr_to_stdout: true)
      :ok
    else
      _not_sudo -> :ok
    end
  end
end
