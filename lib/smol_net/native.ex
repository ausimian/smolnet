defmodule SmolNet.Native do
  @moduledoc false

  use Rustler, otp_app: :smolnet, crate: :smolnet_nif

  @spec health() :: :ok
  def health, do: :erlang.nif_error(:nif_not_loaded)
end
