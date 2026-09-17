defmodule SmolNet.NativeArtifact do
  @moduledoc false

  @spec pinned_checksum!(Path.t(), String.t()) :: String.t()
  def pinned_checksum!(path, asset) do
    unless File.regular?(path) do
      raise ArgumentError,
            "missing #{Path.basename(path)}; maintainers must generate it after NIF assets are published"
    end

    {checksums, _binding} = Code.eval_file(path)

    case checksums do
      %{^asset => "sha256:" <> checksum} when byte_size(checksum) == 64 ->
        if checksum =~ ~r/\A[0-9a-f]{64}\z/ do
          checksum
        else
          raise ArgumentError, "invalid SHA-256 for #{asset} in #{Path.basename(path)}"
        end

      %{} ->
        raise ArgumentError, "no pinned checksum for #{asset} in #{Path.basename(path)}"

      _other ->
        raise ArgumentError, "invalid checksum manifest #{Path.basename(path)}"
    end
  end

  @spec checksum_valid?(Path.t(), String.t()) :: boolean()
  def checksum_valid?(path, expected) do
    File.regular?(path) and secure_compare(sha256_file(path), expected)
  end

  @spec verify_checksum!(Path.t(), String.t()) :: :ok
  def verify_checksum!(path, expected) do
    actual = sha256_file(path)

    if secure_compare(actual, expected) do
      :ok
    else
      raise ArgumentError,
            "precompiled NIF checksum mismatch for #{Path.basename(path)}; " <>
              "expected #{expected}, got #{actual}"
    end
  end

  @spec verify_archive(Path.t(), String.t()) :: :ok | {:error, String.t()}
  def verify_archive(path, expected_entry) do
    case :erl_tar.table(String.to_charlist(path), [:compressed, :verbose]) do
      {:ok, table} ->
        entries = Enum.map(table, fn entry -> {to_string(elem(entry, 0)), elem(entry, 1)} end)
        verify_entries(entries, expected_entry)

      {:error, reason} ->
        {:error, "could not read archive: #{inspect(reason)}"}
    end
  end

  @spec verify_archive!(Path.t(), String.t()) :: :ok
  def verify_archive!(path, expected_entry) do
    case verify_archive(path, expected_entry) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, "refusing precompiled NIF archive: #{reason}"
    end
  end

  @spec verify_entries([{String.t(), atom()}], String.t()) :: :ok | {:error, String.t()}
  def verify_entries([{expected_entry, :regular}], expected_entry), do: :ok

  def verify_entries(entries, expected_entry) do
    details = Enum.map_join(entries, ", ", fn {name, type} -> "#{inspect(name)} (#{type})" end)

    {:error,
     "expected exactly one regular entry named #{inspect(expected_entry)}, got: #{details}"}
  end

  defp sha256_file(path) do
    path
    |> File.read!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp secure_compare(left, right)
       when is_binary(left) and is_binary(right) and byte_size(left) == byte_size(right) do
    left
    |> :binary.bin_to_list()
    |> Enum.zip(:binary.bin_to_list(right))
    |> Enum.reduce(0, fn {a, b}, acc -> Bitwise.bor(acc, Bitwise.bxor(a, b)) end)
    |> Kernel.==(0)
  end

  defp secure_compare(_left, _right), do: false
end
