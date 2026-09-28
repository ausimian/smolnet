defmodule SmolNet.Integration.Crawl.Hosts do
  @moduledoc """
  The hosts a crawl visits: the top of a Tranco list, fetched from
  tranco-list.eu when the run starts, or a list in a file.

  [Tranco](https://tranco-list.eu) is a research-oriented ranking of the
  most popular sites, free to use. Each daily list has a permanent ID, so
  a run records the ID of the list it used, and `tranco:<ID>` fetches that
  list again. The list is fetched through the host's stack, never
  SmolNet's, with two requests, and saved into the run's output directory
  as `tranco-<ID>-top<N>.csv` rather than kept in the repository; a run
  can reuse such a file as its list.

  A list file holds one host per line, either bare or as Tranco's
  `rank,host`; blank lines and lines starting with `#` are skipped, and so
  are duplicates. `fixture/0` is a small committed list, for trying the
  crawl out.
  """

  alias SmolNet.Integration.Https

  @api "https://tranco-list.eu/api/lists"
  @download "https://tranco-list.eu/download"
  @fixture Path.expand("../../../../support/crawl-hosts.csv", __DIR__)
  @max_top 10_000
  @fetch_timeout 60_000
  @citation "Le Pochat et al., Tranco: A Research-Oriented Top Sites Ranking " <>
              "Hardened Against Manipulation, NDSS 2019"

  @type info :: %{atom() => String.t() | non_neg_integer() | nil}

  @doc "Returns the path of the committed fixture list."
  @spec fixture() :: Path.t()
  def fixture, do: @fixture

  @doc "Returns the most hosts `--top` may ask for."
  @spec max_top() :: pos_integer()
  def max_top, do: @max_top

  @doc """
  Loads the first `top` hosts of `list`: `tranco` for the latest Tranco
  list, `tranco:<ID>` for that one, `fixture` for `fixture/0`, or a file's
  path. A Tranco list is saved into `out_dir`.

  Returns the hosts and what they came from, for the verdict.
  """
  @spec load(String.t(), pos_integer(), Path.t()) ::
          {:ok, [String.t()], info()} | {:error, String.t()}
  def load("tranco", top, out_dir), do: fetch(nil, top, out_dir)
  def load("tranco:" <> id, top, out_dir), do: fetch(id, top, out_dir)
  def load("fixture", top, _out_dir), do: read(@fixture, top, "fixture")
  def load(path, top, _out_dir), do: read(Path.expand(path), top, "file")

  @doc """
  Parses a list: one host per line, bare or as `rank,host`. Skips blank
  lines, `#` comments, duplicates and anything that is not a host name.
  """
  @spec parse(String.t()) :: [String.t()]
  def parse(text) do
    text
    |> String.split(["\r\n", "\n"])
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))
    |> Enum.map(fn line -> line |> String.split(",") |> List.last() |> String.trim() end)
    |> Enum.map(&String.downcase/1)
    |> Enum.filter(&host?/1)
    |> Enum.uniq()
  end

  defp host?(name) do
    byte_size(name) <= 253 and
      name =~ ~r/\A[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+\z/
  end

  defp read(path, top, source) do
    case File.read(path) do
      {:ok, text} ->
        hosts = text |> parse() |> Enum.take(top)
        info = %{source: source, file: path, top: top, hosts: length(hosts)}
        if hosts == [], do: {:error, "#{path} lists no hosts"}, else: {:ok, hosts, info}

      {:error, reason} ->
        {:error, "could not read the host list #{path}: #{:file.format_error(reason)}"}
    end
  end

  # The list's metadata first, for its ID and date, then its top `top`.
  defp fetch(id, top, out_dir) do
    metadata = if id, do: "#{@api}/id/#{URI.encode(id)}", else: "#{@api}/date/latest"

    with {:ok, body} <- get(metadata),
         {:ok, %{"list_id" => id} = list} <- decode(body, metadata),
         url = "#{@download}/#{URI.encode(id)}/#{top}",
         {:ok, csv} <- get(url) do
      file = Path.join(out_dir, "tranco-#{id}-top#{top}.csv")
      File.mkdir_p!(out_dir)
      File.write!(file, csv)
      hosts = csv |> parse() |> Enum.take(top)

      info = %{
        source: "tranco",
        id: id,
        created_on: list["created_on"],
        url: url,
        file: file,
        top: top,
        hosts: length(hosts),
        citation: @citation
      }

      if hosts == [], do: {:error, "#{url} listed no hosts"}, else: {:ok, hosts, info}
    end
  end

  defp decode(body, url) do
    case JSON.decode(body) do
      {:ok, %{"list_id" => id, "available" => true} = list} when is_binary(id) -> {:ok, list}
      {:ok, other} -> {:error, "#{url} offered no available list: #{inspect(other, limit: 10)}"}
      {:error, reason} -> {:error, "#{url} did not return JSON: #{inspect(reason)}"}
    end
  end

  defp get(url) do
    {:ok, _started} = Application.ensure_all_started(:inets)

    ssl = [
      verify: :verify_peer,
      cacerts: :public_key.cacerts_get(),
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
    ]

    request =
      {String.to_charlist(url), [{~c"user-agent", String.to_charlist(Https.user_agent())}]}

    options = [ssl: ssl, timeout: @fetch_timeout, connect_timeout: 10_000]

    case :httpc.request(:get, request, options, body_format: :binary) do
      {:ok, {{_version, 200, _reason}, _headers, body}} ->
        {:ok, body}

      {:ok, {{_version, status, _reason}, _headers, _body}} ->
        {:error, "#{url} answered #{status}"}

      {:error, reason} ->
        {:error, "could not fetch #{url}: #{inspect(reason)}"}
    end
  end
end
