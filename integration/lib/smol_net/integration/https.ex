defmodule SmolNet.Integration.Https do
  @moduledoc """
  Just enough HTTP/1.1 over an `:ssl` socket for bulk transfers, as a
  client and as a server: one request per connection, bodies framed by
  `content-length` or chunked encoding, and a SHA-256 of every body.

  Reads are passive (`:ssl.recv/3`) or active (`{:ssl, socket, data}`
  messages, with `{:active, N}`), so that transfers exercise both of
  `:ssl`'s receive paths. Neither side times out: a caller bounds the whole
  exchange with `SmolNet.Integration.Soak.within/4`.

  The server answers two requests:

    * `GET /down?bytes=N` - `N` random bytes, with their SHA-256 in
      `x-sha256`;
    * `POST /up` - reads the body and answers with its length and SHA-256
      in `x-upload-bytes` and `x-sha256`.
  """

  @chunk 65_536
  @active_n 8
  @max_head 65_536
  @max_download 1_073_741_824
  @user_agent "smolnet-integration (+https://github.com/ausimian/smolnet)"

  @type mode :: :passive | :active
  @type response :: %{
          status: non_neg_integer(),
          headers: %{String.t() => String.t()},
          bytes: non_neg_integer(),
          sha256: String.t()
        }

  @doc """
  Sends a `GET` for `path` on `host` and reads the whole response.
  """
  @spec get(:ssl.sslsocket(), String.t(), String.t(), mode()) ::
          {:ok, response()} | {:error, term()}
  def get(socket, host, path, mode) do
    with :ok <- :ssl.send(socket, head("GET", host, path, [])) do
      read_response(socket, mode)
    end
  end

  @doc """
  Sends a `POST` of `body` to `path` on `host` and reads the whole
  response.
  """
  @spec post(:ssl.sslsocket(), String.t(), String.t(), binary(), mode()) ::
          {:ok, response()} | {:error, term()}
  def post(socket, host, path, body, mode) do
    headers = [
      {"content-type", "application/octet-stream"},
      {"content-length", Integer.to_string(byte_size(body))}
    ]

    with :ok <- :ssl.send(socket, head("POST", host, path, headers)),
         :ok <- send_body(socket, body) do
      read_response(socket, mode)
    end
  end

  @doc "Returns the lower-case hex SHA-256 of `data`."
  @spec sha256(iodata()) :: String.t()
  def sha256(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  @doc """
  Serves one request on `socket` and returns what it served: `{:down, n}`
  or `{:up, n}`.
  """
  @spec serve(:ssl.sslsocket(), mode()) ::
          {:ok, {:down | :up, non_neg_integer()}} | {:error, term()}
  def serve(socket, mode) do
    reader = reader(socket, mode)

    with {:ok, {method, target}, headers, reader} <- read_head(reader, :http_request) do
      route(socket, reader, method, URI.parse(target), headers)
    end
  end

  defp route(socket, _reader, :GET, %URI{path: "/down", query: query}, _headers) do
    case Integer.parse(URI.decode_query(query || "")["bytes"] || "") do
      {bytes, ""} when bytes in 0..@max_download ->
        body = :crypto.strong_rand_bytes(bytes)
        headers = [{"content-type", "application/octet-stream"}, {"x-sha256", sha256(body)}]

        with :ok <- respond(socket, 200, headers, byte_size(body)),
             :ok <- send_body(socket, body) do
          {:ok, {:down, bytes}}
        end

      _invalid ->
        respond_error(socket, 400, {:bad_query, query})
    end
  end

  defp route(socket, reader, :POST, %URI{path: "/up"}, headers) do
    with {:ok, bytes, sha256, _reader} <- read_body(reader, headers) do
      headers = [{"x-upload-bytes", Integer.to_string(bytes)}, {"x-sha256", sha256}]

      with :ok <- respond(socket, 200, headers, 0) do
        {:ok, {:up, bytes}}
      end
    end
  end

  defp route(socket, _reader, method, uri, _headers) do
    respond_error(socket, 404, {:not_found, method, URI.to_string(uri)})
  end

  defp respond_error(socket, status, reason) do
    _sent = respond(socket, status, [], 0)
    {:error, reason}
  end

  defp respond(socket, status, headers, length) do
    headers = headers ++ [{"content-length", Integer.to_string(length)}, {"connection", "close"}]
    :ssl.send(socket, ["HTTP/1.1 #{status} #{reason(status)}\r\n", format(headers), "\r\n"])
  end

  defp reason(200), do: "OK"
  defp reason(400), do: "Bad Request"
  defp reason(404), do: "Not Found"

  defp head(method, host, path, headers) do
    headers =
      [{"host", host}, {"user-agent", @user_agent}, {"accept", "*/*"}] ++
        headers ++ [{"connection", "close"}]

    [method, " ", path, " HTTP/1.1\r\n", format(headers), "\r\n"]
  end

  defp format(headers), do: Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end)

  defp send_body(socket, body) when byte_size(body) <= @chunk, do: :ssl.send(socket, body)

  defp send_body(socket, body) do
    <<chunk::binary-size(@chunk), rest::binary>> = body

    with :ok <- :ssl.send(socket, chunk), do: send_body(socket, rest)
  end

  defp read_response(socket, mode) do
    reader = reader(socket, mode)

    with {:ok, status, headers, reader} <- read_head(reader, :http_response),
         {:ok, bytes, sha256, _reader} <- read_body(reader, headers) do
      {:ok, %{status: status, headers: headers, bytes: bytes, sha256: sha256}}
    end
  end

  # The reader: the socket, its receive mode, and the bytes read but not yet
  # consumed.

  defp reader(socket, :active) do
    :ok = :ssl.setopts(socket, active: @active_n)
    %{socket: socket, mode: :active, buffer: <<>>}
  end

  defp reader(socket, :passive), do: %{socket: socket, mode: :passive, buffer: <<>>}

  defp fill(%{buffer: buffer} = reader) do
    with {:ok, data} <- receive_data(reader) do
      {:ok, %{reader | buffer: buffer <> data}}
    end
  end

  defp receive_data(%{mode: :passive, socket: socket}), do: :ssl.recv(socket, 0, :infinity)

  defp receive_data(%{mode: :active, socket: socket} = reader) do
    receive do
      {:ssl, ^socket, data} ->
        {:ok, data}

      {:ssl_passive, ^socket} ->
        with :ok <- :ssl.setopts(socket, active: @active_n), do: receive_data(reader)

      {:ssl_closed, ^socket} ->
        {:error, :closed}

      {:ssl_error, ^socket, reason} ->
        {:error, reason}
    end
  end

  defp read_head(reader, kind) do
    with {:ok, start, reader} <- decode(reader, :http_bin),
         {:ok, first} <- start_line(kind, start),
         {:ok, headers, reader} <- read_headers(reader, %{}) do
      {:ok, first, headers, reader}
    end
  end

  defp start_line(:http_response, {:http_response, {1, _minor}, status, _reason}),
    do: {:ok, status}

  defp start_line(:http_request, {:http_request, method, {:abs_path, path}, {1, _minor}}),
    do: {:ok, {method, path}}

  defp start_line(_kind, other), do: {:error, {:bad_start_line, other}}

  defp read_headers(reader, headers) do
    case decode(reader, :httph_bin) do
      {:ok, :http_eoh, reader} ->
        {:ok, headers, reader}

      {:ok, {:http_header, _index, name, _reserved, value}, reader} ->
        name = name |> to_string() |> String.downcase()
        read_headers(reader, Map.put(headers, name, value))

      {:ok, other, _reader} ->
        {:error, {:bad_header, other}}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode(%{buffer: buffer} = reader, type) do
    case :erlang.decode_packet(type, buffer, []) do
      {:ok, {:http_error, line}, _rest} ->
        {:error, {:http_error, line}}

      {:ok, packet, rest} ->
        {:ok, packet, %{reader | buffer: rest}}

      {:more, _length} when byte_size(buffer) > @max_head ->
        {:error, :head_too_long}

      {:more, _length} ->
        with {:ok, reader} <- fill(reader), do: decode(reader, type)

      {:error, reason} ->
        {:error, {:http_error, reason}}
    end
  end

  defp read_body(reader, headers) do
    cond do
      headers["transfer-encoding"] |> to_string() |> String.downcase() == "chunked" ->
        read_chunks(reader, 0, :crypto.hash_init(:sha256))

      length = headers["content-length"] ->
        case Integer.parse(length) do
          {length, ""} when length >= 0 ->
            read_exactly(reader, length)

          _invalid ->
            {:error, {:bad_content_length, length}}
        end

      true ->
        {:error, :no_body_framing}
    end
  end

  defp read_exactly(reader, length) do
    with {:ok, bytes, hash, reader} <- read_span(reader, length, 0, :crypto.hash_init(:sha256)) do
      {:ok, bytes, finish(hash), reader}
    end
  end

  defp finish(hash), do: hash |> :crypto.hash_final() |> Base.encode16(case: :lower)

  defp read_chunks(reader, bytes, hash) do
    with {:ok, line, reader} <- read_line(reader),
         {:ok, size} <- chunk_size(line) do
      read_chunk(reader, size, bytes, hash)
    end
  end

  # The last chunk is empty, and trailers follow it.
  defp read_chunk(reader, 0, bytes, hash) do
    with {:ok, reader} <- skip_trailers(reader), do: {:ok, bytes, finish(hash), reader}
  end

  defp read_chunk(reader, size, bytes, hash) do
    with {:ok, bytes, hash, reader} <- read_span(reader, size, bytes, hash),
         {:ok, "", reader} <- read_line(reader) do
      read_chunks(reader, bytes, hash)
    else
      {:ok, line, _reader} -> {:error, {:bad_chunk_end, line}}
      {:error, _reason} = error -> error
    end
  end

  # Reads `remaining` more body bytes into the count and the running hash.
  defp read_span(reader, 0, bytes, hash), do: {:ok, bytes, hash, reader}

  defp read_span(%{buffer: <<>>} = reader, remaining, bytes, hash) do
    case fill(reader) do
      {:ok, reader} -> read_span(reader, remaining, bytes, hash)
      {:error, reason} -> {:error, {reason, %{read: bytes, remaining: remaining}}}
    end
  end

  defp read_span(%{buffer: buffer} = reader, remaining, bytes, hash) do
    take = min(byte_size(buffer), remaining)
    <<data::binary-size(take), rest::binary>> = buffer

    read_span(
      %{reader | buffer: rest},
      remaining - take,
      bytes + take,
      :crypto.hash_update(hash, data)
    )
  end

  defp chunk_size(line) do
    [size | _extensions] = String.split(line, ";", parts: 2)

    case Integer.parse(String.trim(size), 16) do
      {size, ""} when size >= 0 -> {:ok, size}
      _invalid -> {:error, {:bad_chunk_size, line}}
    end
  end

  defp skip_trailers(reader) do
    case read_line(reader) do
      {:ok, "", reader} -> {:ok, reader}
      {:ok, _trailer, reader} -> skip_trailers(reader)
      {:error, _reason} = error -> error
    end
  end

  defp read_line(%{buffer: buffer} = reader) do
    case :binary.split(buffer, "\r\n") do
      [line, rest] ->
        {:ok, line, %{reader | buffer: rest}}

      [_partial] when byte_size(buffer) > @max_head ->
        {:error, :line_too_long}

      [_partial] ->
        with {:ok, reader} <- fill(reader), do: read_line(reader)
    end
  end
end
