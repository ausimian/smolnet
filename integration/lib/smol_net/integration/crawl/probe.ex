defmodule SmolNet.Integration.Crawl.Probe do
  @moduledoc """
  One visit to one host over one stack: a TCP connect, a TLS handshake
  over it, and `HEAD /`, each within the visit's timeout, and its outcome
  classified.

  | class | what happened |
  | --- | --- |
  | `ok` | an HTTP response came back, whatever its status |
  | `connect_timeout` | the TCP connect timed out |
  | `refused` | the SYN was answered with a RST |
  | `reset` | a RST ended the connection after it was made |
  | `unreachable` | no route to the host |
  | `system_limit` | the stack had no room for another socket |
  | `tls_alert` | the handshake failed with a TLS alert, sent or received |
  | `tls_timeout`, `tls_closed`, `tls_error` | the handshake timed out, was closed or failed otherwise |
  | `http_timeout`, `http_closed`, `http_error` | so did the request |
  | `other` | anything else, with its reason |
  """

  alias SmolNet.Integration.Https
  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak.Context
  alias SmolNet.Integration.Tls

  @type stage :: :tcp | :tls | :http
  @type outcome :: %{
          class: atom(),
          stage: stage(),
          status: non_neg_integer() | nil,
          reason: String.t() | nil
        }

  @doc """
  Visits `host` at `address` and `port` from `role`, with a timeout of
  `timeout` milliseconds for each stage, `tls` as the client's TLS options
  and `buffer` as a SmolNet socket's buffers (`nil` for its default).
  """
  @spec visit(Context.t(), Network.role(), String.t(), :inet.ip_address(), keyword()) :: outcome()
  def visit(context, role, host, address, options) do
    timeout = Keyword.fetch!(options, :timeout)
    family = if tuple_size(address) == 4, do: :inet, else: :inet6

    tcp_options =
      Network.tcp_options(context, role, family) ++
        Tls.buffer_options(context, role, Keyword.get(options, :buffer)) ++
        [:binary, active: false]

    port = Keyword.get(options, :port, 443)

    result =
      with {:tcp, {:ok, socket}} <- {:tcp, :gen_tcp.connect(address, port, tcp_options, timeout)} do
        tls = Keyword.fetch!(options, :tls)

        case Tls.upgrade(context, role, socket, family, tls, timeout) do
          {:ok, tls_socket} -> request(tls_socket, host, timeout)
          {:error, _reason} = error -> close_tcp(socket, {:tls, error})
        end
      end

    classify(result)
  end

  defp request(tls_socket, host, timeout) do
    {:http, Https.head(tls_socket, host, "/", timeout)}
  after
    :ssl.close(tls_socket)
  end

  defp close_tcp(socket, result) do
    _closed = :gen_tcp.close(socket)
    result
  end

  @doc """
  Classifies the result of a visit's stage: `{stage, {:ok, response}}` for
  a response, or `{stage, {:error, reason}}` for the stage that failed.
  """
  @spec classify({stage(), {:ok, map()} | {:error, term()}}) :: outcome()
  def classify({:http, {:ok, %{status: status}}}),
    do: %{class: :ok, stage: :http, status: status, reason: nil}

  def classify({stage, {:error, reason}}) do
    %{class: class(stage, reason), stage: stage, status: nil, reason: describe(reason)}
  end

  @doc """
  Returns what a failed outcome's cause is, for grouping hosts that failed
  alike: its class, and for a TLS alert the alert's description.
  """
  @spec cause(outcome()) :: String.t()
  def cause(%{class: :tls_alert, reason: reason}) do
    case Regex.run(~r/\{:tls_alert, \{:(\w+)/, reason || "") do
      [_match, alert] -> "tls_alert #{alert}"
      nil -> "tls_alert"
    end
  end

  def cause(%{class: :other, reason: reason}), do: "other #{String.slice(reason || "", 0, 60)}"
  def cause(%{class: class}), do: Atom.to_string(class)

  defp class(_stage, :system_limit), do: :system_limit
  defp class(_stage, :econnreset), do: :reset
  defp class(_stage, {:tls_alert, _alert}), do: :tls_alert
  defp class(_stage, {{:tls_alert, _alert}, _progress}), do: :tls_alert
  defp class(:tcp, reason) when reason in [:timeout, :etimedout], do: :connect_timeout
  defp class(:tcp, :econnrefused), do: :refused
  defp class(:tcp, reason) when reason in [:ehostunreach, :enetunreach], do: :unreachable
  defp class(:tls, :timeout), do: :tls_timeout
  defp class(:tls, :closed), do: :tls_closed
  defp class(:tls, _reason), do: :tls_error
  defp class(:http, :timeout), do: :http_timeout
  defp class(:http, :closed), do: :http_closed
  defp class(:http, _reason), do: :http_error
  defp class(_stage, _reason), do: :other

  defp describe(reason), do: inspect(reason, limit: 20, printable_limit: 300)
end
