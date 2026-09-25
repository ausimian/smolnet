defmodule SmolNet.Native do
  @moduledoc false

  @tcp_default_buffer_bytes 65_536

  version = Mix.Project.config()[:version]
  source_build? = File.dir?(Path.expand("../../native/smolnet_nif", __DIR__))

  use RustlerPrecompiled,
    otp_app: :smolnet,
    crate: "smolnet_nif",
    base_url: "https://github.com/ausimian/smolnet/releases/download/#{version}",
    version: version,
    force_build: source_build?,
    targets: ~w(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin),
    nif_versions: ["2.15"]

  @spec health() :: :ok
  def health, do: :erlang.nif_error(:nif_not_loaded)

  @spec stack_new(map(), map(), integer()) :: {:ok, map()} | {:error, atom()}
  def stack_new(_limits, _config, _now), do: :erlang.nif_error(:nif_not_loaded)

  @spec stack_ingress(reference(), binary(), integer()) :: {:ok, map()} | {:error, atom()}
  def stack_ingress(_stack, _packet, _now), do: :erlang.nif_error(:nif_not_loaded)

  @spec stack_ingress_batch(reference(), [binary()], integer()) ::
          {:ok, map()} | {:error, atom()}
  def stack_ingress_batch(_stack, _packets, _now), do: :erlang.nif_error(:nif_not_loaded)

  @spec stack_poll(reference(), integer()) :: {:ok, map()} | {:error, atom()}
  def stack_poll(_stack, _now), do: :erlang.nif_error(:nif_not_loaded)

  @spec stack_grant_egress(reference(), non_neg_integer(), non_neg_integer(), integer()) ::
          {:ok, map()} | {:error, atom()}
  def stack_grant_egress(_stack, _packets, _bytes, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec stack_shutdown(reference()) :: {:ok, map()} | {:error, atom()}
  def stack_shutdown(_stack), do: :erlang.nif_error(:nif_not_loaded)

  @spec socket_cancel(reference(), map(), atom(), reference()) ::
          {:ok, map()} | {:error, atom()}
  def socket_cancel(_stack, _identity, _operation, _reference),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec socket_validate(reference(), map()) :: {:ok, map()} | {:error, atom()}
  def socket_validate(_stack, _identity), do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_open(reference(), :inet | :inet6) :: {:ok, map()} | {:error, atom()}
  def tcp_open(stack, family),
    do: tcp_open(stack, family, @tcp_default_buffer_bytes, @tcp_default_buffer_bytes)

  @spec tcp_open(reference(), :inet | :inet6, pos_integer(), pos_integer()) ::
          {:ok, map()} | {:error, atom()}
  def tcp_open(_stack, _family, _rcvbuf, _sndbuf), do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_bind(reference(), map(), map()) :: {:ok, map()} | {:error, atom()}
  def tcp_bind(_stack, _identity, _endpoint), do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_listen(reference(), map(), pos_integer(), integer()) ::
          {:ok, map()} | {:error, atom()}
  def tcp_listen(_stack, _identity, _backlog, _now), do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_accept(reference(), map(), pid(), reference(), integer()) ::
          {:ok, map()} | {:error, atom()}
  def tcp_accept(_stack, _identity, _pid, _reference, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_connect(reference(), map(), map(), pid(), reference(), integer()) ::
          {:ok, map()} | {:error, atom()}
  def tcp_connect(_stack, _identity, _endpoint, _pid, _reference, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_send(reference(), map(), binary(), pid(), reference(), integer()) ::
          {:ok, map()} | {:error, atom()}
  def tcp_send(_stack, _identity, _data, _pid, _reference, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_recv(reference(), map(), non_neg_integer(), pid(), reference(), integer()) ::
          {:ok, map()} | {:error, atom()}
  def tcp_recv(_stack, _identity, _length, _pid, _reference, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_shutdown(reference(), map(), :read | :write | :read_write, integer()) ::
          {:ok, map()} | {:error, atom()}
  def tcp_shutdown(_stack, _identity, _how, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_sockname(reference(), map()) :: {:ok, map()} | {:error, atom()}
  def tcp_sockname(_stack, _identity), do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_peername(reference(), map()) :: {:ok, map()} | {:error, atom()}
  def tcp_peername(_stack, _identity), do: :erlang.nif_error(:nif_not_loaded)

  @spec tcp_close(reference(), map(), integer()) :: {:ok, map()} | {:error, atom()}
  def tcp_close(_stack, _identity, _now), do: :erlang.nif_error(:nif_not_loaded)

  @spec udp_open(reference(), :inet | :inet6) :: {:ok, map()} | {:error, atom()}
  def udp_open(_stack, _family), do: :erlang.nif_error(:nif_not_loaded)

  @spec udp_bind(reference(), map(), map()) :: {:ok, map()} | {:error, atom()}
  def udp_bind(_stack, _identity, _endpoint), do: :erlang.nif_error(:nif_not_loaded)

  @spec udp_connect(reference(), map(), map()) :: {:ok, map()} | {:error, atom()}
  def udp_connect(_stack, _identity, _endpoint), do: :erlang.nif_error(:nif_not_loaded)

  @spec udp_sendto(reference(), map(), map(), binary(), pid(), reference(), integer()) ::
          {:ok, map()} | {:error, atom()}
  def udp_sendto(_stack, _identity, _endpoint, _data, _pid, _reference, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec udp_recvfrom(reference(), map(), non_neg_integer(), pid(), reference(), integer()) ::
          {:ok, map()} | {:error, atom()}
  def udp_recvfrom(_stack, _identity, _length, _pid, _reference, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec udp_sockname(reference(), map()) :: {:ok, map()} | {:error, atom()}
  def udp_sockname(_stack, _identity), do: :erlang.nif_error(:nif_not_loaded)

  @spec udp_peername(reference(), map()) :: {:ok, map()} | {:error, atom()}
  def udp_peername(_stack, _identity), do: :erlang.nif_error(:nif_not_loaded)

  @spec udp_close(reference(), map(), integer()) :: {:ok, map()} | {:error, atom()}
  def udp_close(_stack, _identity, _now), do: :erlang.nif_error(:nif_not_loaded)

  @spec stack_snapshot(reference()) :: {:ok, map()} | {:error, atom()}
  def stack_snapshot(_stack), do: :erlang.nif_error(:nif_not_loaded)

  @spec stack_time_until(integer(), integer()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def stack_time_until(_now, _deadline), do: :erlang.nif_error(:nif_not_loaded)

  @spec resource_counts() :: map()
  def resource_counts, do: :erlang.nif_error(:nif_not_loaded)

  @spec test_contention(reference()) :: {:error, :ownership_invariant_violation}
  def test_contention(_stack), do: :erlang.nif_error(:nif_not_loaded)

  @spec test_bounded_work(reference(), map()) :: {:ok, map()} | {:error, atom()}
  def test_bounded_work(_stack, _requested), do: :erlang.nif_error(:nif_not_loaded)

  @spec test_set_budget_checkpoints(reference(), non_neg_integer()) ::
          {:ok, map()} | {:error, atom()}
  def test_set_budget_checkpoints(_stack, _checkpoints),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec test_set_waiter_capacity(reference(), non_neg_integer()) ::
          {:ok, map()} | {:error, atom()}
  def test_set_waiter_capacity(_stack, _waiters),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec test_set_slice_exhaustion(reference(), non_neg_integer()) ::
          {:ok, map()} | {:error, atom()}
  def test_set_slice_exhaustion(_stack, _charges),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec test_maximum_work(reference()) :: {:ok, map()} | {:error, atom()}
  def test_maximum_work(_stack), do: :erlang.nif_error(:nif_not_loaded)

  @spec test_combined_maximum_work(reference(), [map()], integer()) ::
          {:ok, map()} | {:error, atom()}
  def test_combined_maximum_work(_stack, _keys, _now),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec test_prepare_closing(reference(), non_neg_integer()) :: {:ok, map()} | {:error, atom()}
  def test_prepare_closing(_stack, _count), do: :erlang.nif_error(:nif_not_loaded)

  @spec test_prepare_maximum_drop(reference()) :: {:ok, map()} | {:error, atom()}
  def test_prepare_maximum_drop(_stack), do: :erlang.nif_error(:nif_not_loaded)

  @spec test_socket_open(reference(), non_neg_integer()) :: {:ok, map()} | {:error, atom()}
  def test_socket_open(_stack, _internal_handle), do: :erlang.nif_error(:nif_not_loaded)

  @spec test_socket_wait(reference(), map(), map()) :: {:ok, map()} | {:error, atom()}
  def test_socket_wait(_stack, _identity, _wait),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec test_socket_ready(reference(), [map()]) :: {:ok, map()} | {:error, atom()}
  def test_socket_ready(_stack, _keys), do: :erlang.nif_error(:nif_not_loaded)

  @spec test_socket_close(reference(), map(), nil | :read | :write) ::
          {:ok, map()} | {:error, atom()}
  def test_socket_close(_stack, _identity, _wake_direction),
    do: :erlang.nif_error(:nif_not_loaded)
end
