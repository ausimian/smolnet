defmodule SmolNet.Stack.Backend do
  @moduledoc false

  # The protocol engine behind a `SmolNet.Stack`. The Stack process is the
  # shell: it owns the process, the clock, the poll timer, the monitors and
  # the link, and calls the backend for everything protocol-shaped.
  #
  # There is one callback per operation, one-to-one with the NIF calls the
  # Stack made before backends existed. Each takes the backend state where
  # the NIF took its resource, and returns `{reply, state, effects}`:
  #
  #   * `reply` is `{:ok, envelope}` or `{:error, reason}`. The envelope is
  #     `%{result: term, poll_at: deadline | nil, more: boolean}`: the
  #     operation's result, the earliest time the backend next needs a
  #     `stack_poll/2` (nil when the call reports none), and whether it
  #     retained work it wants polled again before more ingress.
  #   * `state` is the backend state for the next call. A backend that keeps
  #     its state elsewhere, as the NIF does in its resource, returns the
  #     same value.
  #   * `effects` is the ordered list of side effects for the shell to carry
  #     out, see `t:effect/0`. Every callback returns one, errors included.
  #
  # The shell carries out a call's wake and abort effects as it meets them,
  # before it replies to the caller, which is when the NIF sends those
  # messages itself. It sends the call's emitted packets to the link in their
  # order, as one `{:smol_stack, link_ref, :egress, packets}` message, after
  # them. The shutdown drain (`stack_shutdown/2` and the polls that follow
  # it) delivers wakes and aborts but drops emitted packets.
  #
  # `SmolNet.Stack.Backend.Native` returns no wake or abort effects, since
  # the NIF sends those messages itself.

  @type state :: term()
  @type identity :: %{id: non_neg_integer(), generation: non_neg_integer()}
  @type now :: integer()
  @type endpoint :: map()
  @type envelope :: %{result: term(), poll_at: integer() | nil, more: boolean()}
  @type reply :: {:ok, envelope()} | {:error, atom()}

  @typedoc """
  A side effect for the shell.

    * `{:emit, packet}` sends one raw IP packet to the link.
    * `{:wake, pid, {id, generation}, reference}` sends `pid` the readiness
      message `{:"$smol_socket", {id, generation}, :select, reference}`.
    * `{:abort, pid, {id, generation}, reference, reason}` sends `pid`
      `{:"$smol_socket", {id, generation}, :abort, reference, reason}`.
  """
  @type effect ::
          {:emit, binary()}
          | {:wake, pid(), {non_neg_integer(), non_neg_integer()}, reference()}
          | {:abort, pid(), {non_neg_integer(), non_neg_integer()}, reference(), atom()}

  @type outcome :: {reply(), state(), [effect()]}

  @callback stack_new(limits :: map(), config :: map(), now()) :: outcome()
  @callback stack_ingress(state(), packet :: binary(), now()) :: outcome()
  @callback stack_ingress_batch(state(), packets :: [binary()], now()) :: outcome()
  @callback stack_poll(state(), now()) :: outcome()
  @callback stack_grant_egress(
              state(),
              packets :: non_neg_integer(),
              bytes :: non_neg_integer(),
              now()
            ) :: outcome()
  @callback stack_shutdown(state(), reason :: :closed | :link_down | :stack_down) :: outcome()
  @callback stack_snapshot(state()) :: outcome()

  @callback socket_cancel(state(), identity(), operation :: atom(), reference()) :: outcome()
  @callback socket_validate(state(), identity()) :: outcome()

  @callback tcp_open(
              state(),
              family :: :inet | :inet6,
              rcvbuf :: pos_integer(),
              sndbuf :: pos_integer()
            ) :: outcome()
  @callback tcp_bind(state(), identity(), endpoint()) :: outcome()
  @callback tcp_listen(state(), identity(), backlog :: pos_integer(), now()) :: outcome()
  @callback tcp_accept(state(), identity(), pid(), reference(), now()) :: outcome()
  @callback tcp_connect(state(), identity(), endpoint(), pid(), reference(), now()) ::
              outcome()
  @callback tcp_send(state(), identity(), data :: binary(), pid(), reference(), now()) ::
              outcome()
  @callback tcp_recv(
              state(),
              identity(),
              length :: non_neg_integer(),
              pid(),
              reference(),
              now()
            ) :: outcome()
  @callback tcp_shutdown(state(), identity(), how :: :read | :write | :read_write, now()) ::
              outcome()
  @callback tcp_sockname(state(), identity()) :: outcome()
  @callback tcp_peername(state(), identity()) :: outcome()
  @callback tcp_set_nodelay(state(), identity(), boolean(), now()) :: outcome()
  @callback tcp_nodelay(state(), identity()) :: outcome()
  @callback tcp_set_keepalive(state(), identity(), boolean(), now()) :: outcome()
  @callback tcp_keepalive(state(), identity()) :: outcome()
  @callback tcp_close(state(), identity(), now()) :: outcome()

  @callback udp_open(state(), family :: :inet | :inet6) :: outcome()
  @callback udp_bind(state(), identity(), endpoint()) :: outcome()
  @callback udp_connect(state(), identity(), endpoint()) :: outcome()
  @callback udp_sendto(
              state(),
              identity(),
              endpoint(),
              data :: binary(),
              pid(),
              reference(),
              now()
            ) :: outcome()
  @callback udp_recvfrom(
              state(),
              identity(),
              length :: non_neg_integer(),
              pid(),
              reference(),
              now()
            ) :: outcome()
  @callback udp_sockname(state(), identity()) :: outcome()
  @callback udp_peername(state(), identity()) :: outcome()
  @callback udp_close(state(), identity(), now()) :: outcome()

  # The NIF's debug hooks, which exist only in a debug build of the NIF and
  # go with it. The first two reply with the NIF's raw result.
  @callback test_contention(state()) :: {term(), state(), [effect()]}
  @callback test_bounded_work(state(), requested :: map()) :: {term(), state(), [effect()]}
  @callback test_socket_open(state(), internal_handle :: non_neg_integer()) :: outcome()
  @callback test_socket_wait(state(), identity(), wait :: map()) :: outcome()
  @callback test_socket_ready(state(), keys :: [map()]) :: outcome()
  @callback test_socket_close(state(), identity(), wake_direction :: nil | :read | :write) ::
              outcome()

  @optional_callbacks test_contention: 1,
                      test_bounded_work: 2,
                      test_socket_open: 2,
                      test_socket_wait: 3,
                      test_socket_ready: 2,
                      test_socket_close: 3

  @backends %{native: SmolNet.Stack.Backend.Native}

  @doc """
  The names the `:backend` stack option accepts.
  """
  @spec names() :: [atom()]
  def names, do: Map.keys(@backends)

  @doc """
  The backend module for a name from `names/0`.

  A module is returned as it is, so the test suite can configure a double
  through the application environment.
  """
  @spec resolve(term()) :: {:ok, module()} | :error
  def resolve(name) when is_map_key(@backends, name), do: {:ok, Map.fetch!(@backends, name)}

  def resolve(module) when is_atom(module) and not is_nil(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :stack_new, 3) do
      {:ok, module}
    else
      :error
    end
  end

  def resolve(_name), do: :error

  @doc """
  The backend a stack uses when it is started without the `:backend` option:
  the application environment's `:backend`, or the NIF.

  The environment exists for the test suite, which runs once per backend and
  swaps in a double; it is not a supported setting.
  """
  @spec default() :: module()
  def default do
    name = Application.get_env(:smolnet, :backend, :native)

    case resolve(name) do
      {:ok, module} -> module
      :error -> raise ArgumentError, "invalid :smolnet :backend #{inspect(name)}"
    end
  end

  @doc """
  The name `stack_info/1` reports for a backend module: its name from
  `names/0`, or the module itself for one that has no name.
  """
  @spec name(module()) :: atom()
  def name(module) do
    Enum.find_value(@backends, module, fn
      {name, ^module} -> name
      _other -> nil
    end)
  end
end
