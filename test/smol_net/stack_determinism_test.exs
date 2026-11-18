defmodule SmolNet.StackDeterminismTest do
  use ExUnit.Case, async: false

  alias SmolNet.Test.ManualClock

  @local {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @peer {0xFD00, 0, 0, 0, 0, 0, 0, 2}
  @now 5_000

  setup do
    previous = Map.new([:clock_module, :manual_clock], &{&1, Application.get_env(:smolnet, &1)})
    Application.put_env(:smolnet, :clock_module, ManualClock)
    restart_clock()

    on_exit(fn ->
      Agent.stop(Application.fetch_env!(:smolnet, :manual_clock))

      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:smolnet, key)
        {key, value} -> Application.put_env(:smolnet, key, value)
      end)
    end)
  end

  test "fixed determinism hooks give identical SYNs from separate stacks" do
    determinism = %{random_seed: 42, timestamp_origin: 1_000, ephemeral_port_start: 50_000}

    first = syn(test_determinism: determinism)
    second = syn(test_determinism: determinism)
    assert first == second

    %{source_port: port, seq: seq, tsval: tsval} = parse_syn(first)
    assert port == 50_000
    # The clock is the connect call's, a second after the stack started.
    assert tsval == 1_000 + @now + 1_000

    reseeded = syn(test_determinism: %{determinism | random_seed: 43})
    assert parse_syn(reseeded).seq != seq
  end

  test "the largest seed and timestamp origin reach the stack, and the clock wraps" do
    determinism = %{random_seed: 0xFFFF_FFFF_FFFF_FFFF, timestamp_origin: 0xFFFF_FFFF}

    assert %{tsval: tsval} = parse_syn(syn(test_determinism: determinism))
    assert tsval == @now + 1_000 - 1
  end

  test "UDP takes its ephemeral ports from the same start" do
    {:ok, stack} =
      SmolNet.start_stack(
        addresses: [{@local, 64}],
        test_determinism: %{ephemeral_port_start: 50_000}
      )

    {:ok, socket} = SmolNet.open(:inet6, :dgram, :udp, stack: stack)
    assert :ok = SmolNet.bind(socket, %{family: :inet6, addr: @local, port: 0})
    assert {:ok, %{port: 50_000}} = SmolNet.sockname(socket)

    :ok = SmolNet.stop_stack(stack)
  end

  test "the hooks are validated" do
    for determinism <- [
          %{random_seed: -1},
          %{timestamp_origin: 0x1_0000_0000},
          %{ephemeral_port_start: 49_151},
          %{ephemeral_port_start: 50_176},
          %{unknown: 1},
          :not_a_map
        ] do
      assert SmolNet.start_stack(test_determinism: determinism) == {:error, :invalid_options}
    end
  end

  # Each stack starts at @now on a fresh clock, so that equal options are
  # equal inputs.
  defp restart_clock do
    case Application.get_env(:smolnet, :manual_clock) do
      pid when is_pid(pid) -> Agent.stop(pid)
      _none -> :ok
    end

    {:ok, clock} = ManualClock.start(@now)
    Application.put_env(:smolnet, :manual_clock, clock)
    clock
  end

  defp syn(options) do
    tag = make_ref()
    clock = restart_clock()

    {:ok, stack} =
      SmolNet.start_stack(
        [
          egress: {self(), tag},
          addresses: [{@local, 64}]
        ] ++ options
      )

    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    :ok = ManualClock.advance(clock, 1_000)

    assert {:select, _select_info} =
             SmolNet.connect(socket, %{family: :inet6, addr: @peer, port: 443}, :nowait)

    assert_receive {:smol_stack, ^tag, :egress, [packet]}
    :ok = SmolNet.stop_stack(stack)
    packet
  end

  defp parse_syn(
         <<6::4, _::28, _length::16, 6, _hops, _source::binary-16, _destination::binary-16,
           source_port::16, _destination_port::16, seq::32, _ack::32, offset::4, _::12,
           _window::16, _checksum::16, _urgent::16, rest::binary>>
       ) do
    options = binary_part(rest, 0, offset * 4 - 20)
    %{source_port: source_port, seq: seq, tsval: tsval(options)}
  end

  defp tsval(<<8, 10, tsval::32, _tsecr::32, _rest::binary>>), do: tsval
  defp tsval(<<0, _rest::binary>>), do: nil
  defp tsval(<<1, rest::binary>>), do: tsval(rest)

  defp tsval(<<_kind, length, rest::binary>>),
    do: tsval(binary_part(rest, length - 2, byte_size(rest) - length + 2))
end
