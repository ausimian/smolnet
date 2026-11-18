defmodule SmolNet.Core.RandTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  # The core's random source against the conformance suite's model of
  # smoltcp's, which the NIF's initial sequence numbers are checked against.

  alias SmolNet.Conformance.Pcg32
  alias SmolNet.Core.Rand

  property "draws smoltcp's initial sequence numbers from a seed" do
    check all(seed <- integer(0..0xFFFF_FFFF_FFFF_FFFF), max_runs: 200) do
      {isns, _state} =
        Enum.map_reduce(1..4, Rand.new(seed), fn _n, state -> Rand.next_u32(state) end)

      assert isns == Pcg32.isns(seed, 4)
    end
  end
end
