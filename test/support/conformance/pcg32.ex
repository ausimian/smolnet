defmodule SmolNet.Conformance.Pcg32 do
  @moduledoc false

  # smoltcp's random source (`src/rand.rs`), sPCG32 over a 64-bit state, and
  # the order a stack draws from it, so that an Elixir stack given the same
  # `test_determinism: %{random_seed: seed}` chooses the NIF's initial
  # sequence numbers and the two can be compared segment for segment.
  #
  # A NIF stack seeds one smoltcp interface with the seed. The interface
  # draws the IPv4 identification start first, as `rand_u16` until it is not
  # zero; no packet shows it, since every IPv4 packet is sent with DF and
  # identification 0. Every initial sequence number after that, active or
  # passive, takes the next `rand_u32`, in the order the connections open.

  import Bitwise

  @multiplier 0xBB2E_FCEC_3C39_611D
  @increment 0x7590_EF39
  @mask 0xFFFF_FFFF_FFFF_FFFF

  @type t :: non_neg_integer()

  @spec new(non_neg_integer()) :: t()
  def new(seed) when seed in 0..@mask, do: seed

  @spec next_u32(t()) :: {non_neg_integer(), t()}
  def next_u32(state) do
    state = band(state * @multiplier + @increment, @mask)
    shift = 29 - (state >>> 61)
    {band(state >>> shift, 0xFFFF_FFFF), state}
  end

  @spec next_u16(t()) :: {non_neg_integer(), t()}
  def next_u16(state) do
    {n, state} = next_u32(state)
    {band(bxor(n, n >>> 16), 0xFFFF), state}
  end

  @doc "The state a new stack with this seed draws its first ISN from."
  @spec stack(non_neg_integer()) :: t()
  def stack(seed) do
    {_ipv4_identification, state} = nonzero_u16(new(seed))
    state
  end

  @doc "The first `count` initial sequence numbers a new stack with this seed chooses."
  @spec isns(non_neg_integer(), non_neg_integer()) :: [non_neg_integer()]
  def isns(seed, count) do
    {isns, _state} =
      Enum.map_reduce(1..count//1, stack(seed), fn _n, state -> next_u32(state) end)

    isns
  end

  defp nonzero_u16(state) do
    case next_u16(state) do
      {0, state} -> nonzero_u16(state)
      result -> result
    end
  end
end
