defmodule SmolNet.Core.Rand do
  @moduledoc false

  # smoltcp's random source (`src/rand.rs`), sPCG32 over a 64-bit state,
  # drawn in the order a NIF stack draws from it, so that a stack given a
  # `random_seed` chooses the NIF's initial sequence numbers: the IPv4
  # identification start first, as `rand_u16` until it is not zero (no
  # packet shows it, since IPv4 is sent with DF and identification 0), then
  # one `rand_u32` per initial sequence number, active or passive, in the
  # order the connections open.

  import Bitwise

  @multiplier 0xBB2E_FCEC_3C39_611D
  @increment 0x7590_EF39
  @mask 0xFFFF_FFFF_FFFF_FFFF

  @type t :: non_neg_integer()

  @doc "A stack's generator from its seed, past the IPv4 identification draw."
  @spec new(non_neg_integer()) :: t()
  def new(seed) do
    {_ipv4_identification, state} = nonzero_u16(band(seed, @mask))
    state
  end

  @spec next_u32(t()) :: {non_neg_integer(), t()}
  def next_u32(state) do
    state = band(state * @multiplier + @increment, @mask)
    shift = 29 - (state >>> 61)
    {band(state >>> shift, 0xFFFF_FFFF), state}
  end

  defp nonzero_u16(state) do
    {n, state} = next_u32(state)

    case band(bxor(n, n >>> 16), 0xFFFF) do
      0 -> nonzero_u16(state)
      value -> {value, state}
    end
  end
end
