defmodule SmolNet.Core.Tcp.SeqTest do
  # Not a port: smoltcp's `TcpSeqNumber` has no unit tests of its own. These
  # check what the rest of the TCP port relies on, that sequence numbers
  # order by their wrapping difference across 2^32, and that `diff/2`
  # raises, as smoltcp's subtraction panics, for a negative distance.
  use ExUnit.Case, async: true

  alias SmolNet.Core.Tcp.Seq

  @top 0xFFFF_FFFF

  test "add and sub wrap at 2^32" do
    assert Seq.add(@top, 1) == 0
    assert Seq.add(@top - 4, 10) == 5
    assert Seq.sub(0, 1) == @top
    assert Seq.sub(5, 10) == @top - 4
  end

  test "ordering holds across the wrap" do
    assert Seq.lt?(@top, 0)
    assert Seq.gt?(0, @top)
    assert Seq.lt?(@top - 10, 10)
    assert Seq.le?(@top, 0)
    assert Seq.ge?(10, @top - 10)
    refute Seq.lt?(10, @top - 10)
    assert Seq.le?(7, 7) and Seq.ge?(7, 7)
    refute Seq.lt?(7, 7) or Seq.gt?(7, 7)
  end

  test "ordering flips at half the space" do
    assert Seq.lt?(0, 0x7FFF_FFFF)
    assert Seq.gt?(0, 0x8000_0001)
    assert Seq.lt?(0x7FFF_FFFF, 0x8000_0000)
  end

  test "max and min follow the wrapping order" do
    assert Seq.max(@top, 3) == 3
    assert Seq.min(@top, 3) == @top
  end

  test "diff measures forward across the wrap" do
    assert Seq.diff(5, @top - 4) == 10
    assert Seq.diff(0, @top) == 1
    assert Seq.diff(42, 42) == 0
  end

  test "diff raises for a negative distance" do
    assert_raise ArithmeticError, fn -> Seq.diff(@top - 4, 5) end
    assert_raise ArithmeticError, fn -> Seq.diff(1, 2) end
  end
end
