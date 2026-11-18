defmodule SmolNet.Core.Tcp.ScoreboardTest do
  # Ported from smoltcp's scoreboard unit tests
  # (native/vendor/smoltcp/src/socket/tcp/scoreboard.rs). Rust builds
  # sequence numbers from `i32`s; `seq/1` masks those to the 32-bit
  # values the Elixir port holds, and `ranges/1` reads them back as
  # `i32`s, so the literals are Rust's. The board is a plain list here, so
  # `board.len` is its length and `is_empty` is `== []`; nothing is
  # skipped.
  use ExUnit.Case, async: true

  import Bitwise

  alias SmolNet.Core.Tcp.Scoreboard
  alias SmolNet.Core.Tcp.Seq

  @scoreboard_size 32
  @i32_max 0x7FFF_FFFF
  @i32_min -0x8000_0000

  defp seq(n), do: band(n, 0xFFFF_FFFF)

  defp ranges(board), do: Enum.map(board, fn {l, r} -> {Seq.signed(l), Seq.signed(r)} end)

  defp add(board, left, right) do
    {_new?, board} = Scoreboard.add(board, seq(left), seq(right))
    board
  end

  test "add_merges_overlapping_and_adjacent_ranges" do
    board = Scoreboard.new()
    assert {true, board} = Scoreboard.add(board, seq(10), seq(20))
    assert {true, board} = Scoreboard.add(board, seq(30), seq(40))
    assert ranges(board) == [{10, 20}, {30, 40}]

    # Adjacent on the left, overlapping on the right.
    assert {true, board} = Scoreboard.add(board, seq(20), seq(35))
    assert ranges(board) == [{10, 40}]

    # A range already held reports nothing new; an empty one neither.
    assert {false, board} = Scoreboard.add(board, seq(12), seq(40))
    assert {false, board} = Scoreboard.add(board, seq(50), seq(50))
    assert ranges(board) == [{10, 40}]
  end

  test "add_reports_a_range_below_a_higher_one" do
    # #115: a range filled in below the highest one is new.
    board = Scoreboard.new()
    assert {true, board} = Scoreboard.add(board, seq(4), seq(5))
    assert {true, board} = Scoreboard.add(board, seq(1), seq(2))
    assert {false, board} = Scoreboard.add(board, seq(4), seq(5))
    assert ranges(board) == [{1, 2}, {4, 5}]
  end

  test "add_forgets_the_lowest_range_when_full" do
    board =
      Enum.reduce(0..(@scoreboard_size - 1), Scoreboard.new(), fn i, board ->
        assert {true, board} = Scoreboard.add(board, seq(10 * i + 10), seq(10 * i + 15))
        board
      end)

    assert {true, board} = Scoreboard.add(board, seq(1_000), seq(1_005))
    assert length(board) == @scoreboard_size
    assert Enum.at(board, 0) == {seq(20), seq(25)}
    assert Enum.at(board, @scoreboard_size - 1) == {seq(1_000), seq(1_005)}
  end

  test "advance_drops_and_trims_passed_ranges" do
    board = Scoreboard.new() |> add(10, 20) |> add(30, 40)
    board = Scoreboard.advance(board, seq(20))
    assert ranges(board) == [{30, 40}]
    board = Scoreboard.advance(board, seq(35))
    assert ranges(board) == [{35, 40}]
    board = Scoreboard.advance(board, seq(40))
    assert board == []
  end

  test "sequence_numbers_wrap" do
    board = Scoreboard.new() |> add(@i32_max - 5, @i32_max) |> add(@i32_min, @i32_min + 5)
    assert ranges(board) == [{@i32_max - 5, @i32_max}, {@i32_min, @i32_min + 5}]
    assert {true, board} = Scoreboard.add(board, seq(@i32_max), seq(@i32_min))
    assert ranges(board) == [{@i32_max - 5, @i32_min + 5}]
    assert Scoreboard.sacked_between(board, seq(@i32_max - 10), seq(@i32_min + 10)) == 11
  end

  test "lost_below_counts_ranges_or_octets_above" do
    mss = 10
    board = Scoreboard.new() |> add(100, 110) |> add(120, 130)
    assert Scoreboard.lost_below(board, mss) == nil

    # Three ranges above an octet make it lost.
    board = add(board, 140, 150)
    assert Scoreboard.lost_below(board, mss) == seq(100)

    # So do more than two segments' worth of octets in fewer ranges.
    board = add(Scoreboard.new(), 100, 121)
    assert Scoreboard.lost_below(board, mss) == seq(100)
    board = add(Scoreboard.new(), 100, 120)
    assert Scoreboard.lost_below(board, mss) == nil
  end

  test "next_hole_skips_recorded_ranges" do
    board = Scoreboard.new()
    assert Scoreboard.next_hole(board, seq(0)) == nil
    board = board |> add(10, 20) |> add(30, 40)
    assert Scoreboard.next_hole(board, seq(0)) == {seq(0), seq(10)}
    assert Scoreboard.next_hole(board, seq(10)) == {seq(20), seq(30)}
    assert Scoreboard.next_hole(board, seq(25)) == {seq(25), seq(30)}
    assert Scoreboard.next_hole(board, seq(30)) == nil
    assert Scoreboard.sacked_between(board, seq(15), seq(35)) == 10
  end

  test "unsacked_run_finds_gaps_and_the_tail" do
    board = Scoreboard.new()
    assert Scoreboard.unsacked_run(board, seq(0), seq(5)) == {seq(0), seq(5)}
    board = board |> add(10, 20) |> add(30, 40)
    assert Scoreboard.unsacked_run(board, seq(0), seq(50)) == {seq(0), seq(10)}
    assert Scoreboard.unsacked_run(board, seq(10), seq(50)) == {seq(20), seq(30)}
    assert Scoreboard.unsacked_run(board, seq(22), seq(25)) == {seq(22), seq(25)}
    assert Scoreboard.unsacked_run(board, seq(35), seq(50)) == {seq(40), seq(50)}
    assert Scoreboard.unsacked_run(board, seq(12), seq(18)) == nil
    assert Scoreboard.unsacked_run(board, seq(30), seq(40)) == nil
    assert Scoreboard.unsacked_run(board, seq(5), seq(5)) == nil
  end
end
