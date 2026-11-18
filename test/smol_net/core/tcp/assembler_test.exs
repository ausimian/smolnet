defmodule SmolNet.Core.Tcp.AssemblerTest do
  # Ported from smoltcp's assembler unit tests
  # (native/vendor/smoltcp/src/storage/assembler.rs), whose test build
  # holds 4 ranges, as `Assembler.new(4)` does. smoltcp keeps
  # `(hole, data)` contigs where the port keeps `{left, right}` ranges, so
  # `contigs/1` turns Rust's `contigs![...]` into ranges and the tests
  # compare `Assembler.ranges/1` (smoltcp's `iter_data`) against it. The
  # assembler is immutable, so `test_rejected_add_keeps_state` and
  # `test_add_then_remove_front_at_front_full` hold by construction; they
  # still check the error. `test_random` is a StreamData property against
  # the same bitmap model. Nothing is skipped.
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias SmolNet.Core.Tcp.Assembler

  @max_segment_count 4

  defp new, do: Assembler.new(@max_segment_count)

  # smoltcp's `contigs![(hole, data), ...]`, as ranges.
  defp contigs(pairs) do
    {ranges, _offset} =
      Enum.flat_map_reduce(pairs, 0, fn {hole, data}, offset ->
        left = offset + hole
        right = left + data
        {if(left < right, do: [{left, right}], else: []), right}
      end)

    ranges
  end

  defp from_contigs(pairs), do: %{new() | ranges: contigs(pairs)}

  defp add(assr, offset, size) do
    assert {:ok, assr} = Assembler.add(assr, offset, size)
    assr
  end

  test "test_new" do
    assert Assembler.ranges(new()) == contigs([])
  end

  test "test_empty_add_full" do
    assert {:ok, assr} = Assembler.add(new(), 0, 16)
    assert Assembler.ranges(assr) == contigs([{0, 16}])
  end

  test "test_empty_add_front" do
    assert {:ok, assr} = Assembler.add(new(), 0, 4)
    assert Assembler.ranges(assr) == contigs([{0, 4}])
  end

  test "test_empty_add_back" do
    assert {:ok, assr} = Assembler.add(new(), 12, 4)
    assert Assembler.ranges(assr) == contigs([{12, 4}])
  end

  test "test_empty_add_mid" do
    assert {:ok, assr} = Assembler.add(new(), 4, 8)
    assert Assembler.ranges(assr) == contigs([{4, 8}])
  end

  test "test_partial_add_front" do
    assert {:ok, assr} = Assembler.add(from_contigs([{4, 8}]), 0, 4)
    assert Assembler.ranges(assr) == contigs([{0, 12}])
  end

  test "test_partial_add_back" do
    assert {:ok, assr} = Assembler.add(from_contigs([{4, 8}]), 12, 4)
    assert Assembler.ranges(assr) == contigs([{4, 12}])
  end

  test "test_partial_add_front_overlap" do
    assert {:ok, assr} = Assembler.add(from_contigs([{4, 8}]), 0, 8)
    assert Assembler.ranges(assr) == contigs([{0, 12}])
  end

  test "test_partial_add_front_overlap_split" do
    assert {:ok, assr} = Assembler.add(from_contigs([{4, 8}]), 2, 6)
    assert Assembler.ranges(assr) == contigs([{2, 10}])
  end

  test "test_partial_add_back_overlap" do
    assert {:ok, assr} = Assembler.add(from_contigs([{4, 8}]), 8, 8)
    assert Assembler.ranges(assr) == contigs([{4, 12}])
  end

  test "test_partial_add_back_overlap_split" do
    assert {:ok, assr} = Assembler.add(from_contigs([{4, 8}]), 10, 4)
    assert Assembler.ranges(assr) == contigs([{4, 10}])
  end

  test "test_partial_add_both_overlap" do
    assert {:ok, assr} = Assembler.add(from_contigs([{4, 8}]), 0, 16)
    assert Assembler.ranges(assr) == contigs([{0, 16}])
  end

  test "test_partial_add_both_overlap_split" do
    assert {:ok, assr} = Assembler.add(from_contigs([{4, 8}]), 2, 12)
    assert Assembler.ranges(assr) == contigs([{2, 12}])
  end

  test "test_rejected_add_keeps_state" do
    assr = Enum.reduce(1..@max_segment_count, new(), fn c, assr -> add(assr, c * 10, 3) end)
    # Maximum of allowed holes is reached
    assr_before = Assembler.ranges(assr)
    assert Assembler.add(assr, 1, 3) == :error
    assert Assembler.ranges(assr) == assr_before
  end

  test "test_empty_remove_front" do
    assert {0, _assr} = Assembler.remove_front(from_contigs([]))
  end

  test "test_trailing_hole_remove_front" do
    assert {4, assr} = Assembler.remove_front(from_contigs([{0, 4}]))
    assert Assembler.ranges(assr) == contigs([])
  end

  test "test_trailing_data_remove_front" do
    assert {4, assr} = Assembler.remove_front(from_contigs([{0, 4}, {4, 4}]))
    assert Assembler.ranges(assr) == contigs([{4, 4}])
  end

  test "test_boundary_case_remove_front" do
    vec = List.replace_at(List.duplicate({1, 1}, @max_segment_count), 0, {0, 2})
    assert {2, assr} = Assembler.remove_front(from_contigs(vec))

    vec =
      List.replace_at(List.duplicate({1, 1}, @max_segment_count), @max_segment_count - 1, {0, 0})

    assert Assembler.ranges(assr) == contigs(vec)
  end

  test "test_shrink_next_hole" do
    assr = new() |> add(100, 10) |> add(50, 10) |> add(40, 30)
    assert Assembler.ranges(assr) == contigs([{40, 30}, {30, 10}])
  end

  test "test_join_two" do
    assr = new() |> add(10, 10) |> add(50, 10) |> add(15, 40)
    assert Assembler.ranges(assr) == contigs([{10, 50}])
  end

  test "test_join_two_reversed" do
    assr = new() |> add(50, 10) |> add(10, 10) |> add(15, 40)
    assert Assembler.ranges(assr) == contigs([{10, 50}])
  end

  test "test_join_two_overlong" do
    assr = new() |> add(50, 10) |> add(10, 10) |> add(15, 60)
    assert Assembler.ranges(assr) == contigs([{10, 65}])
  end

  test "test_iter_empty" do
    assert Assembler.ranges(new()) == []
  end

  test "test_iter_full" do
    assert Assembler.ranges(add(new(), 0, 16)) == [{0, 16}]
  end

  test "test_iter_one_front" do
    assert Assembler.ranges(add(new(), 0, 4)) == [{0, 4}]
  end

  test "test_iter_one_back" do
    assert Assembler.ranges(add(new(), 12, 4)) == [{12, 16}]
  end

  test "test_iter_one_mid" do
    assert Assembler.ranges(add(new(), 4, 8)) == [{4, 12}]
  end

  test "test_iter_one_trailing_gap" do
    assert Assembler.ranges(from_contigs([{4, 8}])) == [{4, 12}]
  end

  test "test_iter_two_split" do
    assert Assembler.ranges(from_contigs([{2, 6}, {4, 1}])) == [{2, 8}, {12, 13}]
  end

  test "test_iter_three_split" do
    assr = from_contigs([{2, 6}, {2, 1}, {2, 2}])
    assert Assembler.ranges(assr) == [{2, 8}, {10, 11}, {13, 15}]
  end

  test "test_issue_694" do
    assr = new() |> add(0, 1) |> add(2, 1)
    assert {:ok, _assr} = Assembler.add(assr, 1, 1)
  end

  test "test_add_then_remove_front" do
    assr = add(new(), 50, 10)
    assert {:ok, 0, assr} = Assembler.add_then_remove_front(assr, 10, 10)
    assert Assembler.ranges(assr) == contigs([{10, 10}, {30, 10}])
  end

  test "test_add_then_remove_front_at_front" do
    assr = add(new(), 50, 10)
    assert {:ok, 10, assr} = Assembler.add_then_remove_front(assr, 0, 10)
    assert Assembler.ranges(assr) == contigs([{40, 10}])
  end

  test "test_add_then_remove_front_at_front_touch" do
    assr = add(new(), 50, 10)
    assert {:ok, 60, assr} = Assembler.add_then_remove_front(assr, 0, 50)
    assert Assembler.ranges(assr) == contigs([])
  end

  test "test_add_then_remove_front_at_front_full" do
    assr = Enum.reduce(1..@max_segment_count, new(), fn c, assr -> add(assr, c * 10, 3) end)
    # Maximum of allowed holes is reached
    assr_before = Assembler.ranges(assr)
    assert Assembler.add_then_remove_front(assr, 1, 3) == :error
    assert Assembler.ranges(assr) == assr_before
  end

  test "test_add_then_remove_front_at_front_full_offset_0" do
    assr = Enum.reduce(1..@max_segment_count, new(), fn c, assr -> add(assr, c * 10, 3) end)
    assert {:ok, 3, _assr} = Assembler.add_then_remove_front(assr, 0, 3)
  end

  # Test against an obviously-correct but inefficient bitmap impl.
  @max_index 256

  property "test_random" do
    check all(
            max_size <- member_of([2, 5, 10, 100]),
            ops <-
              list_of(
                tuple({integer(0..(@max_index - max_size - 2)), integer(1..max_size)}),
                length: 60
              ),
            max_runs: 1_200
          ) do
      Enum.reduce(ops, {new(), MapSet.new()}, fn {offset, size}, {assr, map} ->
        # Real impl
        res = Assembler.add(assr, offset, size)

        # Bitmap impl
        map2 = Enum.into(offset..(offset + size - 1), map)
        contigs = bitmap_contigs(map2)

        # Compare.
        if length(contigs) > @max_segment_count do
          assert res == :error
          {assr, map}
        else
          assert {:ok, assr} = res
          assert Assembler.ranges(assr) == contigs(contigs)
          {assr, map2}
        end
      end)
    end
  end

  # The `(hole, data)` contigs of a bitmap, as smoltcp's `test_random`
  # reads them: a run of data counts once a hole follows it.
  defp bitmap_contigs(map) do
    {contigs, _hole, _data} =
      Enum.reduce(0..(@max_index - 1), {[], 0, 0}, fn i, {contigs, hole, data} ->
        cond do
          MapSet.member?(map, i) -> {contigs, hole, data + 1}
          data != 0 -> {[{hole, data} | contigs], 1, 0}
          true -> {contigs, hole + 1, data}
        end
      end)

    Enum.reverse(contigs)
  end
end
