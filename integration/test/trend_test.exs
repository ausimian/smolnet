defmodule SmolNet.Integration.Soak.TrendTest do
  use ExUnit.Case, async: true

  alias SmolNet.Integration.Soak.Metrics
  alias SmolNet.Integration.Soak.Trend

  @limit [floor: 10, ratio: 0.1]

  test "needs enough samples to judge" do
    assert :insufficient = Trend.check(Enum.to_list(1..11), @limit)
    assert {:rising, _detail} = Trend.check(Enum.map(1..12, &(&1 * 10)), @limit)
  end

  test "a flat metric with noise is flat" do
    samples = for i <- 1..400, do: 1_000 + rem(i * 37, 50)
    assert :flat = Trend.check(samples, @limit)
  end

  test "a steady climb is rising, with its window medians" do
    samples = Enum.map(1..400, &(1_000 + &1))
    assert {:rising, %{medians: medians, growth: growth}} = Trend.check(samples, @limit)
    assert length(medians) == 4
    assert growth == List.last(medians) - hd(medians)
  end

  test "a climb within the floor or the ratio is flat" do
    assert :flat = Trend.check(Enum.map(1..40, &(1_000 + div(&1, 8))), @limit)
    assert :flat = Trend.check(Enum.map(1..40, &(1_000 + &1)), floor: 10, ratio: 0.5)
  end

  test "a single step up is not a steady climb" do
    samples = List.duplicate(1_000, 200) ++ List.duplicate(5_000, 200)
    assert :flat = Trend.check(samples, @limit)
  end

  test "a spike does not move a window's median" do
    samples = 1_000 |> List.duplicate(400) |> List.replace_at(399, 1_000_000)
    assert :flat = Trend.check(samples, @limit)
  end

  test "missing samples are ignored" do
    samples = Enum.flat_map(1..40, &[nil, 1_000 + &1 * 10])
    assert {:rising, _detail} = Trend.check(samples, @limit)
  end

  describe "Metrics.rising/3" do
    test "judges only the samples after warm-up" do
      # The process count climbs for 80 s, then holds.
      samples =
        for second <- 0..99 do
          %{elapsed_s: second, process_count: min(second, 80) * 1_000}
        end

      limits = [process_count: [floor: 10, ratio: 0.1]]
      assert {:ok, [{:process_count, _detail}]} = Metrics.rising(samples, 0, limits)
      assert {:ok, []} = Metrics.rising(samples, 80_000, limits)
      assert :insufficient = Metrics.rising(samples, 95_000, limits)
    end
  end
end
