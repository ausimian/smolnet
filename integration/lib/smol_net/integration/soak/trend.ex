defmodule SmolNet.Integration.Soak.Trend do
  @moduledoc """
  Decides whether a metric trends steadily upward over a run.

  The samples taken after warm-up are split into four consecutive windows of
  equal length, and each window is reduced to its median, which a transient
  spike cannot move. A metric is rising when every window's median is above
  the one before, and the last exceeds the first by more than both an
  absolute floor and a fraction of the first. Both conditions matter: a
  healthy metric wanders, so a staircase alone is not enough, and a leak is
  a staircase, so growth alone could be a single change of phase.
  """

  @windows 4
  @min_per_window 3

  @typedoc "`:floor` is the least growth that counts; `:ratio` the least, relative to the start."
  @type limit :: [floor: number(), ratio: number()]

  @type verdict ::
          :flat
          | :insufficient
          | {:rising, %{medians: [number()], growth: number()}}

  @doc "Returns the fewest samples `check/2` needs to judge a metric."
  @spec min_samples() :: pos_integer()
  def min_samples, do: @windows * @min_per_window

  @doc """
  Judges one metric's samples, in order. `nil` samples (a metric that was
  unavailable at the time) are ignored.
  """
  @spec check([number() | nil], limit()) :: verdict()
  def check(samples, limit) do
    values = Enum.reject(samples, &is_nil/1)

    if length(values) < min_samples() do
      :insufficient
    else
      medians = values |> windows() |> Enum.map(&median/1)
      first = hd(medians)
      growth = List.last(medians) - first
      least = max(Keyword.fetch!(limit, :floor), Keyword.fetch!(limit, :ratio) * first)

      if increasing?(medians) and growth > least do
        {:rising, %{medians: medians, growth: growth}}
      else
        :flat
      end
    end
  end

  defp windows(values) do
    size = div(length(values), @windows)

    # Any remainder belongs to the last window, so the newest samples count.
    {head, last} = Enum.split(values, size * (@windows - 1))
    Enum.chunk_every(head, size) ++ [last]
  end

  defp median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1 do
      Enum.at(sorted, middle)
    else
      (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
    end
  end

  defp increasing?(medians) do
    medians |> Enum.chunk_every(2, 1, :discard) |> Enum.all?(fn [a, b] -> b > a end)
  end
end
