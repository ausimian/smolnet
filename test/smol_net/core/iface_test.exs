defmodule SmolNet.Core.IfaceTest do
  use ExUnit.Case, async: true

  # Routing and source selection, which no ingress reply exercises on an
  # IP medium: ported from smoltcp's `iface/route.rs` and the
  # `get_source_address*` tests in `iface/interface/tests`.

  alias SmolNet.Core.Iface

  defp v6(segments), do: for(segment <- Tuple.to_list(segments), into: <<>>, do: <<segment::16>>)

  describe "routes (route.rs)" do
    # test_fill, without the expiring route: SmolNet's routes never expire.
    test "fill" do
      cidr_1 = v6({0xFE80, 0, 0, 2, 0, 0, 0, 0})
      addr_1a = v6({0xFE80, 0, 0, 2, 0, 0, 0, 1})
      addr_1b = v6({0xFE80, 0, 0, 2, 0, 0, 0, 13})
      addr_1c = v6({0xFE80, 0, 0, 2, 0, 0, 0, 42})
      cidr_2 = v6({0xFE80, 0, 0, 0x3364, 0, 0, 0, 0})
      addr_2a = v6({0xFE80, 0, 0, 0x3364, 0, 0, 0, 1})
      addr_2b = v6({0xFE80, 0, 0, 0x3364, 0, 0, 0, 21})

      empty = Iface.new(1_500, [], [])

      for address <- [addr_1a, addr_1b, addr_1c, addr_2a, addr_2b],
          do: assert(Iface.route(empty, address) == nil)

      one = Iface.new(1_500, [], [{cidr_1, 64, addr_1a}])
      for address <- [addr_1a, addr_1b, addr_1c], do: assert(Iface.route(one, address) == addr_1a)
      for address <- [addr_2a, addr_2b], do: assert(Iface.route(one, address) == nil)

      two = Iface.new(1_500, [], [{cidr_1, 64, addr_1a}, {cidr_2, 64, addr_2a}])
      for address <- [addr_1a, addr_1b, addr_1c], do: assert(Iface.route(two, address) == addr_1a)
      for address <- [addr_2a, addr_2b], do: assert(Iface.route(two, address) == addr_2a)
    end

    test "the longest prefix wins, and the last of equal ones" do
      gateway_a = <<192, 0, 2, 1>>
      gateway_b = <<192, 0, 2, 2>>
      gateway_c = <<192, 0, 2, 3>>

      iface =
        Iface.new(1_500, [{<<192, 0, 2, 10>>, 24}], [
          {<<0, 0, 0, 0>>, 0, gateway_a},
          {<<198, 51, 100, 0>>, 24, gateway_b},
          {<<198, 51, 100, 0>>, 24, gateway_c}
        ])

      assert Iface.route(iface, <<198, 51, 100, 9>>) == gateway_c
      assert Iface.route(iface, <<203, 0, 113, 9>>) == gateway_a
      # On the interface's own subnet, and broadcast, directly.
      assert Iface.route(iface, <<192, 0, 2, 77>>) == <<192, 0, 2, 77>>
      assert Iface.route(iface, <<255, 255, 255, 255>>) == <<255, 255, 255, 255>>
      # A route never crosses families.
      assert Iface.route(iface, v6({0x2001, 0xDB8, 0, 0, 0, 0, 0, 1})) == nil
      assert Iface.has_neighbor?(iface, <<203, 0, 113, 9>>)
      refute Iface.has_neighbor?(Iface.new(1_500, [], []), <<203, 0, 113, 9>>)
    end
  end

  describe "source addresses (iface/interface/tests)" do
    test "get_source_address, IPv4" do
      own_1 = <<172, 18, 1, 2>>
      own_2 = <<172, 24, 24, 14>>
      iface = Iface.new(1_500, [{own_1, 24}, {own_2, 24}], [])

      assert Iface.source_address(iface, <<172, 18, 1, 254>>) == own_1
      assert Iface.source_address(iface, <<172, 24, 24, 12>>) == own_2
      assert Iface.source_address(iface, <<172, 24, 23, 254>>) == own_1
    end

    test "get_source_address_empty_interface, IPv4" do
      iface = Iface.new(1_500, [], [])

      for address <- [<<172, 18, 1, 254>>, <<172, 24, 24, 12>>, <<172, 24, 23, 254>>],
          do: assert(Iface.source_address(iface, address) == nil)
    end

    test "get_source_address, IPv6" do
      own_link_local = v6({0xFE80, 0, 0, 0, 0, 0, 0, 1})
      own_unique_local_1 = v6({0xFD00, 0, 0, 201, 1, 1, 1, 2})
      own_unique_local_2 = v6({0xFD01, 0, 0, 201, 1, 1, 1, 2})
      own_global = v6({0x2001, 0x0DB8, 0x0003, 0, 0, 0, 0, 1})

      iface =
        Iface.new(
          1_500,
          [
            {own_link_local, 64},
            {own_unique_local_1, 64},
            {own_unique_local_2, 64},
            {own_global, 64}
          ],
          []
        )

      for {dst, src} <- [
            {v6({0, 0, 0, 0, 0, 0, 0, 1}), v6({0, 0, 0, 0, 0, 0, 0, 1})},
            {v6({0xFE80, 0, 0, 0, 0, 0, 0, 42}), own_link_local},
            {v6({0xFD00, 0, 0, 201, 1, 1, 1, 1}), own_unique_local_1},
            {v6({0xFD01, 0, 0, 201, 1, 1, 1, 1}), own_unique_local_2},
            {v6({0xFD02, 0, 0, 201, 1, 1, 1, 1}), own_unique_local_1},
            {v6({0xFD01, 0, 0, 201, 1, 1, 1, 3}), own_unique_local_2},
            {v6({0xFF02, 0, 0, 0, 0, 0, 0, 1}), own_link_local},
            {v6({0x2001, 0x0DB8, 0x0003, 0, 0, 0, 0, 2}), own_global},
            {v6({0x2001, 0x0DB9, 0x0003, 0, 0, 0, 0, 2}), own_global}
          ] do
        assert Iface.source_address(iface, dst) == src, inspect(dst)
      end
    end

    test "get_source_address_only_link_local" do
      own = v6({0xFE80, 0, 0, 0, 0, 0, 0, 1})
      iface = Iface.new(1_500, [{own, 64}], [])

      assert Iface.source_address(iface, v6({0, 0, 0, 0, 0, 0, 0, 1})) ==
               v6({0, 0, 0, 0, 0, 0, 0, 1})

      for dst <- [
            v6({0xFE80, 0, 0, 0, 0, 0, 0, 42}),
            v6({0xFD00, 0, 0, 201, 1, 1, 1, 1}),
            v6({0xFD01, 0, 0, 201, 1, 1, 1, 1}),
            v6({0xFD02, 0, 0, 201, 1, 1, 1, 1}),
            v6({0xFF02, 0, 0, 0, 0, 0, 0, 1}),
            v6({0x2001, 0x0DB8, 0x0003, 0, 0, 0, 0, 2}),
            v6({0x2001, 0x0DB9, 0x0003, 0, 0, 0, 0, 2})
          ] do
        assert Iface.source_address(iface, dst) == own, inspect(dst)
      end
    end

    test "get_source_address_empty_interface, IPv6" do
      iface = Iface.new(1_500, [{<<192, 0, 2, 1>>, 24}], [])

      for dst <- [
            v6({0, 0, 0, 0, 0, 0, 0, 1}),
            v6({0xFE80, 0, 0, 0, 0, 0, 0, 42}),
            v6({0xFD00, 0, 0, 201, 1, 1, 1, 1}),
            v6({0xFF02, 0, 0, 0, 0, 0, 0, 1}),
            v6({0x2001, 0x0DB9, 0x0003, 0, 0, 0, 0, 2})
          ] do
        assert Iface.source_address(iface, dst) == v6({0, 0, 0, 0, 0, 0, 0, 1})
      end
    end
  end
end
