defmodule SmolNet.Core.Wire.Ipv6Test do
  # Ported from smoltcp's `wire::ipv6` unit tests
  # (native/vendor/smoltcp/src/wire/ipv6.rs).
  use ExUnit.Case, async: true

  alias SmolNet.Core.Wire.Ipv6

  require Ipv6

  # Rust's `Address::new(a, b, c, d, e, f, g, h)`: eight 16-bit segments.
  defp addr(a, b, c, d, e, f, g, h),
    do: <<a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>

  defp link_local_all_nodes, do: addr(0xFF02, 0, 0, 0, 0, 0, 0, 1)
  defp link_local_all_routers, do: addr(0xFF02, 0, 0, 0, 0, 0, 0, 2)
  defp link_local_addr, do: addr(0xFE80, 0, 0, 0, 0, 0, 0, 1)
  defp unique_local_addr, do: addr(0xFD00, 0, 0, 201, 1, 1, 1, 1)
  defp global_unicast_addr, do: addr(0x2001, 0xDB8, 0x3, 0, 0, 0, 0, 1)
  defp test_sol_node_mcast_addr, do: addr(0xFF02, 0, 0, 0, 0, 1, 0xFF01, 101)

  @udp 17

  # Cidr::contains_subnet, by its definition in smoltcp.
  defp contains_subnet?({network, prefix_len}, {subnet, subnet_prefix_len}),
    do: prefix_len <= subnet_prefix_len and Ipv6.contains?(network, prefix_len, subnet)

  defp predicates(address) do
    %{
      unspecified: Ipv6.unspecified?(address),
      multicast: Ipv6.multicast?(address),
      link_local: Ipv6.link_local?(address),
      loopback: Ipv6.loopback?(address),
      unique_local: Ipv6.unique_local?(address),
      global_unicast: Ipv6.global_unicast?(address),
      solicited_node_multicast: Ipv6.solicited_node_multicast?(address)
    }
  end

  @none %{
    unspecified: false,
    multicast: false,
    link_local: false,
    loopback: false,
    unique_local: false,
    global_unicast: false,
    solicited_node_multicast: false
  }

  test "basic_multicast" do
    assert predicates(link_local_all_routers()) == %{@none | multicast: true}
    assert predicates(link_local_all_nodes()) == %{@none | multicast: true}
    assert Ipv6.link_local_all_nodes() == link_local_all_nodes()
  end

  test "basic_link_local" do
    assert predicates(link_local_addr()) == %{@none | link_local: true}
  end

  test "basic_loopback" do
    assert Ipv6.loopback() == addr(0, 0, 0, 0, 0, 0, 0, 1)
    assert predicates(Ipv6.loopback()) == %{@none | loopback: true}
  end

  test "unique_local" do
    assert predicates(unique_local_addr()) == %{@none | unique_local: true}
  end

  test "global_unicast" do
    assert predicates(global_unicast_addr()) == %{@none | global_unicast: true}
  end

  test "sollicited_node_multicast" do
    assert predicates(test_sol_node_mcast_addr()) ==
             %{@none | multicast: true, solicited_node_multicast: true}
  end

  test "mask" do
    addr = addr(0x0123, 0x4567, 0x89AB, 0, 0, 0, 0, 1)

    assert Ipv6.mask(addr, 11) ==
             <<0x01, 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    assert Ipv6.mask(addr, 15) ==
             <<0x01, 0x22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    assert Ipv6.mask(addr, 26) ==
             <<0x01, 0x23, 0x45, 0x40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    assert Ipv6.mask(addr, 128) ==
             <<0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>

    assert Ipv6.mask(addr, 127) ==
             <<0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
  end

  test "cidr" do
    # fe80::1/56
    cidr = {link_local_addr(), 56}

    inside_subnet = [
      # fe80::2
      <<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02>>,
      # fe80::1122:3344:5566:7788
      <<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x88>>,
      # fe80::ff00:0:0:0
      <<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00>>,
      # fe80::ff
      <<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xFF>>
    ]

    outside_subnet = [
      # fe80:0:0:101::1
      <<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01>>,
      # ::1
      <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01>>,
      # ff02::1
      <<0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01>>,
      # ff02::2
      <<0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02>>
    ]

    subnets = [
      # fe80::ffff:ffff:ffff:ffff/65
      {<<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
         0xFF>>, 65},
      # fe80::1/128
      {<<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
         0x01>>, 128},
      # fe80::1234:5678/96
      {<<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x56,
         0x78>>, 96}
    ]

    not_subnets = [
      # fe80::101:ffff:ffff:ffff:ffff/55
      {<<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
         0xFF>>, 55},
      # fe80::101:ffff:ffff:ffff:ffff/56
      {<<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
         0xFF>>, 56},
      # fe80::101:ffff:ffff:ffff:ffff/57
      {<<0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
         0xFF>>, 57},
      # ::1/128
      {<<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
         0x01>>, 128}
    ]

    {network, prefix_len} = cidr

    for addr <- inside_subnet do
      assert Ipv6.contains?(network, prefix_len, addr)
    end

    for addr <- outside_subnet do
      refute Ipv6.contains?(network, prefix_len, addr)
    end

    for subnet <- subnets do
      assert contains_subnet?(cidr, subnet)
    end

    for subnet <- not_subnets do
      refute contains_subnet?(cidr, subnet)
    end

    # A CIDR without a prefix contains everything.
    assert Ipv6.contains?(link_local_addr(), 0, Ipv6.loopback())
  end

  test "scope" do
    assert Ipv6.scope(addr(0xFF01, 0, 0, 0, 0, 0, 0, 1)) == 0x1
    assert Ipv6.scope(addr(0xFF02, 0, 0, 0, 0, 0, 0, 1)) == 0x2
    assert Ipv6.scope(addr(0xFF03, 0, 0, 0, 0, 0, 0, 1)) == 0xFF
    assert Ipv6.scope(addr(0xFF04, 0, 0, 0, 0, 0, 0, 1)) == 0x4
    assert Ipv6.scope(addr(0xFF05, 0, 0, 0, 0, 0, 0, 1)) == 0x5
    assert Ipv6.scope(addr(0xFF08, 0, 0, 0, 0, 0, 0, 1)) == 0x8
    assert Ipv6.scope(addr(0xFF0E, 0, 0, 0, 0, 0, 0, 1)) == 0xE

    assert Ipv6.scope(link_local_all_nodes()) == 0x2

    # For source address selection, unicast addresses also have a scope:
    assert Ipv6.scope(link_local_addr()) == 0x2
    assert Ipv6.scope(global_unicast_addr()) == 0xE
    assert Ipv6.scope(unique_local_addr()) == 0xE
  end

  @repr_packet_bytes <<0x60, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x11, 0x40, 0xFE, 0x80, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                       0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x02, 0x00, 0x0C, 0x02, 0x4E,
                       0xFF, 0xFF, 0xFF, 0xFF>>

  @repr_payload_bytes <<0x00, 0x01, 0x00, 0x02, 0x00, 0x0C, 0x02, 0x4E, 0xFF, 0xFF, 0xFF, 0xFF>>

  defp packet_repr do
    Ipv6.ipv6(
      src: addr(0xFE80, 0, 0, 0, 0, 0, 0, 1),
      dst: link_local_all_nodes(),
      next_header: @udp,
      payload_len: 12,
      hop_limit: 64
    )
  end

  test "packet_deconstruction" do
    assert {:ok, fields} = Ipv6.fields(@repr_packet_bytes)
    assert {:ok, _repr, _payload} = Ipv6.parse(@repr_packet_bytes)
    assert fields.version == 6
    assert fields.traffic_class == 0
    assert fields.flow_label == 0
    # Packet::total_len is the header length plus the payload length.
    assert Ipv6.header_len() + fields.payload_len == 0x34
    assert fields.payload_len == byte_size(@repr_payload_bytes)
    assert fields.next_header == @udp
    assert fields.hop_limit == 0x40
    assert fields.src == addr(0xFE80, 0, 0, 0, 0, 0, 0, 1)
    assert fields.dst == link_local_all_nodes()
    assert fields.payload == @repr_payload_bytes
  end

  test "packet_construction" do
    expected_bytes =
      <<0x69, 0x95, 0x43, 0x21, 0x00, 0x0C, 0x11, 0xFE, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>

    start = byte_size(expected_bytes) - byte_size(@repr_payload_bytes)
    expected_bytes = binary_part(expected_bytes, 0, start) <> @repr_payload_bytes

    packet =
      Ipv6.build(%{
        version: 6,
        traffic_class: 0x99,
        flow_label: 0x54321,
        payload_len: 0xC,
        next_header: @udp,
        hop_limit: 0xFE,
        src: link_local_all_routers(),
        dst: link_local_all_nodes(),
        payload: @repr_payload_bytes
      })

    # Version, Traffic Class, and Flow Label are not byte aligned; make sure
    # they do not interfere with each other.
    assert {:ok, %{version: 6, traffic_class: 0x99, flow_label: 0x54321}} = Ipv6.fields(packet)

    assert {:ok, %{version: 6, traffic_class: 0x99}} =
             Ipv6.fields(Ipv6.build(%{traffic_class: 0x99}))

    assert {:ok, _repr, _payload} = Ipv6.parse(packet)
    assert packet == expected_bytes
  end

  test "overlong" do
    bytes = @repr_packet_bytes <> <<0>>

    assert {:ok, %{payload: payload}} = Ipv6.fields(bytes)
    assert byte_size(payload) == byte_size(@repr_payload_bytes)
    assert {:ok, _repr, payload} = Ipv6.parse(bytes)
    assert byte_size(payload) == byte_size(@repr_payload_bytes)
  end

  test "total_len_overflow" do
    <<head::binary-4, _payload_len::16, rest::binary>> = @repr_packet_bytes
    bytes = <<head::binary, 0x80::16, rest::binary>>

    assert Ipv6.parse(bytes) == :error
  end

  test "repr_parse_valid" do
    assert {:ok, repr, payload} = Ipv6.parse(@repr_packet_bytes)
    assert repr == packet_repr()
    assert payload == @repr_payload_bytes
  end

  test "repr_parse_bad_version" do
    bytes = Ipv6.build(%{version: 4, payload_len: 0})
    assert byte_size(bytes) == 40
    assert Ipv6.parse(bytes) == :error
  end

  test "repr_parse_smaller_than_header" do
    bytes = Ipv6.build(%{version: 6, payload_len: 39})
    assert byte_size(bytes) == 40
    assert Ipv6.parse(bytes) == :error
  end

  test "repr_parse_smaller_than_payload" do
    bytes = Ipv6.build(%{version: 6, payload_len: 1})
    assert byte_size(bytes) == 40
    assert Ipv6.parse(bytes) == :error
  end

  test "basic_repr_emit" do
    repr = packet_repr()
    assert Ipv6.emit(repr, @repr_payload_bytes) == @repr_packet_bytes
    assert Ipv6.header(repr) == binary_part(@repr_packet_bytes, 0, Ipv6.header_len())
  end
end
