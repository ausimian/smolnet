defmodule SmolNet.Core.Wire.Ipv4Test do
  # Ported from smoltcp's `wire::ipv4` and `wire::ip` unit tests
  # (native/vendor/smoltcp/src/wire/ipv4.rs and ip.rs).
  use ExUnit.Case, async: true

  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Ipv4
  alias SmolNet.Core.Wire.Ipv6

  require Ipv4
  require Ipv6

  @icmp 1
  @udp 17

  @packet_bytes <<0x45, 0x00, 0x00, 0x1E, 0x01, 0x02, 0x62, 0x03, 0x1A, 0x01, 0xD5, 0x6E, 0x11,
                  0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x24, 0xAA, 0x00, 0x00, 0x00, 0x00, 0x00,
                  0x00, 0x00, 0x00, 0xFF>>

  @payload_bytes <<0xAA, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF>>

  @repr_packet_bytes <<0x45, 0x00, 0x00, 0x18, 0x00, 0x00, 0x40, 0x00, 0x40, 0x01, 0xD2, 0x79,
                       0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x24, 0xAA, 0x00, 0x00, 0xFF>>

  @repr_payload_bytes <<0xAA, 0x00, 0x00, 0xFF>>

  defp packet_repr do
    Ipv4.ipv4(
      src: <<0x11, 0x12, 0x13, 0x14>>,
      dst: <<0x21, 0x22, 0x23, 0x24>>,
      protocol: @icmp,
      payload_len: 4,
      hop_limit: 64
    )
  end

  # smoltcp's `Cidr::contains_subnet`.
  defp contains_subnet?(address, prefix_len, subnet, subnet_prefix_len),
    do: prefix_len <= subnet_prefix_len and Ipv4.contains?(address, prefix_len, subnet)

  ## ipv4.rs

  test "deconstruct" do
    assert {:ok, packet} = Ipv4.fields(@packet_bytes)
    assert packet.version == 4
    assert packet.header_len == 20
    assert packet.dscp == 0
    assert packet.ecn == 0
    assert packet.total_len == 30
    assert packet.ident == 0x102
    assert packet.more_frags
    assert packet.dont_frag
    assert packet.frag_offset == 0x203 * 8
    assert packet.hop_limit == 0x1A
    assert packet.protocol == @icmp
    assert packet.checksum == 0xD56E
    assert packet.src == <<0x11, 0x12, 0x13, 0x14>>
    assert packet.dst == <<0x21, 0x22, 0x23, 0x24>>
    assert Checksum.valid?(binary_part(@packet_bytes, 0, packet.header_len))
    assert packet.payload == @payload_bytes
  end

  test "construct" do
    packet =
      Ipv4.build(%{
        version: 4,
        header_len: 20,
        dscp: 0,
        ecn: 0,
        total_len: 30,
        ident: 0x102,
        more_frags: true,
        dont_frag: true,
        frag_offset: 0x203 * 8,
        hop_limit: 0x1A,
        protocol: @icmp,
        src: <<0x11, 0x12, 0x13, 0x14>>,
        dst: <<0x21, 0x22, 0x23, 0x24>>,
        checksum: nil,
        payload: @payload_bytes
      })

    assert packet == @packet_bytes
  end

  test "overlong" do
    bytes = <<@packet_bytes::binary, 0>>

    assert {:ok, packet} = Ipv4.fields(bytes)
    assert byte_size(packet.payload) == byte_size(@payload_bytes)
  end

  test "total_len_overflow" do
    # `set_total_len` leaves the checksum as it was.
    <<head::binary-2, _total_len::16, rest::binary>> = @packet_bytes
    bytes = <<head::binary, 128::16, rest::binary>>

    assert Ipv4.parse(bytes) == :error

    # That packet is also a fragment, which `parse/1` refuses on its own; an
    # unfragmented one with a good checksum isolates the length check.
    {:ok, packet} = Ipv4.fields(@repr_packet_bytes)
    header = Ipv4.build(%{packet | total_len: 128, checksum: nil, payload: nil})
    bytes = <<header::binary, @repr_payload_bytes::binary>>

    assert byte_size(bytes) == byte_size(@repr_packet_bytes)
    assert Ipv4.parse(bytes) == :error
  end

  test "parse" do
    assert Ipv4.parse(@repr_packet_bytes) == {:ok, packet_repr(), @repr_payload_bytes}
  end

  test "parse_bad_version" do
    {:ok, packet} = Ipv4.fields(@repr_packet_bytes)
    bytes = Ipv4.build(%{packet | version: 6, checksum: nil})

    assert Ipv4.parse(bytes) == :error
  end

  test "parse_total_len_less_than_header_len" do
    bytes = <<0x09, 0::size(39)-unit(8)>>
    assert Ipv4.parse(bytes) == :error

    # The same lengths with version 4, so the length check alone refuses it.
    assert Ipv4.parse(<<0x49, 0::size(39)-unit(8)>>) == :error
  end

  test "parse_small_ihl" do
    # `set_header_len` leaves the checksum as it was.
    <<_version_ihl, rest::binary>> = @repr_packet_bytes
    bytes = <<4::4, div(16, 4)::4, rest::binary>>

    assert Ipv4.parse(bytes) == :error
  end

  test "emit" do
    repr = packet_repr()

    assert Ipv4.emit(repr, @repr_payload_bytes) == @repr_packet_bytes
    assert Ipv4.header(repr) == binary_part(@repr_packet_bytes, 0, Ipv4.header_len())
  end

  test "unspecified" do
    address = <<0, 0, 0, 0>>

    assert Ipv4.unspecified?(address)
    refute Ipv4.broadcast?(address)
    refute Ipv4.multicast?(address)
    # `is_link_local` and `is_loopback` (169.254.0.0/16 and 127.0.0.0/8).
    refute Ipv4.contains?(<<169, 254, 0, 0>>, 16, address)
    refute Ipv4.contains?(<<127, 0, 0, 0>>, 8, address)
  end

  test "broadcast" do
    address = <<255, 255, 255, 255>>

    refute Ipv4.unspecified?(address)
    assert Ipv4.broadcast?(address)
    refute Ipv4.multicast?(address)
    # `is_link_local` and `is_loopback` (169.254.0.0/16 and 127.0.0.0/8).
    refute Ipv4.contains?(<<169, 254, 0, 0>>, 16, address)
    refute Ipv4.contains?(<<127, 0, 0, 0>>, 8, address)
  end

  test "cidr" do
    cidr = <<192, 168, 1, 10>>
    prefix_len = 24

    inside_subnet = [
      <<192, 168, 1, 0>>,
      <<192, 168, 1, 1>>,
      <<192, 168, 1, 2>>,
      <<192, 168, 1, 10>>,
      <<192, 168, 1, 127>>,
      <<192, 168, 1, 255>>
    ]

    outside_subnet = [
      <<192, 168, 0, 0>>,
      <<127, 0, 0, 1>>,
      <<192, 168, 2, 0>>,
      <<192, 168, 0, 255>>,
      <<0, 0, 0, 0>>,
      <<255, 255, 255, 255>>
    ]

    subnets = [
      {<<192, 168, 1, 0>>, 32},
      {<<192, 168, 1, 255>>, 24},
      {<<192, 168, 1, 10>>, 30}
    ]

    not_subnets = [
      {<<192, 168, 1, 10>>, 23},
      {<<127, 0, 0, 1>>, 8},
      {<<192, 168, 1, 0>>, 0},
      {<<192, 168, 0, 255>>, 32}
    ]

    for address <- inside_subnet do
      assert Ipv4.contains?(cidr, prefix_len, address)
    end

    for address <- outside_subnet do
      refute Ipv4.contains?(cidr, prefix_len, address)
    end

    for {address, subnet_prefix_len} <- subnets do
      assert contains_subnet?(cidr, prefix_len, address, subnet_prefix_len)
    end

    for {address, subnet_prefix_len} <- not_subnets do
      refute contains_subnet?(cidr, prefix_len, address, subnet_prefix_len)
    end

    assert Ipv4.contains?(cidr, 0, <<127, 0, 0, 1>>)
  end

  test "cidr_from_netmask" do
    assert Ipv4.prefix_from_netmask(<<1, 0, 2, 0>>) == :error
    assert Ipv4.prefix_from_netmask(<<0, 0, 0, 0>>) == :error
    assert Ipv4.prefix_from_netmask(<<255, 255, 255, 0>>) == {:ok, 24}
    assert Ipv4.prefix_from_netmask(<<255, 255, 0, 0>>) == {:ok, 16}
    assert Ipv4.prefix_from_netmask(<<255, 240, 0, 0>>) == {:ok, 12}
    assert Ipv4.prefix_from_netmask(<<255, 255, 255, 0>>) == {:ok, 24}
    assert Ipv4.prefix_from_netmask(<<255, 255, 255, 255>>) == {:ok, 32}
  end

  test "cidr_netmask" do
    assert Ipv4.netmask(0) == <<0, 0, 0, 0>>
    assert Ipv4.netmask(24) == <<255, 255, 255, 0>>
    assert Ipv4.netmask(32) == <<255, 255, 255, 255>>
    assert Ipv4.netmask(8) == <<255, 0, 0, 0>>
    assert Ipv4.netmask(16) == <<255, 255, 0, 0>>
    assert Ipv4.netmask(16) == <<255, 255, 0, 0>>
    assert Ipv4.netmask(17) == <<255, 255, 128, 0>>
    assert Ipv4.netmask(12) == <<255, 240, 0, 0>>
    assert Ipv4.netmask(24) == <<255, 255, 255, 0>>
    assert Ipv4.netmask(32) == <<255, 255, 255, 255>>
  end

  test "cidr_broadcast" do
    assert Ipv4.broadcast(<<0, 0, 0, 0>>, 0) == <<255, 255, 255, 255>>
    assert Ipv4.broadcast(<<0, 0, 0, 1>>, 24) == <<0, 0, 0, 255>>
    assert Ipv4.broadcast(<<0, 0, 0, 0>>, 32) == nil
    assert Ipv4.broadcast(<<127, 0, 0, 0>>, 8) == <<127, 255, 255, 255>>
    assert Ipv4.broadcast(<<192, 168, 0, 0>>, 16) == <<192, 168, 255, 255>>
    assert Ipv4.broadcast(<<192, 168, 1, 1>>, 16) == <<192, 168, 255, 255>>
    assert Ipv4.broadcast(<<192, 168, 1, 1>>, 17) == <<192, 168, 127, 255>>
    assert Ipv4.broadcast(<<172, 16, 0, 1>>, 12) == <<172, 31, 255, 255>>
    assert Ipv4.broadcast(<<255, 255, 255, 1>>, 24) == <<255, 255, 255, 255>>
    assert Ipv4.broadcast(<<255, 255, 255, 254>>, 31) == nil
    assert Ipv4.broadcast(<<255, 255, 255, 255>>, 32) == nil
  end

  test "cidr_network" do
    assert Ipv4.network(<<0, 0, 0, 0>>, 0) == <<0, 0, 0, 0>>
    assert Ipv4.network(<<0, 0, 0, 1>>, 24) == <<0, 0, 0, 0>>
    assert Ipv4.network(<<0, 0, 0, 0>>, 32) == <<0, 0, 0, 0>>
    assert Ipv4.network(<<127, 0, 0, 0>>, 8) == <<127, 0, 0, 0>>
    assert Ipv4.network(<<192, 168, 0, 0>>, 16) == <<192, 168, 0, 0>>
    assert Ipv4.network(<<192, 168, 1, 1>>, 16) == <<192, 168, 0, 0>>
    assert Ipv4.network(<<192, 168, 1, 1>>, 17) == <<192, 168, 0, 0>>
    assert Ipv4.network(<<172, 16, 0, 1>>, 12) == <<172, 16, 0, 0>>
    assert Ipv4.network(<<255, 255, 255, 1>>, 24) == <<255, 255, 255, 0>>
    assert Ipv4.network(<<255, 255, 255, 255>>, 32) == <<255, 255, 255, 255>>
  end

  ## ip.rs

  test "to_prefix_len_ipv4" do
    masks = [
      {0, <<0, 0, 0, 0>>},
      {1, <<128, 0, 0, 0>>},
      {2, <<192, 0, 0, 0>>},
      {3, <<224, 0, 0, 0>>},
      {4, <<240, 0, 0, 0>>},
      {5, <<248, 0, 0, 0>>},
      {6, <<252, 0, 0, 0>>},
      {7, <<254, 0, 0, 0>>},
      {8, <<255, 0, 0, 0>>},
      {9, <<255, 128, 0, 0>>},
      {10, <<255, 192, 0, 0>>},
      {11, <<255, 224, 0, 0>>},
      {12, <<255, 240, 0, 0>>},
      {13, <<255, 248, 0, 0>>},
      {14, <<255, 252, 0, 0>>},
      {15, <<255, 254, 0, 0>>},
      {16, <<255, 255, 0, 0>>},
      {17, <<255, 255, 128, 0>>},
      {18, <<255, 255, 192, 0>>},
      {19, <<255, 255, 224, 0>>},
      {20, <<255, 255, 240, 0>>},
      {21, <<255, 255, 248, 0>>},
      {22, <<255, 255, 252, 0>>},
      {23, <<255, 255, 254, 0>>},
      {24, <<255, 255, 255, 0>>},
      {25, <<255, 255, 255, 128>>},
      {26, <<255, 255, 255, 192>>},
      {27, <<255, 255, 255, 224>>},
      {28, <<255, 255, 255, 240>>},
      {29, <<255, 255, 255, 248>>},
      {30, <<255, 255, 255, 252>>},
      {31, <<255, 255, 255, 254>>},
      {32, <<255, 255, 255, 255>>}
    ]

    for {prefix_len, mask} <- masks do
      assert Ipv4.mask_prefix_len(mask) == prefix_len
    end
  end

  test "to_prefix_len_ipv4_error" do
    assert Ipv4.mask_prefix_len(<<255, 255, 255, 1>>) == nil
  end

  test "to_prefix_len_ipv6" do
    assert Ipv6.mask_prefix_len(<<0::128>>) == 0

    assert Ipv6.mask_prefix_len(
             <<0xFFFF::16, 0xFFFF::16, 0xFFFF::16, 0xFFFF::16, 0xFFFF::16, 0xFFFF::16, 0xFFFF::16,
               0xFFFF::16>>
           ) == 128
  end

  test "to_prefix_len_ipv6_error" do
    assert Ipv6.mask_prefix_len(
             <<0xFFFF::16, 0xFFFF::16, 0xFFFF::16, 0xFFFF::16, 0xFFFF::16, 0xFFFF::16, 0::16,
               1::16>>
           ) == nil
  end

  # SmolNet has no `IpRepr::parse` dispatching on the version nibble; each
  # family's `parse/1` stands in for it.

  test "parse_ipv4_packet" do
    ipv4_packet_bytes =
      <<0x45, 0x00, 0x00, 0x14, 0x00, 0x01, 0x40, 0x00, 0x40, 0x01, 0xD2, 0x7C, 0x11, 0x12, 0x13,
        0x14, 0x21, 0x22, 0x23, 0x24>>

    expected =
      Ipv4.ipv4(
        src: <<0x11, 0x12, 0x13, 0x14>>,
        dst: <<0x21, 0x22, 0x23, 0x24>>,
        protocol: @icmp,
        payload_len: 0,
        hop_limit: 64
      )

    assert Ipv4.parse(ipv4_packet_bytes) == {:ok, expected, <<>>}
    assert Ipv6.parse(ipv4_packet_bytes) == :error
  end

  test "parse_ipv4_packet_error" do
    ipv4_packet_bytes =
      <<0x45, 0x00, 0x00, 0x1E, 0x01, 0x02, 0x62, 0x03, 0x1A, 0x01, 0xD5, 0x6E, 0x11, 0x12, 0x13,
        0x14, 0x21, 0x22, 0x23, 0x24, 0xAA, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>

    assert Ipv4.parse(ipv4_packet_bytes) == :error
  end

  test "parse_ipv6_packet" do
    ipv6_packet_bytes =
      <<0x60, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x11, 0x40, 0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x02, 0x00,
        0x0C, 0x02, 0x4E, 0xFF, 0xFF, 0xFF, 0xFF>>

    expected =
      Ipv6.ipv6(
        src: <<0xFE80::16, 0::16, 0::16, 0::16, 0::16, 0::16, 0::16, 1::16>>,
        dst: <<0xFF02::16, 0::16, 0::16, 0::16, 0::16, 0::16, 0::16, 1::16>>,
        next_header: @udp,
        payload_len: 12,
        hop_limit: 64
      )

    assert Ipv6.parse(ipv6_packet_bytes) ==
             {:ok, expected, binary_part(ipv6_packet_bytes, 40, 12)}

    assert Ipv4.parse(ipv6_packet_bytes) == :error
  end

  test "parse_ipv6_packet_error" do
    ipv6_packet_bytes =
      <<0x60, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x11, 0x40, 0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x02, 0x00,
        0x0C, 0x02, 0x4E, 0xFF, 0xFF, 0xFF>>

    assert Ipv6.parse(ipv6_packet_bytes) == :error
  end

  test "parse_packet_too_short" do
    ip_packet = <<>>

    assert Ipv4.parse(ip_packet) == :error
    assert Ipv6.parse(ip_packet) == :error
  end

  test "parse_packet_invalid_version" do
    packet_bytes = <<0xFF>>

    assert Ipv4.parse(packet_bytes) == :error
    assert Ipv6.parse(packet_bytes) == :error
  end
end
